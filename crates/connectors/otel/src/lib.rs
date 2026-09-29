//! OpenTelemetry sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** OTLP client and TLS configuration, the exact types each signal accepts, the OTLP
//!   record every mapped row becomes, its resource and scope, the Export requests it prepares once
//!   from those records and the observed timestamp they carry, sending a prepared request's exact
//!   bytes over gRPC or HTTP with optional gzip, and OTLP status classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the OpenTelemetry protocol crates.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::{
    io::Write, num::NonZeroU64, ops::Range, str::FromStr, sync::Arc as StdArc, time::Duration,
};

use ahash::{HashMap, HashMapExt as _, HashSet, HashSetExt as _};
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};
use async_trait::async_trait;
use error_stack::Report;
use flate2::{Compression as GzipLevel, write::GzEncoder};
use hyper_util::client::legacy::connect::HttpConnector;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_connector::{
    HttpClientConfig, MappedSinkCarrier, MappedSinkRows, MeasuredRequest, PerRecordOutcome,
    PreparedRowRequest, RejectedSinkRecord, RowRequest, RowRequestLimits, RowRequestPreparation,
    RowRequestSink, SinkHost, SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecordId,
    SinkRecordPosition, SinkRetryDelay, SinkRowRequest, SinkStartError, SinkStartResult,
    client_config_value, client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_models::{ClientConfigEntry, EmitterBatchPolicy, FieldPath, Timestamp};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse},
        metrics::v1::{
            ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        },
        trace::v1::{
            ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
        },
    },
    common::v1::{AnyValue, ArrayValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span, status},
};
use otel_prost::{
    Message as OtelMessage,
    bytes::{Buf, BufMut as _},
};
use otel_tonic::{
    Code as GrpcCode, Request as GrpcRequest, Status as GrpcStatus,
    client::Grpc as GrpcClient,
    codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder},
    codegen::http::uri::PathAndQuery,
    metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};
use otel_tonic_types::StatusExt;
use reqwest::{
    Client as HttpClient, StatusCode,
    header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER},
};
use thiserror::Error;
use tracing::{debug, warn};

const OTEL: &str = "otel";

const OTLP_PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

/// The OpenTelemetry sink, which exports each mapped row as one OTLP record.
pub struct OtelSink {
    client: OtelClient,
    signal: OtelSignal,
    /// Where the signal's Export requests go, and the response each is answered with.
    service: OtelExportService,
    /// The limits each export request keeps to, when the emitter declares `BATCH`.
    batch: Option<RowRequestLimits>,
    /// Where each signal key sits among the mapped columns, resolved once at start.
    value_columns: HashMap<String, usize>,
    /// The attribute keys, in the order their columns follow the signal's own.
    attributes: Vec<String>,
    /// The first mapped column an attribute occupies, which is the number of signal values.
    attribute_offset: usize,
    resource: Resource,
    scope: Option<InstrumentationScope>,
}

/// What one OTEL emitter exports with, from its typed sink plan.
pub struct OtelSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: nervix_dns::DnsResolver,
    pub signal: OtelSignal,
    /// The emitter's `BATCH` limits, which bound the records and the exact protobuf size of
    /// every export request when it declares them.
    pub batch: Option<EmitterBatchPolicy>,
    /// The signal keys this emitter maps, in the order of its mapped columns.
    pub values: Vec<String>,
    /// The attribute keys this emitter maps, whose columns follow the signal's own.
    pub attributes: Vec<String>,
    pub resource: Vec<OtelResourceAttribute>,
    pub scope: Option<OtelScope>,
    /// The mapped columns the host projects, whose exact types this sink validates before it
    /// accepts its first batch.
    pub mapped_schema: StdArc<arrow_schema::Schema>,
}

/// The OTLP signal one emitter exports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OtelSignal {
    Logs,
    Traces,
    Metric(OtelMetric),
}

/// The metric one emitter exports, with the aggregation its data points carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtelMetric {
    pub name: String,
    pub unit: String,
    pub description: Option<String>,
    pub kind: OtelMetricKind,
}

/// The shape of a metric's data points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OtelMetricKind {
    Gauge,
    Sum {
        monotonic: bool,
        temporality: OtelAggregationTemporality,
    },
    Histogram {
        temporality: OtelAggregationTemporality,
    },
}

/// Whether a metric's data points measure a delta or a cumulative total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtelAggregationTemporality {
    Delta,
    Cumulative,
}

/// The instrumentation scope every exported record carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtelScope {
    pub name: String,
    pub version: Option<String>,
}

/// One `RESOURCE` attribute, whose value is fixed for as long as this emitter runs.
#[derive(Debug, Clone, PartialEq)]
pub struct OtelResourceAttribute {
    pub key: String,
    pub value: OtelLiteral,
}

/// A value a `RESOURCE` attribute carries, which never depends on a record.
#[derive(Debug, Clone, PartialEq)]
pub enum OtelLiteral {
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
    Array(Vec<OtelLiteral>),
    Null,
}

impl OtelLiteral {
    /// This literal as OTLP carries it, or nothing for a null the resource simply omits.
    fn any_value(&self) -> OtelValueResult<Option<AnyValue>> {
        let value = match self {
            Self::I64(value) => any_value::Value::IntValue(*value),
            Self::F64(value) => any_value::Value::DoubleValue(*value),
            Self::Bool(value) => any_value::Value::BoolValue(*value),
            Self::String(value) => any_value::Value::StringValue(value.clone()),
            Self::Null => return Ok(None),
            Self::Array(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    let value = item
                        .any_value()?
                        .ok_or_else(|| Report::new(OtelValueError::NullResourceArrayElement))?;
                    values.push(value);
                }
                any_value::Value::ArrayValue(ArrayValue { values })
            }
        };
        Ok(Some(AnyValue { value: Some(value) }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OtelProtocol {
    Grpc,
    HttpProtobuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OtelCompression {
    None,
    Gzip,
}

#[derive(Debug)]
struct OtelClientSettings {
    endpoint: url::Url,
    protocol: OtelProtocol,
    headers: Vec<(String, String)>,
    compression: OtelCompression,
    timeout: Option<Duration>,
}

enum OtelTransport {
    Grpc {
        channel: Channel,
        metadata: MetadataMap,
        compression: OtelCompression,
    },
    HttpProtobuf {
        client: HttpClient,
        endpoint: url::Url,
        headers: HeaderMap,
        compression: OtelCompression,
    },
}

struct OtelClient {
    transport: OtelTransport,
}

enum OtelExportRequest {
    Logs(ExportLogsServiceRequest),
    Traces(ExportTraceServiceRequest),
    Metrics(ExportMetricsServiceRequest),
}

impl OtelExportRequest {
    /// The uncompressed protobuf bytes the receiver decodes, before transport framing or gzip.
    fn encoded_len(&self) -> usize {
        match self {
            Self::Logs(request) => request.encoded_len(),
            Self::Traces(request) => request.encoded_len(),
            Self::Metrics(request) => request.encoded_len(),
        }
    }

    /// Exactly the protobuf bytes the receiver decodes, which every attempt to send this request
    /// writes before transport framing or gzip.
    fn encode_to_vec(&self) -> Vec<u8> {
        match self {
            Self::Logs(request) => request.encode_to_vec(),
            Self::Traces(request) => request.encode_to_vec(),
            Self::Metrics(request) => request.encode_to_vec(),
        }
    }

    /// One ordered range of successfully mapped members, with the same resource and scope.
    fn members(&self, range: Range<usize>) -> Self {
        match self {
            Self::Logs(request) => {
                let resource = request
                    .resource_logs
                    .first()
                    .assured("the OTEL logs request constructor always installs one resource");
                let scope = resource
                    .scope_logs
                    .first()
                    .assured("the OTEL logs request constructor always installs one scope");
                Self::Logs(ExportLogsServiceRequest {
                    resource_logs: vec![ResourceLogs {
                        resource: resource.resource.clone(),
                        scope_logs: vec![ScopeLogs {
                            scope: scope.scope.clone(),
                            log_records: scope.log_records[range].to_vec(),
                            schema_url: scope.schema_url.clone(),
                        }],
                        schema_url: resource.schema_url.clone(),
                    }],
                })
            }
            Self::Traces(request) => {
                let resource = request
                    .resource_spans
                    .first()
                    .assured("the OTEL trace request constructor always installs one resource");
                let scope = resource
                    .scope_spans
                    .first()
                    .assured("the OTEL trace request constructor always installs one scope");
                Self::Traces(ExportTraceServiceRequest {
                    resource_spans: vec![ResourceSpans {
                        resource: resource.resource.clone(),
                        scope_spans: vec![ScopeSpans {
                            scope: scope.scope.clone(),
                            spans: scope.spans[range].to_vec(),
                            schema_url: scope.schema_url.clone(),
                        }],
                        schema_url: resource.schema_url.clone(),
                    }],
                })
            }
            Self::Metrics(request) => {
                let resource = request
                    .resource_metrics
                    .first()
                    .assured("the OTEL metric request constructor always installs one resource");
                let scope = resource
                    .scope_metrics
                    .first()
                    .assured("the OTEL metric request constructor always installs one scope");
                let metric = scope
                    .metrics
                    .first()
                    .assured("the OTEL metric request constructor always installs one metric");
                let data = match metric
                    .data
                    .as_ref()
                    .assured("the OTEL metric request constructor always installs metric data")
                {
                    metric::Data::Gauge(gauge) => Some(metric::Data::Gauge(Gauge {
                        data_points: gauge.data_points[range].to_vec(),
                    })),
                    metric::Data::Sum(sum) => Some(metric::Data::Sum(Sum {
                        data_points: sum.data_points[range].to_vec(),
                        aggregation_temporality: sum.aggregation_temporality,
                        is_monotonic: sum.is_monotonic,
                    })),
                    metric::Data::Histogram(histogram) => {
                        Some(metric::Data::Histogram(Histogram {
                            data_points: histogram.data_points[range].to_vec(),
                            aggregation_temporality: histogram.aggregation_temporality,
                        }))
                    }
                    _ => None,
                }
                .assured("the OTEL metric constructor builds only gauge, sum, or histogram data");
                Self::Metrics(ExportMetricsServiceRequest {
                    resource_metrics: vec![ResourceMetrics {
                        resource: resource.resource.clone(),
                        scope_metrics: vec![ScopeMetrics {
                            scope: scope.scope.clone(),
                            metrics: vec![Metric {
                                name: metric.name.clone(),
                                description: metric.description.clone(),
                                unit: metric.unit.clone(),
                                metadata: metric.metadata.clone(),
                                data: Some(data),
                            }],
                            schema_url: scope.schema_url.clone(),
                        }],
                        schema_url: resource.schema_url.clone(),
                    }],
                })
            }
        }
    }
}

struct OtelPartialSuccess {
    rejected: i64,
    error_message: String,
}

enum OtelTransportOutcome {
    Accepted(Option<OtelPartialSuccess>),
    Rejected(String),
    Failed(Report<SinkPublishError>),
}

#[derive(Debug)]
struct OtelRecordError {
    key: String,
    reason: String,
}

impl OtelRecordError {
    fn new(key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            reason: reason.into(),
        }
    }

    /// The rejection the host delivers for the row this value came from.
    fn rejected(
        self,
        position: SinkRecordPosition,
        occurred_at: Timestamp,
    ) -> RejectedSinkRecord<SinkRecordPosition> {
        RejectedSinkRecord::invalid(
            position,
            occurred_at,
            self.reason,
            [FieldPath::new(format!("otel.{}", self.key))],
        )
    }
}

#[derive(Debug, Error)]
enum OtelValueError {
    #[error("OTEL {signal} VALUES key '{key}' requires {expected}, found {actual}")]
    InvalidMappedType {
        signal: &'static str,
        key: String,
        expected: &'static str,
        actual: DataType,
    },
    #[error("OTEL ATTRIBUTE '{key}' has unsupported exact type {actual}")]
    InvalidAttributeMappingType { key: String, actual: DataType },
    #[error("OTEL RESOURCE arrays do not support NULL elements")]
    NullResourceArrayElement,
    #[error("OTEL array value has an invalid Arrow representation")]
    InvalidArrayRepresentation,
    #[error("OTEL value requires ARRAY or VEC, found {actual}")]
    ExpectedList { actual: DataType },
    #[error("OTEL integer exceeds the OTLP signed 64-bit range")]
    SignedIntegerRange,
    #[error("OTEL unsigned value cannot be negative")]
    NegativeUnsigned,
    #[error("OTEL value requires an integer-family type, found {actual}")]
    ExpectedInteger { actual: DataType },
    #[error("OTEL value requires a numeric type, found {actual}")]
    ExpectedNumeric { actual: DataType },
    #[error("OTEL metric value requires a numeric type, found {actual}")]
    ExpectedMetricNumeric { actual: DataType },
    #[error("OTEL DATETIME has an invalid Arrow representation")]
    InvalidDatetimeRepresentation,
    #[error("OTEL attribute arrays do not support NULL elements")]
    NullAttributeArrayElement,
    #[error("OTEL attribute type {actual} is unsupported")]
    UnsupportedAttributeType { actual: DataType },
    #[error("OTEL VALUES and ATTRIBUTES map {expected} columns, and the host projected {actual}")]
    MappedColumnCount { expected: usize, actual: usize },
}

type OtelValueResult<T> = Result<T, Report<OtelValueError>>;

/// A configuration this sink cannot start with.
fn invalid_configuration(reason: impl std::fmt::Display) -> Report<SinkStartError> {
    Report::new(SinkStartError::InvalidConfiguration { sink: OTEL })
        .attach_printable(reason.to_string())
}

/// A failed export the host retries on its declared backoff.
fn publish_failure(reason: impl std::fmt::Display) -> Report<SinkPublishError> {
    Report::new(SinkPublishError::Publish { sink: OTEL }).attach_printable(reason.to_string())
}

/// A failed export the host retries no sooner than the receiver asked it to.
fn publish_failure_after(
    reason: impl std::fmt::Display,
    delay: Duration,
) -> Report<SinkPublishError> {
    publish_failure(reason).attach(SinkRetryDelay(delay))
}

/// A refused export that no retry would change, such as a receiver that rejects this endpoint.
fn misconfigured(reason: impl std::fmt::Display) -> Report<SinkPublishError> {
    Report::new(SinkPublishError::Misconfigured { sink: OTEL }).attach_printable(reason.to_string())
}

impl OtelClientSettings {
    fn parse(config: &[ClientConfigEntry]) -> SinkStartResult<Self> {
        const ALLOWED_KEYS: &[&str] = &[
            "endpoint",
            "protocol",
            "headers",
            "compression",
            "timeout_ms",
            "tls_ca_file",
            "tls_cert_file",
            "tls_key_file",
        ];
        let mut seen = HashSet::new();
        for entry in config {
            let key = entry.key.to_ascii_lowercase();
            if !ALLOWED_KEYS.contains(&key.as_str()) {
                return Err(invalid_configuration(format!(
                    "unsupported OTEL client config key '{}'",
                    entry.key
                )));
            }
            if !seen.insert(key) {
                return Err(invalid_configuration(format!(
                    "duplicate OTEL client config key '{}'",
                    entry.key
                )));
            }
        }

        let endpoint =
            client_config_value(config, "endpoint", "OTEL").map_err(invalid_configuration)?;
        let endpoint = url::Url::parse(&endpoint)
            .map_err(|error| invalid_configuration(format!("invalid OTEL endpoint: {error}")))?;
        if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
            return Err(invalid_configuration(format!(
                "OTEL endpoint must use http or https, found '{}'",
                endpoint.scheme()
            )));
        }
        if endpoint.host_str().is_none() {
            return Err(invalid_configuration("OTEL endpoint must include a host"));
        }

        let protocol =
            client_config_value(config, "protocol", "OTEL").map_err(invalid_configuration)?;
        let protocol = match protocol.as_str() {
            "grpc" => OtelProtocol::Grpc,
            "http/protobuf" => OtelProtocol::HttpProtobuf,
            _ => {
                return Err(invalid_configuration(format!(
                    "invalid OTEL protocol '{protocol}'; expected 'grpc' or 'http/protobuf'"
                )));
            }
        };

        let compression = match optional_client_config_value(config, "compression") {
            None => OtelCompression::None,
            Some("gzip") => OtelCompression::Gzip,
            Some(compression) => {
                return Err(invalid_configuration(format!(
                    "invalid OTEL compression '{compression}'; expected 'gzip'"
                )));
            }
        };
        let timeout = optional_client_config_value(config, "timeout_ms")
            .map(|raw| -> SinkStartResult<Duration> {
                // `NonZeroU64` rejects both a non-number and a zero, so a request budget that
                // could never allow a request is not representable past this point.
                let millis = raw.parse::<NonZeroU64>().map_err(|_| {
                    invalid_configuration(format!(
                        "invalid OTEL timeout_ms '{raw}'; expected a positive integer"
                    ))
                })?;
                Ok(Duration::from_millis(millis.get()))
            })
            .transpose()?;
        let headers = optional_client_config_value(config, "headers")
            .map(Self::parse_headers)
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            endpoint,
            protocol,
            headers,
            compression,
            timeout,
        })
    }

    fn parse_headers(raw: &str) -> SinkStartResult<Vec<(String, String)>> {
        if raw.trim().is_empty() {
            return Ok(Vec::new());
        }
        raw.split(',')
            .map(|entry| {
                let (key, value) = entry.split_once('=').ok_or_else(|| {
                    invalid_configuration("invalid OTEL headers entry; expected k=v")
                })?;
                let key = key.trim().to_ascii_lowercase();
                let value = value.trim().to_string();
                if key.is_empty() {
                    return Err(invalid_configuration(
                        "OTEL headers entries require a non-empty key",
                    ));
                }
                Ok((key, value))
            })
            .collect()
    }
}

impl OtelSink {
    pub fn new(config: OtelSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let OtelSinkConfig {
            config,
            dns,
            signal,
            batch,
            values,
            attributes,
            resource,
            scope,
            mapped_schema,
        } = config;
        let client = OtelClient {
            transport: Self::transport_from_config(&config, &dns)?,
        };
        Self::validate_mapped_types(&signal, &values, &attributes, &mapped_schema)
            .map_err(|error| invalid_configuration(format!("{error:?}")))?;
        // A key mapped twice keeps its first column, which is the one the signal reads.
        let mut value_columns = HashMap::with_capacity(values.len());
        for (index, key) in values.iter().enumerate() {
            value_columns.entry(key.clone()).or_insert(index);
        }
        let mut attribute_values = Vec::with_capacity(resource.len());
        for attribute in resource {
            let value = attribute
                .value
                .any_value()
                .map_err(|error| invalid_configuration(format!("{error:?}")))?;
            if let Some(value) = value {
                attribute_values.push(KeyValue {
                    key: attribute.key,
                    value: Some(value),
                });
            }
        }
        let resource = Resource {
            attributes: attribute_values,
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        };
        let scope = scope.map(|scope| InstrumentationScope {
            name: scope.name,
            version: scope.version.unwrap_or_default(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        });
        Ok(Self {
            client,
            service: OtelExportService::from(&signal),
            signal,
            batch: batch.map(RowRequestLimits::from),
            attribute_offset: values.len(),
            value_columns,
            attributes,
            resource,
            scope,
        })
    }

    fn transport_from_config(
        config: &[ClientConfigEntry],
        dns: &nervix_dns::DnsResolver,
    ) -> SinkStartResult<OtelTransport> {
        let settings = OtelClientSettings::parse(config)?;
        match settings.protocol {
            OtelProtocol::Grpc => {
                let mut endpoint =
                    Endpoint::from_shared(settings.endpoint.to_string()).map_err(|error| {
                        invalid_configuration(format!("invalid OTEL endpoint: {error}"))
                    })?;
                if let Some(timeout) = settings.timeout {
                    endpoint = endpoint.timeout(timeout).connect_timeout(timeout);
                }
                let tls = client_tls_paths(config);
                if settings.endpoint.scheme() == "https" {
                    let host = settings.endpoint.host_str().verified(
                        "the check above accepted an https endpoint, and the url crate always \
                         gives a special-scheme URL a host",
                    );
                    let mut tls_config = ClientTlsConfig::new()
                        .with_webpki_roots()
                        .domain_name(host.to_string());
                    if let Some(ca_file) = tls.ca_file.as_ref() {
                        let ca = read_tls_file(ca_file, "OTEL TLS CA certificate")
                            .map_err(invalid_configuration)?;
                        tls_config = tls_config.ca_certificate(Certificate::from_pem(ca));
                    }
                    match (&tls.cert_file, &tls.key_file) {
                        (Some(cert_file), Some(key_file)) => {
                            let cert = read_tls_file(cert_file, "OTEL TLS certificate")
                                .map_err(invalid_configuration)?;
                            let key = read_tls_file(key_file, "OTEL TLS private key")
                                .map_err(invalid_configuration)?;
                            tls_config = tls_config.identity(Identity::from_pem(cert, key));
                        }
                        (None, None) => {}
                        _ => {
                            return Err(invalid_configuration(
                                "OTEL TLS client authentication requires both 'tls_cert_file' and \
                                 'tls_key_file'",
                            ));
                        }
                    }
                    endpoint = endpoint.tls_config(tls_config).map_err(|error| {
                        invalid_configuration(format!("invalid OTEL TLS configuration: {error}"))
                    })?;
                } else if !tls.is_empty() {
                    return Err(invalid_configuration(
                        "OTEL TLS files require an https endpoint",
                    ));
                }
                // Tonic keeps the configured URI for authority and TLS. Its custom connector
                // still wraps DNS, TCP and TLS in the endpoint's connection deadline.
                let mut connector = HttpConnector::new_with_resolver(dns.clone());
                connector.enforce_http(false);
                connector.set_nodelay(endpoint.get_tcp_nodelay());
                connector.set_keepalive(endpoint.get_tcp_keepalive());
                connector.set_keepalive_interval(endpoint.get_tcp_keepalive_interval());
                connector.set_keepalive_retries(endpoint.get_tcp_keepalive_retries());
                connector.set_connect_timeout(endpoint.get_connect_timeout());
                let channel = endpoint.connect_with_connector_lazy(connector);
                let metadata = Self::grpc_metadata(&settings.headers)?;
                Ok(OtelTransport::Grpc {
                    channel,
                    metadata,
                    compression: settings.compression,
                })
            }
            OtelProtocol::HttpProtobuf => {
                let client = HttpClientConfig::new(config, "OTEL", dns)
                    .build()
                    .map_err(invalid_configuration)?;
                let headers = Self::http_headers(&settings.headers)?;
                Ok(OtelTransport::HttpProtobuf {
                    client,
                    endpoint: settings.endpoint,
                    headers,
                    compression: settings.compression,
                })
            }
        }
    }

    fn grpc_metadata(headers: &[(String, String)]) -> SinkStartResult<MetadataMap> {
        let mut metadata = MetadataMap::new();
        for (key, value) in headers {
            let key = MetadataKey::<Ascii>::from_bytes(key.as_bytes()).map_err(|error| {
                invalid_configuration(format!("invalid OTEL gRPC header name '{key}': {error}"))
            })?;
            let value = MetadataValue::<Ascii>::from_str(value).map_err(|error| {
                invalid_configuration(format!("invalid OTEL gRPC header value: {error}"))
            })?;
            metadata.insert(key, value);
        }
        Ok(metadata)
    }

    fn http_headers(headers: &[(String, String)]) -> SinkStartResult<HeaderMap> {
        let mut parsed = HeaderMap::new();
        for (key, value) in headers {
            let name = HeaderName::from_bytes(key.as_bytes()).map_err(|error| {
                invalid_configuration(format!("invalid OTEL HTTP header name '{key}': {error}"))
            })?;
            let value = HeaderValue::from_str(value).map_err(|error| {
                invalid_configuration(format!("invalid OTEL HTTP header value: {error}"))
            })?;
            parsed.insert(name, value);
        }
        Ok(parsed)
    }

    /// Checks the exact type of every mapped column before the first batch arrives.
    ///
    /// The signal's own keys come first and its attributes follow, in the order the host projects
    /// them, so a column is read by position rather than by a name a mapping may have used twice.
    fn validate_mapped_types(
        signal: &OtelSignal,
        values: &[String],
        attributes: &[String],
        mapped_schema: &StdArc<arrow_schema::Schema>,
    ) -> OtelValueResult<()> {
        let expected = values
            .len()
            .checked_add(attributes.len())
            .assured("an emitter maps fewer columns than usize can count");
        if mapped_schema.fields().len() != expected {
            return Err(Report::new(OtelValueError::MappedColumnCount {
                expected,
                actual: mapped_schema.fields().len(),
            }));
        }
        for (index, key) in values.iter().enumerate() {
            let field = mapped_schema.field(index);
            let (valid, expected) = Self::valid_value_type(signal, key, field.data_type());
            if !valid {
                return Err(Report::new(OtelValueError::InvalidMappedType {
                    signal: Self::signal_label(signal),
                    key: key.clone(),
                    expected,
                    actual: field.data_type().clone(),
                }));
            }
        }
        for (offset, key) in attributes.iter().enumerate() {
            let index = values
                .len()
                .checked_add(offset)
                .assured("an emitter maps fewer columns than usize can count");
            let field = mapped_schema.field(index);
            if !Self::valid_attribute_type(field.data_type()) {
                return Err(Report::new(OtelValueError::InvalidAttributeMappingType {
                    key: key.clone(),
                    actual: field.data_type().clone(),
                }));
            }
        }
        Ok(())
    }

    fn signal_label(signal: &OtelSignal) -> &'static str {
        match signal {
            OtelSignal::Logs => "LOGS",
            OtelSignal::Traces => "TRACES",
            OtelSignal::Metric(_) => "METRIC",
        }
    }

    fn valid_value_type(signal: &OtelSignal, key: &str, ty: &DataType) -> (bool, &'static str) {
        let string = || (*ty == DataType::Utf8, "STRING");
        let datetime = || (Self::is_datetime_type(ty), "DATETIME");
        match signal {
            OtelSignal::Logs => match key {
                "time" => datetime(),
                "severity_text" | "body" | "trace_id" | "span_id" => string(),
                "severity_number" => (*ty == DataType::Int32, "I32"),
                _ => (false, "a supported LOGS value type"),
            },
            OtelSignal::Traces => match key {
                "trace_id" | "span_id" | "parent_span_id" | "name" | "kind" | "status_code"
                | "status_message" => string(),
                "start_time" | "end_time" => datetime(),
                _ => (false, "a supported TRACES value type"),
            },
            OtelSignal::Metric(metric) => match (&metric.kind, key) {
                (_, "time" | "start_time") => datetime(),
                (OtelMetricKind::Gauge | OtelMetricKind::Sum { .. }, "value") => (
                    Self::is_number_type(ty),
                    "an integer-family, F32, or F64 value",
                ),
                (OtelMetricKind::Histogram { .. }, "count") => {
                    (Self::is_integer_type(ty), "an integer-family value")
                }
                (OtelMetricKind::Histogram { .. }, "sum" | "min" | "max") => (
                    Self::is_number_type(ty),
                    "an integer-family, F32, or F64 value",
                ),
                (OtelMetricKind::Histogram { .. }, "bucket_counts") => (
                    Self::list_element_type(ty).is_some_and(Self::is_integer_type),
                    "an integer ARRAY or VEC",
                ),
                (OtelMetricKind::Histogram { .. }, "explicit_bounds") => (
                    Self::list_element_type(ty).is_some_and(|element| {
                        matches!(element, DataType::Float32 | DataType::Float64)
                    }),
                    "an F32 or F64 ARRAY or VEC",
                ),
                _ => (false, "a value supported by the configured metric shape"),
            },
        }
    }

    fn is_datetime_type(ty: &DataType) -> bool {
        matches!(ty, DataType::Timestamp(TimeUnit::Nanosecond, _))
    }

    fn is_integer_type(ty: &DataType) -> bool {
        matches!(
            ty,
            DataType::UInt8
                | DataType::Int8
                | DataType::UInt16
                | DataType::Int16
                | DataType::UInt32
                | DataType::Int32
                | DataType::UInt64
                | DataType::Int64
        )
    }

    fn is_number_type(ty: &DataType) -> bool {
        Self::is_integer_type(ty) || matches!(ty, DataType::Float32 | DataType::Float64)
    }

    fn list_element_type(ty: &DataType) -> Option<&DataType> {
        match ty {
            DataType::List(field) | DataType::FixedSizeList(field, _) => Some(field.data_type()),
            _ => None,
        }
    }

    fn valid_attribute_type(ty: &DataType) -> bool {
        matches!(
            ty,
            DataType::Utf8 | DataType::Boolean | DataType::Float32 | DataType::Float64
        ) || Self::is_integer_type(ty)
            || Self::is_datetime_type(ty)
            || Self::list_element_type(ty).is_some_and(Self::valid_attribute_type)
    }

    fn timestamp_to_unix_nano(value: i64, key: &str) -> Result<u64, OtelRecordError> {
        u64::try_from(value).map_err(|_| {
            OtelRecordError::new(key, format!("OTEL {key} cannot be before the Unix epoch"))
        })
    }

    fn observation_time_unix_nano() -> Result<u64, OtelRecordError> {
        Self::timestamp_to_unix_nano(
            nervix_connector::physical_time::actual_utc_now().unix_nanos(),
            "observed_time",
        )
    }
}

#[async_trait]
impl SinkLifecycle for OtelSink {}

#[async_trait]
impl RowRequestSink for OtelSink {
    /// Converts every selected row once and divides the converted records of each carrier, in
    /// source order, into Export requests of one resource and one scope, encoding each request
    /// exactly once.
    ///
    /// Every log record carries the observed time this call samples, so a request sent again after
    /// an unknown outcome carries the time of its first attempt. Without `BATCH`, one request
    /// carries every converted record of a carrier. With it, a request holds at most `MAX MESSAGES`
    /// records and at most `MAX SIZE` encoded bytes: a candidate larger than that is halved, and a
    /// record that alone exceeds it is refused.
    async fn prepare(
        &mut self,
        rows: MappedSinkRows<'_>,
    ) -> SinkPublishResult<RowRequestPreparation> {
        let mut preparation = RowRequestPreparation::default();
        let observed_time = OtelSink::observation_time_unix_nano()
            .map_err(|error| publish_failure(error.reason))?;
        for carrier in &rows.carriers {
            tokio::task::consume_budget().await;
            self.prepare_carrier(carrier, observed_time, &mut preparation)
                .await;
        }
        Ok(preparation)
    }

    /// Sends each prepared request, in order, exactly as it was prepared.
    ///
    /// An accepted request is delivered and a request the receiver refused is rejected. The first
    /// request whose outcome is unknown ends the write, which leaves it and every request after it
    /// unanswered for the host to hand over again.
    async fn publish(&mut self, requests: Vec<SinkRowRequest>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(requests.len());
        for request in requests {
            tokio::task::consume_budget().await;
            let SinkRowRequest {
                id,
                body,
                occurred_at,
            } = request;
            match self.client.export(self.service, body).await {
                OtelTransportOutcome::Accepted(partial_success) => {
                    if let Some(partial) = partial_success
                        && (partial.rejected != 0 || !partial.error_message.is_empty())
                    {
                        warn!(
                            rejected_records = partial.rejected,
                            receiver_supplied_message = !partial.error_message.is_empty(),
                            "OTEL receiver returned partial_success; request records are \
                             acknowledged without retry"
                        );
                    }
                    outcome.deliver(id);
                }
                OtelTransportOutcome::Rejected(reason) => {
                    outcome.reject(RejectedSinkRecord::external(id, occurred_at, reason));
                }
                OtelTransportOutcome::Failed(error) => {
                    outcome.fail(error);
                    return outcome;
                }
            }
        }
        outcome
    }
}

impl OtelSink {
    /// Prepares the Export requests that carry the rows of one carrier: one request without
    /// `BATCH`, and with it requests of at most `MAX MESSAGES` records whose protobuf encoding is at
    /// most `MAX SIZE` bytes.
    async fn prepare_carrier(
        &self,
        carrier: &MappedSinkCarrier<'_>,
        observed_time: u64,
        preparation: &mut RowRequestPreparation,
    ) {
        let mapped = OtelMappedBatch {
            batch: carrier.batch,
            value_columns: &self.value_columns,
            attributes: &self.attributes,
            attribute_offset: self.attribute_offset,
        };
        let position = |row: usize| SinkRecordPosition {
            batch_index: carrier.batch_index,
            row_index: row,
        };
        let mut positions = Vec::with_capacity(carrier.selected_rows.len());
        let request = match &self.signal {
            OtelSignal::Logs => {
                let mut records = Vec::with_capacity(carrier.selected_rows.len());
                for row in carrier.selected_rows {
                    tokio::task::consume_budget().await;
                    match mapped.log_record(*row, observed_time) {
                        Ok(record) => {
                            records.push(record);
                            positions.push(position(*row));
                        }
                        Err(error) => preparation
                            .rejected
                            .push(error.rejected(position(*row), carrier.occurred_at)),
                    }
                }
                OtelExportRequest::Logs(ExportLogsServiceRequest {
                    resource_logs: vec![ResourceLogs {
                        resource: Some(self.resource.clone()),
                        scope_logs: vec![ScopeLogs {
                            scope: self.scope.clone(),
                            log_records: records,
                            schema_url: String::new(),
                        }],
                        schema_url: String::new(),
                    }],
                })
            }
            OtelSignal::Traces => {
                let mut spans = Vec::with_capacity(carrier.selected_rows.len());
                for row in carrier.selected_rows {
                    tokio::task::consume_budget().await;
                    match mapped.span(*row) {
                        Ok(span) => {
                            spans.push(span);
                            positions.push(position(*row));
                        }
                        Err(error) => preparation
                            .rejected
                            .push(error.rejected(position(*row), carrier.occurred_at)),
                    }
                }
                OtelExportRequest::Traces(ExportTraceServiceRequest {
                    resource_spans: vec![ResourceSpans {
                        resource: Some(self.resource.clone()),
                        scope_spans: vec![ScopeSpans {
                            scope: self.scope.clone(),
                            spans,
                            schema_url: String::new(),
                        }],
                        schema_url: String::new(),
                    }],
                })
            }
            OtelSignal::Metric(metric) => {
                let metric = mapped
                    .metric(
                        metric,
                        carrier.selected_rows,
                        carrier.batch_index,
                        carrier.occurred_at,
                        &mut positions,
                        &mut preparation.rejected,
                    )
                    .await;
                OtelExportRequest::Metrics(ExportMetricsServiceRequest {
                    resource_metrics: vec![ResourceMetrics {
                        resource: Some(self.resource.clone()),
                        scope_metrics: vec![ScopeMetrics {
                            scope: self.scope.clone(),
                            metrics: vec![metric],
                            schema_url: String::new(),
                        }],
                        schema_url: String::new(),
                    }],
                })
            }
        };
        if positions.is_empty() {
            return;
        }

        let Some(limits) = self.batch else {
            preparation.requests.push(PreparedRowRequest {
                members: positions,
                body: request.encode_to_vec(),
            });
            return;
        };
        let requests = limits.divide(positions.len(), |candidate| {
            let export = request.members(candidate);
            let size = u64::try_from(export.encoded_len())
                .assured("Nervix runs on 64-bit targets, so u64 holds a protobuf length");
            MeasuredRequest {
                size,
                request: export,
            }
        });
        if requests.subdivisions > 0 {
            debug!(
                subdivisions = requests.subdivisions,
                "halved OTEL export requests that exceeded MAX SIZE"
            );
        }
        for request in requests.requests {
            match request {
                RowRequest::Write {
                    members, request, ..
                } => {
                    let carried = positions
                        .get(members)
                        .assured("a request carries records of the carrier it divided");
                    preparation.requests.push(PreparedRowRequest {
                        members: carried.to_vec(),
                        body: request.encode_to_vec(),
                    });
                }
                RowRequest::Oversize { member, oversize } => {
                    preparation.rejected.push(oversize.rejected(
                        positions[member],
                        carrier.occurred_at,
                        "OTEL export request",
                    ));
                }
            }
        }
    }
}

impl OtelClient {
    async fn export(&self, service: OtelExportService, body: Vec<u8>) -> OtelTransportOutcome {
        self.transport.export(service, body).await
    }
}

impl OtelTransport {
    /// Sends one prepared Export request's bytes to `service`, compressed as the client is
    /// configured, and classifies the answer.
    async fn export(&self, service: OtelExportService, body: Vec<u8>) -> OtelTransportOutcome {
        match self {
            Self::Grpc {
                channel,
                metadata,
                compression,
            } => Self::export_grpc(channel, metadata, *compression, service, body).await,
            Self::HttpProtobuf {
                client,
                endpoint,
                headers,
                compression,
            } => Self::export_http(client, endpoint, headers, *compression, service, body).await,
        }
    }

    fn grpc_request<T>(value: T, metadata: &MetadataMap) -> GrpcRequest<T> {
        let mut request = GrpcRequest::new(value);
        *request.metadata_mut() = metadata.clone();
        request
    }

    async fn export_grpc(
        channel: &Channel,
        metadata: &MetadataMap,
        compression: OtelCompression,
        service: OtelExportService,
        body: Vec<u8>,
    ) -> OtelTransportOutcome {
        let mut client = GrpcClient::new(channel.clone());
        if compression == OtelCompression::Gzip {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }
        // A channel that cannot take a request has not sent it. The failure keeps its cause, so it
        // classifies as an export without an answer and the host retries it.
        if let Err(error) = client.ready().await {
            return Self::grpc_failure(GrpcStatus::from_error(error.into()));
        }
        let exported = client
            .unary(
                Self::grpc_request(body, metadata),
                PathAndQuery::from_static(service.grpc_path()),
                PreparedExportCodec { service },
            )
            .await;
        match exported {
            Ok(response) => OtelTransportOutcome::Accepted(response.into_inner()),
            Err(status) => Self::grpc_failure(status),
        }
    }

    /// Classifies a gRPC export that did not succeed.
    ///
    /// tonic keeps the local or transport failure it reports a status for as that status's source,
    /// and a status the receiver answered with carries none. Without an answer — the request timed
    /// out, the connection was lost, or the response could not be read — nothing says whether the
    /// receiver accepted the export, so it is retried. An answer is classified by its code:
    /// `INVALID_ARGUMENT` refuses the request, `RESOURCE_EXHAUSTED` and the codes the OTLP
    /// specification lists as retryable are retried no sooner than the receiver's `RetryInfo`, and
    /// any other code says the endpoint cannot take this export as it is configured.
    fn grpc_failure(status: GrpcStatus) -> OtelTransportOutcome {
        if std::error::Error::source(&status).is_some() {
            return OtelTransportOutcome::Failed(publish_failure(format!(
                "OTEL gRPC export ended without an answer from the receiver: {} ({})",
                status.code(),
                status.message()
            )));
        }
        match status.code() {
            GrpcCode::InvalidArgument => OtelTransportOutcome::Rejected(
                "OTEL receiver rejected the request with gRPC INVALID_ARGUMENT".to_string(),
            ),
            GrpcCode::Cancelled
            | GrpcCode::DeadlineExceeded
            | GrpcCode::ResourceExhausted
            | GrpcCode::Aborted
            | GrpcCode::OutOfRange
            | GrpcCode::Unavailable
            | GrpcCode::DataLoss => {
                let message = format!("OTEL gRPC export failed with {}", status.code());
                let retry_delay = status
                    .get_details_retry_info()
                    .and_then(|info| info.retry_delay);
                OtelTransportOutcome::Failed(match retry_delay {
                    Some(delay) => publish_failure_after(message, delay),
                    None => publish_failure(message),
                })
            }
            _ => OtelTransportOutcome::Failed(misconfigured(format!(
                "OTEL gRPC export failed with non-retryable {}",
                status.code()
            ))),
        }
    }

    async fn export_http(
        client: &HttpClient,
        endpoint: &url::Url,
        headers: &HeaderMap,
        compression: OtelCompression,
        service: OtelExportService,
        body: Vec<u8>,
    ) -> OtelTransportOutcome {
        let body = match Self::http_body(body, compression) {
            Ok(body) => body,
            Err(error) => return OtelTransportOutcome::Failed(error),
        };
        let mut url = endpoint.clone();
        let base_path = url.path().trim_end_matches('/');
        url.set_path(&format!("{base_path}/v1/{}", service.http_path()));
        let mut request = client
            .post(url)
            .headers(headers.clone())
            .header(CONTENT_TYPE, OTLP_PROTOBUF_CONTENT_TYPE);
        if compression == OtelCompression::Gzip {
            request = request.header(CONTENT_ENCODING, "gzip");
        }
        let response = match request.body(body).send().await {
            Ok(response) => response,
            Err(error) => {
                return OtelTransportOutcome::Failed(publish_failure(format!(
                    "OTEL HTTP export request failed: {error}"
                )));
            }
        };
        let status = response.status();
        if status == StatusCode::BAD_REQUEST {
            return OtelTransportOutcome::Rejected(
                "OTEL receiver rejected the request with HTTP 400 Bad Request".to_string(),
            );
        }
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            let delay = Self::http_retry_after(
                response
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok()),
                nervix_connector::physical_time::actual_utc_now().into_datetime(),
            );
            let message = format!("OTEL HTTP export returned status {status}");
            return OtelTransportOutcome::Failed(match delay {
                Some(delay) => publish_failure_after(message, delay),
                None => publish_failure(message),
            });
        }
        if !status.is_success() {
            return OtelTransportOutcome::Failed(misconfigured(format!(
                "OTEL HTTP export returned non-retryable status {status}"
            )));
        }
        let body = match response.bytes().await {
            Ok(body) => body,
            Err(error) => {
                return OtelTransportOutcome::Failed(publish_failure(format!(
                    "failed to read OTEL HTTP response: {error}"
                )));
            }
        };
        match service.decode_response(&body[..]) {
            Ok(partial) => OtelTransportOutcome::Accepted(partial),
            Err(error) => OtelTransportOutcome::Failed(publish_failure(format!(
                "failed to decode OTEL HTTP protobuf response: {error}"
            ))),
        }
    }

    fn http_body(body: Vec<u8>, compression: OtelCompression) -> SinkPublishResult<Vec<u8>> {
        if compression == OtelCompression::None {
            return Ok(body);
        }
        let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::default());
        encoder.write_all(&body).map_err(|error| {
            publish_failure(format!("failed to gzip OTEL HTTP request: {error}"))
        })?;
        encoder.finish().map_err(|error| {
            publish_failure(format!("failed to finish OTEL HTTP gzip request: {error}"))
        })
    }

    fn http_retry_after(
        value: Option<&str>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<Duration> {
        let value = value?.trim();
        if let Ok(seconds) = value.parse::<f64>()
            && seconds.is_finite()
            && seconds >= 0.0
            && let Ok(delay) = Duration::try_from_secs_f64(seconds)
        {
            return Some(delay);
        }
        let Ok(deadline) = chrono::DateTime::parse_from_rfc2822(value) else {
            return None;
        };
        deadline
            .with_timezone(&chrono::Utc)
            .signed_duration_since(now)
            .to_std()
            .ok()
    }
}

macro_rules! declare_otel_export_services {
    ($($Service:ident => $http_path:literal, $grpc_path:literal, $Response:ident,
        $PartialSuccess:ident, $rejected:ident;)+) => {
        /// The OTLP export service one signal's requests are sent to, which fixes where a request
        /// goes over either transport and which response the receiver answers it with.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum OtelExportService {
            $($Service,)+
        }

        impl OtelExportService {
            /// The path segment an OTLP/HTTP request is posted to under `/v1/`.
            fn http_path(self) -> &'static str {
                match self {
                    $(Self::$Service => $http_path,)+
                }
            }

            /// The method an OTLP/gRPC request calls.
            fn grpc_path(self) -> &'static str {
                match self {
                    $(Self::$Service => $grpc_path,)+
                }
            }

            /// The partial success the receiver's Export response reports, if it reports one.
            fn decode_response(
                self,
                body: impl Buf,
            ) -> Result<Option<OtelPartialSuccess>, otel_prost::DecodeError> {
                match self {
                    $(Self::$Service => Ok($Response::decode(body)?
                        .partial_success
                        .map(Into::into)),)+
                }
            }
        }

        $(impl From<$PartialSuccess> for OtelPartialSuccess {
            fn from(value: $PartialSuccess) -> Self {
                Self {
                    rejected: value.$rejected,
                    error_message: value.error_message,
                }
            }
        })+
    };
}

declare_otel_export_services! {
    Logs => "logs", "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
        ExportLogsServiceResponse, ExportLogsPartialSuccess, rejected_log_records;
    Traces => "traces", "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
        ExportTraceServiceResponse, ExportTracePartialSuccess, rejected_spans;
    Metrics => "metrics", "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
        ExportMetricsServiceResponse, ExportMetricsPartialSuccess, rejected_data_points;
}

impl From<&OtelSignal> for OtelExportService {
    fn from(signal: &OtelSignal) -> Self {
        match signal {
            OtelSignal::Logs => Self::Logs,
            OtelSignal::Traces => Self::Traces,
            OtelSignal::Metric(_) => Self::Metrics,
        }
    }
}

/// The gRPC codec that sends a prepared Export request's bytes exactly as they were prepared and
/// reads the receiver's response for one export service.
struct PreparedExportCodec {
    service: OtelExportService,
}

impl Codec for PreparedExportCodec {
    type Encode = Vec<u8>;
    type Decode = Option<OtelPartialSuccess>;
    type Encoder = PreparedExportEncoder;
    type Decoder = ExportResponseDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        PreparedExportEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        ExportResponseDecoder {
            service: self.service,
        }
    }
}

/// Writes a prepared request's bytes as the gRPC message, unchanged.
struct PreparedExportEncoder;

impl Encoder for PreparedExportEncoder {
    type Item = Vec<u8>;
    type Error = GrpcStatus;

    fn encode(&mut self, item: Vec<u8>, dst: &mut EncodeBuf<'_>) -> Result<(), GrpcStatus> {
        dst.put_slice(&item);
        Ok(())
    }
}

/// Reads the receiver's Export response for one service.
struct ExportResponseDecoder {
    service: OtelExportService,
}

impl Decoder for ExportResponseDecoder {
    type Item = Option<OtelPartialSuccess>;
    type Error = GrpcStatus;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, GrpcStatus> {
        match self.service.decode_response(src) {
            Ok(partial_success) => Ok(Some(partial_success)),
            // A response that cannot be read says nothing about whether the export was accepted,
            // so the status keeps the decode error as its source and classifies as no answer.
            Err(error) => Err(GrpcStatus::from_error(Box::new(error))),
        }
    }
}

/// The mapped columns of one batch, read by the position the host projected them in.
struct OtelMappedBatch<'a> {
    batch: &'a RecordBatch,
    /// Where each signal key sits among the mapped columns.
    value_columns: &'a HashMap<String, usize>,
    /// The attribute keys, in the order their columns follow the signal's own.
    attributes: &'a [String],
    attribute_offset: usize,
}

impl OtelMappedBatch<'_> {
    /// The column one signal key was mapped to, or nothing when the emitter does not map it.
    fn value_array(&self, key: &str) -> Result<Option<ArrayRef>, OtelRecordError> {
        let Some(index) = self.value_columns.get(key).copied() else {
            return Ok(None);
        };
        let array = self.batch.columns().get(index).ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES output omitted key '{key}'"))
        })?;
        Ok(Some(array.clone()))
    }

    fn required_string(&self, key: &str, row: usize) -> Result<String, OtelRecordError> {
        self.optional_string(key, row)?.ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES key '{key}' cannot be NULL"))
        })
    }

    fn optional_string(&self, key: &str, row: usize) -> Result<Option<String>, OtelRecordError> {
        let Some(array) = self.value_array(key)? else {
            return Ok(None);
        };
        if array.is_null(row) {
            return Ok(None);
        }
        let array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                OtelRecordError::new(key, format!("OTEL VALUES key '{key}' is not STRING"))
            })?;
        Ok(Some(array.value(row).to_string()))
    }

    fn required_timestamp(&self, key: &str, row: usize) -> Result<u64, OtelRecordError> {
        self.optional_timestamp(key, row)?.ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES key '{key}' cannot be NULL"))
        })
    }

    fn optional_timestamp(&self, key: &str, row: usize) -> Result<Option<u64>, OtelRecordError> {
        let Some(array) = self.value_array(key)? else {
            return Ok(None);
        };
        if array.is_null(row) {
            return Ok(None);
        }
        let array = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| {
                OtelRecordError::new(key, format!("OTEL VALUES key '{key}' is not DATETIME"))
            })?;
        OtelSink::timestamp_to_unix_nano(array.value(row), key).map(Some)
    }

    fn attributes(&self, row: usize) -> Result<Vec<KeyValue>, OtelRecordError> {
        let mut values = Vec::with_capacity(self.attributes.len());
        for (offset, key) in self.attributes.iter().enumerate() {
            let index = self
                .attribute_offset
                .checked_add(offset)
                .assured("an emitter maps fewer columns than usize can count");
            let array = self.batch.columns().get(index).ok_or_else(|| {
                OtelRecordError::new(
                    key.clone(),
                    format!("OTEL ATTRIBUTES output omitted key '{key}'"),
                )
            })?;
            if let Some(value) = any_value_at(array, row)
                .map_err(|error| OtelRecordError::new(key.clone(), error.to_string()))?
            {
                values.push(KeyValue {
                    key: key.clone(),
                    value: Some(value),
                });
            }
        }
        Ok(values)
    }

    fn log_record(&self, row: usize, observed_time: u64) -> Result<LogRecord, OtelRecordError> {
        let severity_number = match self.value_array("severity_number")? {
            Some(array) if !array.is_null(row) => {
                let value = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        OtelRecordError::new("severity_number", "OTEL severity_number is not I32")
                    })?
                    .value(row);
                parse_severity_number(value)?
            }
            _ => 0,
        };
        let trace_id = self
            .optional_string("trace_id", row)?
            .map(|value| parse_hex_id(&value, 16, "trace_id"))
            .transpose()?
            .unwrap_or_default();
        let span_id = self
            .optional_string("span_id", row)?
            .map(|value| parse_hex_id(&value, 8, "span_id"))
            .transpose()?
            .unwrap_or_default();
        Ok(LogRecord {
            time_unix_nano: self.required_timestamp("time", row)?,
            observed_time_unix_nano: observed_time,
            severity_number,
            severity_text: self
                .optional_string("severity_text", row)?
                .unwrap_or_default(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    self.required_string("body", row)?,
                )),
            }),
            attributes: self.attributes(row)?,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id,
            span_id,
            event_name: String::new(),
        })
    }

    fn span(&self, row: usize) -> Result<Span, OtelRecordError> {
        let kind = match self.optional_string("kind", row)?.as_deref() {
            None => i32::from(span::SpanKind::Unspecified),
            Some("INTERNAL") => i32::from(span::SpanKind::Internal),
            Some("SERVER") => i32::from(span::SpanKind::Server),
            Some("CLIENT") => i32::from(span::SpanKind::Client),
            Some("PRODUCER") => i32::from(span::SpanKind::Producer),
            Some("CONSUMER") => i32::from(span::SpanKind::Consumer),
            Some(_) => {
                return Err(OtelRecordError::new(
                    "kind",
                    "OTEL span kind must be INTERNAL, SERVER, CLIENT, PRODUCER, or CONSUMER",
                ));
            }
        };
        let status_code = match self.optional_string("status_code", row)?.as_deref() {
            None => None,
            Some("UNSET") => Some(i32::from(status::StatusCode::Unset)),
            Some("OK") => Some(i32::from(status::StatusCode::Ok)),
            Some("ERROR") => Some(i32::from(status::StatusCode::Error)),
            Some(_) => {
                return Err(OtelRecordError::new(
                    "status_code",
                    "OTEL span status_code must be UNSET, OK, or ERROR",
                ));
            }
        };
        let status_message = self.optional_string("status_message", row)?;
        let status = match (status_code, status_message) {
            (None, None) => None,
            (code, message) => Some(Status {
                message: message.unwrap_or_default(),
                code: code.unwrap_or(i32::from(status::StatusCode::Unset)),
            }),
        };
        Ok(Span {
            trace_id: parse_hex_id(&self.required_string("trace_id", row)?, 16, "trace_id")?,
            span_id: parse_hex_id(&self.required_string("span_id", row)?, 8, "span_id")?,
            trace_state: String::new(),
            parent_span_id: self
                .optional_string("parent_span_id", row)?
                .map(|value| parse_hex_id(&value, 8, "parent_span_id"))
                .transpose()?
                .unwrap_or_default(),
            flags: 0,
            name: self.required_string("name", row)?,
            kind,
            start_time_unix_nano: self.required_timestamp("start_time", row)?,
            end_time_unix_nano: self.required_timestamp("end_time", row)?,
            attributes: self.attributes(row)?,
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status,
        })
    }

    async fn metric(
        &self,
        model: &OtelMetric,
        selected_rows: &[usize],
        batch_index: usize,
        occurred_at: Timestamp,
        positions: &mut Vec<SinkRecordPosition>,
        rejected: &mut Vec<RejectedSinkRecord<SinkRecordPosition>>,
    ) -> Metric {
        let position = |row: usize| SinkRecordPosition {
            batch_index,
            row_index: row,
        };
        let data = match &model.kind {
            OtelMetricKind::Gauge | OtelMetricKind::Sum { .. } => {
                let require_start_time = matches!(
                    model.kind,
                    OtelMetricKind::Sum {
                        temporality: OtelAggregationTemporality::Delta,
                        ..
                    }
                );
                let mut points = Vec::with_capacity(selected_rows.len());
                for row in selected_rows {
                    tokio::task::consume_budget().await;
                    match self.number_point(*row, require_start_time) {
                        Ok(point) => {
                            points.push(point);
                            positions.push(position(*row));
                        }
                        Err(error) => rejected.push(error.rejected(position(*row), occurred_at)),
                    }
                }
                match &model.kind {
                    OtelMetricKind::Gauge => metric::Data::Gauge(Gauge {
                        data_points: points,
                    }),
                    OtelMetricKind::Sum {
                        monotonic,
                        temporality,
                    } => metric::Data::Sum(Sum {
                        data_points: points,
                        aggregation_temporality: aggregation_temporality(*temporality),
                        is_monotonic: *monotonic,
                    }),
                    OtelMetricKind::Histogram { .. } => {
                        unreachable!("the enclosing match arm accepted only a gauge or a sum")
                    }
                }
            }
            OtelMetricKind::Histogram { temporality } => {
                let require_start_time = *temporality == OtelAggregationTemporality::Delta;
                let mut points = Vec::with_capacity(selected_rows.len());
                for row in selected_rows {
                    tokio::task::consume_budget().await;
                    match self.histogram_point(*row, require_start_time) {
                        Ok(point) => {
                            points.push(point);
                            positions.push(position(*row));
                        }
                        Err(error) => rejected.push(error.rejected(position(*row), occurred_at)),
                    }
                }
                metric::Data::Histogram(Histogram {
                    data_points: points,
                    aggregation_temporality: aggregation_temporality(*temporality),
                })
            }
        };
        Metric {
            name: model.name.clone(),
            description: model.description.clone().unwrap_or_default(),
            unit: model.unit.clone(),
            metadata: Vec::new(),
            data: Some(data),
        }
    }

    fn number_point(
        &self,
        row: usize,
        require_start_time: bool,
    ) -> Result<NumberDataPoint, OtelRecordError> {
        let array = self.value_array("value")?.ok_or_else(|| {
            OtelRecordError::new("value", "OTEL metric VALUES requires key 'value'")
        })?;
        if array.is_null(row) {
            return Err(OtelRecordError::new(
                "value",
                "OTEL metric value cannot be NULL",
            ));
        }
        let value = number_value_at(&array, row)
            .map_err(|error| OtelRecordError::new("value", error.to_string()))?;
        Ok(NumberDataPoint {
            attributes: self.attributes(row)?,
            start_time_unix_nano: if require_start_time {
                self.required_timestamp("start_time", row)?
            } else {
                self.optional_timestamp("start_time", row)?.unwrap_or(0)
            },
            time_unix_nano: self.required_timestamp("time", row)?,
            exemplars: Vec::new(),
            flags: 0,
            value: Some(value),
        })
    }

    fn histogram_point(
        &self,
        row: usize,
        require_start_time: bool,
    ) -> Result<HistogramDataPoint, OtelRecordError> {
        let count = self.required_u64("count", row)?;
        let bucket_counts = self.required_u64_list("bucket_counts", row)?;
        let explicit_bounds = self.required_f64_list("explicit_bounds", row)?;
        validate_histogram_buckets(&bucket_counts, &explicit_bounds)?;
        Ok(HistogramDataPoint {
            attributes: self.attributes(row)?,
            start_time_unix_nano: if require_start_time {
                self.required_timestamp("start_time", row)?
            } else {
                self.optional_timestamp("start_time", row)?.unwrap_or(0)
            },
            time_unix_nano: self.required_timestamp("time", row)?,
            count,
            sum: self.optional_f64("sum", row)?,
            bucket_counts,
            explicit_bounds,
            exemplars: Vec::new(),
            flags: 0,
            min: self.optional_f64("min", row)?,
            max: self.optional_f64("max", row)?,
        })
    }

    fn required_u64(&self, key: &str, row: usize) -> Result<u64, OtelRecordError> {
        let array = self.value_array(key)?.ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES requires key '{key}'"))
        })?;
        if array.is_null(row) {
            return Err(OtelRecordError::new(
                key,
                format!("OTEL VALUES key '{key}' cannot be NULL"),
            ));
        }
        integer_as_u64(&array, row).map_err(|error| OtelRecordError::new(key, error.to_string()))
    }

    fn optional_f64(&self, key: &str, row: usize) -> Result<Option<f64>, OtelRecordError> {
        let Some(array) = self.value_array(key)? else {
            return Ok(None);
        };
        if array.is_null(row) {
            return Ok(None);
        }
        numeric_as_f64(&array, row)
            .map(Some)
            .map_err(|error| OtelRecordError::new(key, error.to_string()))
    }

    fn required_u64_list(&self, key: &str, row: usize) -> Result<Vec<u64>, OtelRecordError> {
        let array = self.value_array(key)?.ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES requires key '{key}'"))
        })?;
        let values = list_value(&array, row)
            .map_err(|error| OtelRecordError::new(key, error.to_string()))?
            .ok_or_else(|| {
                OtelRecordError::new(key, format!("OTEL VALUES key '{key}' cannot be NULL"))
            })?;
        (0..values.len())
            .map(|index| {
                if values.is_null(index) {
                    return Err(OtelRecordError::new(
                        key,
                        format!("OTEL {key} cannot contain NULL elements"),
                    ));
                }
                integer_as_u64(&values, index)
                    .map_err(|error| OtelRecordError::new(key, error.to_string()))
            })
            .collect()
    }

    fn required_f64_list(&self, key: &str, row: usize) -> Result<Vec<f64>, OtelRecordError> {
        let array = self.value_array(key)?.ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES requires key '{key}'"))
        })?;
        let values = list_value(&array, row)
            .map_err(|error| OtelRecordError::new(key, error.to_string()))?
            .ok_or_else(|| {
                OtelRecordError::new(key, format!("OTEL VALUES key '{key}' cannot be NULL"))
            })?;
        (0..values.len())
            .map(|index| {
                if values.is_null(index) {
                    return Err(OtelRecordError::new(
                        key,
                        format!("OTEL {key} cannot contain NULL elements"),
                    ));
                }
                numeric_as_f64(&values, index)
                    .map_err(|error| OtelRecordError::new(key, error.to_string()))
            })
            .collect()
    }
}

fn aggregation_temporality(value: OtelAggregationTemporality) -> i32 {
    match value {
        OtelAggregationTemporality::Delta => i32::from(AggregationTemporality::Delta),
        OtelAggregationTemporality::Cumulative => i32::from(AggregationTemporality::Cumulative),
    }
}

fn parse_severity_number(value: i32) -> Result<i32, OtelRecordError> {
    if (0..=24).contains(&value) {
        Ok(value)
    } else {
        Err(OtelRecordError::new(
            "severity_number",
            "OTEL severity_number must be in the range 0..=24",
        ))
    }
}

fn parse_hex_id(value: &str, byte_len: usize, key: &str) -> Result<Vec<u8>, OtelRecordError> {
    let bytes = value.as_bytes();
    if bytes.len() != byte_len * 2 || !bytes.iter().all(u8::is_ascii_hexdigit) {
        return Err(OtelRecordError::new(
            key,
            format!(
                "OTEL {key} must contain exactly {} hexadecimal characters",
                byte_len * 2
            ),
        ));
    }
    let decoded = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|digits| {
            let digits = std::str::from_utf8(digits).verified(
                "the guard above rejected every value that is not an even-length run of ASCII hex \
                 digits",
            );
            u8::from_str_radix(digits, 16).verified(
                "the guard above rejected every value that is not an even-length run of ASCII hex \
                 digits",
            )
        })
        .collect::<Vec<_>>();
    if decoded.iter().all(|byte| *byte == 0) {
        return Err(OtelRecordError::new(
            key,
            format!("OTEL {key} cannot be all zeroes"),
        ));
    }
    Ok(decoded)
}

fn validate_histogram_buckets(
    bucket_counts: &[u64],
    explicit_bounds: &[f64],
) -> Result<(), OtelRecordError> {
    if bucket_counts.len() != explicit_bounds.len() + 1 {
        return Err(OtelRecordError::new(
            "bucket_counts",
            "OTEL histogram bucket_counts length must equal explicit_bounds length plus one",
        ));
    }
    Ok(())
}

fn list_value(array: &ArrayRef, row: usize) -> OtelValueResult<Option<ArrayRef>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::List(_) => match array.as_any().downcast_ref::<ListArray>() {
            Some(array) => Ok(Some(array.value(row))),
            None => Err(Report::new(OtelValueError::InvalidArrayRepresentation)),
        },
        DataType::FixedSizeList(_, _) => {
            match array.as_any().downcast_ref::<FixedSizeListArray>() {
                Some(array) => Ok(Some(array.value(row))),
                None => Err(Report::new(OtelValueError::InvalidArrayRepresentation)),
            }
        }
        ty => Err(Report::new(OtelValueError::ExpectedList {
            actual: ty.clone(),
        })),
    }
}

fn integer_as_i64(array: &ArrayRef, row: usize) -> OtelValueResult<i64> {
    match array.data_type() {
        DataType::UInt8 => Ok(array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::Int8 => Ok(array
            .as_any()
            .downcast_ref::<Int8Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::UInt16 => Ok(array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::UInt32 => Ok(array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::UInt64 => i64::try_from(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )
        .map_err(|_| Report::new(OtelValueError::SignedIntegerRange)),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)),
        ty => Err(Report::new(OtelValueError::ExpectedInteger {
            actual: ty.clone(),
        })),
    }
}

fn integer_as_u64(array: &ArrayRef, row: usize) -> OtelValueResult<u64> {
    match array.data_type() {
        DataType::UInt8 => Ok(array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::UInt16 => Ok(array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::UInt32 => Ok(array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::UInt64 => Ok(array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)),
        DataType::Int8 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )
        .map_err(|_| Report::new(OtelValueError::NegativeUnsigned)),
        DataType::Int16 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )
        .map_err(|_| Report::new(OtelValueError::NegativeUnsigned)),
        DataType::Int32 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )
        .map_err(|_| Report::new(OtelValueError::NegativeUnsigned)),
        DataType::Int64 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )
        .map_err(|_| Report::new(OtelValueError::NegativeUnsigned)),
        ty => Err(Report::new(OtelValueError::ExpectedInteger {
            actual: ty.clone(),
        })),
    }
}

fn numeric_as_f64(array: &ArrayRef, row: usize) -> OtelValueResult<f64> {
    match array.data_type() {
        DataType::Float32 => Ok(array
            .as_any()
            .downcast_ref::<Float32Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .into()),
        DataType::Float64 => Ok(array
            .as_any()
            .downcast_ref::<Float64Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)),
        DataType::UInt8 => Ok(f64::from(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )),
        DataType::Int8 => Ok(f64::from(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )),
        DataType::UInt16 => Ok(f64::from(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )),
        DataType::Int16 => Ok(f64::from(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )),
        DataType::UInt32 => Ok(f64::from(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )),
        DataType::Int32 => Ok(f64::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )),
        DataType::UInt64 => Ok(array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .approx_into()),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .verified(
                "the match arm above narrowed this array's data type, which fixes its concrete \
                 Arrow array type",
            )
            .value(row)
            .approx_into()),
        ty => Err(Report::new(OtelValueError::ExpectedNumeric {
            actual: ty.clone(),
        })),
    }
}

fn number_value_at(array: &ArrayRef, row: usize) -> OtelValueResult<number_data_point::Value> {
    match array.data_type() {
        DataType::Float32 => Ok(number_data_point::Value::AsDouble(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row)
                .into(),
        )),
        DataType::Float64 => Ok(number_data_point::Value::AsDouble(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        )),
        ty if OtelSink::is_integer_type(ty) => {
            integer_as_i64(array, row).map(number_data_point::Value::AsInt)
        }
        ty => Err(Report::new(OtelValueError::ExpectedMetricNumeric {
            actual: ty.clone(),
        })),
    }
}

fn any_value_at(array: &ArrayRef, row: usize) -> OtelValueResult<Option<AnyValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Utf8 => any_value::Value::StringValue(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row)
                .to_string(),
        ),
        DataType::Boolean => any_value::Value::BoolValue(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        ),
        DataType::Float32 => any_value::Value::DoubleValue(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row)
                .into(),
        ),
        DataType::Float64 => any_value::Value::DoubleValue(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .verified(
                    "the match arm above narrowed this array's data type, which fixes its \
                     concrete Arrow array type",
                )
                .value(row),
        ),
        ty if OtelSink::is_integer_type(ty) => {
            any_value::Value::IntValue(integer_as_i64(array, row)?)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Report::new(OtelValueError::InvalidDatetimeRepresentation))?
                .value(row);
            any_value::Value::StringValue(
                Timestamp::from_unix_nanos(nanos).as_datetime().to_rfc3339(),
            )
        }
        DataType::List(_) | DataType::FixedSizeList(_, _) => {
            let values = list_value(array, row)?.verified(
                "any_value_at returns early for a null row, so this list value is present",
            );
            let mut converted = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                converted.push(
                    any_value_at(&values, index)?
                        .ok_or_else(|| Report::new(OtelValueError::NullAttributeArrayElement))?,
                );
            }
            any_value::Value::ArrayValue(ArrayValue { values: converted })
        }
        ty => {
            return Err(Report::new(OtelValueError::UnsupportedAttributeType {
                actual: ty.clone(),
            }));
        }
    };
    Ok(Some(AnyValue { value: Some(value) }))
}

#[cfg(test)]
mod prepared_request_tests;

#[cfg(test)]
mod tests {
    use nervix_dns::{DnsConfiguration, NameServers};
    use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};

    use super::*;

    fn config(entries: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
        entries
            .iter()
            .map(|(key, value)| ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect()
    }

    #[test]
    fn config_requires_explicit_protocol_and_rejects_unknown_keys() {
        let missing_protocol =
            OtelClientSettings::parse(&config(&[("endpoint", "http://127.0.0.1:4317")]))
                .expect_err("protocol must be explicit");
        assert!(format!("{missing_protocol:?}").contains("protocol"));

        let unknown = OtelClientSettings::parse(&config(&[
            ("endpoint", "http://127.0.0.1:4317"),
            ("protocol", "grpc"),
            ("future", "value"),
        ]))
        .expect_err("unknown config keys must be rejected");
        assert!(format!("{unknown:?}").contains("unsupported"));
    }

    #[test]
    fn protobuf_request_members_preserve_signal_values_resource_scope_and_order() {
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("checkout".to_string())),
                }),
            }],
            ..Resource::default()
        };
        let scope = InstrumentationScope {
            name: "nervix/test".to_string(),
            ..InstrumentationScope::default()
        };
        let logs = OtelExportRequest::Logs(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource.clone()),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope.clone()),
                    log_records: [(11, "first"), (22, "second"), (33, "third")]
                        .into_iter()
                        .map(|(time_unix_nano, value)| LogRecord {
                            time_unix_nano,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(value.to_string())),
                            }),
                            ..LogRecord::default()
                        })
                        .collect(),
                    ..ScopeLogs::default()
                }],
                ..ResourceLogs::default()
            }],
        });
        let OtelExportRequest::Logs(logs) = logs.members(1..3) else {
            panic!("a log request must stay a log request");
        };
        let encoded = logs.encode_to_vec();
        assert_eq!(logs.encoded_len(), encoded.len());
        let decoded = ExportLogsServiceRequest::decode(encoded.as_slice())
            .assured("prost decodes the request bytes it just encoded");
        let resource_logs = &decoded.resource_logs[0];
        assert_eq!(resource_logs.resource.as_ref(), Some(&resource));
        let scope_logs = &resource_logs.scope_logs[0];
        assert_eq!(scope_logs.scope.as_ref(), Some(&scope));
        assert_eq!(scope_logs.log_records.len(), 2);
        assert_eq!(scope_logs.log_records[0].time_unix_nano, 22);
        assert_eq!(scope_logs.log_records[1].time_unix_nano, 33);
        assert_eq!(
            scope_logs.log_records[0]
                .body
                .as_ref()
                .and_then(|body| body.value.as_ref()),
            Some(&any_value::Value::StringValue("second".to_string()))
        );
        assert_eq!(
            scope_logs.log_records[1]
                .body
                .as_ref()
                .and_then(|body| body.value.as_ref()),
            Some(&any_value::Value::StringValue("third".to_string()))
        );

        let traces = OtelExportRequest::Traces(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource.clone()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(scope.clone()),
                    spans: [(11, "first"), (22, "second"), (33, "third")]
                        .into_iter()
                        .map(|(start_time_unix_nano, name)| Span {
                            name: name.to_string(),
                            start_time_unix_nano,
                            ..Span::default()
                        })
                        .collect(),
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        });
        let OtelExportRequest::Traces(traces) = traces.members(1..3) else {
            panic!("a trace request must stay a trace request");
        };
        let encoded = traces.encode_to_vec();
        assert_eq!(traces.encoded_len(), encoded.len());
        let decoded = ExportTraceServiceRequest::decode(encoded.as_slice())
            .assured("prost decodes the request bytes it just encoded");
        let resource_spans = &decoded.resource_spans[0];
        assert_eq!(resource_spans.resource.as_ref(), Some(&resource));
        let scope_spans = &resource_spans.scope_spans[0];
        assert_eq!(scope_spans.scope.as_ref(), Some(&scope));
        assert_eq!(scope_spans.spans[0].start_time_unix_nano, 22);
        assert_eq!(scope_spans.spans[1].start_time_unix_nano, 33);
        assert_eq!(
            scope_spans
                .spans
                .iter()
                .map(|span| span.name.as_str())
                .collect::<Vec<_>>(),
            vec!["second", "third"]
        );
    }

    #[test]
    fn protobuf_metric_request_members_keep_each_supported_point_container() {
        let number_points = [1, 2, 3]
            .into_iter()
            .map(|time_unix_nano| NumberDataPoint {
                time_unix_nano,
                ..NumberDataPoint::default()
            })
            .collect::<Vec<_>>();
        let histogram_points = [1, 2, 3]
            .into_iter()
            .map(|time_unix_nano| HistogramDataPoint {
                time_unix_nano,
                ..HistogramDataPoint::default()
            })
            .collect::<Vec<_>>();
        let data = [
            metric::Data::Gauge(Gauge {
                data_points: number_points.clone(),
            }),
            metric::Data::Sum(Sum {
                data_points: number_points,
                aggregation_temporality: i32::from(AggregationTemporality::Delta),
                is_monotonic: true,
            }),
            metric::Data::Histogram(Histogram {
                data_points: histogram_points,
                aggregation_temporality: i32::from(AggregationTemporality::Cumulative),
            }),
        ];
        for data in data {
            let request = OtelExportRequest::Metrics(ExportMetricsServiceRequest {
                resource_metrics: vec![ResourceMetrics {
                    resource: Some(Resource::default()),
                    scope_metrics: vec![ScopeMetrics {
                        scope: Some(InstrumentationScope::default()),
                        metrics: vec![Metric {
                            name: "nervix.test".to_string(),
                            data: Some(data),
                            ..Metric::default()
                        }],
                        ..ScopeMetrics::default()
                    }],
                    ..ResourceMetrics::default()
                }],
            });
            let OtelExportRequest::Metrics(request) = request.members(1..3) else {
                panic!("a metric request must stay a metric request");
            };
            let encoded = request.encode_to_vec();
            assert_eq!(request.encoded_len(), encoded.len());
            let decoded = ExportMetricsServiceRequest::decode(encoded.as_slice())
                .assured("prost decodes the request bytes it just encoded");
            let metric = &decoded.resource_metrics[0].scope_metrics[0].metrics[0];
            assert_eq!(metric.name, "nervix.test");
            let point_times = match metric
                .data
                .as_ref()
                .assured("the test constructed one metric with data and protobuf preserved it")
            {
                metric::Data::Gauge(gauge) => gauge
                    .data_points
                    .iter()
                    .map(|point| point.time_unix_nano)
                    .collect::<Vec<_>>(),
                metric::Data::Sum(sum) => sum
                    .data_points
                    .iter()
                    .map(|point| point.time_unix_nano)
                    .collect::<Vec<_>>(),
                metric::Data::Histogram(histogram) => histogram
                    .data_points
                    .iter()
                    .map(|point| point.time_unix_nano)
                    .collect::<Vec<_>>(),
                _ => panic!("the test only constructs supported metric point containers"),
            };
            assert_eq!(point_times, vec![2, 3]);
        }
    }

    #[tokio::test]
    async fn grpc_transport_initialization_does_not_require_a_reachable_endpoint()
    -> Result<(), Report<nervix_dns::DnsConfigurationError>> {
        let dns = nervix_dns::DnsResolver::load(nervix_dns::DnsConfiguration::system()).await?;
        let transport = OtelSink::transport_from_config(
            &config(&[
                ("endpoint", "http://127.0.0.1:0"),
                ("protocol", "grpc"),
                ("timeout_ms", "1"),
            ]),
            &dns,
        )
        .unwrap_or_else(|error| {
            panic!("an unavailable endpoint must initialize for publish-time retry: {error:?}")
        });
        let outcome = transport.export(OtelExportService::Logs, Vec::new()).await;
        let OtelTransportOutcome::Failed(error) = outcome else {
            panic!("an unavailable endpoint must fail as infrastructure");
        };
        assert_eq!(
            error.current_context(),
            &SinkPublishError::Publish { sink: OTEL },
            "an unreachable receiver is a failure the host retries"
        );
        Ok(())
    }

    #[tokio::test]
    async fn lazy_grpc_channel_resolves_only_when_an_export_needs_a_connection() {
        let authority = DnsAuthority::start_on_loopback()
            .await
            .assured("a loopback DNS port is available");
        let name = "otel-grpc.nervix.test";
        authority.set(
            name,
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::from_secs(1),
            },
        );
        let files = tempfile::tempdir().assured("a DNS fixture directory can be created");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        std::fs::write(
            &resolver_configuration,
            "search --\noptions ndots:1 timeout:1 attempts:1\n",
        )
        .assured("the fixture resolver configuration can be written");
        std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
        let dns = nervix_dns::DnsResolver::load(DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        })
        .await
        .assured("the fixture DNS configuration is valid");
        let transport = OtelSink::transport_from_config(
            &config(&[
                ("endpoint", "http://otel-grpc.nervix.test:4317"),
                ("protocol", "grpc"),
                ("timeout_ms", "2000"),
            ]),
            &dns,
        )
        .assured("a named endpoint constructs a lazy gRPC channel");
        tokio::task::yield_now().await;
        assert_eq!(authority.questions_for(name), 0);
        let outcome = transport
            .export(
                OtelExportService::Logs,
                ExportLogsServiceRequest {
                    resource_logs: Vec::new(),
                }
                .encode_to_vec(),
            )
            .await;
        assert!(matches!(outcome, OtelTransportOutcome::Failed(_)));
        assert!(authority.questions_for(name) > 0);
    }

    #[test]
    fn validates_hex_ids_severity_and_histogram_lengths() {
        assert_eq!(
            parse_hex_id("00112233445566778899aabbccddeeff", 16, "trace_id")
                .expect("valid trace ID")
                .len(),
            16
        );
        assert!(parse_hex_id("not-hex", 16, "trace_id").is_err());
        assert!(parse_hex_id("0000000000000000", 8, "span_id").is_err());
        assert_eq!(parse_severity_number(24).expect("valid severity"), 24);
        assert!(parse_severity_number(25).is_err());
        validate_histogram_buckets(&[1, 2, 3], &[0.5, 1.0]).expect("matching histogram shapes");
        assert!(validate_histogram_buckets(&[1, 2], &[0.5, 1.0]).is_err());

        let unsigned: ArrayRef = StdArc::new(UInt64Array::from(vec![u64::MAX]));
        assert_eq!(
            numeric_as_f64(&unsigned, 0).expect("histogram numerics accept the U64 range"),
            u64::MAX.approx_into::<f64>()
        );
    }

    #[test]
    fn typed_otel_value_errors_classify_invalid_exact_types_and_ranges() {
        let error = OtelLiteral::Array(vec![OtelLiteral::Null])
            .any_value()
            .expect_err("resource arrays cannot contain nulls");
        assert!(matches!(
            error.current_context(),
            OtelValueError::NullResourceArrayElement
        ));

        let unsigned: ArrayRef = StdArc::new(UInt64Array::from(vec![u64::MAX]));
        let error = integer_as_i64(&unsigned, 0)
            .expect_err("the OTLP signed integer range must be enforced");
        assert!(matches!(
            error.current_context(),
            OtelValueError::SignedIntegerRange
        ));

        let negative_i8: ArrayRef = StdArc::new(Int8Array::from(vec![-1]));
        let negative_i16: ArrayRef = StdArc::new(Int16Array::from(vec![-1]));
        let negative_i32: ArrayRef = StdArc::new(Int32Array::from(vec![-1]));
        let negative_i64: ArrayRef = StdArc::new(Int64Array::from(vec![-1]));
        for array in [&negative_i8, &negative_i16, &negative_i32, &negative_i64] {
            let error = integer_as_u64(array, 0)
                .expect_err("negative integers cannot become OTLP unsigned values");
            assert!(matches!(
                error.current_context(),
                OtelValueError::NegativeUnsigned
            ));
        }

        let boolean: ArrayRef = StdArc::new(BooleanArray::from(vec![true]));
        for error in [
            integer_as_i64(&boolean, 0).expect_err("a boolean is not an integer"),
            integer_as_u64(&boolean, 0).expect_err("a boolean is not an unsigned integer"),
        ] {
            assert!(matches!(
                error.current_context(),
                OtelValueError::ExpectedInteger { .. }
            ));
        }
        let numeric = numeric_as_f64(&boolean, 0).expect_err("a boolean is not numeric");
        assert!(matches!(
            numeric.current_context(),
            OtelValueError::ExpectedNumeric { .. }
        ));
        let metric = number_value_at(&boolean, 0).expect_err("a boolean is not a metric number");
        assert!(matches!(
            metric.current_context(),
            OtelValueError::ExpectedMetricNumeric { .. }
        ));
        let list = list_value(&boolean, 0).expect_err("a boolean is not a list");
        assert!(matches!(
            list.current_context(),
            OtelValueError::ExpectedList { .. }
        ));

        let nullable_list: ArrayRef = StdArc::new(ListArray::from_iter_primitive::<
            arrow_array::types::Int64Type,
            _,
            _,
        >([Some(vec![None])]));
        let null_element = any_value_at(&nullable_list, 0)
            .expect_err("OTLP attribute arrays cannot contain null elements");
        assert!(matches!(
            null_element.current_context(),
            OtelValueError::NullAttributeArrayElement
        ));

        let binary: ArrayRef = StdArc::new(arrow_array::BinaryArray::from(vec![Some(
            b"opaque".as_slice(),
        )]));
        let unsupported =
            any_value_at(&binary, 0).expect_err("binary values are not an OTLP attribute type");
        assert!(matches!(
            unsupported.current_context(),
            OtelValueError::UnsupportedAttributeType { .. }
        ));
    }

    #[test]
    fn classifies_otlp_statuses_and_extracts_http_retry_after() {
        assert!(matches!(
            OtelTransport::grpc_failure(GrpcStatus::invalid_argument("bad record")),
            OtelTransportOutcome::Rejected(_)
        ));
        assert!(matches!(
            OtelTransport::grpc_failure(GrpcStatus::unavailable("retry")),
            OtelTransportOutcome::Failed(_)
        ));
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-24T12:00:00Z")
            .expect("fixed time")
            .with_timezone(&chrono::Utc);
        assert_eq!(
            OtelTransport::http_retry_after(Some("3.5"), now),
            Some(Duration::from_millis(3500))
        );
    }

    #[test]
    fn observation_timestamp_samples_actual_utc() {
        let before = OtelSink::timestamp_to_unix_nano(
            nervix_connector::physical_time::actual_utc_now().unix_nanos(),
            "before",
        )
        .expect("actual UTC must be after the Unix epoch");
        let observed = OtelSink::observation_time_unix_nano()
            .expect("actual UTC observation time must be after the Unix epoch");
        let after = OtelSink::timestamp_to_unix_nano(
            nervix_connector::physical_time::actual_utc_now().unix_nanos(),
            "after",
        )
        .expect("actual UTC must be after the Unix epoch");

        assert!(before <= observed);
        assert!(observed <= after);
        assert!(observed > 978_307_200_000_000_000);
    }
}
