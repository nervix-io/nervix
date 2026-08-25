use std::{io::Write, str::FromStr};

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, StringArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};
use flate2::{Compression as GzipLevel, write::GzEncoder};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{
            ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
            logs_service_client::LogsServiceClient,
        },
        metrics::v1::{
            ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
            metrics_service_client::MetricsServiceClient,
        },
        trace::v1::{
            ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
            trace_service_client::TraceServiceClient,
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
use otel_prost::Message as OtelMessage;
use otel_tonic::{
    Code as GrpcCode, Request as GrpcRequest, Status as GrpcStatus,
    codec::CompressionEncoding,
    metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};
use otel_tonic_types::StatusExt;
use reqwest::{
    Client as HttpClient, StatusCode,
    header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER},
};

use super::*;

const OTLP_PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

pub(in crate::runtime) struct OtelEmitter {
    client: Option<OtelClient>,
    program: Option<CompiledSqlValuesProgram>,
    resource: Option<Resource>,
    scope: Option<InstrumentationScope>,
}

pub(super) struct OtelEmitterInit<'a> {
    pub(super) client: &'a CreateClientOtel,
    pub(super) resolved: Option<&'a ResolvedClientConfig>,
    pub(super) context: &'a EmitterSinkContext,
    pub(super) signal: &'a OtelSignal,
    pub(super) values: &'a [OtelValueMapping],
    pub(super) attributes: &'a [OtelValueMapping],
    pub(super) resource: &'a [OtelValueMapping],
    pub(super) scope: Option<&'a OtelScope>,
    pub(super) input_schema: StdArc<arrow_schema::Schema>,
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
    fault_injector: Arc<OtelClientFaultInjector>,
    emitter: Identifier,
}

enum OtelExportRequest {
    Logs(ExportLogsServiceRequest),
    Traces(ExportTraceServiceRequest),
    Metrics(ExportMetricsServiceRequest),
}

struct OtelPartialSuccess {
    rejected: i64,
    error_message: String,
}

enum OtelTransportOutcome {
    Accepted(Option<OtelPartialSuccess>),
    Rejected(String),
    Failed(Report<EmitterRuntimeError>),
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

    fn structured(self) -> StructuredMessageError {
        structured_message_error(
            MessageErrorCode::Validation,
            self.reason,
            MessageErrorOperation::Values,
            None,
            [FieldPath::new(format!("otel.{}", self.key))],
        )
    }
}

impl OtelClientSettings {
    fn parse(config: &[ClientConfigEntry]) -> EmitterRuntimeResult<Self> {
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
        let mut seen = HashSet::default();
        for entry in config {
            let key = entry.key.to_ascii_lowercase();
            if !ALLOWED_KEYS.contains(&key.as_str()) {
                return Err(emitter_config_error(format!(
                    "unsupported OTEL client config key '{}'",
                    entry.key
                )));
            }
            if !seen.insert(key) {
                return Err(emitter_config_error(format!(
                    "duplicate OTEL client config key '{}'",
                    entry.key
                )));
            }
        }

        let endpoint = emitter_config_value(config, "endpoint", || {
            "missing OTEL client config key 'endpoint'".to_string()
        })?;
        let endpoint = url::Url::parse(&endpoint)
            .map_err(|error| emitter_config_error(format!("invalid OTEL endpoint: {error}")))?;
        if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
            return Err(emitter_config_error(format!(
                "OTEL endpoint must use http or https, found '{}'",
                endpoint.scheme()
            )));
        }
        if endpoint.host_str().is_none() {
            return Err(emitter_config_error("OTEL endpoint must include a host"));
        }

        let protocol = emitter_config_value(config, "protocol", || {
            "missing OTEL client config key 'protocol'".to_string()
        })?;
        let protocol = match protocol.as_str() {
            "grpc" => OtelProtocol::Grpc,
            "http/protobuf" => OtelProtocol::HttpProtobuf,
            _ => {
                return Err(emitter_config_error(format!(
                    "invalid OTEL protocol '{protocol}'; expected 'grpc' or 'http/protobuf'"
                )));
            }
        };

        let compression = match optional_client_config_value(config, "compression") {
            None => OtelCompression::None,
            Some("gzip") => OtelCompression::Gzip,
            Some(compression) => {
                return Err(emitter_config_error(format!(
                    "invalid OTEL compression '{compression}'; expected 'gzip'"
                )));
            }
        };
        let timeout = optional_client_config_value(config, "timeout_ms")
            .map(|raw| {
                let millis = raw.parse::<u64>().map_err(|_| {
                    emitter_config_error(format!("invalid OTEL timeout_ms '{raw}'"))
                })?;
                if millis == 0 {
                    return Err(emitter_config_error(
                        "OTEL timeout_ms must be greater than zero",
                    ));
                }
                Ok(Duration::from_millis(millis))
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

    fn parse_headers(raw: &str) -> EmitterRuntimeResult<Vec<(String, String)>> {
        if raw.trim().is_empty() {
            return Ok(Vec::new());
        }
        raw.split(',')
            .map(|entry| {
                let (key, value) = entry.split_once('=').ok_or_else(|| {
                    emitter_config_error("invalid OTEL headers entry; expected k=v")
                })?;
                let key = key.trim().to_ascii_lowercase();
                let value = value.trim().to_string();
                if key.is_empty() {
                    return Err(emitter_config_error(
                        "OTEL headers entries require a non-empty key",
                    ));
                }
                Ok((key, value))
            })
            .collect()
    }
}

impl OtelEmitter {
    pub(in crate::runtime) fn new(init: OtelEmitterInit<'_>) -> Self {
        let OtelEmitterInit {
            client,
            resolved,
            context,
            signal,
            values,
            attributes,
            resource,
            scope,
            input_schema,
        } = init;
        let config = resolved
            .map(|config| config.entries.as_slice())
            .unwrap_or(client.config.as_slice());
        let client = match Self::transport_from_config(config) {
            Ok(transport) => Some(OtelClient {
                transport,
                fault_injector: context.runtime.otel_client_faults.clone(),
                emitter: context.emitter.clone(),
            }),
            Err(error) => {
                context.report_init_error("otel", &emitter_error_message(&error));
                None
            }
        };

        let mut mappings = Vec::with_capacity(values.len() + attributes.len());
        mappings.extend_from_slice(values);
        mappings.extend_from_slice(attributes);
        let program = match compile_sql_values_program(
            "OTEL",
            "otel",
            &context.domain,
            &context.emitter,
            &mappings,
            input_schema,
            context.udfs.as_ref(),
        ) {
            Ok(program) => match Self::validate_program_types(signal, values, attributes, &program)
            {
                Ok(()) => Some(program),
                Err(reason) => {
                    context.report_init_error("otel", &reason);
                    None
                }
            },
            Err(error) => {
                context.report_init_error("otel", &error.to_string());
                None
            }
        };
        let resource = match Self::resource_from_mappings(resource) {
            Ok(resource) => Some(resource),
            Err(reason) => {
                context.report_init_error("otel", &reason);
                None
            }
        };
        let scope = scope.map(|scope| InstrumentationScope {
            name: scope.name.clone(),
            version: scope.version.clone().unwrap_or_default(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        });

        Self {
            client,
            program,
            resource,
            scope,
        }
    }

    fn transport_from_config(config: &[ClientConfigEntry]) -> EmitterRuntimeResult<OtelTransport> {
        let settings = OtelClientSettings::parse(config)?;
        match settings.protocol {
            OtelProtocol::Grpc => {
                let mut endpoint =
                    Endpoint::from_shared(settings.endpoint.to_string()).map_err(|error| {
                        emitter_config_error(format!("invalid OTEL endpoint: {error}"))
                    })?;
                if let Some(timeout) = settings.timeout {
                    endpoint = endpoint.timeout(timeout).connect_timeout(timeout);
                }
                let tls = client_tls_paths(config);
                if settings.endpoint.scheme() == "https" {
                    let host = settings
                        .endpoint
                        .host_str()
                        .expect("validated OTEL URL must retain its host");
                    let mut tls_config = ClientTlsConfig::new()
                        .with_webpki_roots()
                        .domain_name(host.to_string());
                    if let Some(ca_file) = tls.ca_file.as_ref() {
                        let ca = read_tls_file(ca_file, "OTEL TLS CA certificate")
                            .map_err(emitter_config_error)?;
                        tls_config = tls_config.ca_certificate(Certificate::from_pem(ca));
                    }
                    match (&tls.cert_file, &tls.key_file) {
                        (Some(cert_file), Some(key_file)) => {
                            let cert = read_tls_file(cert_file, "OTEL TLS certificate")
                                .map_err(emitter_config_error)?;
                            let key = read_tls_file(key_file, "OTEL TLS private key")
                                .map_err(emitter_config_error)?;
                            tls_config = tls_config.identity(Identity::from_pem(cert, key));
                        }
                        (None, None) => {}
                        _ => {
                            return Err(emitter_config_error(
                                "OTEL TLS client authentication requires both 'tls_cert_file' and \
                                 'tls_key_file'",
                            ));
                        }
                    }
                    endpoint = endpoint.tls_config(tls_config).map_err(|error| {
                        emitter_config_error(format!("invalid OTEL TLS configuration: {error}"))
                    })?;
                } else if !tls.is_empty() {
                    return Err(emitter_config_error(
                        "OTEL TLS files require an https endpoint",
                    ));
                }
                let channel = endpoint.connect_lazy();
                let metadata = Self::grpc_metadata(&settings.headers)?;
                Ok(OtelTransport::Grpc {
                    channel,
                    metadata,
                    compression: settings.compression,
                })
            }
            OtelProtocol::HttpProtobuf => {
                let client = HttpClientConfig::new(config, "OTEL")
                    .build()
                    .map_err(emitter_config_error)?;
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

    fn grpc_metadata(headers: &[(String, String)]) -> EmitterRuntimeResult<MetadataMap> {
        let mut metadata = MetadataMap::new();
        for (key, value) in headers {
            let key = MetadataKey::<Ascii>::from_bytes(key.as_bytes()).map_err(|error| {
                emitter_config_error(format!("invalid OTEL gRPC header name '{key}': {error}"))
            })?;
            let value = MetadataValue::<Ascii>::from_str(value).map_err(|error| {
                emitter_config_error(format!("invalid OTEL gRPC header value: {error}"))
            })?;
            metadata.insert(key, value);
        }
        Ok(metadata)
    }

    fn http_headers(headers: &[(String, String)]) -> EmitterRuntimeResult<HeaderMap> {
        let mut parsed = HeaderMap::new();
        for (key, value) in headers {
            let name = HeaderName::from_bytes(key.as_bytes()).map_err(|error| {
                emitter_config_error(format!("invalid OTEL HTTP header name '{key}': {error}"))
            })?;
            let value = HeaderValue::from_str(value).map_err(|error| {
                emitter_config_error(format!("invalid OTEL HTTP header value: {error}"))
            })?;
            parsed.insert(name, value);
        }
        Ok(parsed)
    }

    fn validate_program_types(
        signal: &OtelSignal,
        values: &[OtelValueMapping],
        attributes: &[OtelValueMapping],
        program: &CompiledSqlValuesProgram,
    ) -> Result<(), String> {
        for (index, mapping) in values.iter().enumerate() {
            let field = program.program.output_schema.field(index);
            let (valid, expected) =
                Self::valid_value_type(signal, &mapping.column, field.data_type());
            if !valid {
                return Err(format!(
                    "OTEL {} VALUES key '{}' requires {expected}, found {}",
                    Self::signal_label(signal),
                    mapping.column,
                    field.data_type()
                ));
            }
        }
        for (offset, mapping) in attributes.iter().enumerate() {
            let field = program.program.output_schema.field(values.len() + offset);
            if !Self::valid_attribute_type(field.data_type()) {
                return Err(format!(
                    "OTEL ATTRIBUTE '{}' has unsupported exact type {}",
                    mapping.column,
                    field.data_type()
                ));
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

    fn resource_from_mappings(mappings: &[OtelValueMapping]) -> Result<Resource, String> {
        let mut attributes = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            if let Some(value) = Self::literal_any_value(&mapping.expression)? {
                attributes.push(KeyValue {
                    key: mapping.column.clone(),
                    value: Some(value),
                });
            }
        }
        Ok(Resource {
            attributes,
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        })
    }

    fn literal_any_value(
        expression: &nervix_models::Expression,
    ) -> Result<Option<AnyValue>, String> {
        let value = match expression {
            nervix_models::Expression::Literal(ModelLiteral::I64(value)) => {
                Some(any_value::Value::IntValue(*value))
            }
            nervix_models::Expression::Literal(ModelLiteral::F64(value)) => {
                Some(any_value::Value::DoubleValue(value.value()))
            }
            nervix_models::Expression::Literal(ModelLiteral::Bool(value)) => {
                Some(any_value::Value::BoolValue(*value))
            }
            nervix_models::Expression::Literal(ModelLiteral::String(value)) => {
                Some(any_value::Value::StringValue(value.clone()))
            }
            nervix_models::Expression::Literal(ModelLiteral::Null) => return Ok(None),
            nervix_models::Expression::Array(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    let value = Self::literal_any_value(item)?.ok_or_else(|| {
                        "OTEL RESOURCE arrays do not support NULL elements".to_string()
                    })?;
                    values.push(value);
                }
                Some(any_value::Value::ArrayValue(ArrayValue { values }))
            }
            _ => {
                return Err(
                    "OTEL RESOURCE values must contain only literal values or literal arrays"
                        .to_string(),
                );
            }
        };
        Ok(Some(AnyValue { value }))
    }

    pub(super) async fn publish_pending_rows(
        &self,
        batch_index: usize,
        signal: &OtelSignal,
        values: &[OtelValueMapping],
        attributes: &[OtelValueMapping],
        batch: &RelayRecordBatch,
        pending_rows: &[usize],
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        if pending_rows.is_empty() {
            return outcome;
        }
        let (Some(client), Some(program), Some(resource)) = (
            self.client.as_ref(),
            self.program.as_ref(),
            self.resource.as_ref(),
        ) else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized OTEL sink client"),
            );
            return outcome;
        };
        let output = match execute_sql_values_program(program, batch, current_timestamp()).await {
            Ok(output) => output,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let mapped = OtelMappedBatch {
            output: &output,
            values,
            attributes,
        };
        let observed_time =
            match Self::timestamp_to_unix_nano(current_timestamp().unix_nanos(), "observed_time") {
                Ok(value) => value,
                Err(error) => {
                    outcome.fail(emitter_publish_error(error.reason));
                    return outcome;
                }
            };
        let mut positions = Vec::with_capacity(pending_rows.len());
        let request = match signal {
            OtelSignal::Logs => {
                let mut records = Vec::with_capacity(pending_rows.len());
                for row in pending_rows {
                    tokio::task::consume_budget().await;
                    if let Some(error) = Self::side_error(program, &output, *row) {
                        outcome.reject_structured((batch_index, *row), error);
                        continue;
                    }
                    match mapped.log_record(*row, observed_time) {
                        Ok(record) => {
                            records.push(record);
                            positions.push((batch_index, *row));
                        }
                        Err(error) => {
                            outcome.reject_structured((batch_index, *row), error.structured())
                        }
                    }
                }
                OtelExportRequest::Logs(ExportLogsServiceRequest {
                    resource_logs: vec![ResourceLogs {
                        resource: Some(resource.clone()),
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
                let mut spans = Vec::with_capacity(pending_rows.len());
                for row in pending_rows {
                    tokio::task::consume_budget().await;
                    if let Some(error) = Self::side_error(program, &output, *row) {
                        outcome.reject_structured((batch_index, *row), error);
                        continue;
                    }
                    match mapped.span(*row) {
                        Ok(span) => {
                            spans.push(span);
                            positions.push((batch_index, *row));
                        }
                        Err(error) => {
                            outcome.reject_structured((batch_index, *row), error.structured())
                        }
                    }
                }
                OtelExportRequest::Traces(ExportTraceServiceRequest {
                    resource_spans: vec![ResourceSpans {
                        resource: Some(resource.clone()),
                        scope_spans: vec![ScopeSpans {
                            scope: self.scope.clone(),
                            spans,
                            schema_url: String::new(),
                        }],
                        schema_url: String::new(),
                    }],
                })
            }
            OtelSignal::Metric(metric_model) => {
                let mut metric_rows = Vec::with_capacity(pending_rows.len());
                for row in pending_rows {
                    tokio::task::consume_budget().await;
                    if let Some(error) = Self::side_error(program, &output, *row) {
                        outcome.reject_structured((batch_index, *row), error);
                    } else {
                        metric_rows.push(*row);
                    }
                }
                let metric = mapped
                    .metric(
                        metric_model,
                        &metric_rows,
                        batch_index,
                        &mut positions,
                        &mut outcome,
                    )
                    .await;
                OtelExportRequest::Metrics(ExportMetricsServiceRequest {
                    resource_metrics: vec![ResourceMetrics {
                        resource: Some(resource.clone()),
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
            return outcome;
        }

        match client.export(request).await {
            OtelTransportOutcome::Accepted(partial_success) => {
                if let Some(partial) = partial_success
                    && (partial.rejected != 0 || !partial.error_message.is_empty())
                {
                    warn!(
                        rejected_records = partial.rejected,
                        receiver_supplied_message = !partial.error_message.is_empty(),
                        "OTEL receiver returned partial_success; request records are acknowledged \
                         without retry"
                    );
                }
                for position in positions {
                    outcome.deliver(position);
                }
            }
            OtelTransportOutcome::Rejected(reason) => {
                for position in positions {
                    outcome.reject(position, reason.clone());
                }
            }
            OtelTransportOutcome::Failed(error) => outcome.fail(error),
        }
        outcome
    }

    fn side_error(
        program: &CompiledSqlValuesProgram,
        output: &VmTypedBatch,
        row: usize,
    ) -> Option<StructuredMessageError> {
        output.errors().get(row)?.first().map(|side_error| {
            program.structured_side_error(
                format!(
                    "OTEL VALUES side error {}: {} at {}",
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
                side_error.span,
            )
        })
    }

    fn timestamp_to_unix_nano(value: i64, key: &str) -> Result<u64, OtelRecordError> {
        u64::try_from(value).map_err(|_| {
            OtelRecordError::new(key, format!("OTEL {key} cannot be before the Unix epoch"))
        })
    }
}

impl OtelClient {
    async fn export(&self, request: OtelExportRequest) -> OtelTransportOutcome {
        if self.fault_injector.is_unavailable(&self.emitter) {
            let reason = match &self.transport {
                OtelTransport::Grpc { .. } => {
                    "OTEL client fault injector returned gRPC UNAVAILABLE"
                }
                OtelTransport::HttpProtobuf { .. } => {
                    "OTEL client fault injector returned HTTP 503 Service Unavailable"
                }
            };
            return OtelTransportOutcome::Failed(emitter_publish_error(reason));
        }
        self.transport.export(request).await
    }
}

impl OtelTransport {
    async fn export(&self, request: OtelExportRequest) -> OtelTransportOutcome {
        match self {
            Self::Grpc {
                channel,
                metadata,
                compression,
            } => Self::export_grpc(channel, metadata, *compression, request).await,
            Self::HttpProtobuf {
                client,
                endpoint,
                headers,
                compression,
            } => Self::export_http(client, endpoint, headers, *compression, request).await,
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
        request: OtelExportRequest,
    ) -> OtelTransportOutcome {
        let response = match request {
            OtelExportRequest::Logs(request) => {
                let mut client = LogsServiceClient::new(channel.clone());
                if compression == OtelCompression::Gzip {
                    client = client.send_compressed(CompressionEncoding::Gzip);
                }
                match client.export(Self::grpc_request(request, metadata)).await {
                    Ok(response) => {
                        return OtelTransportOutcome::Accepted(
                            response.into_inner().partial_success.map(Into::into),
                        );
                    }
                    Err(status) => status,
                }
            }
            OtelExportRequest::Traces(request) => {
                let mut client = TraceServiceClient::new(channel.clone());
                if compression == OtelCompression::Gzip {
                    client = client.send_compressed(CompressionEncoding::Gzip);
                }
                match client.export(Self::grpc_request(request, metadata)).await {
                    Ok(response) => {
                        return OtelTransportOutcome::Accepted(
                            response.into_inner().partial_success.map(Into::into),
                        );
                    }
                    Err(status) => status,
                }
            }
            OtelExportRequest::Metrics(request) => {
                let mut client = MetricsServiceClient::new(channel.clone());
                if compression == OtelCompression::Gzip {
                    client = client.send_compressed(CompressionEncoding::Gzip);
                }
                match client.export(Self::grpc_request(request, metadata)).await {
                    Ok(response) => {
                        return OtelTransportOutcome::Accepted(
                            response.into_inner().partial_success.map(Into::into),
                        );
                    }
                    Err(status) => status,
                }
            }
        };
        Self::grpc_failure(response)
    }

    fn grpc_failure(status: GrpcStatus) -> OtelTransportOutcome {
        if status.code() == GrpcCode::InvalidArgument {
            return OtelTransportOutcome::Rejected(
                "OTEL receiver rejected the request with gRPC INVALID_ARGUMENT".to_string(),
            );
        }
        if matches!(
            status.code(),
            GrpcCode::Unavailable | GrpcCode::ResourceExhausted
        ) {
            let message = format!("OTEL gRPC export failed with {}", status.code());
            let retry_delay = status
                .get_details_retry_info()
                .and_then(|info| info.retry_delay);
            return OtelTransportOutcome::Failed(match retry_delay {
                Some(delay) => emitter_publish_error_with_minimum_retry_delay(message, delay),
                None => emitter_publish_error(message),
            });
        }
        OtelTransportOutcome::Failed(emitter_config_error(format!(
            "OTEL gRPC export failed with non-retryable {}",
            status.code()
        )))
    }

    async fn export_http(
        client: &HttpClient,
        endpoint: &url::Url,
        headers: &HeaderMap,
        compression: OtelCompression,
        request: OtelExportRequest,
    ) -> OtelTransportOutcome {
        let (path, body, response_kind) = match request {
            OtelExportRequest::Logs(request) => {
                ("logs", request.encode_to_vec(), OtelHttpResponseKind::Logs)
            }
            OtelExportRequest::Traces(request) => (
                "traces",
                request.encode_to_vec(),
                OtelHttpResponseKind::Traces,
            ),
            OtelExportRequest::Metrics(request) => (
                "metrics",
                request.encode_to_vec(),
                OtelHttpResponseKind::Metrics,
            ),
        };
        let body = match Self::http_body(body, compression) {
            Ok(body) => body,
            Err(error) => return OtelTransportOutcome::Failed(error),
        };
        let mut url = endpoint.clone();
        let base_path = url.path().trim_end_matches('/');
        url.set_path(&format!("{base_path}/v1/{path}"));
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
                return OtelTransportOutcome::Failed(emitter_publish_error(format!(
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
                chrono::Utc::now(),
            );
            let message = format!("OTEL HTTP export returned status {status}");
            return OtelTransportOutcome::Failed(match delay {
                Some(delay) => emitter_publish_error_with_minimum_retry_delay(message, delay),
                None => emitter_publish_error(message),
            });
        }
        if !status.is_success() {
            return OtelTransportOutcome::Failed(emitter_config_error(format!(
                "OTEL HTTP export returned non-retryable status {status}"
            )));
        }
        let body = match response.bytes().await {
            Ok(body) => body,
            Err(error) => {
                return OtelTransportOutcome::Failed(emitter_publish_error(format!(
                    "failed to read OTEL HTTP response: {error}"
                )));
            }
        };
        match response_kind.decode(&body) {
            Ok(partial) => OtelTransportOutcome::Accepted(partial),
            Err(error) => OtelTransportOutcome::Failed(emitter_publish_error(format!(
                "failed to decode OTEL HTTP protobuf response: {error}"
            ))),
        }
    }

    fn http_body(body: Vec<u8>, compression: OtelCompression) -> EmitterRuntimeResult<Vec<u8>> {
        if compression == OtelCompression::None {
            return Ok(body);
        }
        let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::default());
        encoder.write_all(&body).map_err(|error| {
            emitter_report(
                EmitterRuntimeError::EncodeBatch,
                format!("failed to gzip OTEL HTTP request: {error}"),
            )
        })?;
        encoder.finish().map_err(|error| {
            emitter_report(
                EmitterRuntimeError::EncodeBatch,
                format!("failed to finish OTEL HTTP gzip request: {error}"),
            )
        })
    }

    fn http_retry_after(
        value: Option<&str>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<Duration> {
        let value = value?.trim();
        value
            .parse::<f64>()
            .ok()
            .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
            .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
            .or_else(|| {
                chrono::DateTime::parse_from_rfc2822(value)
                    .ok()
                    .and_then(|deadline| {
                        deadline
                            .with_timezone(&chrono::Utc)
                            .signed_duration_since(now)
                            .to_std()
                            .ok()
                    })
            })
    }
}

enum OtelHttpResponseKind {
    Logs,
    Traces,
    Metrics,
}

impl OtelHttpResponseKind {
    fn decode(&self, body: &[u8]) -> Result<Option<OtelPartialSuccess>, otel_prost::DecodeError> {
        match self {
            Self::Logs => Ok(ExportLogsServiceResponse::decode(body)?
                .partial_success
                .map(Into::into)),
            Self::Traces => Ok(ExportTraceServiceResponse::decode(body)?
                .partial_success
                .map(Into::into)),
            Self::Metrics => Ok(ExportMetricsServiceResponse::decode(body)?
                .partial_success
                .map(Into::into)),
        }
    }
}

impl From<ExportLogsPartialSuccess> for OtelPartialSuccess {
    fn from(value: ExportLogsPartialSuccess) -> Self {
        Self {
            rejected: value.rejected_log_records,
            error_message: value.error_message,
        }
    }
}

impl From<ExportTracePartialSuccess> for OtelPartialSuccess {
    fn from(value: ExportTracePartialSuccess) -> Self {
        Self {
            rejected: value.rejected_spans,
            error_message: value.error_message,
        }
    }
}

impl From<ExportMetricsPartialSuccess> for OtelPartialSuccess {
    fn from(value: ExportMetricsPartialSuccess) -> Self {
        Self {
            rejected: value.rejected_data_points,
            error_message: value.error_message,
        }
    }
}

struct OtelMappedBatch<'a> {
    output: &'a VmTypedBatch,
    values: &'a [OtelValueMapping],
    attributes: &'a [OtelValueMapping],
}

impl OtelMappedBatch<'_> {
    fn value_array(&self, key: &str) -> Result<Option<ArrayRef>, OtelRecordError> {
        let Some(index) = self.values.iter().position(|mapping| mapping.column == key) else {
            return Ok(None);
        };
        let array = self.output.columns().get(index).ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES output omitted key '{key}'"))
        })?;
        Ok(Some(array.to_array_ref()))
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
        OtelEmitter::timestamp_to_unix_nano(array.value(row), key).map(Some)
    }

    fn attributes(&self, row: usize) -> Result<Vec<KeyValue>, OtelRecordError> {
        let mut values = Vec::with_capacity(self.attributes.len());
        for (offset, mapping) in self.attributes.iter().enumerate() {
            let index = self.values.len() + offset;
            let array = self.output.columns().get(index).ok_or_else(|| {
                OtelRecordError::new(
                    mapping.column.clone(),
                    format!("OTEL ATTRIBUTES output omitted key '{}'", mapping.column),
                )
            })?;
            if let Some(value) = any_value_at(&array.to_array_ref(), row)
                .map_err(|reason| OtelRecordError::new(mapping.column.clone(), reason))?
            {
                values.push(KeyValue {
                    key: mapping.column.clone(),
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
            None => span::SpanKind::Unspecified as i32,
            Some("INTERNAL") => span::SpanKind::Internal as i32,
            Some("SERVER") => span::SpanKind::Server as i32,
            Some("CLIENT") => span::SpanKind::Client as i32,
            Some("PRODUCER") => span::SpanKind::Producer as i32,
            Some("CONSUMER") => span::SpanKind::Consumer as i32,
            Some(_) => {
                return Err(OtelRecordError::new(
                    "kind",
                    "OTEL span kind must be INTERNAL, SERVER, CLIENT, PRODUCER, or CONSUMER",
                ));
            }
        };
        let status_code = match self.optional_string("status_code", row)?.as_deref() {
            None => None,
            Some("UNSET") => Some(status::StatusCode::Unset as i32),
            Some("OK") => Some(status::StatusCode::Ok as i32),
            Some("ERROR") => Some(status::StatusCode::Error as i32),
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
                code: code.unwrap_or(status::StatusCode::Unset as i32),
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
        pending_rows: &[usize],
        batch_index: usize,
        positions: &mut Vec<BrokerRecordPosition>,
        outcome: &mut PerRecordPublishOutcome,
    ) -> Metric {
        let data = match &model.kind {
            OtelMetricKind::Gauge | OtelMetricKind::Sum { .. } => {
                let require_start_time = matches!(
                    model.kind,
                    OtelMetricKind::Sum {
                        temporality: OtelAggregationTemporality::Delta,
                        ..
                    }
                );
                let mut points = Vec::with_capacity(pending_rows.len());
                for row in pending_rows {
                    tokio::task::consume_budget().await;
                    match self.number_point(*row, require_start_time) {
                        Ok(point) => {
                            points.push(point);
                            positions.push((batch_index, *row));
                        }
                        Err(error) => {
                            outcome.reject_structured((batch_index, *row), error.structured())
                        }
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
                    OtelMetricKind::Histogram { .. } => unreachable!(),
                }
            }
            OtelMetricKind::Histogram { temporality } => {
                let require_start_time = *temporality == OtelAggregationTemporality::Delta;
                let mut points = Vec::with_capacity(pending_rows.len());
                for row in pending_rows {
                    tokio::task::consume_budget().await;
                    match self.histogram_point(*row, require_start_time) {
                        Ok(point) => {
                            points.push(point);
                            positions.push((batch_index, *row));
                        }
                        Err(error) => {
                            outcome.reject_structured((batch_index, *row), error.structured())
                        }
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
        let value =
            number_value_at(&array, row).map_err(|reason| OtelRecordError::new("value", reason))?;
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
        integer_as_u64(&array, row).map_err(|reason| OtelRecordError::new(key, reason))
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
            .map_err(|reason| OtelRecordError::new(key, reason))
    }

    fn required_u64_list(&self, key: &str, row: usize) -> Result<Vec<u64>, OtelRecordError> {
        let array = self.value_array(key)?.ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES requires key '{key}'"))
        })?;
        let values = list_value(&array, row)
            .map_err(|reason| OtelRecordError::new(key, reason))?
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
                integer_as_u64(&values, index).map_err(|reason| OtelRecordError::new(key, reason))
            })
            .collect()
    }

    fn required_f64_list(&self, key: &str, row: usize) -> Result<Vec<f64>, OtelRecordError> {
        let array = self.value_array(key)?.ok_or_else(|| {
            OtelRecordError::new(key, format!("OTEL VALUES requires key '{key}'"))
        })?;
        let values = list_value(&array, row)
            .map_err(|reason| OtelRecordError::new(key, reason))?
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
                numeric_as_f64(&values, index).map_err(|reason| OtelRecordError::new(key, reason))
            })
            .collect()
    }
}

fn aggregation_temporality(value: OtelAggregationTemporality) -> i32 {
    match value {
        OtelAggregationTemporality::Delta => AggregationTemporality::Delta as i32,
        OtelAggregationTemporality::Cumulative => AggregationTemporality::Cumulative as i32,
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
            let digits = std::str::from_utf8(digits).expect("validated hex is ASCII");
            u8::from_str_radix(digits, 16).expect("validated hex pair must decode")
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

fn list_value(array: &ArrayRef, row: usize) -> Result<Option<ArrayRef>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|array| Some(array.value(row)))
            .ok_or_else(|| "OTEL array value has an invalid Arrow representation".to_string()),
        DataType::FixedSizeList(_, _) => array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map(|array| Some(array.value(row)))
            .ok_or_else(|| "OTEL array value has an invalid Arrow representation".to_string()),
        ty => Err(format!("OTEL value requires ARRAY or VEC, found {ty}")),
    }
}

fn integer_as_i64(array: &ArrayRef, row: usize) -> Result<i64, String> {
    match array.data_type() {
        DataType::UInt8 => Ok(array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::Int8 => Ok(array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::UInt16 => Ok(array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::UInt32 => Ok(array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::UInt64 => i64::try_from(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row),
        )
        .map_err(|_| "OTEL integer exceeds the OTLP signed 64-bit range".to_string()),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)),
        ty => Err(format!(
            "OTEL value requires an integer-family type, found {ty}"
        )),
    }
}

fn integer_as_u64(array: &ArrayRef, row: usize) -> Result<u64, String> {
    match array.data_type() {
        DataType::UInt8 => Ok(array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::UInt16 => Ok(array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::UInt32 => Ok(array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::UInt64 => Ok(array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row)),
        DataType::Int8 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row),
        )
        .map_err(|_| "OTEL unsigned value cannot be negative".to_string()),
        DataType::Int16 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row),
        )
        .map_err(|_| "OTEL unsigned value cannot be negative".to_string()),
        DataType::Int32 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        )
        .map_err(|_| "OTEL unsigned value cannot be negative".to_string()),
        DataType::Int64 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )
        .map_err(|_| "OTEL unsigned value cannot be negative".to_string()),
        ty => Err(format!(
            "OTEL value requires an integer-family type, found {ty}"
        )),
    }
}

fn numeric_as_f64(array: &ArrayRef, row: usize) -> Result<f64, String> {
    match array.data_type() {
        DataType::Float32 => Ok(array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)
            .into()),
        DataType::Float64 => Ok(array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)),
        DataType::UInt8 => Ok(array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row) as f64),
        DataType::Int8 => Ok(array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row) as f64),
        DataType::UInt16 => Ok(array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row) as f64),
        DataType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row) as f64),
        DataType::UInt32 => Ok(array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row) as f64),
        DataType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row) as f64),
        DataType::UInt64 => Ok(array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row) as f64),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row) as f64),
        ty => Err(format!("OTEL value requires a numeric type, found {ty}")),
    }
}

fn number_value_at(array: &ArrayRef, row: usize) -> Result<number_data_point::Value, String> {
    match array.data_type() {
        DataType::Float32 => Ok(number_data_point::Value::AsDouble(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row)
                .into(),
        )),
        DataType::Float64 => Ok(number_data_point::Value::AsDouble(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        )),
        ty if OtelEmitter::is_integer_type(ty) => {
            integer_as_i64(array, row).map(number_data_point::Value::AsInt)
        }
        ty => Err(format!(
            "OTEL metric value requires a numeric type, found {ty}"
        )),
    }
}

fn any_value_at(array: &ArrayRef, row: usize) -> Result<Option<AnyValue>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Utf8 => any_value::Value::StringValue(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::Boolean => any_value::Value::BoolValue(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Float32 => any_value::Value::DoubleValue(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        DataType::Float64 => any_value::Value::DoubleValue(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        ty if OtelEmitter::is_integer_type(ty) => {
            any_value::Value::IntValue(integer_as_i64(array, row)?)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "OTEL DATETIME has an invalid Arrow representation".to_string())?
                .value(row);
            any_value::Value::StringValue(
                Timestamp::from_unix_nanos(nanos).as_datetime().to_rfc3339(),
            )
        }
        DataType::List(_) | DataType::FixedSizeList(_, _) => {
            let values = list_value(array, row)?.expect("non-null list must contain a child array");
            let mut converted = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                converted.push(any_value_at(&values, index)?.ok_or_else(|| {
                    "OTEL attribute arrays do not support NULL elements".to_string()
                })?);
            }
            any_value::Value::ArrayValue(ArrayValue { values: converted })
        }
        ty => return Err(format!("OTEL attribute type {ty} is unsupported")),
    };
    Ok(Some(AnyValue { value: Some(value) }))
}

#[cfg(test)]
mod tests {
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
        assert!(emitter_error_message(&missing_protocol).contains("protocol"));

        let unknown = OtelClientSettings::parse(&config(&[
            ("endpoint", "http://127.0.0.1:4317"),
            ("protocol", "grpc"),
            ("future", "value"),
        ]))
        .expect_err("unknown config keys must be rejected");
        assert!(emitter_error_message(&unknown).contains("unsupported"));
    }

    #[tokio::test]
    async fn grpc_transport_initialization_does_not_require_a_reachable_endpoint() {
        let transport = OtelEmitter::transport_from_config(&config(&[
            ("endpoint", "http://127.0.0.1:0"),
            ("protocol", "grpc"),
            ("timeout_ms", "1"),
        ]))
        .unwrap_or_else(|error| {
            panic!(
                "an unavailable endpoint must initialize for publish-time retry: {}",
                emitter_error_message(&error)
            )
        });
        let outcome = transport
            .export(OtelExportRequest::Logs(ExportLogsServiceRequest {
                resource_logs: Vec::new(),
            }))
            .await;
        let OtelTransportOutcome::Failed(error) = outcome else {
            panic!("an unavailable endpoint must fail as infrastructure");
        };
        assert!(emitter_publish_error_is_retryable(&error));
    }

    #[tokio::test]
    async fn client_fault_injector_returns_retryable_unavailable_without_a_server() {
        let emitter = Identifier::parse("otel_output").expect("valid emitter name");
        let fault_injector = Arc::new(OtelClientFaultInjector::default());
        fault_injector.fail_unavailable(emitter.as_str());
        let client = OtelClient {
            transport: OtelEmitter::transport_from_config(&config(&[
                ("endpoint", "http://127.0.0.1:0"),
                ("protocol", "grpc"),
                ("timeout_ms", "1"),
            ]))
            .expect("lazy gRPC client must initialize"),
            fault_injector,
            emitter,
        };

        let outcome = client
            .export(OtelExportRequest::Logs(ExportLogsServiceRequest {
                resource_logs: Vec::new(),
            }))
            .await;
        let OtelTransportOutcome::Failed(error) = outcome else {
            panic!("injected unavailability must fail the client request");
        };
        assert!(emitter_publish_error_is_retryable(&error));
        assert_eq!(
            emitter_error_message(&error),
            "OTEL client fault injector returned gRPC UNAVAILABLE"
        );
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
            u64::MAX as f64
        );
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
}
