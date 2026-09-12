//! What each external system requires of the model that talks to it.
//!
//! Layer: decisions.
//!
//! - **Owns.** The source and sink contracts: the client an ingestor or emitter may use, the
//!   publishing and mapping rules of each sink, the headers each side supports, the timestamp
//!   source an ingestor declares, and the hostnames and paths a listener claims.
//! - **Depends on.** The connector Models and the schema rules.
//! - **Must not know.** How a connector is instantiated or driven.

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use error_stack::Report;
use meticulous::ResultExt;
use nervix_models::{
    Assignment, AssignmentTarget, CreateEmitter, CreateIngestor, CreateSchema,
    CreateSignalingProtocol, DomainName, EmitSink, EndpointName, Expression, FieldName,
    IngestSource, IngestTimestampSource, Model, ModelIndex, ModelName, OtelAggregationTemporality,
    OtelMetricKind, OtelSignal, OtelValueMapping, ParseAsType, ProcessorOutput, RelayName,
    RouteConstruction, SchemaField, SchemaName, SignalingWireFormat, SqsFifoGroup, VhostName,
};
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SemanticNamespaces,
    compile_program_with_options_for_bindings_with_sensitivity, lower_route_construction,
    lower_transforming_route, program::FunctionName,
};

use crate::{
    jaq_program::StatefulJaqProgram,
    registry::{
        error::RegistryError,
        validation::{
            branching::relay_declared_branch,
            expression::{
                LookupHashMapRewriteResult, lookup_hash_map_bindings, program_uses_header_reads,
                rewrite_lookup_hash_map_program,
            },
            materialized_state::referenced_materialized_stream_bindings,
            processor::{
                ModelValidationContext, processor_first_input_relay,
                validate_where_program_for_internal_schemas,
            },
            schema::{
                arrow_schema_for_internal_schema, readonly_binding_for_internal_schema,
                schema_sensitivity_for_internal_schema, writable_binding_for_internal_schema,
            },
            vm::udf_compile_options,
        },
    },
};
pub(in crate::registry) fn validate_ingestor_source(
    domain: &DomainName,
    identifier: &ModelName,
    ingestor: &CreateIngestor,
) -> Result<(), Report<RegistryError>> {
    let invalid = |reason: String| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason,
        })
    };
    let quiesce = ingestor.source.quiesce();
    if !ingestor.source.supports_quiesce(quiesce) {
        return Err(invalid(format!(
            "{} ingestors do not support ON QUIESCE {}",
            ingestor.source.transport_label(),
            quiesce.kind_label()
        )));
    }
    match quiesce {
        nervix_models::IngestQuiesceMode::Buffer { max_size, .. }
        | nervix_models::IngestQuiesceMode::EndpointBuffer { max_size } => {
            let parsed = max_size.parse::<ubyte::ByteUnit>().map_err(|error| {
                invalid(format!(
                    "invalid quiesce BUFFER MAX SIZE '{max_size}': {error}"
                ))
            })?;
            if parsed.as_u64() == 0 {
                return Err(invalid(
                    "quiesce BUFFER MAX SIZE must be greater than 0".to_string(),
                ));
            }
        }
        nervix_models::IngestQuiesceMode::Reject { retry_after } => {
            humantime::parse_duration(retry_after).map_err(|error| {
                invalid(format!(
                    "invalid quiesce REJECT RETRY AFTER duration '{retry_after}': {error}"
                ))
            })?;
        }
        nervix_models::IngestQuiesceMode::Suspend | nervix_models::IngestQuiesceMode::Drop => {}
    }
    if let IngestSource::Mqtt { topic, .. } = &ingestor.source
        && topic.is_empty()
    {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "MQTT topic filter must not be empty".to_string(),
        }));
    }
    Ok(())
}

pub(in crate::registry) fn validate_emitter_publishing_contract(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    emitter: &CreateEmitter,
) -> Result<(), Report<RegistryError>> {
    let invalid = |reason: String| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason,
        })
    };

    if !emitter
        .sink
        .accepts_publishing_mode(&emitter.publishing_mode)
    {
        return Err(invalid(format!(
            "{} emitter does not support MODE {}",
            emitter.sink.transport_label(),
            emitter.publishing_mode.kind_label()
        )));
    }

    let requires_codec = emitter.sink.requires_codec();
    if requires_codec && emitter.encode_using_codec.is_none() {
        return Err(invalid(format!(
            "{} emitter requires ENCODE USING",
            emitter.sink.transport_label()
        )));
    }
    if !requires_codec && emitter.encode_using_codec.is_some() {
        return Err(invalid(format!(
            "{} emitter does not support ENCODE USING",
            emitter.sink.transport_label()
        )));
    }

    let retry = emitter.publishing_mode.retry_policy();
    let backoff = humantime::parse_duration(&retry.backoff).map_err(|error| {
        invalid(format!(
            "invalid MODE RETRY POLICY BACKOFF '{}': {error}",
            retry.backoff
        ))
    })?;
    let max_backoff = humantime::parse_duration(&retry.max_backoff).map_err(|error| {
        invalid(format!(
            "invalid MODE RETRY POLICY MAX '{}': {error}",
            retry.max_backoff
        ))
    })?;
    if backoff.is_zero() {
        return Err(invalid(
            "MODE RETRY POLICY BACKOFF must be greater than zero".to_string(),
        ));
    }
    if max_backoff < backoff {
        return Err(invalid(format!(
            "MODE RETRY POLICY MAX '{}' must be at least BACKOFF '{}'",
            retry.max_backoff, retry.backoff
        )));
    }

    if let Some(timeout) = emitter.publishing_mode.ack_timeout() {
        let timeout = humantime::parse_duration(timeout)
            .map_err(|error| invalid(format!("invalid MODE ACK TIMEOUT '{timeout}': {error}")))?;
        if timeout.is_zero() {
            return Err(invalid(
                "MODE ACK TIMEOUT must be greater than zero".to_string(),
            ));
        }
    }

    match emitter.sink.as_ref() {
        EmitSink::Sqs {
            queue, fifo_group, ..
        } => {
            let fifo_queue = queue.ends_with(".fifo");
            if fifo_queue && fifo_group.is_none() {
                return Err(invalid(format!(
                    "SQS FIFO queue '{queue}' requires FIFO GROUP"
                )));
            }
            if !fifo_queue && fifo_group.is_some() {
                return Err(invalid(format!(
                    "SQS FIFO GROUP requires a queue name ending in .fifo, found '{queue}'"
                )));
            }
            if let Some(SqsFifoGroup::FromBranch) = fifo_group {
                processor_first_input_relay(
                    domain,
                    identifier,
                    &emitter.from,
                    "SQS FIFO emitter input",
                )?;
                for input_relay in emitter.from.relays() {
                    if relay_declared_branch(domain, identifier, models, input_relay)?.is_none() {
                        return Err(invalid(format!(
                            "SQS FIFO GROUP FROM BRANCH requires branched input; relay '{}' is \
                             unbranched",
                            input_relay.as_str()
                        )));
                    }
                }
            }
        }
        EmitSink::Otel {
            signal,
            values,
            attributes,
            resource,
            ..
        } => {
            validate_otel_mapping_contract(signal, values, attributes, resource).map_err(invalid)?
        }
        EmitSink::Kafka { .. }
        | EmitSink::Pulsar { .. }
        | EmitSink::RabbitMq { .. }
        | EmitSink::Redis { .. }
        | EmitSink::Mqtt { .. }
        | EmitSink::Nats { .. }
        | EmitSink::ZeroMq { .. }
        | EmitSink::Syslog { .. }
        | EmitSink::Sentry { .. }
        | EmitSink::Iceberg { .. }
        | EmitSink::ClickHouse { .. }
        | EmitSink::Postgres { .. }
        | EmitSink::MySql { .. }
        | EmitSink::MongoDb { .. } => {}
    }

    Ok(())
}

fn validate_otel_mapping_contract(
    signal: &OtelSignal,
    values: &[OtelValueMapping],
    attributes: &[OtelValueMapping],
    resource: &[OtelValueMapping],
) -> Result<(), String> {
    /// The `VALUES` contract one OTEL signal imposes: what it may name, what it must name, and
    /// whether delta temporality adds `start_time` to the required keys.
    struct SignalContract {
        label: &'static str,
        allowed: &'static [&'static str],
        required: &'static [&'static str],
        delta: bool,
    }

    let SignalContract {
        label: signal_label,
        allowed,
        required,
        delta,
    } = match signal {
        OtelSignal::Logs => SignalContract {
            label: "LOGS",
            allowed: &[
                "time",
                "severity_text",
                "severity_number",
                "body",
                "trace_id",
                "span_id",
            ],
            required: &["time", "body"],
            delta: false,
        },
        OtelSignal::Traces => SignalContract {
            label: "TRACES",
            allowed: &[
                "trace_id",
                "span_id",
                "parent_span_id",
                "name",
                "kind",
                "start_time",
                "end_time",
                "status_code",
                "status_message",
            ],
            required: &["trace_id", "span_id", "name", "start_time", "end_time"],
            delta: false,
        },
        OtelSignal::Metric(metric) => match metric.kind {
            OtelMetricKind::Gauge => SignalContract {
                label: "METRIC GAUGE",
                allowed: &["time", "start_time", "value"],
                required: &["time", "value"],
                delta: false,
            },
            OtelMetricKind::Sum { temporality, .. } => SignalContract {
                label: "METRIC SUM",
                allowed: &["time", "start_time", "value"],
                required: &["time", "value"],
                delta: temporality == OtelAggregationTemporality::Delta,
            },
            OtelMetricKind::Histogram { temporality } => SignalContract {
                label: "METRIC HISTOGRAM",
                allowed: &[
                    "time",
                    "start_time",
                    "count",
                    "sum",
                    "bucket_counts",
                    "explicit_bounds",
                    "min",
                    "max",
                ],
                required: &["time", "count", "bucket_counts", "explicit_bounds"],
                delta: temporality == OtelAggregationTemporality::Delta,
            },
        },
    };

    let mut value_keys = HashSet::default();
    for mapping in values {
        if !allowed.contains(&mapping.column.as_str()) {
            return Err(format!(
                "OTEL {signal_label} VALUES does not support key '{}'",
                mapping.column
            ));
        }
        if !value_keys.insert(mapping.column.as_str()) {
            return Err(format!(
                "OTEL {signal_label} VALUES contains duplicate key '{}'",
                mapping.column
            ));
        }
    }
    for key in required {
        if !value_keys.contains(key) {
            return Err(format!("OTEL {signal_label} VALUES requires key '{key}'"));
        }
    }
    if delta && !value_keys.contains("start_time") {
        return Err(format!(
            "OTEL {signal_label} DELTA VALUES requires key 'start_time'"
        ));
    }

    for (label, mappings) in [("ATTRIBUTES", attributes), ("RESOURCE", resource)] {
        let mut keys = HashSet::default();
        for mapping in mappings {
            if !keys.insert(mapping.column.as_str()) {
                return Err(format!(
                    "OTEL {label} contains duplicate key '{}'",
                    mapping.column
                ));
            }
        }
    }

    Ok(())
}

pub(in crate::registry) fn validate_sqs_fifo_group_expression(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    emitter: &CreateEmitter,
    input_schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    let EmitSink::Sqs {
        fifo_group: Some(SqsFifoGroup::Expression(expression)),
        ..
    } = emitter.sink.as_ref()
    else {
        return Ok(());
    };

    let target = FieldName::parse("fifo_group").map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("invalid internal SQS FIFO group target: {error}"),
        })
    })?;
    let output_schema = CreateSchema {
        name: SchemaName::parse("fifo_group").map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("invalid internal SQS FIFO group schema: {error}"),
            })
        })?,
        fields: vec![SchemaField {
            name: target.clone(),
            ty: ParseAsType::String,
            optional: false,
            sensitive: false,
        }],
    };
    let input_arrow_schema = arrow_schema_for_internal_schema(input_schema);
    let output_arrow_schema = arrow_schema_for_internal_schema(&output_schema);
    let parsed = lower_transforming_route(
        &RouteConstruction {
            assignments: vec![Assignment {
                target: AssignmentTarget::bare(target.clone()),
                value: expression.clone(),
            }],
            ..RouteConstruction::default()
        },
        input_arrow_schema.as_ref(),
        output_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("SQS FIFO GROUP expression is invalid: {reason}"),
        })
    })?;
    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", input_schema),
        writable_binding_for_internal_schema("output", &output_schema),
    ];
    let local_namespaces = HashSet::from_iter(["input".to_string(), "output".to_string()]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "SQS FIFO GROUP expression",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(&output_schema),
        bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                allow_sensitive_output: false,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "SQS FIFO GROUP expression requires an exact non-sensitive STRING value: {}",
                error.message
            ),
        })
    })?;

    Ok(())
}

pub(in crate::registry) fn ensure_signaling_protocol_is_valid(
    domain: &DomainName,
    identifier: &ModelName,
    protocol: &CreateSignalingProtocol,
) -> Result<(), Report<RegistryError>> {
    let invalid = |reason: String| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason,
        })
    };

    if protocol.on_connect.sends().next().is_none() {
        return Err(invalid(
            "signaling protocol must declare at least one SEND JAQ program".to_string(),
        ));
    }
    if protocol
        .on_connect
        .wait_steps()
        .all(|wait| wait.matchers.is_empty())
    {
        return Err(invalid(
            "signaling protocol must declare at least one WAIT JAQ matcher".to_string(),
        ));
    }
    if let Some(position) = protocol
        .on_connect
        .wait_steps()
        .position(|wait| wait.matchers.is_empty())
    {
        return Err(invalid(format!(
            "signaling protocol WAIT JAQ step #{} declares no matcher",
            position + 1
        )));
    }

    let compile = |clause: &str, index: usize, program: &str| {
        StatefulJaqProgram::compile(program)
            .map(|_| ())
            .map_err(|error| {
                invalid(format!(
                    "signaling protocol {clause} program #{} is invalid: {error}",
                    index + 1
                ))
            })
    };
    for (index, program) in protocol.on_connect.sends().enumerate() {
        compile("SEND JAQ", index, program)?;
    }
    for (index, wait) in protocol.on_connect.wait_steps().enumerate() {
        for matcher in &wait.matchers {
            compile("WAIT JAQ", index, matcher)?;
        }
        if let Some(capture) = wait.capture.as_deref() {
            compile("CAPTURE", index, capture)?;
        }
        for matcher in &wait.fail_matchers {
            compile("FAIL JAQ", index, matcher)?;
        }
        if wait.capture.is_some() && wait.matchers.len() > 1 {
            return Err(invalid(
                "CAPTURE describes one matched frame, so it requires a single WAIT JAQ matcher"
                    .to_string(),
            ));
        }
    }
    for (index, matcher) in protocol.on_connect.fail_matchers.iter().enumerate() {
        compile("FAIL JAQ", index, matcher)?;
    }

    if let SignalingWireFormat::Protobuf(config) = &protocol.format {
        if config.send_message.trim().is_empty() {
            return Err(invalid(
                "protobuf signaling protocol must declare a SEND MESSAGE type".to_string(),
            ));
        }
        if config.wait_message.trim().is_empty() {
            return Err(invalid(
                "protobuf signaling protocol must declare a WAIT MESSAGE type".to_string(),
            ));
        }
    }

    humantime::parse_duration(&protocol.on_connect.timeout).map_err(|error| {
        invalid(format!(
            "invalid signaling protocol timeout '{}': {error}",
            protocol.on_connect.timeout
        ))
    })?;
    Ok(())
}

pub(in crate::registry) fn ensure_ingestor_timestamp_source(
    domain: &DomainName,
    identifier: &ModelName,
    ingestor: &CreateIngestor,
    schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    match &ingestor.timestamp_source {
        None | Some(IngestTimestampSource::Now) => Ok(()),
        Some(IngestTimestampSource::At(timestamp_field)) => {
            let Some(field) = schema
                .fields
                .iter()
                .find(|field| field.name == *timestamp_field)
            else {
                return Err(Report::new(RegistryError::IncompatibleSchema {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "TIMESTAMP field '{}' is missing from schema '{}'",
                        timestamp_field.as_str(),
                        schema.name.as_str()
                    ),
                }));
            };

            if let ParseAsType::Datetime = field.ty {
                return Ok(());
            }

            Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "TIMESTAMP field '{}' must use DATETIME in schema '{}'",
                    timestamp_field.as_str(),
                    schema.name.as_str()
                ),
            }))
        }
    }
}

pub(in crate::registry) fn validate_ingestor_filter_where_for_internal_schemas(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    filter_where: Option<&Expression>,
    source: &IngestSource,
) -> Result<(), Report<RegistryError>> {
    let Some(filter_where) = filter_where else {
        return Ok(());
    };
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(filter_where.clone()),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("input", "__invalid_filter_target"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER WHERE is invalid: {reason}"),
        })
    })?;
    if program_uses_header_reads(&parsed.inner) && !ingest_source_supports_headers(source) {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} ingestors do not support read_header or read_headers",
                source.transport_label()
            ),
        }));
    }
    validate_where_program_for_internal_schemas(
        ModelValidationContext {
            domain,
            identifier,
            models,
        },
        input_schemas,
        branch_schema,
        filter_where,
        "FILTER WHERE",
        CompileOptions {
            allow_header_reads: true,
            ..CompileOptions::default()
        },
    )
}

pub(in crate::registry) fn effective_ingestor_output_filter_map_schema(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    ingestor: &CreateIngestor,
    input_schema: &CreateSchema,
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
) -> Result<CreateSchema, Report<RegistryError>> {
    let input_arrow_schema = arrow_schema_for_internal_schema(input_schema);
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_transforming_route(
        &output.construction,
        input_arrow_schema.as_ref(),
        output_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("ingestor output route is invalid: {reason}"),
        })
    })?;
    if program_uses_header_reads(&parsed.inner) && !ingest_source_supports_headers(&ingestor.source)
    {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} ingestors do not support read_header or read_headers",
                ingestor.source.transport_label()
            ),
        }));
    }
    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;

    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", input_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(metadata_schema) = ingestor_filter_map_metadata_schema(&ingestor.source) {
        bindings.push(CompileBinding::readonly(
            "metadata",
            arrow_schema_for_internal_schema(&metadata_schema),
        ));
    }
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        "metadata".to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "FILTER-MAP",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(output_schema),
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                allow_header_reads: true,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
        })
    })?;

    Ok(output_schema.clone())
}

pub(in crate::registry) fn ingest_source_supports_headers(source: &IngestSource) -> bool {
    matches!(
        source,
        IngestSource::Endpoint { .. }
            | IngestSource::Http { .. }
            | IngestSource::Kafka { .. }
            | IngestSource::Nats { .. }
            | IngestSource::Pulsar { .. }
            | IngestSource::RabbitMq { .. }
            | IngestSource::Sqs { .. }
    )
}

fn ingestor_filter_map_metadata_schema(source: &IngestSource) -> Option<CreateSchema> {
    match source {
        IngestSource::Kafka { .. } => Some(CreateSchema {
            name: SchemaName::parse("ingestor_metadata")
                .assured("this is a constant literal that satisfies the identifier grammar"),
            fields: vec![
                SchemaField {
                    name: FieldName::parse("topic").assured(
                        "this is a constant literal that satisfies the identifier grammar",
                    ),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: FieldName::parse("partition").assured(
                        "this is a constant literal that satisfies the identifier grammar",
                    ),
                    ty: ParseAsType::I32,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: FieldName::parse("offset").assured(
                        "this is a constant literal that satisfies the identifier grammar",
                    ),
                    ty: ParseAsType::I64,
                    optional: true,
                    sensitive: false,
                },
            ],
        }),
        IngestSource::Syslog { .. } => Some(CreateSchema {
            name: SchemaName::parse("ingestor_metadata")
                .assured("this is a constant literal that satisfies the identifier grammar"),
            fields: vec![SchemaField {
                name: FieldName::parse("peer_addr")
                    .assured("this is a constant literal that satisfies the identifier grammar"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            }],
        }),
        _ => None,
    }
}

fn emit_sink_supports_headers(sink: &EmitSink) -> bool {
    matches!(
        sink,
        EmitSink::Kafka { .. }
            | EmitSink::Pulsar { .. }
            | EmitSink::RabbitMq { .. }
            | EmitSink::Nats { .. }
            | EmitSink::Sqs { .. }
    )
}

pub(in crate::registry) fn effective_emitter_filter_map_schema(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    emitter: &nervix_models::CreateEmitter,
    input_schema: &CreateSchema,
    output_schema: &CreateSchema,
) -> Result<CreateSchema, Report<RegistryError>> {
    let codec_route = emitter.encode_using_codec.is_some();
    if !codec_route
        && (emitter.construction.inherit.is_some()
            || !emitter.construction.assignments.is_empty()
            || !emitter.construction.invocations.is_empty())
    {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "direct emitter routes support VALUES and WHERE only".to_string(),
        }));
    }
    if emitter.construction.is_empty() && !codec_route {
        return Ok(input_schema.clone());
    }
    let input_arrow_schema = arrow_schema_for_internal_schema(input_schema);
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = if codec_route {
        lower_transforming_route(
            &emitter.construction,
            input_arrow_schema.as_ref(),
            output_arrow_schema.as_ref(),
        )
    } else {
        lower_route_construction(
            &emitter.construction,
            SemanticNamespaces::new("input", "__invalid_direct_emitter_output"),
        )
    }
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("emitter route is invalid: {reason}"),
        })
    })?;
    let invokes_write_header = parsed
        .inner
        .invoke
        .iter()
        .any(|invocation| invocation.inner.function == FunctionName::WriteHeader);
    if invokes_write_header && !emit_sink_supports_headers(&emitter.sink) {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} emitters do not support write_header",
                emitter.sink.transport_label()
            ),
        }));
    }

    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut body_bindings = if codec_route {
        vec![
            readonly_binding_for_internal_schema("input", input_schema),
            writable_binding_for_internal_schema("output", output_schema),
        ]
    } else {
        vec![
            writable_binding_for_internal_schema("input", input_schema),
            readonly_binding_for_internal_schema("message", input_schema),
        ]
    };
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "message".to_string(),
        "output".to_string(),
    ]);
    body_bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "emitter route",
    )?);
    body_bindings.extend(lookup_hash_map_bindings(lookup_fields));
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        body_bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: if codec_route {
                    OutputMode::ExplicitOnly
                } else {
                    OutputMode::PassthroughByName
                },
                allow_sensitive_output: false,
                allow_header_writes: true,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
        })
    })?;

    Ok(output_schema.clone())
}

pub(in crate::registry) fn validate_vhost_hostnames(
    domain: &DomainName,
    models: &ModelIndex,
) -> Result<(), Report<RegistryError>> {
    let mut owners = HashMap::<String, VhostName>::new();

    for (key, model) in models {
        let Model::Vhost(vhost) = model else {
            continue;
        };
        let identifier = &key.identifier;

        let mut seen_in_vhost = HashSet::new();
        for hostname in &vhost.hostnames {
            let normalized = hostname.to_ascii_lowercase();
            if !seen_in_vhost.insert(normalized.clone()) {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!("hostname '{hostname}' is listed more than once"),
                }));
            }

            if let Some(existing) = owners.insert(normalized, VhostName::from(identifier)) {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "hostname '{hostname}' is already assigned to vhost '{}'",
                        existing.as_str()
                    ),
                }));
            }
        }
    }

    Ok(())
}

pub(in crate::registry) fn validate_endpoint_paths(
    domain: &DomainName,
    models: &ModelIndex,
) -> Result<(), Report<RegistryError>> {
    let mut routes = HashMap::<(VhostName, String), EndpointName>::new();

    for (key, model) in models {
        let Model::Endpoint(endpoint) = model else {
            continue;
        };
        let identifier = &key.identifier;

        let key = (endpoint.on_vhost.clone(), endpoint.path.clone());
        if let Some(existing) = routes.insert(key, EndpointName::from(identifier)) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "path '{}' is already assigned to endpoint '{}' on vhost '{}'",
                    endpoint.path,
                    existing.as_str(),
                    endpoint.on_vhost.as_str()
                ),
            }));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{
        AckMode, AlterEmitter, AlterEmitterOperation, ClientConfigEntry, ClientName, CodecName,
        ConsumerGroupName, CreateClientHttp, CreateClientSqs, CreateWireSchema, EmitterAckWindow,
        EmitterPublishingMode, ErrorPolicies, FlushPolicy, GeneralErrorPolicy, IngestorName,
        JsonType, KafkaIngestMode, KafkaOffsetMode, MaterializedRelayState, MessageErrorPolicy,
        MqttIngestMode, MqttQos, MqttSession, OtelMetric, OutputBranch, ProcessorInputs,
        ProcessorOutputs, RetryPolicy, SignalingProtobufConfig, TopicName, WireSchemaField,
        WireSchemaName,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::{
        mutation::RegistryMutation,
        storage::Registry,
        test_fixtures::{
            branch, branch_for_relay, branch_schema, branch_schema_with_types, branched_by,
            client_model, codec, emitter, explicitly_unbranched_relay, named, relay,
            relay_branched_by, relay_branched_by_relay_branch, schema, signaling_protocol,
            temp_db_path, unbranched_transforming_outputs, vhost, wire_schema,
        },
    };

    fn validate_signaling_protocol(
        protocol: &CreateSignalingProtocol,
    ) -> Result<(), Report<RegistryError>> {
        let domain = DomainName::parse("default").expect("valid domain");
        ensure_signaling_protocol_is_valid(&domain, &ModelName::from(&protocol.name), protocol)
    }

    #[test]
    fn signaling_protocols_accept_valid_jaq_programs() {
        validate_signaling_protocol(&signaling_protocol(
            SignalingWireFormat::Json,
            &["{id: 1}"],
            &[".id == 1 and .result == null"],
            &[".error"],
        ))
        .expect("valid signaling protocol must be accepted");

        validate_signaling_protocol(&signaling_protocol(
            SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                resource: named("proto_bundle"),
                resource_version: Some(1),
                config: Vec::new(),
                send_message: "nervix.test.Subscribe".to_string(),
                wait_message: "nervix.test.Ack".to_string(),
            }),
            &["{id: 1}"],
            &[".id == 1"],
            &[],
        ))
        .expect("valid protobuf signaling protocol must be accepted");
    }

    #[test]
    fn signaling_protocols_reject_invalid_jaq_programs() {
        let error = validate_signaling_protocol(&signaling_protocol(
            SignalingWireFormat::Json,
            &["{id: 1}", ".["],
            &[".id == 1"],
            &[],
        ))
        .expect_err("invalid send program must be rejected");
        assert!(
            error.to_string().contains("SEND JAQ program #2 is invalid"),
            "unexpected error: {error:?}"
        );

        let error = validate_signaling_protocol(&signaling_protocol(
            SignalingWireFormat::Json,
            &["{id: 1}"],
            &[".id == 1"],
            &[".error", "if ."],
        ))
        .expect_err("invalid fail matcher must be rejected");
        assert!(
            error.to_string().contains("FAIL JAQ program #2 is invalid"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn signaling_protocols_require_send_and_wait_programs() {
        let error = validate_signaling_protocol(&signaling_protocol(
            SignalingWireFormat::Json,
            &[],
            &[".id == 1"],
            &[],
        ))
        .expect_err("missing send program must be rejected");
        assert!(
            error.to_string().contains("at least one SEND JAQ program"),
            "unexpected error: {error:?}"
        );

        let error = validate_signaling_protocol(&signaling_protocol(
            SignalingWireFormat::Json,
            &["{id: 1}"],
            &[],
            &[],
        ))
        .expect_err("missing wait matcher must be rejected");
        assert!(
            error.to_string().contains("at least one WAIT JAQ matcher"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn protobuf_signaling_protocols_require_both_message_types() {
        let error = validate_signaling_protocol(&signaling_protocol(
            SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                resource: named("proto_bundle"),
                resource_version: None,
                config: Vec::new(),
                send_message: "nervix.test.Subscribe".to_string(),
                wait_message: "  ".to_string(),
            }),
            &["{id: 1}"],
            &[".id == 1"],
            &[],
        ))
        .expect_err("missing wait message type must be rejected");

        assert!(
            error.to_string().contains("WAIT MESSAGE type"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn emitter_publishing_contract_rejects_model_level_bypasses() {
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out")
        else {
            unreachable!("emitter helper must build an emitter model")
        };
        let models = ModelIndex::new();

        emitter.publishing_mode = EmitterPublishingMode::NoAck {
            retry_policy: RetryPolicy {
                backoff: "0s".to_string(),
                max_backoff: "1s".to_string(),
            },
        };
        let error = validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect_err("zero retry backoff must be rejected");
        assert!(format!("{error:#}").contains("BACKOFF must be greater than zero"));

        emitter.publishing_mode = EmitterPublishingMode::BrokerAck {
            window: EmitterAckWindow::Sequential,
            ack_timeout: "0s".to_string(),
            retry_policy: RetryPolicy {
                backoff: "10ms".to_string(),
                max_backoff: "1s".to_string(),
            },
        };
        let error = validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect_err("zero confirmation timeout must be rejected");
        assert!(format!("{error:#}").contains("ACK TIMEOUT must be greater than zero"));

        emitter.publishing_mode = EmitterPublishingMode::MqttQos0 {
            retry_policy: RetryPolicy {
                backoff: "10ms".to_string(),
                max_backoff: "1s".to_string(),
            },
        };
        let error = validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect_err("foreign publishing modes must be rejected");
        assert!(format!("{error:#}").contains("KAFKA emitter does not support MODE QOS 0"));

        *emitter.sink = EmitSink::Sqs {
            client: named("sqs_main"),
            queue: "events".to_string(),
            fifo_group: Some(SqsFifoGroup::Expression(Expression::Literal(
                nervix_models::Literal::String("group".to_string()),
            ))),
        };
        emitter.publishing_mode = EmitterPublishingMode::SqsSingle {
            retry_policy: RetryPolicy {
                backoff: "1s".to_string(),
                max_backoff: "100ms".to_string(),
            },
        };
        let error = validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect_err("retry maxima below their initial backoff must be rejected");
        assert!(format!("{error:#}").contains("must be at least BACKOFF"));

        if let EmitterPublishingMode::SqsSingle { retry_policy } = &mut emitter.publishing_mode {
            retry_policy.max_backoff = "1s".to_string();
        }
        let error = validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect_err("FIFO GROUP on a standard queue must be rejected");
        assert!(format!("{error:#}").contains("requires a queue name ending in .fifo"));
    }

    fn otel_mapping(key: &str) -> OtelValueMapping {
        OtelValueMapping {
            column: key.to_string(),
            expression: Expression::Literal(nervix_models::Literal::String("value".to_string())),
        }
    }

    #[test]
    fn otel_mapping_contract_validates_signal_keys_before_runtime() {
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out")
        else {
            unreachable!("emitter helper must build an emitter model")
        };
        emitter.encode_using_codec = None;
        emitter.publishing_mode = EmitterPublishingMode::RequestAck {
            retry_policy: RetryPolicy {
                backoff: "10ms".to_string(),
                max_backoff: "1s".to_string(),
            },
        };
        let models = ModelIndex::new();

        *emitter.sink = EmitSink::Otel {
            client: named("otel_main"),
            signal: OtelSignal::Logs,
            values: vec![otel_mapping("time"), otel_mapping("body")],
            attributes: Vec::new(),
            resource: Vec::new(),
            scope: None,
        };
        validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect("complete OTEL LOGS mappings must be accepted");

        let EmitSink::Otel { values, .. } = emitter.sink.as_mut() else {
            unreachable!("test emitter must remain OTEL")
        };
        values.push(otel_mapping("body"));
        let error = validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect_err("duplicate OTEL VALUES keys must be rejected");
        assert!(format!("{error:#}").contains("duplicate key 'body'"));

        *emitter.sink = EmitSink::Otel {
            client: named("otel_main"),
            signal: OtelSignal::Metric(OtelMetric {
                name: "requests".to_string(),
                unit: "1".to_string(),
                description: None,
                kind: OtelMetricKind::Sum {
                    monotonic: true,
                    temporality: OtelAggregationTemporality::Delta,
                },
            }),
            values: vec![otel_mapping("time"), otel_mapping("value")],
            attributes: Vec::new(),
            resource: Vec::new(),
            scope: None,
        };
        let error = validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect_err("DELTA metric streams without start_time must be rejected");
        assert!(format!("{error:#}").contains("DELTA VALUES requires key 'start_time'"));
    }

    #[test]
    fn sqs_fifo_group_is_validated_at_emitter_creation() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    explicitly_unbranched_relay("events", "event_schema"),
                    branch_schema_with_types(
                        "tenant_branch_schema",
                        &[("tenant", ParseAsType::String)],
                    ),
                    branch("tenant_branch", "tenant_branch_schema"),
                    relay_branched_by("tenant_events", "event_schema", "tenant_branch"),
                    Model::ClientSqs(CreateClientSqs {
                        name: named("sqs_main"),
                        mount: None,
                        config: vec![ClientConfigEntry {
                            key: "region".to_string(),
                            value: "us-east-1".to_string(),
                        }],
                    }),
                ],
            )
            .expect("SQS FIFO validation fixtures should install");

        let Model::Emitter(mut valid) = emitter("valid_fifo", "events", "event_codec", "sqs_main")
        else {
            unreachable!("emitter helper must build an emitter model")
        };
        valid.sink = Box::new(EmitSink::Sqs {
            client: named("sqs_main"),
            queue: "events.fifo".to_string(),
            fifo_group: Some(SqsFifoGroup::Expression(
                nervix_nspl::parse_expression("input.value").expect("valid FIFO group expression"),
            )),
        });
        valid.publishing_mode = EmitterPublishingMode::SqsSingle {
            retry_policy: RetryPolicy {
                backoff: "10ms".to_string(),
                max_backoff: "1s".to_string(),
            },
        };
        registry
            .apply_batch(&domain, vec![Model::Emitter(valid.clone())])
            .expect("non-sensitive STRING FIFO expressions should be accepted");

        let mut wrong_type = valid.clone();
        wrong_type.name = named("wrong_fifo_type");
        if let EmitSink::Sqs { fifo_group, .. } = wrong_type.sink.as_mut() {
            *fifo_group = Some(SqsFifoGroup::Expression(Expression::Literal(
                nervix_models::Literal::I64(42),
            )));
        }
        let error = registry
            .apply_batch(&domain, vec![Model::Emitter(wrong_type)])
            .expect_err("non-STRING FIFO expressions must be rejected");
        assert!(format!("{error:#}").contains("requires an exact non-sensitive STRING value"));

        let mut branch_fifo = valid.clone();
        branch_fifo.name = named("branch_fifo");
        branch_fifo.from = ProcessorInputs::single(named("tenant_events"));
        if let EmitSink::Sqs { fifo_group, .. } = branch_fifo.sink.as_mut() {
            *fifo_group = Some(SqsFifoGroup::FromBranch);
        }
        registry
            .apply_batch(&domain, vec![Model::Emitter(branch_fifo)])
            .expect("FIFO GROUP FROM BRANCH should accept a wholly branched input set");

        let mut mixed_inputs = valid.clone();
        mixed_inputs.name = named("mixed_fifo_inputs");
        mixed_inputs.from.from = vec![named("tenant_events"), named("events")];
        if let EmitSink::Sqs { fifo_group, .. } = mixed_inputs.sink.as_mut() {
            *fifo_group = Some(SqsFifoGroup::FromBranch);
        }
        let error = registry
            .apply_batch(&domain, vec![Model::Emitter(mixed_inputs)])
            .expect_err("every FIFO FROM BRANCH input must be branched");
        assert!(format!("{error:#}").contains("FROM BRANCH requires branched input"));

        let error = registry
            .apply_mutation_batch(
                &domain,
                vec![RegistryMutation::AlterEmitter(AlterEmitter {
                    emitter: named("branch_fifo"),
                    operations: vec![AlterEmitterOperation::AddFrom {
                        relay: named("events"),
                        where_clause: None,
                    }],
                })],
            )
            .expect_err("ALTER ADD FROM must not add an unbranched FIFO input");
        assert!(format!("{error:#}").contains("FROM BRANCH requires branched input"));

        let mut from_branch = valid;
        from_branch.name = named("unbranched_fifo");
        if let EmitSink::Sqs { fifo_group, .. } = from_branch.sink.as_mut() {
            *fifo_group = Some(SqsFifoGroup::FromBranch);
        }
        let error = registry
            .apply_batch(&domain, vec![Model::Emitter(from_branch)])
            .expect_err("FROM BRANCH on unbranched input must be rejected");
        assert!(format!("{error:#}").contains("FROM BRANCH requires branched input"));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_header_invocations_are_rejected_for_unsupported_sinks() {
        let domain = DomainName::parse("default").expect("valid domain");
        let schema = CreateSchema {
            name: named("event_schema"),
            fields: vec![SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        };
        let mut emitter = CreateEmitter {
            name: named("emit"),
            from: ProcessorInputs::single(named("events")),
            encode_using_codec: Some(named("events_codec")),
            sink: Box::new(EmitSink::ZeroMq {
                client: named("zeromq_main"),
            }),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            flush_policy: FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            construction: nervix_nspl::parse_route_construction(
                "INHERIT ALL INVOKE write_header(\"tenant\", input.tenant)",
            )
            .expect("valid construction"),
            materialized_state: Vec::new(),
        };

        let error = super::effective_emitter_filter_map_schema(
            &domain,
            &ModelName::from(&emitter.name),
            &ModelIndex::new(),
            &emitter,
            &schema,
            &schema,
        )
        .expect_err("ZeroMQ emitters must reject write_header");
        assert!(format!("{error:#}").contains("ZEROMQ emitters do not support write_header"));

        *emitter.sink = EmitSink::Syslog {
            client: named("syslog_main"),
        };
        let error = super::effective_emitter_filter_map_schema(
            &domain,
            &ModelName::from(&emitter.name),
            &ModelIndex::new(),
            &emitter,
            &schema,
            &schema,
        )
        .expect_err("Syslog emitters must reject write_header");
        assert!(format!("{error:#}").contains("SYSLOG emitters do not support write_header"));

        *emitter.sink = EmitSink::Kafka {
            client: named("kafka_main"),
            topic: named("events_out"),
        };
        super::effective_emitter_filter_map_schema(
            &domain,
            &ModelName::from(&emitter.name),
            &ModelIndex::new(),
            &emitter,
            &schema,
            &schema,
        )
        .expect("Kafka emitters must accept write_header");
    }

    #[test]
    fn mqtt_instances_greater_than_one_are_valid() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let result = registry.apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                client_model("mqtt_main"),
                relay("notifications", "event_schema"),
                Model::Ingestor(CreateIngestor {
                    name: IngestorName::parse("mqtt_ing").expect("valid identifier"),
                    output_routes: unbranched_transforming_outputs("notifications"),
                    decode_using_codec: CodecName::parse("event_codec").expect("valid identifier"),
                    timestamp_source: None,
                    source: IngestSource::Mqtt {
                        client: ClientName::parse("mqtt_main").expect("valid identifier"),
                        topic: "notifications".to_string(),
                        instances: nonzero!(2u64),
                        mode: MqttIngestMode::NoAckSequential {
                            session: MqttSession::Clean,
                            qos: MqttQos::AtMostOnce,
                        },
                        quiesce: nervix_models::IngestQuiesceMode::Drop,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ],
        );

        result.expect("MQTT multi-instance ingestors should not expose subscription mode");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_timestamp_field_must_use_rfc3339_schema_type() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let result = registry.apply_batch(
            &domain,
            vec![
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("event_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: FieldName::parse("occurred_at").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                Model::WireJsonSchema(CreateWireSchema {
                    name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                    strictness: Default::default(),
                    fields: vec![
                        WireSchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                        WireSchemaField {
                            name: FieldName::parse("occurred_at").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                    ],
                }),
                codec("event_codec", "event_schema"),
                client_model("broker"),
                relay("notifications", "event_schema"),
                Model::Ingestor(CreateIngestor {
                    name: IngestorName::parse("ing").expect("valid identifier"),
                    output_routes: unbranched_transforming_outputs("notifications"),
                    decode_using_codec: CodecName::parse("event_codec").expect("valid identifier"),
                    timestamp_source: Some(IngestTimestampSource::At(
                        FieldName::parse("occurred_at").expect("valid field name"),
                    )),
                    source: IngestSource::Kafka {
                        client: ClientName::parse("broker").expect("valid identifier"),
                        topic: TopicName::parse("notifications").expect("valid identifier"),
                        offset_mode: KafkaOffsetMode::ConsumerGroup(
                            ConsumerGroupName::parse("cg").expect("valid consumer group"),
                        ),
                        instances: nonzero!(1u64),
                        mode: KafkaIngestMode::NoAckParallel,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ],
        );

        let error = result.expect_err("timestamp field with non-DATETIME type must fail");
        assert!(
            format!("{error:#}").contains("TIMESTAMP field 'occurred_at' must use DATETIME"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_route_validation_accepts_explicit_projection() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: FieldName::parse("value").expect("valid identifier"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: FieldName::parse("tenant").expect("valid identifier"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: FieldName::parse("raw").expect("valid identifier"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("transformed_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: FieldName::parse("tenant").expect("valid identifier"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: FieldName::parse("total").expect("valid identifier"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireJsonSchema(CreateWireSchema {
                        name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: FieldName::parse("value").expect("valid identifier"),
                                ty: JsonType::Integer,
                                optional: false,
                            },
                            WireSchemaField {
                                name: FieldName::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: FieldName::parse("raw").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    }),
                    codec("event_codec", "event_schema"),
                    client_model("broker"),
                    relay_branched_by_relay_branch("notifications", "transformed_schema"),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("notifications", "tenant_branch"),
                    Model::Ingestor(CreateIngestor {
                        name: IngestorName::parse("ing").expect("valid identifier"),
                        output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                            relay: RelayName::parse("notifications").expect("valid identifier"),
                            construction: nervix_nspl::parse_route_construction(
                                "SET total = input.value, tenant = input.tenant",
                            )
                            .expect("route construction must parse"),
                            flush_policy: None,
                            message_error_policy: MessageErrorPolicy::Log,
                            branch: Some(branched_by("notifications", &["tenant"])),
                        }]))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                        decode_using_codec: CodecName::parse("event_codec")
                            .expect("valid identifier"),
                        timestamp_source: None,
                        source: IngestSource::Kafka {
                            client: ClientName::parse("broker").expect("valid identifier"),
                            topic: TopicName::parse("notifications").expect("valid identifier"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(
                                ConsumerGroupName::parse("cg").expect("valid consumer group"),
                            ),
                            instances: nonzero!(1u64),
                            mode: KafkaIngestMode::NoAckParallel,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                ],
            )
            .expect("batch with valid FILTER-MAP should succeed");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_filter_map_compile_errors_are_reported_on_leader() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let result = registry.apply_batch(
            &domain,
            vec![
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("event_schema").expect("valid identifier"),
                    fields: vec![SchemaField {
                        name: FieldName::parse("value").expect("valid identifier"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }),
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("transformed_schema").expect("valid identifier"),
                    fields: vec![SchemaField {
                        name: FieldName::parse("total").expect("valid identifier"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }),
                Model::WireJsonSchema(CreateWireSchema {
                    name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                    strictness: Default::default(),
                    fields: vec![WireSchemaField {
                        name: FieldName::parse("value").expect("valid identifier"),
                        ty: JsonType::Integer,
                        optional: false,
                    }],
                }),
                codec("event_codec", "event_schema"),
                client_model("broker"),
                relay("notifications", "transformed_schema"),
                Model::Ingestor(CreateIngestor {
                    name: IngestorName::parse("ing").expect("valid identifier"),
                    output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                        relay: RelayName::parse("notifications").expect("valid identifier"),
                        construction: nervix_nspl::parse_route_construction(
                            "SET total = input.missing + 1",
                        )
                        .expect("route construction must parse"),
                        flush_policy: None,
                        message_error_policy: MessageErrorPolicy::Log,
                        branch: Some(OutputBranch::Unbranched),
                    }]))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                    decode_using_codec: CodecName::parse("event_codec").expect("valid identifier"),
                    timestamp_source: None,
                    source: IngestSource::Kafka {
                        client: ClientName::parse("broker").expect("valid identifier"),
                        topic: TopicName::parse("notifications").expect("valid identifier"),
                        offset_mode: KafkaOffsetMode::ConsumerGroup(
                            ConsumerGroupName::parse("cg").expect("valid consumer group"),
                        ),
                        instances: nonzero!(1u64),
                        mode: KafkaIngestMode::NoAckParallel,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ],
        );

        let error = result.expect_err("invalid FILTER-MAP must fail");
        assert!(
            format!("{error:#}").contains("unknown input field 'missing'"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_inherit_all_except_rejects_required_uninitialized_field() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let result = registry.apply_batch(
            &domain,
            vec![
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("event_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: ParseAsType::I64,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: FieldName::parse("tenant").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                Model::WireJsonSchema(CreateWireSchema {
                    name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                    strictness: Default::default(),
                    fields: vec![
                        WireSchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: JsonType::Integer,
                            optional: false,
                        },
                        WireSchemaField {
                            name: FieldName::parse("tenant").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                    ],
                }),
                codec("event_codec", "event_schema"),
                client_model("broker"),
                relay("notifications", "event_schema"),
                Model::Ingestor(CreateIngestor {
                    name: IngestorName::parse("ing").expect("valid identifier"),
                    output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                        relay: RelayName::parse("notifications").expect("valid identifier"),
                        construction: nervix_nspl::parse_route_construction(
                            "INHERIT ALL EXCEPT value",
                        )
                        .expect("route construction must parse"),
                        flush_policy: None,
                        message_error_policy: MessageErrorPolicy::Log,
                        branch: Some(OutputBranch::Unbranched),
                    }]))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                    decode_using_codec: CodecName::parse("event_codec").expect("valid identifier"),
                    timestamp_source: None,
                    source: IngestSource::Kafka {
                        client: ClientName::parse("broker").expect("valid identifier"),
                        topic: TopicName::parse("notifications").expect("valid identifier"),
                        offset_mode: KafkaOffsetMode::ConsumerGroup(
                            ConsumerGroupName::parse("cg").expect("valid consumer group"),
                        ),
                        instances: nonzero!(1u64),
                        mode: KafkaIngestMode::NoAckParallel,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ],
        );

        let error = result.expect_err("excluded required output must remain uninitialized");
        assert!(
            format!("{error:#}").contains("required output field 'value' remains uninitialized"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_accepts_same_schema_inputs_from_different_named_branches() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Emitter(mut emitter) = emitter("emit", "source_a", "event_codec", "broker_out")
        else {
            unreachable!("emitter helper must build an emitter model")
        };
        emitter.from = ProcessorInputs::new(
            vec![named("source_a"), named("source_b")],
            vec![
                nervix_models::ProcessorInputWhere {
                    relay: named("source_a"),
                    where_clause: nervix_nspl::parse_expression("input.value = 'one'")
                        .expect("valid source filter"),
                },
                nervix_models::ProcessorInputWhere {
                    relay: named("source_b"),
                    where_clause: nervix_nspl::parse_expression("input.value = 'two'")
                        .expect("valid source filter"),
                },
            ],
        );

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_out"),
                    relay_branched_by("source_a", "event_schema", "branch_a"),
                    relay_branched_by("source_b", "event_schema", "branch_b"),
                    branch_schema("value_branch", &["value"]),
                    branch("branch_a", "value_branch"),
                    branch("branch_b", "value_branch"),
                    Model::Emitter(emitter),
                ],
            )
            .expect("emitters may consume different named branches of one declared schema");

        let dataflow = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());
        let edges = dataflow
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert!(edges.contains(&("relay:source_a", "emitter:emit")));
        assert!(edges.contains(&("relay:source_b", "emitter:emit")));
        let sink_edge = dataflow
            .edges
            .iter()
            .find(|edge| edge.target == "client_sink:broker_out")
            .expect("emitter sink edge must exist");
        assert_eq!(
            sink_edge
                .metric
                .as_ref()
                .expect("emitter sink edge must carry a metric")
                .relay,
            None,
            "multi-input sent metrics must aggregate without a misleading relay label"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_rejects_inputs_with_different_declared_schema_names() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Emitter(mut emitter) = emitter("emit", "source_a", "event_codec", "broker_out")
        else {
            unreachable!("emitter helper must build an emitter model")
        };
        emitter.from = ProcessorInputs::new(vec![named("source_a"), named("source_b")], Vec::new());

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    schema("same_shape_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_out"),
                    relay("source_a", "event_schema"),
                    relay("source_b", "same_shape_schema"),
                    Model::Emitter(emitter),
                ],
            )
            .expect_err("emitter inputs must use the same declared schema");

        assert!(
            format!("{error:#}").contains(
                "input relay 'source_b' declares schema 'same_shape_schema', but all emitter \
                 inputs must declare schema 'event_schema'"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_materialized_state_must_match_every_input_branch() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Emitter(mut emitter) = emitter("emit", "source_a", "event_codec", "broker_out")
        else {
            unreachable!("emitter helper must build an emitter model")
        };
        emitter.from = ProcessorInputs::new(vec![named("source_a"), named("source_b")], Vec::new());
        emitter.materialized_state = vec![nervix_models::MaterializedStateDependency {
            relay: named("profiles"),
            policy: nervix_models::MaterializedStatePolicy::RequiredSkip,
        }];
        let Model::Relay(mut profiles) = relay_branched_by("profiles", "event_schema", "branch_a")
        else {
            unreachable!("relay helper must build a relay model")
        };
        profiles.materialized_state = Some(MaterializedRelayState::LastByTimestamp);

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_out"),
                    relay_branched_by("source_a", "event_schema", "branch_a"),
                    relay_branched_by("source_b", "event_schema", "branch_b"),
                    Model::Relay(profiles),
                    branch_schema("value_branch", &["value"]),
                    branch("branch_a", "value_branch"),
                    branch("branch_b", "value_branch"),
                    Model::Emitter(emitter),
                ],
            )
            .expect_err("materialized state must match every emitter input branch");

        assert!(
            format!("{error:#}").contains(
                "emitter materialized state requires relay 'source_b' and materialized relay \
                 'profiles' to use the same exact branch"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn sentry_emitter_rejects_http_client() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Emitter(mut sentry_emitter) =
            emitter("emit", "notifications", "event_codec", "sentry_main")
        else {
            unreachable!("emitter helper must build an emitter model")
        };
        sentry_emitter.sink = Box::new(EmitSink::Sentry {
            client: named("sentry_main"),
        });
        sentry_emitter.publishing_mode = EmitterPublishingMode::RequestAck {
            retry_policy: RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        };

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    Model::ClientHttp(CreateClientHttp {
                        name: named("sentry_main"),
                        mount: None,
                        config: vec![ClientConfigEntry {
                            key: "dsn".to_string(),
                            value: "https://key@sentry.example/42".to_string(),
                        }],
                    }),
                    relay("notifications", "event_schema"),
                    Model::Emitter(sentry_emitter),
                ],
            )
            .expect_err("Sentry emitter must reject an HTTP client");

        assert!(
            format!("{error:#}").contains(
                "SENTRY emitter requires a SENTRY client, found HTTP client 'sentry_main'"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_duplicate_vhost_hostnames() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    vhost("edge", &["api.example.com"]),
                    vhost("edge_internal", &["api.example.com"]),
                ],
            )
            .expect_err("duplicate hostname should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("hostname 'api.example.com' is already assigned"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }
}
