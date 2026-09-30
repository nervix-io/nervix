//! What each external system requires of the model that talks to it.
//!
//! Layer: decisions.
//!
//! - **Owns.** The source and sink contracts: the client an ingestor or emitter may use, the
//!   publishing and mapping rules of each sink, the headers each side supports, the timestamp
//!   source an ingestor declares, and the hostnames and paths a listener claims.
//! - **Depends on.** The connector Models and the schema rules.
//! - **Must not know.** How a connector is instantiated or driven.

use std::time::Duration;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_jaq::StatefulJaqProgram;
use nervix_models::{
    Assignment, AssignmentTarget, ClientIngestMode, CodecBatchContainer, CreateClientHttp,
    CreateCodec, CreateEmitter, CreateIngestor, CreateSchema, CreateSignalingProtocol, DomainName,
    EmitSink, EndpointName, Expression, FieldName, HttpApplicationHeaders, HttpBodyMode,
    HttpHeaderName, HttpHeaderValue, HttpMethod, HttpOrigin, IngestSource, IngestTimestampSource,
    IngestorInput, Model, ModelIndex, ModelName, OtelAggregationTemporality, OtelMetricKind,
    OtelSignal, OtelValueMapping, ParseAsType, ProcessorOutput, RelayName, RouteConstruction,
    SchemaField, SchemaName, SignalingWireFormat, SqsFifoGroup, VhostName,
};
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SchemaSensitivity, SemanticScopePolicy,
    compile_program_with_options_for_bindings_with_sensitivity,
    infer_set_expr_types_for_bindings_with_udfs, lower_route_construction,
    lower_transforming_route, program::FunctionName,
};

use crate::registry::{
    error::{OtelMappingIssue, OtelMappingSection, OtelMappingSignal, RegistryError},
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
    let quiesce = ingestor.input.quiesce();
    if !ingestor.input.supports_quiesce(quiesce) {
        return Err(invalid(format!(
            "{} ingestors do not support ON QUIESCE {}",
            ingestor.input.source_label(),
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
    match &ingestor.input {
        IngestorInput::Transport(input) => {
            if let IngestSource::Mqtt { topic, .. } = &input.source
                && topic.is_empty()
            {
                return Err(invalid("MQTT topic filter must not be empty".to_string()));
            }
        }
        IngestorInput::Client(source) => validate_client_ingest_mode(&source.mode, invalid)?,
    }
    Ok(())
}

/// A client source's ACK timeout and retry policy are positive durations, and its retry ceiling
/// is at least its first backoff, so the plan and every producer read one valid policy.
fn validate_client_ingest_mode(
    mode: &ClientIngestMode,
    invalid: impl Fn(String) -> Report<RegistryError>,
) -> Result<(), Report<RegistryError>> {
    // A producer is told each policy duration in whole nanoseconds, so each must be one.
    let positive = |clause: &str, value: &str| {
        let parsed = humantime::parse_duration(value)
            .map_err(|error| invalid(format!("invalid {clause} duration '{value}': {error}")))?;
        if parsed.is_zero() {
            return Err(invalid(format!("{clause} must be greater than zero")));
        }
        if u64::try_from(parsed.as_nanos()).is_err() {
            return Err(invalid(format!(
                "{clause} {value} is longer than the longest duration a producer is told"
            )));
        }
        Ok(parsed)
    };
    positive("ACK TIMEOUT", &mode.ack_timeout)?;
    let backoff = positive("RETRY POLICY BACKOFF", &mode.retry_policy.backoff)?;
    let max_backoff = positive("RETRY POLICY MAX", &mode.retry_policy.max_backoff)?;
    if max_backoff < backoff {
        return Err(invalid(format!(
            "RETRY POLICY MAX {} is shorter than its BACKOFF {}",
            mode.retry_policy.max_backoff, mode.retry_policy.backoff
        )));
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

    match (emitter.sink.as_ref(), &emitter.body) {
        (EmitSink::Client { .. }, nervix_models::EmitterBody::Client) => {}
        (EmitSink::Client { .. }, _) => {
            return Err(invalid(
                "CLIENT emitter requires native Arrow output".to_string(),
            ));
        }
        (_, nervix_models::EmitterBody::Client) => {
            return Err(invalid(
                "native CLIENT output requires TO CLIENT SCHEMA".to_string(),
            ));
        }
        (
            EmitSink::Http { .. },
            nervix_models::EmitterBody::Codec { .. } | nervix_models::EmitterBody::WithoutBody,
        ) => {}
        (EmitSink::Http { .. }, nervix_models::EmitterBody::Values) => {
            return Err(invalid(
                "HTTP emitter requires ENCODE USING or WITHOUT BODY".to_string(),
            ));
        }
        (_, nervix_models::EmitterBody::Codec { .. }) if emitter.sink.requires_codec() => {}
        (_, nervix_models::EmitterBody::Values) if !emitter.sink.requires_codec() => {}
        (_, nervix_models::EmitterBody::WithoutBody) => {
            return Err(invalid(
                "WITHOUT BODY is only supported by HTTP emitters".to_string(),
            ));
        }
        (_, nervix_models::EmitterBody::Values) => {
            return Err(invalid(format!(
                "{} emitter requires ENCODE USING",
                emitter.sink.transport_label()
            )));
        }
        (_, nervix_models::EmitterBody::Codec { .. }) => {
            return Err(invalid(format!(
                "{} emitter does not support ENCODE USING",
                emitter.sink.transport_label()
            )));
        }
    }

    emitter
        .validate_batch()
        .map_err(|error| invalid(error.current_context().to_string()))?;

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
            validate_otel_mapping_contract(
                domain, identifier, signal, values, attributes, resource,
            )?;
        }
        EmitSink::Client { .. }
        | EmitSink::Http { .. }
        | EmitSink::Kafka { .. }
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

pub(in crate::registry) fn validate_direct_values_sensitivity(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    emitter: &CreateEmitter,
    input_schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    let mappings = emitter.sink.direct_value_mappings().collect::<Vec<_>>();
    if mappings.is_empty() {
        return Ok(());
    }

    let assignments = mappings
        .iter()
        .enumerate()
        .map(|(index, mapping)| {
            let field = FieldName::parse(&format!("c{index}"))
                .assured("c followed by decimal digits is a valid generated field name");
            Assignment {
                target: AssignmentTarget::bare(field),
                value: mapping.expression.clone(),
            }
        })
        .collect();
    let program = lower_route_construction(
        &RouteConstruction {
            assignments,
            ..RouteConstruction::default()
        },
        SemanticScopePolicy::read_write("input", "emitted"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("emitter VALUES is invalid: {reason}"),
        })
    })?;
    let empty_output =
        std::sync::Arc::new(arrow_schema::Schema::new(Vec::<arrow_schema::Field>::new()));
    let bindings = [
        CompileBinding::writeonly("emitted", empty_output),
        readonly_binding_for_internal_schema("input", input_schema),
        readonly_binding_for_internal_schema("message", input_schema),
    ];
    let udf_signatures = udf_compile_options(models, CompileOptions::default()).udf_signatures;
    let inferred = infer_set_expr_types_for_bindings_with_udfs(&program, bindings, udf_signatures)
        .map_err(|error| {
            let message = error.current_context().message.clone();
            error.change_context(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("emitter VALUES type inference failed: {}", message),
            })
        })?;
    for (index, field) in inferred.iter().enumerate() {
        if !field.sensitive {
            continue;
        }
        let mapping = mappings
            .get(index)
            .assured("one unique generated field is inferred for each VALUES mapping");
        return Err(Report::new(RegistryError::SensitiveEmitterValue {
            domain: domain.clone(),
            emitter: identifier.clone(),
            sink: emitter.sink.transport_label(),
            target: mapping.column.clone(),
        }));
    }
    Ok(())
}

/// Checks that the codec a batching emitter encodes through has a container its sink can publish
/// one batch in.
pub(in crate::registry) fn validate_emitter_batch_container(
    domain: &DomainName,
    identifier: &ModelName,
    emitter: &CreateEmitter,
    codec: &CreateCodec,
) -> Result<(), Report<RegistryError>> {
    let invalid = |reason: String| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason,
        })
    };
    if emitter.batch.is_none() {
        return Ok(());
    }
    let container = codec.wire_format.batch_container();
    if let CodecBatchContainer::Undeclared = container {
        return Err(invalid(format!(
            "a batching emitter requires protobuf codec '{}' to declare a BATCH MESSAGE, because \
             protobuf has no self-delimiting sequence",
            codec.name.as_str()
        )));
    }
    if emitter.sink.requires_batch_transformation() && container.transformation().is_none() {
        return Err(invalid(format!(
            "a batching {} emitter requires codec '{}' to declare an ON EMITTING BATCH \
             transformation, because one envelope carries at most one event",
            emitter.sink.transport_label(),
            codec.name.as_str()
        )));
    }
    Ok(())
}

fn validate_otel_mapping_contract(
    domain: &DomainName,
    identifier: &ModelName,
    signal: &OtelSignal,
    values: &[OtelValueMapping],
    attributes: &[OtelValueMapping],
    resource: &[OtelValueMapping],
) -> error_stack::Result<(), RegistryError> {
    /// The `VALUES` contract one OTEL signal imposes: what it may name, what it must name, and
    /// whether delta temporality adds `start_time` to the required keys.
    struct SignalContract {
        signal: OtelMappingSignal,
        allowed: &'static [&'static str],
        required: &'static [&'static str],
        delta: bool,
    }

    let SignalContract {
        signal,
        allowed,
        required,
        delta,
    } = match signal {
        OtelSignal::Logs => SignalContract {
            signal: OtelMappingSignal::Logs,
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
            signal: OtelMappingSignal::Traces,
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
                signal: OtelMappingSignal::MetricGauge,
                allowed: &["time", "start_time", "value"],
                required: &["time", "value"],
                delta: false,
            },
            OtelMetricKind::Sum { temporality, .. } => SignalContract {
                signal: OtelMappingSignal::MetricSum,
                allowed: &["time", "start_time", "value"],
                required: &["time", "value"],
                delta: temporality == OtelAggregationTemporality::Delta,
            },
            OtelMetricKind::Histogram { temporality } => SignalContract {
                signal: OtelMappingSignal::MetricHistogram,
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

    let invalid = |issue| {
        Report::new(RegistryError::InvalidOtelMapping {
            domain: domain.clone(),
            identifier: identifier.clone(),
            issue,
        })
    };

    let mut value_keys = HashSet::default();
    for mapping in values {
        if !allowed.contains(&mapping.column.as_str()) {
            return Err(invalid(OtelMappingIssue::UnsupportedValue {
                signal,
                key: mapping.column.clone(),
            }));
        }
        if !value_keys.insert(mapping.column.as_str()) {
            return Err(invalid(OtelMappingIssue::DuplicateValue {
                signal,
                key: mapping.column.clone(),
            }));
        }
    }
    for key in required {
        if !value_keys.contains(key) {
            return Err(invalid(OtelMappingIssue::MissingValue { signal, key }));
        }
    }
    if delta && !value_keys.contains("start_time") {
        return Err(invalid(OtelMappingIssue::MissingDeltaValue {
            signal,
            key: "start_time",
        }));
    }

    for (section, mappings) in [
        (OtelMappingSection::Attributes, attributes),
        (OtelMappingSection::Resource, resource),
    ] {
        let mut keys = HashSet::default();
        for mapping in mappings {
            if !keys.insert(mapping.column.as_str()) {
                return Err(invalid(OtelMappingIssue::DuplicateMetadata {
                    signal,
                    section,
                    key: mapping.column.clone(),
                }));
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
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "SQS FIFO GROUP expression requires an exact non-sensitive STRING value: {}",
                message
            ),
        })
    })?;

    Ok(())
}

/// Checks that an HTTP emitter's `METHOD` and `PATH` are exact, non-null and non-sensitive `STRING`
/// expressions over the scopes its body selection gives them.
pub(in crate::registry) fn validate_http_request_expressions(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    emitter: &CreateEmitter,
    input_schema: &CreateSchema,
    payload_schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    let EmitSink::Http { method, path, .. } = emitter.sink.as_ref() else {
        return Ok(());
    };

    let mut fields = Vec::with_capacity(2);
    let mut assignments = Vec::with_capacity(2);
    for (name, expression) in [("method", method), ("path", path)] {
        let target = FieldName::parse(name).assured("HTTP request field names are valid literals");
        fields.push(SchemaField {
            name: target.clone(),
            ty: ParseAsType::String,
            optional: false,
            sensitive: false,
        });
        assignments.push(Assignment {
            target: AssignmentTarget::bare(target),
            value: expression.clone(),
        });
    }
    let output_schema = CreateSchema {
        name: SchemaName::parse("http_request")
            .assured("HTTP request schema name is a valid literal"),
        fields,
    };
    let output_arrow_schema = arrow_schema_for_internal_schema(&output_schema);
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments,
            ..RouteConstruction::default()
        },
        SemanticScopePolicy::read_write("message", "http_request"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("HTTP METHOD or PATH expression is invalid: {reason}"),
        })
    })?;
    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", input_schema),
        writable_binding_for_internal_schema("http_request", &output_schema),
    ];
    let working_schema = if emitter.body.codec().is_some() {
        bindings.push(readonly_binding_for_internal_schema(
            "output",
            payload_schema,
        ));
        payload_schema
    } else {
        input_schema
    };
    bindings.push(readonly_binding_for_internal_schema(
        "message",
        working_schema,
    ));
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "message".to_string(),
        "output".to_string(),
        "http_request".to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "HTTP request expression",
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
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "HTTP METHOD and PATH require exact non-sensitive STRING values: {}",
                message
            ),
        })
    })?;

    Ok(())
}

/// The origin accepted by an HTTP emitter, once its client has an explicit, usable request timeout
/// and complete TLS identity settings. The returned URL is safe to use as the base for
/// request-target validation; its credentials, path, query and fragment have been ruled out.
pub(in crate::registry) fn validate_http_emitter_client(
    domain: &DomainName,
    identifier: &ModelName,
    client: &CreateClientHttp,
) -> Result<HttpOrigin, Report<RegistryError>> {
    let invalid = |reason: &'static str| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: reason.to_string(),
        })
    };
    let entry = |key: &str| {
        client
            .config
            .iter()
            .find(|entry| entry.key.eq_ignore_ascii_case(key))
            .map(|entry| entry.value.as_str())
    };

    let endpoint = entry("endpoint").ok_or_else(|| invalid("HTTP client requires endpoint"))?;
    let origin = HttpOrigin::parse(endpoint)
        .map_err(|_| invalid("HTTP client endpoint must be an http or https origin"))?;

    let timeout =
        entry("timeout_ms").ok_or_else(|| invalid("HTTP emitter client requires timeout_ms"))?;
    if timeout.is_empty() || !timeout.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid(
            "HTTP emitter client timeout_ms must be a positive schedulable integer",
        ));
    }
    let timeout = timeout.parse::<u64>().map_err(|_| {
        invalid("HTTP emitter client timeout_ms must be a positive schedulable integer")
    })?;
    if timeout == 0 || Duration::from_millis(timeout).as_nanos() > u128::from(u64::MAX / 2) {
        return Err(invalid(
            "HTTP emitter client timeout_ms must be a positive schedulable integer",
        ));
    }

    if entry("tls_cert_file").is_some() != entry("tls_key_file").is_some() {
        return Err(invalid(
            "HTTP client TLS requires both tls_cert_file and tls_key_file",
        ));
    }
    Ok(origin)
}

pub(in crate::registry) fn validate_http_literal_request_fields(
    domain: &DomainName,
    identifier: &ModelName,
    origin: &HttpOrigin,
    emitter: &CreateEmitter,
) -> Result<(), Report<RegistryError>> {
    struct KnownHeader {
        invocation: usize,
        name: HttpHeaderName,
        value: HttpHeaderValue,
    }

    let EmitSink::Http { method, path, .. } = emitter.sink.as_ref() else {
        return Ok(());
    };
    let invalid = |reason: String| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason,
        })
    };

    if let Expression::Literal(nervix_models::Literal::String(value)) = method {
        let body = match emitter.body {
            nervix_models::EmitterBody::Codec { .. } => HttpBodyMode::Codec,
            nervix_models::EmitterBody::WithoutBody => HttpBodyMode::WithoutBody,
            nervix_models::EmitterBody::Values => {
                return Err(invalid(
                    "HTTP publish method requires ENCODE USING or WITHOUT BODY".to_string(),
                ));
            }
            nervix_models::EmitterBody::Client => {
                return Err(invalid(
                    "HTTP publish method cannot use native CLIENT output".to_string(),
                ));
            }
        };
        HttpMethod::parse(value, body)
            .map_err(|error| invalid(format!("HTTP publish method {}", error.current_context())))?;
    }

    if let Expression::Literal(nervix_models::Literal::String(value)) = path {
        origin
            .target(value)
            .map_err(|_| invalid("HTTP publish path is invalid".to_string()))?;
    }

    let mut known_headers = Vec::new();
    let mut all_headers_known = true;
    for (index, invocation) in emitter.construction.invocations.iter().enumerate() {
        let invocation_index = index
            .checked_add(1)
            .assured("an index into an in-memory invocation vector is below isize::MAX");
        let [name, value] = invocation.arguments.as_slice() else {
            continue;
        };
        let name = if let Expression::Literal(nervix_models::Literal::String(name)) = name {
            Some(HttpHeaderName::parse(name).map_err(|_| {
                invalid(format!(
                    "HTTP invoke #{invocation_index} header name is invalid or reserved"
                ))
            })?)
        } else {
            all_headers_known = false;
            None
        };
        let value = if let Expression::Literal(nervix_models::Literal::String(value)) = value {
            Some(HttpHeaderValue::parse(value).map_err(|_| {
                invalid(format!(
                    "HTTP invoke #{invocation_index} header value is invalid"
                ))
            })?)
        } else {
            all_headers_known = false;
            None
        };
        if let (Some(name), Some(value)) = (name, value) {
            let size = name
                .as_str()
                .len()
                .checked_add(value.as_str().len())
                .ok_or_else(|| {
                    invalid(format!(
                        "HTTP invoke #{invocation_index} header exceeds 32 KiB"
                    ))
                })?;
            if size > 32 * 1024 {
                return Err(invalid(format!(
                    "HTTP invoke #{invocation_index} header exceeds 32 KiB"
                )));
            }
            known_headers.push(KnownHeader {
                invocation: invocation_index,
                name,
                value,
            });
        }
    }
    if all_headers_known {
        let mut headers = HttpApplicationHeaders::default();
        let final_invocation = known_headers.last().map(|header| header.invocation);
        for known in known_headers {
            headers.insert(known.name, known.value).map_err(|_| {
                invalid(format!(
                    "HTTP invoke #{} application headers exceed the count or 32 KiB limit",
                    known.invocation
                ))
            })?;
        }
        if headers.validate_total().is_err() {
            let invocation = final_invocation
                .assured("a nonzero total requires at least one literal header invocation");
            return Err(invalid(format!(
                "HTTP invoke #{invocation} application headers exceed the 32 KiB limit"
            )));
        }
    }
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
    input: &IngestorInput,
) -> Result<(), Report<RegistryError>> {
    let Some(filter_where) = filter_where else {
        return Ok(());
    };
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(filter_where.clone()),
            ..RouteConstruction::default()
        },
        SemanticScopePolicy::read_only("input"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER WHERE is invalid: {reason}"),
        })
    })?;
    if program_uses_header_reads(&parsed.inner) && !input.reads_headers() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} ingestors do not support read_header or read_headers",
                input.source_label()
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
    if program_uses_header_reads(&parsed.inner) && !ingestor.input.reads_headers() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} ingestors do not support read_header or read_headers",
                ingestor.input.source_label()
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
    if let Some(metadata_schema) = ingestor_filter_map_metadata_schema(&ingestor.input) {
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
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", message),
        })
    })?;

    Ok(output_schema.clone())
}

/// The typed metadata scope an ingestor's programs read as `metadata`: Kafka's position and
/// Syslog's peer address. Every other transport, and every client source, exposes none.
fn ingestor_filter_map_metadata_schema(input: &IngestorInput) -> Option<CreateSchema> {
    let IngestorInput::Transport(input) = input else {
        return None;
    };
    match &input.source {
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
    sink.capabilities().writes_headers()
}

pub(in crate::registry) fn effective_emitter_filter_map_schema(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    emitter: &nervix_models::CreateEmitter,
    input_schema: &CreateSchema,
    output_schema: &CreateSchema,
) -> Result<CreateSchema, Report<RegistryError>> {
    let codec_route = emitter.body.codec().is_some()
        || matches!(emitter.body, nervix_models::EmitterBody::Client);
    let has_output_construction =
        emitter.construction.inherit.is_some() || !emitter.construction.assignments.is_empty();
    let invalid_direct_construction = match emitter.body {
        nervix_models::EmitterBody::Codec { .. } => false,
        nervix_models::EmitterBody::Client => false,
        nervix_models::EmitterBody::WithoutBody => has_output_construction,
        nervix_models::EmitterBody::Values => {
            has_output_construction || !emitter.construction.invocations.is_empty()
        }
    };
    if invalid_direct_construction {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "emitter body selection does not support the retained construction".to_string(),
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
            SemanticScopePolicy::read_only("input"),
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
    let output_sensitivity = match (emitter.sink.as_ref(), codec_route) {
        (EmitSink::Http { .. } | EmitSink::Client { .. }, true) => SchemaSensitivity::default(),
        _ => schema_sensitivity_for_internal_schema(output_schema),
    };
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        output_sensitivity,
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
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", message),
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
#[path = "connector_tests.rs"]
mod tests;
