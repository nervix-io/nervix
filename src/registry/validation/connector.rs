//! What each external system requires of the model that talks to it.
//!
//! Layer: decisions.
//!
//! - **Owns.** The source and emitter contracts: the client an ingestor or emitter may use, the
//!   publishing and mapping rules of each emitter, the headers each side supports, the timestamp
//!   source an ingestor declares, and the hostnames and paths a listener claims.
//! - **Depends on.** The connector Models and the schema rules.
//! - **Must not know.** How a connector is instantiated or driven.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_jaq::StatefulJaqProgram;
use nervix_models::{
    Assignment, AssignmentTarget, ClientDeadline, ClientIngestMode, CodecBatchContainer,
    CreateClientHttp, CreateCodec, CreateEmitter, CreateIngestor, CreateSchema,
    CreateSignalingProtocol, DomainName, EmitterEntity, EndpointName, EnvelopeClauseForm,
    Expression, FieldName, HttpApplicationHeaders, HttpBodyMode, HttpEmitter, HttpHeaderName,
    HttpHeaderValue, HttpMethod, HttpOrigin, HttpRequestEmitter, IngestTimestampSource,
    IngestorInput, LiteralHeaderWrite, Model, ModelIndex, ModelKind, ModelName, NodeRef,
    OtelEmitter, OtelMappings, OtelMetricKind, OtelMetricsEmitter, ParseAsType, ProcessorOutput,
    RelayName, RouteConstruction, SchemaField, SchemaName, SignalingWireFormat, SqsEmitter,
    SqsFifoGroup, SqsQueueEmitter, VhostName, parse_duration_text,
};
use nervix_primitives::sync::StdArc;
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SchemaSensitivity, SemanticScopePolicy,
    compile_program_with_options_for_bindings_with_sensitivity,
    infer_set_expr_types_for_bindings_with_udfs, lower_route_construction,
    lower_transforming_route,
    program::{Expr, FunctionName, Literal, Program, SpannedNode},
};

use crate::{
    emitter_execution_plan::EmitterExecutionPlan,
    registry::{
        error::{
            ConnectorDeclarationIssue, ConnectorDurationSetting, EnvelopeValueSite,
            OtelMappingIssue, OtelMappingSection, OtelMappingSignal, RegistryError,
        },
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
    if let Some(nervix_models::IngestorEntity::Websockets(
        nervix_models::WebsocketsIngestor::Connection(source),
    )) = ingestor.input.transport_entity()
        && let Err(report) = nervix_models::WebsocketsUpgradeHeaders::parse(&source.headers)
    {
        let issue = report.current_context().clone();
        return Err(
            report.change_context(RegistryError::InvalidWebsocketsUpgradeHeaders {
                domain: domain.clone(),
                ingestor: identifier.clone(),
                issue,
            }),
        );
    }
    let invalid = |reason: String| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason,
        })
    };
    if let Some(nervix_models::IngestorEntity::Http(nervix_models::HttpIngestor::Request(source))) =
        ingestor.input.transport_entity()
    {
        HttpMethod::parse(&source.method, HttpBodyMode::WithoutBody)
            .map_err(|report| invalid(report.current_context().to_string()))?;
        let mut headers = HttpApplicationHeaders::default();
        for header in &source.headers {
            let name = HttpHeaderName::parse(&header.name)
                .map_err(|report| invalid(report.current_context().to_string()))?;
            let value = HttpHeaderValue::parse(&header.value)
                .map_err(|report| invalid(report.current_context().to_string()))?;
            headers
                .insert(name, value)
                .map_err(|report| invalid(report.current_context().to_string()))?;
        }
        headers
            .validate_total()
            .map_err(|report| invalid(report.current_context().to_string()))?;
    }
    let quiesce = ingestor.input.quiesce();
    if !ingestor.input.supports_quiesce(quiesce) {
        return Err(invalid(format!(
            "{} ingestors do not support ON QUIESCE {}",
            ingestor.input.source_phrase(),
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
            parse_duration_text(retry_after).map_err(|error| {
                invalid(format!(
                    "invalid quiesce REJECT RETRY AFTER duration '{retry_after}': {error}"
                ))
            })?;
        }
        nervix_models::IngestQuiesceMode::Suspend | nervix_models::IngestQuiesceMode::Drop => {}
    }
    if let IngestorInput::Client(source) = &ingestor.input {
        validate_client_ingest_mode(&source.mode, invalid)?;
    }
    if let Some(acknowledgement) = ingestor.input.transport_acknowledgement() {
        validate_transport_delivery(domain, identifier, acknowledgement)?;
    }
    Ok(())
}

impl ConnectorDurationSetting {
    /// Checks a duration without reading a clock: the shared deadline bound fits every
    /// monotonic timer Nervix schedules, including the first batch's gather deadline.
    fn parse(
        self,
        value: &str,
        domain: &DomainName,
        node: &nervix_models::NodeRef,
    ) -> error_stack::Result<std::time::Duration, RegistryError> {
        let invalid = |issue| RegistryError::InvalidConnectorDeclaration {
            domain: domain.clone(),
            node: node.clone(),
            issue,
        };
        let duration = parse_duration_text(value).change_context_lazy(|| {
            invalid(ConnectorDeclarationIssue::InvalidDuration {
                setting: self,
                value: value.to_string(),
            })
        })?;
        if duration.is_zero() && self != Self::BatchTimeout {
            return Err(Report::new(invalid(
                ConnectorDeclarationIssue::ZeroDuration { setting: self },
            )));
        }
        if duration > ClientDeadline::LONGEST {
            return Err(Report::new(invalid(
                ConnectorDeclarationIssue::UnschedulableDuration { setting: self },
            )));
        }
        Ok(duration)
    }
}

/// The acknowledgement view is shared by every transport entity, so no connector-specific
/// mode table or duration parser is needed at the registry boundary.
fn validate_transport_delivery(
    domain: &DomainName,
    identifier: &ModelName,
    acknowledgement: nervix_models::IngestAcknowledgement<'_>,
) -> error_stack::Result<(), RegistryError> {
    use nervix_models::IngestAcknowledgement;

    let node = nervix_models::NodeRef::new(nervix_models::ModelKind::Ingestor, identifier.clone());

    let retry = match acknowledgement {
        IngestAcknowledgement::Unacknowledged => return Ok(()),
        IngestAcknowledgement::Request { timeout, .. } => {
            ConnectorDurationSetting::AckTimeout.parse(timeout, domain, &node)?;
            return Ok(());
        }
        IngestAcknowledgement::Sequential { timeout, retry } => {
            ConnectorDurationSetting::AckTimeout.parse(timeout, domain, &node)?;
            retry
        }
        IngestAcknowledgement::Parallel {
            batch_timeout,
            timeout,
            retry,
            ..
        } => {
            ConnectorDurationSetting::BatchTimeout.parse(batch_timeout, domain, &node)?;
            ConnectorDurationSetting::AckTimeout.parse(timeout, domain, &node)?;
            retry
        }
    };
    let backoff = ConnectorDurationSetting::RetryBackoff.parse(&retry.backoff, domain, &node)?;
    let maximum =
        ConnectorDurationSetting::RetryMaximum.parse(&retry.max_backoff, domain, &node)?;
    if maximum < backoff {
        return Err(Report::new(RegistryError::InvalidConnectorDeclaration {
            domain: domain.clone(),
            node: node.clone(),
            issue: ConnectorDeclarationIssue::InvertedRetry,
        }));
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
        let parsed = parse_duration_text(value)
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

    let expected = emitter.connector.capabilities().body;
    if !expected.admits(&emitter.body) {
        return Err(Report::new(RegistryError::InvalidConnectorDeclaration {
            domain: domain.clone(),
            node: nervix_models::NodeRef::new(
                nervix_models::ModelKind::Emitter,
                identifier.clone(),
            ),
            issue: ConnectorDeclarationIssue::Body {
                entity: emitter.connector.kind(),
                expected,
            },
        }));
    }

    emitter
        .validate_batch()
        .map_err(|error| invalid(error.current_context().to_string()))?;
    validate_emitter_envelope_declaration(domain, identifier, emitter)?;

    let retry = emitter.connector.publishing().retry_policy();
    let backoff = parse_duration_text(&retry.backoff).map_err(|error| {
        invalid(format!(
            "invalid MODE RETRY POLICY BACKOFF '{}': {error}",
            retry.backoff
        ))
    })?;
    let max_backoff = parse_duration_text(&retry.max_backoff).map_err(|error| {
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

    if let Some(timeout) = emitter.connector.publishing().ack_timeout() {
        let timeout = parse_duration_text(timeout)
            .map_err(|error| invalid(format!("invalid MODE ACK TIMEOUT '{timeout}': {error}")))?;
        if timeout.is_zero() {
            return Err(invalid(
                "MODE ACK TIMEOUT must be greater than zero".to_string(),
            ));
        }
    }

    match emitter.connector.as_ref() {
        EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
            queue, fifo_group, ..
        })) => {
            let fifo_queue = queue.is_fifo();
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
        EmitterEntity::Otel(connector) => {
            validate_otel_mapping_contract(domain, identifier, connector)?;
        }
        EmitterEntity::Client(_)
        | EmitterEntity::Http(_)
        | EmitterEntity::Kafka(_)
        | EmitterEntity::Pulsar(_)
        | EmitterEntity::RabbitMq(_)
        | EmitterEntity::Redis(_)
        | EmitterEntity::Mqtt(_)
        | EmitterEntity::Nats(_)
        | EmitterEntity::ZeroMq(_)
        | EmitterEntity::Syslog(_)
        | EmitterEntity::Sentry(_)
        | EmitterEntity::Iceberg(_)
        | EmitterEntity::ClickHouse(_)
        | EmitterEntity::Postgres(_)
        | EmitterEntity::MySql(_)
        | EmitterEntity::MongoDb(_) => {}
    }

    validate_emitter_value_keys(domain, identifier, emitter)?;
    Ok(())
}

/// Each declared direct-value body names at least one distinct destination column. Protocol
/// entities validate their named fields before this common column rule.
fn validate_emitter_value_keys(
    domain: &DomainName,
    identifier: &ModelName,
    emitter: &CreateEmitter,
) -> error_stack::Result<(), RegistryError> {
    if emitter.connector.mapped_value_types().is_none() {
        return Ok(());
    }
    let mappings = emitter.connector.value_mappings();
    let invalid = |issue| {
        Report::new(RegistryError::InvalidConnectorDeclaration {
            domain: domain.clone(),
            node: nervix_models::NodeRef::new(
                nervix_models::ModelKind::Emitter,
                identifier.clone(),
            ),
            issue,
        })
    };
    if mappings.values.is_empty() {
        return Err(invalid(ConnectorDeclarationIssue::EmptyValues));
    }
    let mut keys = HashSet::default();
    for mapping in mappings.values {
        if let EmitterEntity::MongoDb(nervix_models::MongoDbEmitter::Collection(mongodb)) =
            emitter.connector.as_ref()
            && !mongodb.admits_value_key(&mapping.column)
        {
            return Err(invalid(ConnectorDeclarationIssue::MongoDbFieldName {
                key: mapping.column.clone(),
            }));
        }
        if !keys.insert(&mapping.column) {
            return Err(invalid(ConnectorDeclarationIssue::DuplicateValue {
                key: mapping.column.clone(),
            }));
        }
    }
    Ok(())
}

pub(in crate::registry) fn validate_direct_value_mappings(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    emitter: &CreateEmitter,
    input_schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    let mappings = emitter
        .connector
        .value_mappings()
        .declared()
        .collect::<Vec<_>>();
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
                value: mapping.mapping.expression.clone(),
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
    let empty_output = nervix_primitives::sync::StdArc::new(arrow_schema::Schema::new(Vec::<
        arrow_schema::Field,
    >::new(
    )));
    let bindings = [
        CompileBinding::writeonly("emitted", empty_output),
        readonly_binding_for_internal_schema("input", input_schema),
        readonly_binding_for_internal_schema("message", input_schema),
    ];
    let udf_signatures = udf_compile_options(models, CompileOptions::default()).udf_signatures;
    let inferred = {
        let mapping_input =
            infer_set_expr_types_for_bindings_with_udfs(&program, bindings, udf_signatures);
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let message = error.current_context().message.clone();
                error.change_context(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!("emitter VALUES type inference failed: {}", message),
                })
            }),
        }
    }?;
    let invalid = |issue| {
        Report::new(RegistryError::InvalidConnectorDeclaration {
            domain: domain.clone(),
            node: nervix_models::NodeRef::new(
                nervix_models::ModelKind::Emitter,
                identifier.clone(),
            ),
            issue,
        })
    };
    let types = emitter.connector.mapped_value_types().ok_or_else(|| {
        invalid(ConnectorDeclarationIssue::MissingMappedValueContract {
            entity: emitter.connector.kind(),
        })
    })?;
    for (index, field) in inferred.iter().enumerate() {
        let mapping = mappings
            .get(index)
            .assured("one unique generated field is inferred for each VALUES mapping");
        let expected = types
            .value_type(mapping.section, &mapping.mapping.column)
            .ok_or_else(|| {
                invalid(ConnectorDeclarationIssue::UnsupportedMappedKey {
                    section: mapping.section,
                    key: mapping.mapping.column.clone(),
                })
            })?;
        if !expected.admits(&field.data_type) {
            return Err(invalid(ConnectorDeclarationIssue::MappedType {
                section: mapping.section,
                key: mapping.mapping.column.clone(),
                expected,
                actual: field.data_type.clone(),
            }));
        }
        if field.sensitive {
            return Err(Report::new(RegistryError::SensitiveEmitterValue {
                domain: domain.clone(),
                emitter: identifier.clone(),
                connector: emitter.connector.transport_label(),
                target: mapping.mapping.column.clone(),
            }));
        }
    }
    Ok(())
}

/// Checks that the codec a batching emitter encodes through has a container its emitter can publish
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
    if emitter.connector.requires_batch_transformation() && container.transformation().is_none() {
        return Err(invalid(format!(
            "a batching {} emitter requires codec '{}' to declare an ON EMITTING BATCH \
             transformation, because one envelope carries at most one event",
            emitter.connector.transport_label(),
            codec.name.as_str()
        )));
    }
    Ok(())
}

fn validate_otel_mapping_contract(
    domain: &DomainName,
    identifier: &ModelName,
    connector: &OtelEmitter,
) -> error_stack::Result<(), RegistryError> {
    let signal = match connector {
        OtelEmitter::Logs(_) => OtelMappingSignal::Logs,
        OtelEmitter::Traces(_) => OtelMappingSignal::Traces,
        OtelEmitter::Metrics(OtelMetricsEmitter { metric, .. }) => match metric.kind {
            OtelMetricKind::Gauge => OtelMappingSignal::MetricGauge,
            OtelMetricKind::Sum { .. } => OtelMappingSignal::MetricSum,
            OtelMetricKind::Histogram { .. } => OtelMappingSignal::MetricHistogram,
        },
    };

    let invalid = |issue| {
        Report::new(RegistryError::InvalidOtelMapping {
            domain: domain.clone(),
            identifier: identifier.clone(),
            issue,
        })
    };

    let OtelMappings {
        values,
        attributes,
        resource,
        scope: _,
    } = connector.mappings();
    let types = nervix_models::EmitterEntityBody::mapped_value_types(connector)
        .verified("every OTEL signal declares its mapped field types");
    let mut value_expressions = HashMap::default();
    for mapping in values {
        if types
            .value_type(nervix_models::MappedValueSection::Values, &mapping.column)
            .is_none()
        {
            return Err(invalid(OtelMappingIssue::UnsupportedValue {
                signal,
                key: mapping.column.clone(),
            }));
        }
        if value_expressions
            .insert(mapping.column.as_str(), &mapping.expression)
            .is_some()
        {
            return Err(invalid(OtelMappingIssue::DuplicateValue {
                signal,
                key: mapping.column.clone(),
            }));
        }
    }
    for field in connector.value_fields() {
        if let Some(expression) = value_expressions.get(field.key) {
            if !field.rule.admits_literal(expression) {
                return Err(invalid(OtelMappingIssue::LiteralValue {
                    signal,
                    key: field.key,
                    rule: field.rule,
                }));
            }
            continue;
        }
        if !field.presence.is_required(connector.is_delta()) {
            continue;
        }
        let issue = match field.presence {
            nervix_models::OtelValuePresence::Delta => OtelMappingIssue::MissingDeltaValue {
                signal,
                key: field.key,
            },
            nervix_models::OtelValuePresence::Required
            | nervix_models::OtelValuePresence::Optional => OtelMappingIssue::MissingValue {
                signal,
                key: field.key,
            },
        };
        return Err(invalid(issue));
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
    let EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
        fifo_group: Some(SqsFifoGroup::Expression(expression)),
        ..
    })) = emitter.connector.as_ref()
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
    {
        let mapping_input = compile_program_with_options_for_bindings_with_sensitivity(
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
        );
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let message = error.current_context().message.clone();
                error.change_context(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "SQS FIFO GROUP expression requires an exact non-sensitive STRING value: \
                         {}",
                        message
                    ),
                })
            }),
        }
    }?;

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
    let EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter { method, path, .. })) =
        emitter.connector.as_ref()
    else {
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
    {
        let mapping_input = compile_program_with_options_for_bindings_with_sensitivity(
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
        );
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let message = error.current_context().message.clone();
                error.change_context(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "HTTP METHOD and PATH require exact non-sensitive STRING values: {}",
                        message
                    ),
                })
            }),
        }
    }?;

    Ok(())
}

/// Checks that an emitter's entity declares every envelope clause and property the emitter
/// writes, that it sets each property once, and that each duration it writes is one a transport
/// can be given.
fn validate_emitter_envelope_declaration(
    domain: &DomainName,
    identifier: &ModelName,
    emitter: &CreateEmitter,
) -> Result<(), Report<RegistryError>> {
    let node = NodeRef::new(ModelKind::Emitter, identifier.clone());
    if let Some(violation) = emitter
        .envelope
        .declaration_violation(emitter.connector.kind())
    {
        return Err(Report::new(RegistryError::InvalidConnectorDeclaration {
            domain: domain.clone(),
            node,
            issue: ConnectorDeclarationIssue::Envelope(violation),
        }));
    }
    if let Some(duration) = &emitter.envelope.deliver_after {
        ConnectorDurationSetting::DeliverAfter.parse(duration, domain, &node)?;
    }
    if let Some(duration) = &emitter.envelope.delay {
        ConnectorDurationSetting::Delay.parse(duration, domain, &node)?;
    }
    Ok(())
}

/// One value an emitter's envelope computes for every record: where it is written, the
/// expression that computes it and the exact type it has.
struct EnvelopeValue<'a> {
    site: EnvelopeValueSite,
    expression: &'a Expression,
    expected: ParseAsType,
}

/// The values an emitter's envelope computes, in the order its statement writes them.
///
/// A property its entity does not declare has no type to check against here; the publishing
/// contract refuses it.
fn envelope_values(emitter: &CreateEmitter) -> Vec<EnvelopeValue<'_>> {
    let mut values = Vec::new();
    for clause in emitter.envelope.clauses() {
        let EnvelopeClauseForm::Expression(expected) = clause.kind().form() else {
            continue;
        };
        let expression = clause
            .expression()
            .assured("a clause whose form is an expression holds the expression it evaluates");
        values.push(EnvelopeValue {
            site: EnvelopeValueSite::Clause(clause.kind()),
            expression,
            expected,
        });
    }
    let declared = emitter.connector.capabilities();
    for property in &emitter.envelope.properties {
        let Some(declaration) = declared.property(&property.name) else {
            continue;
        };
        values.push(EnvelopeValue {
            site: EnvelopeValueSite::Property(property.name.clone()),
            expression: &property.expression,
            expected: declaration.ty.into(),
        });
    }
    values
}

/// Checks that every expression an emitter's envelope writes computes an exact, non-null,
/// non-sensitive value of the type its clause or property declares.
///
/// An envelope expression reads what a header write reads: the source record as `input`, the
/// record the emitter publishes as `message`, its constructed output as `output` where a route
/// constructs one, and the materialized state and lookups the emitter may reach.
pub(in crate::registry) fn validate_emitter_envelope_expressions(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    emitter: &CreateEmitter,
    input_schema: &CreateSchema,
    output_schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    let values = envelope_values(emitter);
    if values.is_empty() {
        return Ok(());
    }
    let scopes = EnvelopeScopes {
        input: input_schema,
        output: EmitterExecutionPlan::constructs_output(emitter).then_some(output_schema),
    };
    for value in values {
        validate_envelope_value(domain, identifier, models, &scopes, value)?;
    }
    Ok(())
}

/// The schemas an envelope expression reads.
struct EnvelopeScopes<'a> {
    /// The source record, read as `input`.
    input: &'a CreateSchema,
    /// The output a route constructs, read as `output` and `message`. Without one, `message` is
    /// the source record.
    output: Option<&'a CreateSchema>,
}

/// The internal namespace an envelope value is written into while it is checked.
const ENVELOPE_NAMESPACE: &str = "envelope";

fn validate_envelope_value(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    scopes: &EnvelopeScopes<'_>,
    value: EnvelopeValue<'_>,
) -> Result<(), Report<RegistryError>> {
    let refused = || RegistryError::InvalidConnectorDeclaration {
        domain: domain.clone(),
        node: NodeRef::new(ModelKind::Emitter, identifier.clone()),
        issue: ConnectorDeclarationIssue::EnvelopeValue {
            site: value.site.clone(),
            expected: value.expected.clone(),
        },
    };
    let target = FieldName::parse("value").assured("value is a valid field literal");
    let output_schema = CreateSchema {
        name: SchemaName::parse(ENVELOPE_NAMESPACE)
            .assured("the envelope namespace is a valid schema name"),
        fields: vec![SchemaField {
            name: target.clone(),
            ty: value.expected.clone(),
            optional: false,
            sensitive: false,
        }],
    };
    let output_arrow_schema = arrow_schema_for_internal_schema(&output_schema);
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments: vec![Assignment {
                target: AssignmentTarget::bare(target),
                value: value.expression.clone(),
            }],
            ..RouteConstruction::default()
        },
        SemanticScopePolicy::read_write("message", ENVELOPE_NAMESPACE),
    )
    .change_context_lazy(refused)?;
    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", scopes.input),
        writable_binding_for_internal_schema(ENVELOPE_NAMESPACE, &output_schema),
    ];
    let message = match scopes.output {
        Some(output) => {
            bindings.push(readonly_binding_for_internal_schema("output", output));
            output
        }
        None => scopes.input,
    };
    bindings.push(readonly_binding_for_internal_schema("message", message));
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "message".to_string(),
        "output".to_string(),
        ENVELOPE_NAMESPACE.to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "emitter envelope",
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
    .change_context_lazy(refused)?;
    Ok(())
}

/// Refuses a Kafka client whose CONFIG sets a key Nervix decides for every Kafka consumer. The
/// key is matched without regard to ASCII case, so no spelling of it reaches the driver.
/// The origin accepted by an HTTP emitter, once its client has an explicit, usable request timeout
/// and complete TLS identity settings. The returned URL is safe to use as the base for
/// request-target validation; its credentials, path, query and fragment have been ruled out.
pub(in crate::registry) fn validate_http_emitter_client(
    domain: &DomainName,
    identifier: &ModelName,
    client: &CreateClientHttp,
) -> Result<HttpOrigin, Report<RegistryError>> {
    let invalid = |reason: &str| {
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
    let origin = HttpOrigin::parse(endpoint).map_err(|cause| {
        cause.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "HTTP client endpoint must be an http or https origin".to_string(),
        })
    })?;

    let kind = <nervix_models::HttpRequestEmitter as nervix_models::EmitterEntityModel>::KIND;
    let timeout = entry("timeout_ms")
        .ok_or_else(|| invalid(&format!("{kind} emitter client requires timeout_ms")))?;
    ClientDeadline::parse(timeout).change_context(RegistryError::InvalidModel {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!("{kind} emitter client timeout_ms must be a positive schedulable integer"),
    })?;

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

    let EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter { method, path, .. })) =
        emitter.connector.as_ref()
    else {
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
        HttpMethod::parse(value, body).map_err(|cause| {
            cause.change_context(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "HTTP publish method is invalid".to_string(),
            })
        })?;
    }

    if let Expression::Literal(nervix_models::Literal::String(value)) = path {
        origin.target(value).map_err(|cause| {
            cause.change_context(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "HTTP publish path is invalid".to_string(),
            })
        })?;
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
            Some(HttpHeaderName::parse(name).map_err(|cause| {
                cause.change_context(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "HTTP invoke #{invocation_index} header name is invalid or reserved"
                    ),
                })
            })?)
        } else {
            all_headers_known = false;
            None
        };
        let value = if let Expression::Literal(nervix_models::Literal::String(value)) = value {
            Some(HttpHeaderValue::parse(value).map_err(|cause| {
                cause.change_context(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!("HTTP invoke #{invocation_index} header value is invalid"),
                })
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
            headers.insert(known.name, known.value).map_err(|cause| {
                cause.change_context(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "HTTP invoke #{} application headers exceed the count or 32 KiB limit",
                        known.invocation
                    ),
                })
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
        let mapping_input = StatefulJaqProgram::compile(program).map(|_| ());
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err(invalid(format!(
                "signaling protocol {clause} program #{} is invalid: {error}",
                index + 1
            ))),
        }
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

    parse_duration_text(&protocol.on_connect.timeout).map_err(|error| {
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
                input.source_phrase()
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
                ingestor.input.source_phrase()
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
    let metadata_scope = ingestor.input.entity_kind().capabilities().metadata;
    if let Some(metadata_schema) = metadata_scope.arrow_schema() {
        bindings.push(CompileBinding::readonly(
            "metadata",
            StdArc::new(metadata_schema),
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

    {
        let mapping_input = compile_program_with_options_for_bindings_with_sensitivity(
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
        );
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let message = error.current_context().message.clone();
                error.change_context(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!("FILTER-MAP compile failed: {}", message),
                })
            }),
        }
    }?;

    Ok(output_schema.clone())
}

fn emitter_entity_supports_headers(connector: &EmitterEntity) -> bool {
    connector.capabilities().writes_headers()
}

/// Refuses an emitter whose connector keeps one value per header name and whose `INVOKE` clause writes
/// one literal name twice: on every record the later write would replace the earlier one, so the
/// earlier write could never reach a message. Names compare as the emitter's header model records
/// them, and a name an expression computes is left to the emitter at run time.
fn refuse_replaced_header_writes(
    domain: &DomainName,
    identifier: &ModelName,
    emitter: &CreateEmitter,
    program: &SpannedNode<Program>,
) -> Result<(), Report<RegistryError>> {
    let Some(model) = emitter.connector.capabilities().headers else {
        return Ok(());
    };
    let mut writes = Vec::new();
    for (index, invocation) in program.inner.invoke.iter().enumerate() {
        if invocation.inner.function != FunctionName::WriteHeader {
            continue;
        }
        let Some(name) = invocation.inner.args.first() else {
            continue;
        };
        let Expr::Literal(Literal::String(name)) = &name.inner else {
            continue;
        };
        let position = index
            .checked_add(1)
            .assured("an index into an in-memory invocation vector is below usize::MAX");
        writes.push(LiteralHeaderWrite { position, name });
    }
    let Some(replaced) = model.replaced_write(writes) else {
        return Ok(());
    };
    Err(Report::new(RegistryError::ReplacedHeaderWrite {
        domain: domain.clone(),
        emitter: identifier.clone(),
        connector: emitter.connector.kind(),
        header: model.names.recorded(replaced.later.name).into_owned(),
        earlier: replaced.earlier.position,
        later: replaced.later.position,
    }))
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
    if invokes_write_header && !emitter_entity_supports_headers(&emitter.connector) {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} emitters do not support write_header",
                emitter.connector.transport_label()
            ),
        }));
    }
    refuse_replaced_header_writes(domain, identifier, emitter, &parsed)?;

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
    let output_sensitivity = match (emitter.connector.as_ref(), codec_route) {
        (EmitterEntity::Http(_) | EmitterEntity::Client(_), true) => SchemaSensitivity::default(),
        _ => schema_sensitivity_for_internal_schema(output_schema),
    };
    {
        let mapping_input = compile_program_with_options_for_bindings_with_sensitivity(
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
        );
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let message = error.current_context().message.clone();
                error.change_context(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!("FILTER-MAP compile failed: {}", message),
                })
            }),
        }
    }?;

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
