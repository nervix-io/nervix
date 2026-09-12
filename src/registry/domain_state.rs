//! One domain's stored Models and the graph they validate into.
//!
//! Layer: decisions.
//!
//! - **Owns.** Building a domain's active graph from its Models: every node, every edge, and
//!   every check that must hold before the graph is allowed to exist.
//! - **Depends on.** The validation rules, the graph types, and the vocabulary.
//! - **Must not know.** How the graph is stored, placed or executed.

use ahash::{HashMap, HashMapExt};
use error_stack::Report;
use meticulous::{OptionExt, ResultExt};
use nervix_models::{
    DomainName, EndpointType, IngestSource, Model, ModelIndex, ModelKind, NodeRef, RelayName,
};
use petgraph::graph::DiGraph;
use triomphe::Arc;

pub(in crate::registry) use crate::registry::validation::{
    processor::{
        ModelValidationContext, ProcessorOutputSchemaCompatibility, processor_first_input_relay,
    },
    schema::{SensitivityCompatibility, ensure_internal_schema_compatibility, expect_schema_model},
};
use crate::registry::{
    error::RegistryError,
    graph::{ActiveGraph, ActiveNode, EdgeKind, expect_kind, expect_node, has_required_by_cycle},
    placement::PlacementAnalysis,
    storage::StoredModelRecord,
    validation::{
        branching::{
            add_output_branch_dependency_edges, branch_model, branching_schema_fields,
            ensure_output_branch, infer_stream_branchings, model_branch_selection,
            relay_declared_branch, relay_declared_branch_schema, resolved_branch_selection,
            validate_branch_model, validate_processing_branch_selections,
        },
        connector::{
            effective_emitter_filter_map_schema, effective_ingestor_output_filter_map_schema,
            ensure_ingestor_timestamp_source, ensure_signaling_protocol_is_valid,
            validate_emitter_publishing_contract, validate_endpoint_paths,
            validate_ingestor_filter_where_for_internal_schemas, validate_ingestor_source,
            validate_sqs_fifo_group_expression, validate_vhost_hostnames,
        },
        expression::add_udf_dependency_edges,
        materialized_state::{
            add_materialized_state_dependency_edges, ensure_stream_is_materialized,
            model_materialized_state_dependencies, validate_declared_materialized_state_references,
        },
        message_error::{
            add_message_error_policy_edges, add_output_message_error_policy_edges,
            validate_model_message_error_policies,
        },
        processor::{
            add_correlation_timeout_action_edges, add_processor_output_edges,
            effective_processor_output_filter_map_schema, ensure_deduplicator_key_compiles,
            ensure_inferencer_input_mappings, ensure_lookup_key_field_exists,
            ensure_processor_output_flush_policies, ensure_processor_output_schemas,
            ensure_wasm_processor_output_schemas, ensure_window_processor_output_schemas,
            parse_window_bound_duration, processor_input_schemas, validate_correlator,
            validate_correlator_input_sides_do_not_overlap, validate_correlator_output,
            validate_filter_where_for_internal_schemas, validate_from_where_for_internal_schemas,
            validate_generator_output, validate_inferencer_output_filter_map,
            validate_scoped_from_where_for_internal_schemas,
        },
        schema::{
            ensure_internal_schema_compatibility_with_policy, ensure_schema_has_fields,
            ensure_wire_schema_has_fields,
        },
        vm::{BRANCH_NAMESPACE, INGEST_MESSAGE_NAMESPACE},
        wire::{
            DomainModelWireSchemas, ensure_codec_schema_compatibility,
            ensure_codec_supports_decoding, ensure_codec_supports_encoding, expect_codec_model,
            schema_for_ack_model, schema_for_codec_model,
        },
    },
};
#[derive(Debug, Clone)]
pub(in crate::registry) struct RegistryState {
    pub(in crate::registry) domains: HashMap<DomainName, DomainState>,
}

impl RegistryState {
    pub(in crate::registry) fn from_records(
        records: Vec<StoredModelRecord>,
    ) -> Result<Self, Report<RegistryError>> {
        let mut grouped = HashMap::<DomainName, ModelIndex>::new();

        for record in records {
            grouped
                .entry(record.domain)
                .or_default()
                .insert(record.model);
        }

        let mut domains = HashMap::new();
        for (domain, models) in grouped {
            let state = DomainState::build(&domain, &models)?;
            domains.insert(domain, state);
        }

        Ok(Self { domains })
    }
}

#[derive(Debug, Clone)]
pub(in crate::registry) struct DomainState {
    pub(in crate::registry) models: ModelIndex,
    pub(in crate::registry) graph: ActiveGraph,
}

impl DomainState {
    pub(in crate::registry) fn build(
        domain: &DomainName,
        models: &ModelIndex,
    ) -> Result<Self, Report<RegistryError>> {
        let mut graph = DiGraph::<ActiveNode, EdgeKind>::new();
        let mut indices = HashMap::new();

        for (key, model) in models {
            let (effective_branching, effective_branching_schema) = match model {
                Model::Relay(relay) => {
                    if let Some(branch_ref) = relay.branching.branch() {
                        let branch = branch_model(domain, &key.identifier, models, branch_ref)?;
                        (
                            Some(branching_schema_fields(
                                domain,
                                &key.identifier,
                                models,
                                &branch.schema,
                            )?),
                            Some(branch.schema.clone()),
                        )
                    } else {
                        (Some(Vec::new()), None)
                    }
                }
                _ => {
                    if let Some(branched_by) = model_branch_selection(model) {
                        let branching = resolved_branch_selection(
                            domain,
                            &key.identifier,
                            models,
                            branched_by,
                        )?;
                        (Some(branching.fields), branching.schema)
                    } else {
                        (None, None)
                    }
                }
            };
            let node = ActiveNode {
                identifier: key.identifier.clone(),
                kind: key.kind,
                config: Arc::new(model.clone()),
                effective_branching,
                effective_branching_schema,
            };
            let index = graph.add_node(node);
            indices.insert(key.clone(), index);
        }

        for (key, model) in models {
            let identifier = &key.identifier;
            let validation = ModelValidationContext {
                domain,
                identifier,
                models,
            };
            let source = *indices
                .get(key)
                .verified("the pass above added a graph node for every model in this map");

            if let Some(branched_by) = model_branch_selection(model)
                && let Some(branch_ref) = branched_by.branch_ref()
            {
                let branch = expect_kind(
                    domain,
                    identifier,
                    models,
                    &indices,
                    branch_ref,
                    ModelKind::Branch,
                )?;
                graph.add_edge(branch, source, EdgeKind::RequiredBy);
            }
            match model {
                Model::Ingestor(ingestor) => add_output_branch_dependency_edges(
                    domain,
                    identifier,
                    models,
                    &indices,
                    &mut graph,
                    source,
                    &ingestor.output_routes,
                )?,
                Model::Reingestor(reingestor) => add_output_branch_dependency_edges(
                    domain,
                    identifier,
                    models,
                    &indices,
                    &mut graph,
                    source,
                    &reingestor.output_routes,
                )?,
                _ => {}
            }

            let materialized_state = model_materialized_state_dependencies(model);
            add_materialized_state_dependency_edges(
                domain,
                identifier,
                models,
                &indices,
                &mut graph,
                source,
                materialized_state,
            )?;
            validate_declared_materialized_state_references(
                domain,
                identifier,
                model,
                materialized_state,
            )?;
            add_udf_dependency_edges(domain, identifier, model, &indices, &mut graph, source)?;

            match model {
                Model::Schema(schema) => {
                    ensure_schema_has_fields(domain, identifier, &schema.fields, "schema")?;
                }
                Model::Branch(branch) => {
                    let branch_schema = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &branch.schema,
                        ModelKind::Schema,
                    )?;
                    graph.add_edge(branch_schema, source, EdgeKind::RequiredBy);
                    validate_branch_model(domain, identifier, models, branch)?;
                }
                Model::WireJsonSchema(schema) | Model::WireCborSchema(schema) => {
                    ensure_wire_schema_has_fields(domain, identifier, schema)?;
                }
                Model::WireAvroSchema(schema) => {
                    ensure_wire_schema_has_fields(domain, identifier, schema)?;
                }
                Model::ClientKafka(_)
                | Model::ClientPulsar(_)
                | Model::ClientHttp(_)
                | Model::ClientSentry(_)
                | Model::ClientOtel(_)
                | Model::ClientPrometheus(_)
                | Model::ClientRabbitMq(_)
                | Model::ClientRedis(_)
                | Model::ClientMqtt(_)
                | Model::ClientNats(_)
                | Model::ClientZeroMq(_)
                | Model::ClientSqs(_)
                | Model::ClientClickHouse(_)
                | Model::ClientPostgres(_)
                | Model::ClientMySql(_)
                | Model::ClientMongoDb(_)
                | Model::ClientS3(_)
                | Model::ClientGcs(_)
                | Model::ClientAzureBlob(_)
                | Model::ClientIcebergRest(_)
                | Model::ClientSyslog(_)
                | Model::Vhost(_) => {}
                Model::Udf(udf) => {
                    if !udf.has_valid_code_hash() {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "UDF source does not match its content hash".to_string(),
                        }));
                    }
                    if udf.arguments.is_empty() || udf.arguments.len() > 8 {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "UDF arity must be between 1 and 8".to_string(),
                        }));
                    }
                    if udf.code.len() > 64 * 1024 {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "UDF code exceeds the 64 KiB limit".to_string(),
                        }));
                    }
                }
                Model::ClientWebsockets(client) => {
                    if let Some(signaling_protocol) = client.signaling_protocol.as_ref() {
                        let signaling_protocol = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            signaling_protocol,
                            ModelKind::SignalingProtocol,
                        )?;
                        graph.add_edge(signaling_protocol, source, EdgeKind::RequiredBy);
                    }
                }
                Model::Placement(_) => {}
                Model::SignalingProtocol(protocol) => {
                    ensure_signaling_protocol_is_valid(domain, identifier, protocol)?;
                }
                Model::Generator(generator) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &generator.output_routes,
                    )?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &generator.output_routes,
                    )?;
                    let input = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &generator.materialized_relay,
                        ModelKind::Relay,
                    )?;
                    graph.add_edge(input, source, EdgeKind::RequiredBy);
                    graph.add_edge(input, source, EdgeKind::SendsTo);
                    ensure_stream_is_materialized(
                        domain,
                        identifier,
                        models,
                        &generator.materialized_relay,
                    )?;
                    for output in generator.output_routes.outputs() {
                        validate_generator_output(domain, identifier, models, generator, output)?;
                    }
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &generator.output_routes,
                    )?;
                }
                Model::Inferencer(processor) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &processor.output_routes,
                    )?;
                    processor.execution_mode().map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &processor.from,
                        "inferencer input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &processor.from,
                        "inferencer input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &processor.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        processor.filter_where.as_ref(),
                    )?;
                    ensure_inferencer_input_mappings(
                        domain,
                        identifier,
                        models,
                        processor,
                        &input_schemas,
                    )?;
                    for output in processor.output_routes.outputs() {
                        let consumer_schema =
                            schema_for_ack_model(domain, identifier, models, &output.relay)?;
                        validate_inferencer_output_filter_map(
                            domain,
                            identifier,
                            models,
                            output,
                            consumer_schema,
                            branch_schema,
                            processor,
                        )?;
                    }
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.output_routes,
                    )?;
                }
                Model::WasmProcessor(processor) => {
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.output_routes,
                    )?;
                    let input_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &processor.from,
                        "wasm processor input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &processor.from,
                        "wasm processor input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &processor.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        processor.filter_where.as_ref(),
                    )?;
                    ensure_wasm_processor_output_schemas(
                        domain,
                        identifier,
                        models,
                        processor,
                        &input_schemas,
                        branch_schema,
                    )?;

                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.output_routes,
                    )?;
                }
                Model::Codec(codec) => {
                    if let Some(reference) = codec.wire_format.wire_schema_reference() {
                        let wire_schema =
                            expect_node(domain, identifier, models, &indices, &reference)?;
                        graph.add_edge(wire_schema, source, EdgeKind::RequiredBy);
                    }
                    let schema = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &codec.schema,
                        ModelKind::Schema,
                    )?;
                    graph.add_edge(schema, source, EdgeKind::RequiredBy);

                    let schema_model =
                        expect_schema_model(domain, identifier, models, &codec.schema)?;
                    let wire_schemas = DomainModelWireSchemas {
                        domain,
                        identifier,
                        models,
                    };
                    let wire_format = codec.wire_format.resolve(&wire_schemas)?;
                    ensure_codec_schema_compatibility(
                        domain,
                        identifier,
                        wire_format,
                        schema_model,
                        &codec.encoding_rules,
                    )?;
                }
                Model::Ingestor(ingestor) => {
                    validate_ingestor_source(domain, identifier, ingestor)?;
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &ingestor.output_routes,
                    )?;

                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &ingestor.output_routes,
                    )?;

                    let codec = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &ingestor.decode_using_codec,
                        ModelKind::Codec,
                    )?;
                    graph.add_edge(codec, source, EdgeKind::RequiredBy);
                    let codec_model = expect_codec_model(
                        domain,
                        identifier,
                        models,
                        &ingestor.decode_using_codec,
                    )?;
                    ensure_codec_supports_decoding(domain, identifier, codec_model)?;

                    match &ingestor.source {
                        IngestSource::Http { client, .. }
                        | IngestSource::Kafka { client, .. }
                        | IngestSource::Pulsar { client, .. }
                        | IngestSource::Prometheus { client, .. }
                        | IngestSource::RabbitMq { client, .. }
                        | IngestSource::RedisPubSub { client, .. }
                        | IngestSource::Mqtt { client, .. }
                        | IngestSource::Nats { client, .. }
                        | IngestSource::ZeroMq { client, .. }
                        | IngestSource::Sqs { client, .. }
                        | IngestSource::Websockets { client, .. } => {
                            let client = expect_kind(
                                domain,
                                identifier,
                                models,
                                &indices,
                                client,
                                ModelKind::Client,
                            )?;
                            graph.add_edge(client, source, EdgeKind::RequiredBy);
                        }
                        IngestSource::Syslog { client, .. } => {
                            let client_node = expect_kind(
                                domain,
                                identifier,
                                models,
                                &indices,
                                client,
                                ModelKind::Client,
                            )?;
                            let client_model = models
                                .get(&NodeRef::new(ModelKind::Client, client.clone()))
                                .verified(
                                    "expect_kind above resolved this client reference against the \
                                     same model set",
                                );
                            if let Model::ClientSyslog(_) = client_model {
                            } else {
                                return Err(Report::new(RegistryError::InvalidModel {
                                    domain: domain.as_str().to_string(),
                                    identifier: identifier.as_str().to_string(),
                                    reason: format!(
                                        "SYSLOG ingestor requires a SYSLOG client, found {} \
                                         client '{}'",
                                        client_model.client_type_label().verified(
                                            "this model was resolved as a client above, and every \
                                             client model carries a type label"
                                        ),
                                        client.as_str(),
                                    ),
                                }));
                            }
                            graph.add_edge(client_node, source, EdgeKind::RequiredBy);
                        }
                        IngestSource::Endpoint { endpoint, .. } => {
                            let endpoint = expect_kind(
                                domain,
                                identifier,
                                models,
                                &indices,
                                endpoint,
                                ModelKind::Endpoint,
                            )?;
                            graph.add_edge(endpoint, source, EdgeKind::RequiredBy);
                        }
                    }

                    let producer_schema = schema_for_codec_model(
                        domain,
                        identifier,
                        models,
                        &ingestor.decode_using_codec,
                    )?;
                    let message_namespace = RelayName::parse(INGEST_MESSAGE_NAMESPACE).assured(
                        "this is a constant literal that satisfies the identifier grammar",
                    );
                    validate_ingestor_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &[(&message_namespace, producer_schema)],
                        None,
                        ingestor.filter_where.as_ref(),
                        &ingestor.source,
                    )?;
                    for output in ingestor.output_routes.outputs() {
                        let consumer_schema =
                            schema_for_ack_model(domain, identifier, models, &output.relay)?;
                        let effective_schema = effective_ingestor_output_filter_map_schema(
                            domain,
                            identifier,
                            models,
                            ingestor,
                            producer_schema,
                            output,
                            consumer_schema,
                        )?;
                        ensure_internal_schema_compatibility(
                            domain,
                            identifier,
                            &effective_schema,
                            consumer_schema,
                            "ingestor output",
                        )?;
                        ensure_output_branch(
                            domain,
                            identifier,
                            models,
                            output,
                            producer_schema,
                            &effective_schema,
                            None,
                        )?;
                    }
                    ensure_ingestor_timestamp_source(
                        domain,
                        identifier,
                        ingestor,
                        producer_schema,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &ingestor.output_routes,
                    )?;
                }
                Model::Relay(stream) => {
                    if identifier.as_str().eq_ignore_ascii_case(BRANCH_NAMESPACE)
                        || stream.name.as_str().eq_ignore_ascii_case(BRANCH_NAMESPACE)
                    {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "'branch' is a reserved namespace and cannot be used as a \
                                     relay name"
                                .to_string(),
                        }));
                    }
                    let schema = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &stream.schema,
                        ModelKind::Schema,
                    )?;
                    graph.add_edge(schema, source, EdgeKind::RequiredBy);
                    if let Some(branch_ref) = stream.branching.branch() {
                        let branch = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            branch_ref,
                            ModelKind::Branch,
                        )?;
                        graph.add_edge(branch, source, EdgeKind::RequiredBy);
                    }
                }
                Model::Reingestor(reingestor) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &reingestor.output_routes,
                    )?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reingestor.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &reingestor.from,
                        "reingestor input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &reingestor.from,
                        "reingestor input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &reingestor.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        reingestor.filter_where.as_ref(),
                    )?;
                    for output in reingestor.output_routes.outputs() {
                        let consumer_schema =
                            schema_for_ack_model(domain, identifier, models, &output.relay)?;
                        let effective_schema = effective_processor_output_filter_map_schema(
                            domain,
                            identifier,
                            models,
                            &input_schemas,
                            output,
                            consumer_schema,
                            branch_schema,
                        )?;
                        ensure_internal_schema_compatibility(
                            domain,
                            identifier,
                            &effective_schema,
                            consumer_schema,
                            "reingestor flow",
                        )?;
                        let incoming_branch =
                            relay_declared_branch(domain, identifier, models, first_input_relay)?;
                        ensure_output_branch(
                            domain,
                            identifier,
                            models,
                            output,
                            input_schemas[0].1,
                            &effective_schema,
                            incoming_branch,
                        )?;
                    }
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reingestor.output_routes,
                    )?;
                }
                Model::Endpoint(endpoint) => {
                    let vhost = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &endpoint.on_vhost,
                        ModelKind::Vhost,
                    )?;
                    graph.add_edge(vhost, source, EdgeKind::RequiredBy);
                    if let Some(signaling_protocol) = endpoint.signaling_protocol.as_ref() {
                        if endpoint.endpoint_type != EndpointType::Websockets {
                            return Err(Report::new(RegistryError::InvalidModel {
                                domain: domain.as_str().to_string(),
                                identifier: identifier.as_str().to_string(),
                                reason: "SIGNALING PROTOCOL is only valid for WEBSOCKETS endpoints"
                                    .to_string(),
                            }));
                        }
                        let signaling_protocol = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            signaling_protocol,
                            ModelKind::SignalingProtocol,
                        )?;
                        graph.add_edge(signaling_protocol, source, EdgeKind::RequiredBy);
                    }
                }
                Model::Lookup(lookup) => {
                    let codec = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &lookup.decode_using_codec,
                        ModelKind::Codec,
                    )?;
                    graph.add_edge(codec, source, EdgeKind::RequiredBy);
                    let codec_model =
                        expect_codec_model(domain, identifier, models, &lookup.decode_using_codec)?;
                    ensure_codec_supports_decoding(domain, identifier, codec_model)?;

                    let schema = schema_for_codec_model(
                        domain,
                        identifier,
                        models,
                        &lookup.decode_using_codec,
                    )?;
                    ensure_lookup_key_field_exists(domain, identifier, lookup, schema)?;
                }
                Model::Deduplicator(deduplicator) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &deduplicator.output_routes,
                    )?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &deduplicator.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &deduplicator.from,
                        "deduplicator input",
                    )?;
                    ensure_deduplicator_key_compiles(
                        domain,
                        identifier,
                        models,
                        deduplicator,
                        &input_schemas,
                    )?;
                    humantime::parse_duration(&deduplicator.max_time).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: format!(
                                "invalid deduplicator MAX TIME '{}': {error}",
                                deduplicator.max_time
                            ),
                        })
                    })?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &deduplicator.from,
                        "deduplicator input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &deduplicator.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        deduplicator.filter_where.as_ref(),
                    )?;
                    ensure_processor_output_schemas(
                        validation,
                        &deduplicator.output_routes,
                        &input_schemas,
                        branch_schema,
                        "deduplicator flow",
                        ProcessorOutputSchemaCompatibility::Compatible,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &deduplicator.output_routes,
                    )?;
                }
                Model::Correlator(correlator) => {
                    let left_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.left,
                        "correlator left input",
                    )?;
                    let right_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.right,
                        "correlator right input",
                    )?;
                    validate_correlator_input_sides_do_not_overlap(domain, identifier, correlator)?;

                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.output_routes,
                    )?;

                    add_correlation_timeout_action_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.timeout_policy.left,
                    )?;
                    add_correlation_timeout_action_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.timeout_policy.right,
                    )?;

                    let Some((left_relay, _left_schema)) = left_schemas.first().copied() else {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "correlator left input requires at least one input relay"
                                .to_string(),
                        }));
                    };
                    let Some((_right_relay, _right_schema)) = right_schemas.first().copied() else {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "correlator right input requires at least one input relay"
                                .to_string(),
                        }));
                    };
                    let branch_schema =
                        relay_declared_branch_schema(domain, identifier, models, left_relay)?;
                    validate_scoped_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &left_schemas,
                        branch_schema,
                        correlator.left.where_clauses(),
                        "left",
                    )?;
                    validate_scoped_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &right_schemas,
                        branch_schema,
                        correlator.right.where_clauses(),
                        "right",
                    )?;
                    let mut input_schemas =
                        Vec::with_capacity(left_schemas.len() + right_schemas.len());
                    input_schemas.extend(left_schemas.iter().copied());
                    input_schemas.extend(right_schemas.iter().copied());
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        correlator.filter_where.as_ref(),
                    )?;
                    validate_correlator(
                        domain,
                        identifier,
                        models,
                        correlator,
                        &left_schemas,
                        &right_schemas,
                    )?;
                    for output in correlator.output_routes.outputs() {
                        let output_schema =
                            schema_for_ack_model(domain, identifier, models, &output.relay)?;
                        validate_correlator_output(
                            validation,
                            &left_schemas,
                            &right_schemas,
                            output,
                            output_schema,
                            branch_schema,
                        )?;
                    }
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.output_routes,
                    )?;
                }
                Model::Reorderer(reorderer) => {
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reorderer.output_routes,
                    )?;

                    humantime::parse_duration(&reorderer.max_time).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: format!(
                                "invalid reorderer MAX TIME '{}': {error}",
                                reorderer.max_time
                            ),
                        })
                    })?;
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &reorderer.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &reorderer.from,
                        "reorderer input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &reorderer.from,
                        "reorderer input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &reorderer.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        reorderer.filter_where.as_ref(),
                    )?;
                    ensure_processor_output_schemas(
                        validation,
                        &reorderer.output_routes,
                        &input_schemas,
                        branch_schema,
                        "reorderer flow",
                        ProcessorOutputSchemaCompatibility::Compatible,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reorderer.output_routes,
                    )?;
                }
                Model::Junction(junction) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &junction.output_routes,
                    )?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &junction.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &junction.from,
                        "junction input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &junction.from,
                        "junction input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &junction.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        junction.filter_where.as_ref(),
                    )?;
                    ensure_processor_output_schemas(
                        validation,
                        &junction.output_routes,
                        &input_schemas,
                        branch_schema,
                        "junction flow",
                        ProcessorOutputSchemaCompatibility::Equal,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &junction.output_routes,
                    )?;
                }
                Model::WindowProcessor(window_processor) => {
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &window_processor.output_routes,
                    )?;

                    parse_window_bound_duration(
                        domain,
                        identifier,
                        "WIDTH",
                        window_processor.width.duration.as_deref(),
                    )?;
                    parse_window_bound_duration(
                        domain,
                        identifier,
                        "STEP",
                        window_processor.step.duration.as_deref(),
                    )?;
                    let input_schemas = processor_input_schemas(
                        validation,
                        &indices,
                        &mut graph,
                        source,
                        &window_processor.from,
                        "window processor input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &window_processor.from,
                        "window processor input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &window_processor.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        window_processor.filter_where.as_ref(),
                    )?;
                    ensure_window_processor_output_schemas(
                        domain,
                        identifier,
                        models,
                        window_processor,
                        &input_schemas,
                        branch_schema,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &window_processor.output_routes,
                    )?;
                }
                Model::Emitter(emitter) => {
                    validate_emitter_publishing_contract(domain, identifier, models, emitter)?;
                    let input_schemas = processor_input_schemas(
                        ModelValidationContext {
                            domain,
                            identifier,
                            models,
                        },
                        &indices,
                        &mut graph,
                        source,
                        &emitter.from,
                        "emitter input",
                    )?;
                    let producer_schema = input_schemas
                        .first()
                        .map(|(_relay, schema)| *schema)
                        .verified("the emitter inputs were validated as non-empty above");
                    for (relay, schema) in &input_schemas {
                        if schema.name != producer_schema.name {
                            return Err(Report::new(RegistryError::InvalidModel {
                                domain: domain.as_str().to_string(),
                                identifier: identifier.as_str().to_string(),
                                reason: format!(
                                    "emitter input relay '{}' declares schema '{}', but all \
                                     emitter inputs must declare schema '{}'",
                                    relay.as_str(),
                                    schema.name.as_str(),
                                    producer_schema.name.as_str(),
                                ),
                            }));
                        }
                    }
                    validate_sqs_fifo_group_expression(
                        domain,
                        identifier,
                        models,
                        emitter,
                        producer_schema,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        None,
                        &emitter.from.r#where,
                    )?;

                    if let Some(codec_name) = &emitter.encode_using_codec {
                        let codec = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            codec_name,
                            ModelKind::Codec,
                        )?;
                        graph.add_edge(codec, source, EdgeKind::RequiredBy);
                        let codec_model =
                            expect_codec_model(domain, identifier, models, codec_name)?;
                        let codec_schema =
                            schema_for_codec_model(domain, identifier, models, codec_name)?;
                        ensure_codec_supports_encoding(
                            domain,
                            identifier,
                            codec_model,
                            codec_schema,
                        )?;
                    }

                    let client_name = emitter.sink.client();
                    let client = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        client_name,
                        ModelKind::Client,
                    )?;
                    let client_model = models
                        .get(&NodeRef::new(ModelKind::Client, client_name.clone()))
                        .verified(
                            "expect_kind above resolved this client reference against the same \
                             model set",
                        );
                    if !emitter.sink.accepts_client(client_model) {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: format!(
                                "{} emitter requires a {} client, found {} client '{}'",
                                emitter.sink.transport_label(),
                                emitter.sink.expected_client_type(),
                                client_model.client_type_label().verified(
                                    "this model was resolved as a client above, and every client \
                                     model carries a type label"
                                ),
                                client_name.as_str(),
                            ),
                        }));
                    }
                    graph.add_edge(client, source, EdgeKind::RequiredBy);

                    if let Some(catalog_client_name) = emitter.sink.iceberg_catalog_client() {
                        let catalog_client = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            catalog_client_name,
                            ModelKind::Client,
                        )?;
                        let catalog_client_model = models
                            .get(&NodeRef::new(
                                ModelKind::Client,
                                catalog_client_name.clone(),
                            ))
                            .verified(
                                "expect_kind above resolved this client reference against the \
                                 same model set",
                            );
                        if let Model::ClientIcebergRest(_) = catalog_client_model {
                        } else {
                            return Err(Report::new(RegistryError::InvalidModel {
                                domain: domain.as_str().to_string(),
                                identifier: identifier.as_str().to_string(),
                                reason: format!(
                                    "ICEBERG emitter requires an ICEBERG_REST catalog client, \
                                     found {} client '{}'",
                                    catalog_client_model.client_type_label().verified(
                                        "this model was resolved as a client above, and every \
                                         client model carries a type label"
                                    ),
                                    catalog_client_name.as_str(),
                                ),
                            }));
                        }
                        graph.add_edge(catalog_client, source, EdgeKind::RequiredBy);
                    }

                    let output_schema = if let Some(codec_name) = &emitter.encode_using_codec {
                        schema_for_codec_model(domain, identifier, models, codec_name)?
                    } else {
                        producer_schema
                    };
                    let effective_schema = effective_emitter_filter_map_schema(
                        domain,
                        identifier,
                        models,
                        emitter,
                        producer_schema,
                        output_schema,
                    )?;
                    if let Some(codec_name) = &emitter.encode_using_codec {
                        let consumer_schema =
                            schema_for_codec_model(domain, identifier, models, codec_name)?;
                        ensure_internal_schema_compatibility_with_policy(
                            domain,
                            identifier,
                            &effective_schema,
                            consumer_schema,
                            "emitter input",
                            SensitivityCompatibility::AllowSensitiveProducer,
                        )?;
                    }
                    add_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &emitter.error_policies.message,
                    )?;
                }
            }
            validate_model_message_error_policies(domain, identifier, models, model)?;
        }

        if has_required_by_cycle(&graph) {
            return Err(Report::new(RegistryError::ConfigurationCycle {
                domain: domain.as_str().to_string(),
            }));
        }

        validate_vhost_hostnames(domain, models)?;
        validate_endpoint_paths(domain, models)?;
        infer_stream_branchings(domain, models, &indices, &mut graph)?;
        validate_processing_branch_selections(domain, models, &indices, &graph)?;
        let placement = PlacementAnalysis::build(domain, models, &indices, &mut graph)?;

        Ok(Self {
            models: models.clone(),
            graph: ActiveGraph {
                graph,
                indices,
                placement,
            },
        })
    }
}
