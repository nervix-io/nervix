use chumsky::prelude::*;
use nervix_models::{Model, Statement};

use crate::{
    lexer::Token,
    parser_support::{
        ParseError, ParseFromSourceError, boxed_choice, current_word_prefix, into_parse_error,
        lex_input, suggestions_from_errors,
    },
};

pub fn statement_parser<'src>()
-> impl Parser<'src, &'src [Token], Statement, extra::Err<ParseError<'src>>> + Clone {
    let domain_and_core = boxed_choice!(
        crate::domain::create_domain_parser().map(Statement::CreateDomain),
        crate::create_resource::create_resource_parser().map(Statement::CreateResource),
        crate::domain::start_domain_parser().map(Statement::StartDomain),
        crate::domain::stop_domain_parser().map(Statement::StopDomain),
        crate::branch::create_branch_parser()
            .map(|create| Statement::Create(create.map_body(Model::Branch).map_body(Box::new))),
        crate::describe_deduplicator::describe_deduplicator_parser()
            .map(Statement::DescribeDeduplicator),
        crate::describe_domain::describe_domain_parser().map(Statement::DescribeDomain),
        crate::describe_endpoint::describe_endpoint_parser().map(Statement::DescribeEndpoint),
        crate::describe_ingestor::describe_ingestor_parser().map(Statement::DescribeIngestor),
        crate::describe_lookup::describe_lookup_parser().map(Statement::DescribeLookup),
        crate::describe_resource::describe_resource_parser().map(Statement::DescribeResource),
        crate::describe_stream::describe_stream_parser().map(Statement::DescribeRelay),
        crate::describe_window_processor::describe_window_processor_parser()
            .map(Statement::DescribeWindowProcessor),
        crate::lookup_query::lookup_query_parser().map(Statement::LookupQuery),
        crate::generator::create_generator_parser()
            .map(|create| Statement::Create(create.map_body(Model::Generator).map_body(Box::new))),
        crate::inferencer::create_inferencer_parser()
            .map(|create| Statement::Create(create.map_body(Model::Inferencer).map_body(Box::new))),
        crate::lookup::create_lookup_parser()
            .map(|create| Statement::Create(create.map_body(Model::Lookup).map_body(Box::new))),
        crate::reingestor::create_reingestor_parser()
            .map(|create| Statement::Create(create.map_body(Model::Reingestor).map_body(Box::new))),
        crate::reorderer::create_reorderer_parser()
            .map(|create| Statement::Create(create.map_body(Model::Reorderer).map_body(Box::new))),
        crate::codec::create_codec_parser()
            .map(|create| Statement::Create(create.map_body(Model::Codec).map_body(Box::new))),
        crate::junction::create_junction_parser()
            .map(|create| Statement::Create(create.map_body(Model::Junction).map_body(Box::new))),
        crate::deduplicator::create_deduplicator_parser().map(|create| {
            Statement::Create(create.map_body(Model::Deduplicator).map_body(Box::new))
        }),
        crate::window_processor::create_window_processor_parser().map(|create| {
            Statement::Create(create.map_body(Model::WindowProcessor).map_body(Box::new))
        }),
        crate::vhost::create_vhost_parser()
            .map(|create| Statement::Create(create.map_body(Model::Vhost).map_body(Box::new))),
        crate::udf::create_udf_parser()
            .map(|create| Statement::Create(create.map_body(Model::Udf).map_body(Box::new))),
    );
    let processing_and_io = boxed_choice!(
        crate::user::create_user_parser().map(Statement::CreateUser),
        crate::describe_correlator::describe_correlator_parser().map(Statement::DescribeCorrelator),
        crate::endpoint::create_endpoint_parser()
            .map(|create| Statement::Create(create.map_body(Model::Endpoint).map_body(Box::new))),
        crate::signaling_protocol::create_signaling_protocol_parser().map(|create| {
            Statement::Create(create.map_body(Model::SignalingProtocol).map_body(Box::new))
        }),
        crate::describe_emitter::describe_emitter_parser().map(Statement::DescribeEmitter),
        crate::describe_wasm_processor::describe_wasm_processor_parser()
            .map(Statement::DescribeWasmProcessor),
        crate::wasm_processor::create_wasm_processor_parser().map(|create| {
            Statement::Create(create.map_body(Model::WasmProcessor).map_body(Box::new))
        }),
        crate::describe_reingestor::describe_reingestor_parser().map(Statement::DescribeReingestor),
        crate::describe_reorderer::describe_reorderer_parser().map(Statement::DescribeReorderer),
        crate::correlator::create_correlator_parser()
            .map(|create| Statement::Create(create.map_body(Model::Correlator).map_body(Box::new))),
        crate::emitter::create_emitter_parser()
            .map(|create| Statement::Create(create.map_body(Model::Emitter).map_body(Box::new))),
        crate::ingestor::create_ingestor_parser()
            .map(|create| Statement::Create(create.map_body(Model::Ingestor).map_body(Box::new))),
        crate::relay::create_relay_parser()
            .map(|create| Statement::Create(create.map_body(Model::Relay).map_body(Box::new))),
        crate::schema::create_wire_schema_parser_any()
            .map(|create| Statement::Create(create.map_body(Model::WireSchema).map_body(Box::new))),
        crate::schema::create_schema_parser()
            .map(|create| Statement::Create(create.map_body(Model::Schema).map_body(Box::new))),
    );
    let clients = boxed_choice!(
        crate::client::create_client_kafka_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientKafka).map_body(Box::new))
        }),
        crate::client::create_client_pulsar_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientPulsar).map_body(Box::new))
        }),
        crate::client::create_client_kinesis_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientKinesis).map_body(Box::new))
        }),
        crate::client::create_client_http_parser()
            .map(|create| Statement::Create(create.map_body(Model::ClientHttp).map_body(Box::new))),
        crate::client::create_client_prometheus_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientPrometheus).map_body(Box::new))
        }),
        crate::client::create_client_rabbitmq_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientRabbitMq).map_body(Box::new))
        }),
        crate::client::create_client_redis_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientRedis).map_body(Box::new))
        }),
        crate::client::create_client_mqtt_parser()
            .map(|create| Statement::Create(create.map_body(Model::ClientMqtt).map_body(Box::new))),
        crate::client::create_client_nats_parser()
            .map(|create| Statement::Create(create.map_body(Model::ClientNats).map_body(Box::new))),
        crate::client::create_client_zeromq_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientZeroMq).map_body(Box::new))
        }),
        crate::client::create_client_sqs_parser()
            .map(|create| Statement::Create(create.map_body(Model::ClientSqs).map_body(Box::new))),
        crate::client::create_client_s3_parser()
            .map(|create| Statement::Create(create.map_body(Model::ClientS3).map_body(Box::new))),
        crate::client::create_client_gcs_parser()
            .map(|create| Statement::Create(create.map_body(Model::ClientGcs).map_body(Box::new))),
        crate::client::create_client_azure_blob_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientAzureBlob).map_body(Box::new))
        }),
        crate::client::create_client_iceberg_rest_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientIcebergRest).map_body(Box::new))
        }),
        crate::client::create_client_websockets_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientWebsockets).map_body(Box::new))
        }),
        crate::client::create_client_clickhouse_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientClickHouse).map_body(Box::new))
        }),
        crate::client::create_client_postgres_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientPostgres).map_body(Box::new))
        }),
        crate::client::create_client_mysql_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientMySql).map_body(Box::new))
        }),
        crate::client::create_client_mongodb_parser().map(|create| {
            Statement::Create(create.map_body(Model::ClientMongoDb).map_body(Box::new))
        }),
    );
    let administration = boxed_choice!(
        crate::relay::alter_relay_parser().map(Statement::AlterRelay),
        crate::node_control::cordon_node_parser().map(Statement::CordonNode),
        crate::node_control::uncordon_node_parser().map(Statement::UncordonNode),
        crate::node_control::drain_node_parser().map(Statement::DrainNode),
        crate::drop_stmt::drop_node_parser().map(Statement::DropNode),
        crate::drop_stmt::drop_parser().map(Statement::Drop),
        crate::show_cluster_status::show_cluster_status_parser().map(Statement::ShowClusterStatus),
        crate::show_create::show_create_parser().map(Statement::ShowCreate),
        crate::show_stream_state::show_stream_materialized_state_parser()
            .map(Statement::ShowRelayMaterializedState),
        crate::udf::describe_udf_parser().map(Statement::DescribeUdf),
        crate::udf::show_udfs_parser().map(Statement::ShowUdfs),
    );

    choice([domain_and_core, processing_and_io, clients, administration]).boxed()
}

pub fn parse_statement_tokens(tokens: &[Token]) -> Result<Statement, Vec<ParseError<'_>>> {
    let out = statement_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_statement(input: &str) -> Result<Statement, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_statement_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_statement(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = statement_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        let trimmed = prefix_src.trim_end();
        let normalized = trimmed.to_ascii_uppercase();
        if prefix_src.len() > trimmed.len() && normalized == "START" {
            return vec![";".to_string(), "AT".to_string()];
        }
        if prefix_src.len() > trimmed.len()
            && normalized.starts_with("START AT ")
            && !normalized.contains(" TIME RATE ")
        {
            return vec![";".to_string(), "TIME RATE".to_string()];
        }
        if prefix_src.len() > trimmed.len()
            && normalized.starts_with("DESCRIBE RESOURCE ")
            && !normalized.contains(" VERSION ")
        {
            return vec!["VERSION".to_string()];
        }
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
    use bolero::check;
    use nervix_models::{
        AckMode, AlterRelay, AlterRelayOperation, AvroType, BranchSelection, CodecWireFormat,
        CordonNode, CreateClientAzureBlob, CreateClientGcs, CreateClientIcebergRest,
        CreateClientKafka, CreateClientMqtt, CreateClientNats, CreateClientPrometheus,
        CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis, CreateClientS3,
        CreateClientSqs, CreateClientZeroMq, CreateCodec, CreateDeduplicator, CreateEmitter,
        CreateEndpoint, CreateGenerator, CreateIngestor, CreateJunction, CreateRelay, CreateSchema,
        CreateSignalingProtocol, CreateWireSchema, CreateWireSchemaStmt, DescribeRelay, DrainNode,
        DropModel, DropNode, EmitSink, EndpointIngestMode, EndpointType, ErrorPolicies,
        GeneralErrorPolicy, Identifier as ModelIdentifier, IngestSource, JsonType,
        KafkaConfigEntry, KafkaIngestMode, KafkaOffsetMode, Model, ModelKind, MqttIngestMode,
        MqttQos, MqttSession, NatsIngestMode, OutputBranch, ParseAsType, ProcessorInputs,
        ProcessorOutput, ProcessorOutputs, PulsarIngestMode, RabbitMqIngestMode,
        RedisPubSubIngestMode, RetryPolicy, SchemaField, SignalingProtocolOnConnect, SqsIngestMode,
        Statement, SubscriptionBinding, SubscriptionLiteral, UncordonNode, WireSchemaField,
        ZeroMqIngestMode,
    };

    use super::*;

    fn processor_branched_by(schema: ModelIdentifier) -> BranchSelection {
        BranchSelection::branched_by(schema)
    }

    fn flushed_output(relay: ModelIdentifier, filter_map: Option<String>) -> ProcessorOutput {
        let mut output = ProcessorOutput::with_flush_policy(
            relay,
            "100ms".to_string(),
            Some("1MiB".to_string()),
        );
        output.construction = filter_map
            .map(|source| {
                crate::semantic_program::parse_route_construction(&source)
                    .expect("test route construction must parse")
            })
            .unwrap_or_default();
        output
    }

    fn flushed_outputs(relay: ModelIdentifier) -> ProcessorOutputs {
        ProcessorOutputs::new(vec![flushed_output(relay, None)])
    }

    fn flushed_ingestor_outputs(relay: ModelIdentifier) -> ProcessorOutputs {
        ProcessorOutputs::new(vec![
            flushed_output(relay, None).with_branch(OutputBranch::Unbranched),
        ])
    }

    struct ByteGen<'a> {
        bytes: &'a [u8],
        index: usize,
    }

    impl<'a> ByteGen<'a> {
        fn new(bytes: &'a [u8]) -> Self {
            Self { bytes, index: 0 }
        }

        fn next_u8(&mut self) -> u8 {
            if self.bytes.is_empty() {
                return 0;
            }
            let b = self.bytes[self.index % self.bytes.len()];
            self.index = self.index.wrapping_add(1);
            b
        }

        fn choose<T: Clone>(&mut self, items: &[T]) -> T {
            let idx = (self.next_u8() as usize) % items.len();
            items[idx].clone()
        }

        fn bool(&mut self) -> bool {
            self.next_u8().is_multiple_of(2)
        }

        fn bounded_u64(&mut self, min: u64, max: u64) -> u64 {
            let span = max - min + 1;
            min + (self.next_u8() as u64 % span)
        }

        fn ident(&mut self) -> ModelIdentifier {
            // Keep identifiers parser-valid and deterministic after canonical render.
            let len = (self.next_u8() as usize % 8) + 1;
            let mut s = String::with_capacity(len);
            for i in 0..len {
                let raw = self.next_u8();
                let ch = if i == 0 {
                    (b'a' + (raw % 26)) as char
                } else {
                    match raw % 3 {
                        0 => (b'a' + (raw % 26)) as char,
                        1 => (b'0' + (raw % 10)) as char,
                        _ => '_',
                    }
                };
                s.push(ch);
            }

            ModelIdentifier::try_from(s.as_str()).expect("generator must produce valid identifier")
        }

        fn transport_literal(&mut self) -> String {
            // No quotes/newlines so canonical serializer always succeeds.
            let len = (self.next_u8() as usize % 16) + 1;
            let mut out = String::with_capacity(len);
            for _ in 0..len {
                let b = self.next_u8();
                let ch = match b % 7 {
                    0 => (b'a' + (b % 26)) as char,
                    1 => (b'0' + (b % 10)) as char,
                    2 => '.',
                    3 => ':',
                    4 => ',',
                    5 => '_',
                    _ => '-',
                };
                out.push(ch);
            }
            out
        }
    }

    fn gen_model(bytes: &[u8]) -> Model {
        let mut g = ByteGen::new(bytes);
        match g.next_u8() % 29 {
            0 => {
                let field_count = (g.next_u8() as usize % 5) + 1;
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    fields.push(SchemaField {
                        name: g.ident(),
                        ty: g.choose(&[
                            ParseAsType::U8,
                            ParseAsType::I8,
                            ParseAsType::U16,
                            ParseAsType::I16,
                            ParseAsType::U32,
                            ParseAsType::I32,
                            ParseAsType::U64,
                            ParseAsType::I64,
                            ParseAsType::Bool,
                            ParseAsType::String,
                            ParseAsType::Datetime,
                            ParseAsType::F32,
                            ParseAsType::F64,
                        ]),
                        optional: g.bool(),
                        sensitive: false,
                    });
                }

                Model::Schema(CreateSchema {
                    name: g.ident(),
                    fields,
                })
            }
            25 => {
                let materialized_relay = g.ident();
                let output_relay = g.ident();
                let output = flushed_output(
                    output_relay,
                    Some(format!(
                        "SET value = relay_state.{}.value",
                        materialized_relay.as_str()
                    )),
                );
                Model::Generator(CreateGenerator {
                    name: g.ident(),
                    materialized_relay,
                    branched_by: processor_branched_by(g.ident()),
                    each: format!("{}ms", g.bounded_u64(1, 5000)),
                    output_routes: ProcessorOutputs::new(vec![output]),
                })
            }
            1 => {
                let is_json = g.bool();
                let field_count = (g.next_u8() as usize % 5) + 1;
                if is_json {
                    let mut fields = Vec::with_capacity(field_count);
                    for _ in 0..field_count {
                        fields.push(WireSchemaField {
                            name: g.ident(),
                            ty: g.choose(&[
                                JsonType::String,
                                JsonType::Number,
                                JsonType::Integer,
                                JsonType::Object,
                                JsonType::Array,
                                JsonType::Boolean,
                                JsonType::Null,
                            ]),
                            optional: g.bool(),
                        });
                    }
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: g.ident(),
                        strictness: Default::default(),
                        fields,
                    }))
                } else {
                    let mut fields = Vec::with_capacity(field_count);
                    for _ in 0..field_count {
                        fields.push(WireSchemaField {
                            name: g.ident(),
                            ty: g.choose(&[
                                AvroType::Null,
                                AvroType::Boolean,
                                AvroType::Int,
                                AvroType::Long,
                                AvroType::Float,
                                AvroType::Double,
                                AvroType::Bytes,
                                AvroType::String,
                                AvroType::Record,
                                AvroType::Enum,
                                AvroType::Array,
                                AvroType::Map,
                                AvroType::Fixed,
                            ]),
                            optional: g.bool(),
                        });
                    }
                    Model::WireSchema(CreateWireSchemaStmt::Avro(CreateWireSchema {
                        name: g.ident(),
                        strictness: Default::default(),
                        fields,
                    }))
                }
            }
            2 => Model::Codec(CreateCodec {
                name: g.ident(),
                wire_format: CodecWireFormat::Json,
                wire_schema: Some(g.ident()),
                schema: g.ident(),
                encoding_rules: Vec::new(),
            }),
            3 => {
                let count = (g.next_u8() as usize % 6) + 1;
                let mut config = Vec::with_capacity(count);
                for _ in 0..count {
                    config.push(KafkaConfigEntry {
                        key: g.transport_literal(),
                        value: g.transport_literal(),
                    });
                }

                if g.bool() {
                    Model::ClientKafka(CreateClientKafka {
                        name: g.ident(),
                        mount: None,
                        config,
                    })
                } else {
                    Model::ClientPulsar(CreateClientPulsar {
                        name: g.ident(),
                        mount: None,
                        config: vec![
                            KafkaConfigEntry {
                                key: "addr".to_string(),
                                value: "pulsar://127.0.0.1:6650".to_string(),
                            },
                            KafkaConfigEntry {
                                key: "namespace".to_string(),
                                value: "public/default".to_string(),
                            },
                        ],
                    })
                }
            }
            4 => {
                if g.bool() {
                    let mode = match g.next_u8() % 3 {
                        0 => KafkaIngestMode::AckParallel {
                            max: g.bounded_u64(1, 1024),
                            batch_timeout: format!("{}ms", g.bounded_u64(1, 1_000)),
                            timeout: format!("{}s", g.bounded_u64(1, 300)),
                            retry_policy: RetryPolicy {
                                backoff: format!("{}ms", g.bounded_u64(1, 1_000)),
                                max_backoff: format!("{}s", g.bounded_u64(1, 300)),
                            },
                        },
                        1 => KafkaIngestMode::AckSequential {
                            timeout: format!("{}s", g.bounded_u64(1, 300)),
                            retry_policy: RetryPolicy {
                                backoff: format!("{}ms", g.bounded_u64(1, 1_000)),
                                max_backoff: format!("{}s", g.bounded_u64(1, 300)),
                            },
                        },
                        _ => KafkaIngestMode::NoAckParallel {
                            max: g.bounded_u64(1, 1024),
                        },
                    };

                    if g.bool() {
                        Model::Ingestor(CreateIngestor {
                            name: g.ident(),
                            output_routes: flushed_ingestor_outputs(g.ident()),
                            decode_using_codec: g.ident(),
                            timestamp_source: None,
                            source: IngestSource::Kafka {
                                client: g.ident(),
                                topic: g.ident(),
                                offset_mode: KafkaOffsetMode::ConsumerGroup(g.ident()),
                                instances: 1,
                                mode,
                            },
                            general_error_policy: GeneralErrorPolicy::Log,
                            filter_where: None,
                        })
                    } else {
                        Model::Ingestor(CreateIngestor {
                            name: g.ident(),
                            output_routes: flushed_ingestor_outputs(g.ident()),
                            decode_using_codec: g.ident(),
                            timestamp_source: None,
                            source: IngestSource::Pulsar {
                                client: g.ident(),
                                topic: g.ident(),
                                subscription: g.ident(),
                                instances: 1,
                                mode: match mode {
                                    KafkaIngestMode::AckParallel {
                                        max,
                                        batch_timeout,
                                        timeout,
                                        retry_policy,
                                    } => PulsarIngestMode::AckParallel {
                                        max,
                                        batch_timeout,
                                        timeout,
                                        retry_policy,
                                    },
                                    KafkaIngestMode::AckSequential {
                                        timeout,
                                        retry_policy,
                                    } => PulsarIngestMode::AckSequential {
                                        timeout,
                                        retry_policy,
                                    },
                                    KafkaIngestMode::NoAckParallel { max } => {
                                        PulsarIngestMode::NoAckParallel { max }
                                    }
                                },
                            },
                            general_error_policy: GeneralErrorPolicy::Log,
                            filter_where: None,
                        })
                    }
                } else {
                    Model::Ingestor(CreateIngestor {
                        name: g.ident(),
                        output_routes: flushed_ingestor_outputs(g.ident()),
                        decode_using_codec: g.ident(),
                        timestamp_source: None,
                        source: IngestSource::RabbitMq {
                            client: g.ident(),
                            queue: g.ident(),
                            instances: 1,
                            mode: RabbitMqIngestMode::AckSequential {
                                timeout: format!("{}s", g.bounded_u64(1, 300)),
                                retry_policy: RetryPolicy {
                                    backoff: format!("{}ms", g.bounded_u64(1, 1_000)),
                                    max_backoff: format!("{}s", g.bounded_u64(1, 300)),
                                },
                            },
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    })
                }
            }
            5 => Model::ClientRabbitMq(CreateClientRabbitMq {
                name: g.ident(),
                mount: None,
                config: vec![KafkaConfigEntry {
                    key: "addr".to_string(),
                    value: "amqp://guest:guest@localhost:5672/%2f".to_string(),
                }],
            }),
            6 => Model::ClientRedis(CreateClientRedis {
                name: g.ident(),
                mount: None,
                config: vec![KafkaConfigEntry {
                    key: "addr".to_string(),
                    value: "redis://127.0.0.1:6379/".to_string(),
                }],
            }),
            7 => Model::Relay(CreateRelay {
                name: g.ident(),
                schema: g.ident(),
                buffer: g.bounded_u64(1, 1024) as usize,
                branching: nervix_models::RelayBranching::unbranched(),
                materialized_state: None,
            }),
            8 => Model::ClientMqtt(CreateClientMqtt {
                name: g.ident(),
                mount: None,
                config: vec![KafkaConfigEntry {
                    key: "addr".to_string(),
                    value: "mqtt://127.0.0.1:1883".to_string(),
                }],
            }),
            9 => Model::Junction(CreateJunction {
                name: g.ident(),
                from: ProcessorInputs::new(vec![g.ident(), g.ident(), g.ident()], Vec::new()),
                output_routes: flushed_outputs(g.ident()),
                branched_by: processor_branched_by(g.ident()),
                mode: if g.bool() {
                    AckMode::Attached
                } else {
                    AckMode::Detached
                },
                filter_where: None,
                materialized_state: Vec::new(),
            }),
            10 => Model::Deduplicator(CreateDeduplicator {
                name: g.ident(),
                from: ProcessorInputs::single(g.ident()),
                output_routes: flushed_outputs(g.ident()),
                branched_by: processor_branched_by(g.ident()),
                deduplicate_on: vec![nervix_models::Expression::Field(
                    nervix_models::FieldReference::bare(g.ident()),
                )],
                max_time: "10m".to_string(),
                mode: if g.bool() {
                    AckMode::Attached
                } else {
                    AckMode::Detached
                },
                filter_where: None,
                materialized_state: Vec::new(),
            }),
            11 => Model::ClientPrometheus(CreateClientPrometheus {
                name: g.ident(),
                mount: None,
                config: vec![KafkaConfigEntry {
                    key: "addr".to_string(),
                    value: "http://127.0.0.1:9090".to_string(),
                }],
            }),
            12 => Model::Ingestor(CreateIngestor {
                name: g.ident(),
                output_routes: flushed_ingestor_outputs(g.ident()),
                decode_using_codec: g.ident(),
                timestamp_source: None,
                source: IngestSource::RedisPubSub {
                    client: g.ident(),
                    channel: g.ident(),
                    mode: RedisPubSubIngestMode::NoAckSequential,
                },
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            }),
            13 => {
                if g.bool() {
                    Model::Ingestor(CreateIngestor {
                        name: g.ident(),
                        output_routes: flushed_ingestor_outputs(g.ident()),
                        decode_using_codec: g.ident(),
                        timestamp_source: None,
                        source: IngestSource::Mqtt {
                            client: g.ident(),
                            topic: g.ident().as_str().to_string(),
                            instances: 1,
                            mode: MqttIngestMode::NoAckSequential {
                                session: MqttSession::Clean,
                                qos: MqttQos::AtMostOnce,
                            },
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    })
                } else if g.bool() {
                    Model::Ingestor(CreateIngestor {
                        name: g.ident(),
                        output_routes: flushed_ingestor_outputs(g.ident()),
                        decode_using_codec: g.ident(),
                        timestamp_source: None,
                        source: IngestSource::Prometheus {
                            client: g.ident(),
                            query: r#"label_replace(vector(42.5), "source", "local", "", "")"#
                                .to_string(),
                            every: "15s".to_string(),
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    })
                } else {
                    let sink = match g.next_u8() % 3 {
                        0 => EmitSink::Kafka {
                            client: g.ident(),
                            topic: g.ident(),
                        },
                        1 => EmitSink::Pulsar {
                            client: g.ident(),
                            topic: g.ident(),
                        },
                        _ => EmitSink::Mqtt {
                            client: g.ident(),
                            topic: g.ident(),
                        },
                    };

                    Model::Emitter(CreateEmitter {
                        name: g.ident(),
                        from_relay: g.ident(),
                        encode_using_codec: Some(g.ident()),
                        sink,
                        flush_each: "100ms".to_string(),
                        max_batch_size: Some("1MiB".to_string()),
                        mode: if g.bool() {
                            AckMode::Attached
                        } else {
                            AckMode::Detached
                        },
                        error_policies: ErrorPolicies::handled_by_log(),
                        construction: nervix_models::RouteConstruction::default(),
                        materialized_state: Vec::new(),
                    })
                }
            }
            14 => Model::Endpoint(CreateEndpoint {
                name: g.ident(),
                on_vhost: g.ident(),
                path: "/ws".to_string(),
                endpoint_type: EndpointType::Websockets,
                signaling_protocol: None,
            }),
            15 => Model::ClientNats(CreateClientNats {
                name: g.ident(),
                mount: None,
                config: vec![KafkaConfigEntry {
                    key: "addr".to_string(),
                    value: "nats://127.0.0.1:4222".to_string(),
                }],
            }),
            16 => Model::ClientZeroMq(CreateClientZeroMq {
                name: g.ident(),
                mount: None,
                config: vec![
                    KafkaConfigEntry {
                        key: "addr".to_string(),
                        value: "tcp://127.0.0.1:5555".to_string(),
                    },
                    KafkaConfigEntry {
                        key: "bind".to_string(),
                        value: "false".to_string(),
                    },
                ],
            }),
            17 => Model::ClientSqs(CreateClientSqs {
                name: g.ident(),
                mount: None,
                config: vec![
                    KafkaConfigEntry {
                        key: "endpoint".to_string(),
                        value: "http://127.0.0.1:9324".to_string(),
                    },
                    KafkaConfigEntry {
                        key: "region".to_string(),
                        value: "us-east-1".to_string(),
                    },
                ],
            }),
            18 => Model::Emitter(CreateEmitter {
                name: g.ident(),
                from_relay: g.ident(),
                encode_using_codec: Some(g.ident()),
                sink: EmitSink::ZeroMq { client: g.ident() },
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: if g.bool() {
                    AckMode::Attached
                } else {
                    AckMode::Detached
                },
                error_policies: ErrorPolicies::handled_by_log(),
                construction: nervix_models::RouteConstruction::default(),
                materialized_state: Vec::new(),
            }),
            19 => Model::Ingestor(CreateIngestor {
                name: g.ident(),
                output_routes: flushed_ingestor_outputs(g.ident()),
                decode_using_codec: g.ident(),
                timestamp_source: None,
                source: IngestSource::ZeroMq {
                    client: g.ident(),
                    mode: ZeroMqIngestMode::NoAckSequential,
                },
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            }),
            20 => Model::Emitter(CreateEmitter {
                name: g.ident(),
                from_relay: g.ident(),
                encode_using_codec: Some(g.ident()),
                sink: EmitSink::Nats {
                    client: g.ident(),
                    subject: g.ident(),
                },
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: if g.bool() {
                    AckMode::Attached
                } else {
                    AckMode::Detached
                },
                error_policies: ErrorPolicies::handled_by_log(),
                construction: nervix_models::RouteConstruction::default(),
                materialized_state: Vec::new(),
            }),
            21 => Model::Ingestor(CreateIngestor {
                name: g.ident(),
                output_routes: flushed_ingestor_outputs(g.ident()),
                decode_using_codec: g.ident(),
                timestamp_source: None,
                source: IngestSource::Nats {
                    client: g.ident(),
                    subject: g.ident(),
                    queue_group: g.ident(),
                    instances: g.bounded_u64(1, 10),
                    mode: NatsIngestMode::NoAckSequential,
                },
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            }),
            22 => {
                let from_relay = ModelIdentifier::try_from("source").expect("valid identifier");
                let error_condition = r#"input.level = "error""#;
                let warn_condition = r#"input.level = "warn""#;
                Model::Reingestor(nervix_models::CreateReingestor {
                    name: g.ident(),
                    from: ProcessorInputs::single(from_relay),
                    output_routes: ProcessorOutputs::new(vec![
                        flushed_output(g.ident(), Some(format!("WHERE {error_condition}")))
                            .with_branch(OutputBranch::Unbranched),
                        flushed_output(g.ident(), Some(format!("WHERE {warn_condition}")))
                            .with_branch(OutputBranch::Unbranched),
                        flushed_output(g.ident(), None).with_branch(OutputBranch::Unbranched),
                    ]),
                    mode: if g.bool() {
                        AckMode::Attached
                    } else {
                        AckMode::Detached
                    },
                    filter_where: None,
                    materialized_state: Vec::new(),
                })
            }
            23 => Model::ClientS3(CreateClientS3 {
                name: g.ident(),
                mount: None,
                config: vec![
                    KafkaConfigEntry {
                        key: "endpoint".to_string(),
                        value: "http://127.0.0.1:9000".to_string(),
                    },
                    KafkaConfigEntry {
                        key: "region".to_string(),
                        value: "us-east-1".to_string(),
                    },
                ],
            }),
            24 => Model::ClientGcs(CreateClientGcs {
                name: g.ident(),
                mount: None,
                config: vec![
                    KafkaConfigEntry {
                        key: "service_path".to_string(),
                        value: "http://127.0.0.1:4443".to_string(),
                    },
                    KafkaConfigEntry {
                        key: "no_auth".to_string(),
                        value: "true".to_string(),
                    },
                ],
            }),
            26 => Model::ClientAzureBlob(CreateClientAzureBlob {
                name: g.ident(),
                mount: None,
                config: vec![
                    KafkaConfigEntry {
                        key: "account_name".to_string(),
                        value: "devstoreaccount1".to_string(),
                    },
                    KafkaConfigEntry {
                        key: "account_key".to_string(),
                        value: "local-key".to_string(),
                    },
                ],
            }),
            27 => Model::ClientIcebergRest(CreateClientIcebergRest {
                name: g.ident(),
                mount: None,
                config: vec![
                    KafkaConfigEntry {
                        key: "uri".to_string(),
                        value: "http://127.0.0.1:8181".to_string(),
                    },
                    KafkaConfigEntry {
                        key: "warehouse".to_string(),
                        value: "s3://nervix-iceberg/warehouse".to_string(),
                    },
                ],
            }),
            28 => Model::SignalingProtocol(CreateSignalingProtocol {
                name: g.ident(),
                on_connect: SignalingProtocolOnConnect {
                    send_bodies: vec![r#"{"method":"SUBSCRIBE","id":1}"#.to_string()],
                    wait_bodies: vec![r#"{"id":1,"result":null}"#.to_string()],
                    timeout: "5s".to_string(),
                },
            }),
            _ => Model::Ingestor(CreateIngestor {
                name: g.ident(),
                output_routes: flushed_ingestor_outputs(g.ident()),
                decode_using_codec: g.ident(),
                timestamp_source: None,
                source: if g.bool() {
                    IngestSource::Endpoint {
                        endpoint: g.ident(),
                        mode: EndpointIngestMode::NoAckSequential,
                    }
                } else {
                    IngestSource::Sqs {
                        client: g.ident(),
                        queue: g.ident(),
                        instances: 1,
                        mode: SqsIngestMode::AckSequential {
                            timeout: format!("{}s", g.bounded_u64(1, 300)),
                            retry_policy: RetryPolicy {
                                backoff: format!("{}ms", g.bounded_u64(1, 1_000)),
                                max_backoff: format!("{}s", g.bounded_u64(1, 300)),
                            },
                        },
                    }
                },
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            }),
        }
    }

    #[test]
    fn client_context_suggestions_do_not_leak_schema_keywords() {
        let input = "CREATE CLIENT kafka_main TYPE ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"KAFKA".to_string()));
        assert!(suggestions.contains(&"PULSAR".to_string()));
        assert!(suggestions.contains(&"KINESIS".to_string()));
        assert!(suggestions.contains(&"PULSAR".to_string()));
        assert!(suggestions.contains(&"HTTP".to_string()));
        assert!(suggestions.contains(&"PROMETHEUS".to_string()));
        assert!(suggestions.contains(&"RABBITMQ".to_string()));
        assert!(suggestions.contains(&"REDIS".to_string()));
        assert!(suggestions.contains(&"MQTT".to_string()));
        assert!(suggestions.contains(&"NATS".to_string()));
        assert!(suggestions.contains(&"ZEROMQ".to_string()));
        assert!(suggestions.contains(&"SQS".to_string()));
        assert!(suggestions.contains(&"S3".to_string()));
        assert!(suggestions.contains(&"GCS".to_string()));
        assert!(suggestions.contains(&"AZURE_BLOB".to_string()));
        assert!(suggestions.contains(&"ICEBERG_REST".to_string()));
        assert!(suggestions.contains(&"WEBSOCKETS".to_string()));
        assert!(suggestions.contains(&"CLICKHOUSE".to_string()));
        assert!(suggestions.contains(&"POSTGRES".to_string()));
        assert!(suggestions.contains(&"MYSQL".to_string()));
        assert!(suggestions.contains(&"MONGODB".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn create_if_not_exists_completion_suggests_compound_keyword_without_leakage() {
        let input = "CREATE ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"IF NOT EXISTS".to_string()));
        assert!(!suggestions.contains(&"IF_NOT_EXISTS".to_string()));
    }

    #[test]
    fn ingestor_mode_context_suggestions_do_not_leak_schema_keywords() {
        let input = "CREATE INGESTOR i FROM KAFKA t TOPIC top OFFSET BY CONSUMER GROUP g MODE ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO_ACK".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn ingestor_branch_context_suggests_only_branch_selection_keywords() {
        let input =
            "CREATE INGESTOR i FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL DECODE USING sch TO s ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(!suggestions.contains(&"BY".to_string()));
    }

    #[test]
    fn ingestor_bare_by_is_rejected() {
        let input = "CREATE INGESTOR i TO s FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR \
                     LOG DECODE USING sch BY u_branch FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL ON \
                     GENERAL ERROR LOG;";

        parse_statement(input).expect_err("bare BY is not a branch selection mode");
    }

    #[test]
    fn attached_stream_context_suggestions_do_not_leak_schema_keywords() {
        let input = "CREATE RELAY p ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"SCHEMA".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn show_create_context_suggestions_do_not_leak_format_keywords() {
        let input = "SHOW CREATE ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"SCHEMA".to_string()));
        assert!(suggestions.contains(&"CODEC".to_string()));
        assert!(suggestions.contains(&"CLIENT".to_string()));
        assert!(suggestions.contains(&"VHOST".to_string()));
        assert!(suggestions.contains(&"ENDPOINT".to_string()));
        assert!(suggestions.contains(&"INGESTOR".to_string()));
        assert!(suggestions.contains(&"RELAY".to_string()));
        assert!(suggestions.contains(&"JUNCTION".to_string()));
        assert!(suggestions.contains(&"DEDUPLICATOR".to_string()));
        assert!(suggestions.contains(&"EMITTER".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn show_context_suggestions_include_cluster_without_entity_leakage() {
        let input = "SHOW ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"CREATE".to_string()));
        assert!(suggestions.contains(&"CLUSTER".to_string()));
        assert!(!suggestions.contains(&"SCHEMA".to_string()));
        assert!(!suggestions.contains(&"CLIENT".to_string()));
    }

    #[test]
    fn emitter_context_suggestions_do_not_leak_schema_keywords() {
        let input = "CREATE EMITTER emit FROM p99 ENCODE USING my_codec TO ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"KAFKA".to_string()));
        assert!(suggestions.contains(&"RABBITMQ".to_string()));
        assert!(suggestions.contains(&"REDIS".to_string()));
        assert!(suggestions.contains(&"MQTT".to_string()));
        assert!(suggestions.contains(&"NATS".to_string()));
        assert!(suggestions.contains(&"ZEROMQ".to_string()));
        assert!(suggestions.contains(&"SQS".to_string()));
        assert!(suggestions.contains(&"CLICKHOUSE".to_string()));
        assert!(suggestions.contains(&"POSTGRES".to_string()));
        assert!(suggestions.contains(&"MYSQL".to_string()));
        assert!(suggestions.contains(&"MONGODB".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn deduplicator_context_suggestions_do_not_leak_schema_keywords() {
        let input = "CREATE DEDUPLICATOR dedup FROM ss1 DEDUPLICATE ON input.transaction_id MAX ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"TIME".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn from_relay_context_suggests_where_without_schema_keyword_leakage() {
        let input = "CREATE DEDUPLICATOR dedup FROM ss1 ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"WHERE".to_string()));
        assert!(suggestions.contains(&"DEDUPLICATE ON".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn junction_context_suggestions_do_not_leak_schema_keywords() {
        let input = "CREATE JUNCTION merge FROM orders_a, orders_b ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn conditional_expression_body_does_not_change_route_completion_context() {
        let literal = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = 1 ";
        let conditional = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = \
                           CASE WHEN input.active THEN 1 ELSE 0 END ";

        assert_eq!(
            suggest_statement(literal, literal.len()),
            suggest_statement(conditional, conditional.len())
        );
    }

    #[test]
    fn reingestor_output_context_suggestions_do_not_leak_schema_keywords() {
        let input = "CREATE REINGESTOR log_splitter FROM incoming_logs TO errors_ss FLUSH \
                     IMMEDIATE ON MESSAGE ERROR LOG ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(!suggestions.contains(&"BY".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn alter_context_suggestions_include_relay_without_schema_keyword_leakage() {
        let input = "ALTER ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"RELAY".to_string()));
        assert!(!suggestions.contains(&"SCHEMA".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn alter_relay_capacity_context_suggestions_do_not_leak_schema_keywords() {
        let input = "ALTER RELAY notifications SET CAPACITY ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"relay_capacity".to_string()));
        assert!(!suggestions.contains(&"SCHEMA".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn parses_drop_statement() {
        let parsed = parse_statement("DROP SCHEMA event_schema;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::Drop(DropModel {
                kind: ModelKind::Schema,
                name: ModelIdentifier::try_from("event_schema").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_alter_relay_set_capacity_statement() {
        let parsed = parse_statement("ALTER RELAY notifications SET CAPACITY 32;")
            .expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::AlterRelay(AlterRelay {
                relay: ModelIdentifier::try_from("notifications").expect("valid identifier"),
                operation: AlterRelayOperation::SetCapacity { capacity: 32 },
            })
        );
    }

    #[test]
    fn rejects_alter_relay_zero_capacity_statement() {
        let error = parse_statement("ALTER RELAY notifications SET CAPACITY 0;")
            .expect_err("parse should fail");

        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error");
        };
        assert!(!diagnostics.is_empty());
    }

    #[test]
    fn parses_drop_node_statement() {
        let parsed = parse_statement("DROP NODE node-2;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DropNode(DropNode {
                node_id: "node-2".to_string(),
            })
        );
    }

    #[test]
    fn parses_cordon_node_statement() {
        let parsed = parse_statement("CORDON NODE node-2;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::CordonNode(CordonNode {
                node_id: "node-2".to_string(),
            })
        );
    }

    #[test]
    fn parses_uncordon_node_statement() {
        let parsed = parse_statement("UNCORDON NODE node-2;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::UncordonNode(UncordonNode {
                node_id: "node-2".to_string(),
            })
        );
    }

    #[test]
    fn parses_drain_node_statement() {
        let parsed = parse_statement("DRAIN NODE node-2;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DrainNode(DrainNode {
                node_id: "node-2".to_string(),
            })
        );
    }

    #[test]
    fn parses_describe_resource_statement() {
        let parsed = parse_statement("DESCRIBE RESOURCE fraud_model VERSION 7;")
            .expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeResource(nervix_models::DescribeResource {
                identifier: ModelIdentifier::parse("fraud_model").expect("valid identifier"),
                version: Some(7),
            })
        );
    }

    #[test]
    fn parses_describe_resource_summary_statement() {
        let parsed =
            parse_statement("DESCRIBE RESOURCE fraud_model;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeResource(nervix_models::DescribeResource {
                identifier: ModelIdentifier::parse("fraud_model").expect("valid identifier"),
                version: None,
            })
        );
    }

    #[test]
    fn describe_context_suggestions_include_resource_and_stream() {
        let input = "DESCRIBE ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"DEDUPLICATOR".to_string()));
        assert!(suggestions.contains(&"DOMAIN".to_string()));
        assert!(suggestions.contains(&"EMITTER".to_string()));
        assert!(suggestions.contains(&"INGESTOR".to_string()));
        assert!(suggestions.contains(&"REINGESTOR".to_string()));
        assert!(suggestions.contains(&"RESOURCE".to_string()));
        assert!(suggestions.contains(&"RELAY".to_string()));
        assert!(suggestions.contains(&"WINDOW".to_string()));
    }

    #[test]
    fn parses_describe_domain_statement() {
        let parsed = parse_statement("DESCRIBE DOMAIN;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeDomain(nervix_models::DescribeDomain)
        );
    }

    #[test]
    fn parses_describe_ingestor_statement() {
        let parsed = parse_statement("DESCRIBE INGESTOR kafka_notifications;")
            .expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeIngestor(nervix_models::DescribeIngestor {
                ingestor: ModelIdentifier::parse("kafka_notifications").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_describe_endpoint_statement() {
        let parsed = parse_statement("DESCRIBE ENDPOINT http_notifications_endpoint;")
            .expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeEndpoint(nervix_models::DescribeEndpoint {
                name: ModelIdentifier::parse("http_notifications_endpoint")
                    .expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_describe_relay_statement() {
        let parsed = parse_statement("DESCRIBE RELAY notifications WHERE (user_id = 42);")
            .expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeRelay(DescribeRelay {
                relay: ModelIdentifier::parse("notifications").expect("valid identifier"),
                bindings: vec![SubscriptionBinding {
                    field: ModelIdentifier::parse("user_id").expect("valid identifier"),
                    value: SubscriptionLiteral::Number("42".to_string()),
                }],
            })
        );
    }

    #[test]
    fn parses_describe_deduplicator_statement() {
        let parsed =
            parse_statement("DESCRIBE DEDUPLICATOR dedup_txns;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeDeduplicator(nervix_models::DescribeDeduplicator {
                name: ModelIdentifier::parse("dedup_txns").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_describe_reingestor_statement() {
        let parsed =
            parse_statement("DESCRIBE REINGESTOR repartition;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeReingestor(nervix_models::DescribeReingestor {
                name: ModelIdentifier::parse("repartition").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_describe_correlator_statement() {
        let parsed = parse_statement("DESCRIBE CORRELATOR correlate_profiles;")
            .expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeCorrelator(nervix_models::DescribeCorrelator {
                name: ModelIdentifier::parse("correlate_profiles").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_describe_reorderer_statement() {
        let parsed = parse_statement("DESCRIBE REORDERER order_notifications;")
            .expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeReorderer(nervix_models::DescribeReorderer {
                name: ModelIdentifier::parse("order_notifications").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_describe_emitter_statement() {
        let parsed = parse_statement("DESCRIBE EMITTER kafka_out;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeEmitter(nervix_models::DescribeEmitter {
                name: ModelIdentifier::parse("kafka_out").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_describe_window_processor_statement() {
        let parsed = parse_statement("DESCRIBE WINDOW PROCESSOR latency_window;")
            .expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeWindowProcessor(nervix_models::DescribeWindowProcessor {
                name: ModelIdentifier::parse("latency_window").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn parses_describe_wasm_processor_statement() {
        let parsed =
            parse_statement("DESCRIBE WASM PROCESSOR filter_even;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::DescribeWasmProcessor(nervix_models::DescribeWasmProcessor {
                name: ModelIdentifier::parse("filter_even").expect("valid identifier"),
            })
        );
    }

    #[test]
    fn describe_resource_summary_context_suggests_version_keyword() {
        let input = "DESCRIBE RESOURCE fraud_model ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"VERSION".to_string()));
    }

    #[test]
    fn parses_create_resource_statement() {
        let parsed = parse_statement("CREATE RESOURCE fraud_model;").expect("parse should succeed");
        assert_eq!(
            parsed,
            Statement::CreateResource(nervix_models::CreateStatement::new(
                nervix_models::CreateResource {
                    identifier: ModelIdentifier::parse("fraud_model").expect("valid identifier"),
                },
                false,
            ))
        );
    }

    #[test]
    fn rejects_client_only_upload_resource_statement() {
        parse_statement("UPLOAD RESOURCE fraud_model VERSION '/tmp/model';")
            .expect_err("UPLOAD RESOURCE is a client-side command");
    }

    #[test]
    fn parses_junction_statement_with_implicit_attached_mode() {
        let parsed = parse_statement(
            "CREATE JUNCTION join_streams FROM notifications_a, notifications_b BRANCHED BY \
             tenant TO merged_notifications INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
             MESSAGE ERROR LOG;",
        )
        .expect("parse should succeed");

        let Statement::Create(parsed) = parsed else {
            panic!("expected junction statement");
        };
        let Model::Junction(junction) = parsed.body.as_ref() else {
            panic!("expected junction statement");
        };
        assert_eq!(junction.mode, AckMode::Attached);
        assert_eq!(junction.from.from.len(), 2);
    }

    #[test]
    fn parses_deduplicator_statement_with_implicit_attached_mode() {
        let parsed = parse_statement(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.transaction_id MAX TIME \
             10m BRANCHED BY tenant TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
             MESSAGE ERROR LOG;",
        )
        .expect("parse should succeed");

        let Statement::Create(parsed) = parsed else {
            panic!("expected deduplicator statement");
        };
        let Model::Deduplicator(deduplicator) = parsed.body.as_ref() else {
            panic!("expected deduplicator statement");
        };
        assert_eq!(deduplicator.mode, AckMode::Attached);
        assert_eq!(deduplicator.max_time, "10m");
    }

    #[test]
    fn parses_inferencer_statement() {
        let parsed = parse_statement(
            r#"CREATE INFERENCER score_model FROM features USING RESOURCE fraud_model VERSION 3 FILE 'models/fraud.onnx' INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector } OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] } UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;"#,
        )
        .expect("parse should succeed");

        let Statement::Create(parsed) = parsed else {
            panic!("expected inferencer statement");
        };
        let Model::Inferencer(processor) = parsed.body.as_ref() else {
            panic!("expected inferencer statement");
        };
        assert_eq!(processor.mode, AckMode::Attached);
        assert_eq!(processor.resource.as_str(), "fraud_model");
        assert_eq!(processor.resource_version, Some(3));
        assert_eq!(processor.inputs.len(), 1);
        assert_eq!(processor.output_schema.len(), 1);
        assert_eq!(
            processor.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "IMMEDIATE"
        );
        let canonical = processor
            .to_canonical_nspl()
            .expect("inferencer should render canonically");
        assert!(canonical.contains("TO scored SET score = score FLUSH IMMEDIATE"));
        assert!(canonical.contains("OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] }"));
    }

    #[test]
    fn parses_reingestor_statement_with_flush_each() {
        let parsed = parse_statement(
            "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications BRANCHED BY \
             tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
             MESSAGE ERROR LOG;",
        )
        .expect("parse should succeed");

        let Statement::Create(parsed) = parsed else {
            panic!("expected reingestor statement");
        };
        let Model::Reingestor(reingestor) = parsed.body.as_ref() else {
            panic!("expected reingestor statement");
        };
        assert_eq!(
            reingestor.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "100ms"
        );
    }

    #[test]
    fn parses_reingestor_statement_with_multiple_output_routes() {
        let parsed = parse_statement(
            r#"CREATE REINGESTOR log_splitter FROM incoming_logs FILTER WHERE input.active TO errors_ss SET severity = lower(input.level) WHERE output.severity = "error" BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG TO warnings_ss WHERE input.level = "warn" BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG TO info_ss INHERIT ALL BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"#,
        )
        .expect("parse should succeed");

        let Statement::Create(parsed) = parsed else {
            panic!("expected reingestor statement");
        };
        let Model::Reingestor(reingestor) = parsed.body.as_ref() else {
            panic!("expected reingestor statement");
        };
        assert_eq!(reingestor.mode, AckMode::Attached);
        assert_eq!(reingestor.output_routes.routes.len(), 3);
        assert_eq!(
            reingestor
                .output_routes
                .routes
                .get(2)
                .map(|output| output.relay.as_str()),
            Some("info_ss")
        );
        assert_eq!(
            reingestor.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "100ms"
        );
        assert_eq!(
            reingestor.filter_where,
            Some(crate::parse_expression("input.active").expect("valid expression"))
        );
    }

    #[test]
    fn parses_single_reingestor_output_route_with_filter_map() {
        let parsed = parse_statement(
            "CREATE REINGESTOR fw1 FROM ss1 TO ss3 SET normalized = lower(input.raw) WHERE \
             output.normalized != '' BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH \
             EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        )
        .expect("parse should succeed");

        let Statement::Create(parsed) = parsed else {
            panic!("expected reingestor statement");
        };
        let Model::Reingestor(reingestor) = parsed.body.as_ref() else {
            panic!("expected reingestor statement");
        };
        assert_eq!(reingestor.mode, AckMode::Attached);
        assert_eq!(reingestor.from.from[0].as_str(), "ss1");
        assert_eq!(reingestor.output_routes.routes.len(), 1);
        let output = reingestor
            .output_routes
            .routes
            .first()
            .expect("output route should parse");
        assert_eq!(output.relay.as_str(), "ss3");
        assert_eq!(
            reingestor.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "100ms"
        );
        assert!(!output.construction.assignments.is_empty());
        assert!(output.construction.where_clause.is_some());
    }

    #[test]
    fn parses_emitter_statement_with_implicit_attached_mode() {
        let parsed = parse_statement(
            "CREATE EMITTER emit FROM notifications ENCODE USING notification_codec TO KAFKA \
             kafka_main TOPIC notifications_out FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE \
             ERROR LOG ON GENERAL ERROR LOG;",
        )
        .expect("parse should succeed");

        let Statement::Create(parsed) = parsed else {
            panic!("expected emitter statement");
        };
        let Model::Emitter(emitter) = parsed.body.as_ref() else {
            panic!("expected emitter statement");
        };
        assert_eq!(emitter.mode, AckMode::Attached);
    }

    #[test]
    fn runtime_nodes_require_supported_error_policy_blocks() {
        let external_cases = [
            (
                "ingestor",
                "CREATE INGESTOR http_notifications FROM ENDPOINT http_notifications_endpoint \
                 MODE NO_ACK SEQUENTIAL DECODE USING notification_codec TO notifications BRANCHED \
                 BY user_id_branch SET user_id = message.user_id FLUSH EACH 100ms MAX BATCH SIZE \
                 1MiB ON MESSAGE ERROR LOG",
                " ON GENERAL ERROR LOG;",
            ),
            (
                "emitter",
                "CREATE EMITTER kafka_emit FROM notifications ENCODE USING notification_codec TO \
                 KAFKA kafka_main TOPIC notifications_out FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                 MESSAGE ERROR LOG",
                " ON GENERAL ERROR LOG;",
            ),
        ];

        for (node, prefix, suffix) in external_cases {
            let full = format!("{prefix}{suffix}");
            parse_statement(&full).unwrap_or_else(|error| {
                panic!("{node} should parse with message and general error policies: {error:?}")
            });

            let missing_both = format!("{};", prefix.replace(" ON MESSAGE ERROR LOG", ""));
            assert!(
                parse_statement(&missing_both).is_err(),
                "{node} should reject missing error policies"
            );

            let missing_general = format!("{prefix};");
            assert!(
                parse_statement(&missing_general).is_err(),
                "{node} should reject missing general error policy"
            );
        }

        let processor_cases = [
            (
                "reingestor",
                "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications \
                 BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH \
                 SIZE 1MiB ON MESSAGE ERROR LOG",
            ),
            (
                "junction",
                "CREATE JUNCTION join_streams FROM notifications_a, notifications_b BRANCHED BY \
                 tenant_branch TO notifications_all INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE \
                 1MiB ON MESSAGE ERROR LOG",
            ),
            (
                "deduplicator",
                "CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id \
                 MAX TIME 10m BRANCHED BY tenant_branch TO deduped INHERIT ALL FLUSH EACH 100ms \
                 MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG",
            ),
            (
                "window processor",
                "CREATE WINDOW PROCESSOR latency_window FROM metrics WIDTH 10s DURATION STEP 5s \
                 DURATION BRANCHED BY tenant_branch TO metric_summaries SET total_latency = \
                 SUM(input.latency) ON MESSAGE ERROR LOG",
            ),
            (
                "generator",
                "CREATE GENERATOR synth USING MATERIALIZED STATE notifications EACH 100ms \
                 BRANCHED BY tenant_branch TO alerts SET user_id = \
                 relay_state.notifications.user_id FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                 MESSAGE ERROR LOG",
            ),
            (
                "inferencer",
                r#"CREATE INFERENCER score_model FROM features USING RESOURCE fraud_model VERSION 3 FILE 'models/fraud.onnx' INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector } OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] } UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG"#,
            ),
        ];

        for (node, prefix) in processor_cases {
            let full = format!("{prefix};");
            parse_statement(&full).unwrap_or_else(|error| {
                panic!("{node} should parse with message error policy: {error:?}")
            });

            let missing_both = format!("{};", prefix.replace(" ON MESSAGE ERROR LOG", ""));
            assert!(
                parse_statement(&missing_both).is_err(),
                "{node} should reject missing error policies"
            );

            let with_general =
                format!("{prefix} ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;");
            assert!(
                parse_statement(&with_general).is_err(),
                "{node} should reject general error policy"
            );
        }
    }

    #[test]
    fn message_error_policy_accepts_send_to_and_rejects_legacy_dlq() {
        let parsed = parse_statement(
            "CREATE REINGESTOR pass_through FROM notifications TO forwarded_notifications \
             BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH \
             SIZE 1MiB ON MESSAGE ERROR SEND TO error_stream SET error_message = error.message;",
        )
        .expect("message error SEND TO policy should parse");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("policy should render");
        assert!(
            canonical.contains(
                "ON MESSAGE ERROR SEND TO error_stream SET error_message = error.message"
            )
        );
        assert!(!canonical.contains("ON MESSAGE ERROR DLQ"));

        assert!(
            parse_statement(
                "CREATE REINGESTOR pass_through FROM notifications TO forwarded_notifications \
                 BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH \
                 SIZE 1MiB ON MESSAGE ERROR DLQ error_stream SET error_message = error.message;",
            )
            .is_err(),
            "legacy DLQ syntax must be rejected"
        );
    }

    #[test]
    fn message_error_policy_completion_suggests_send_to_without_branch_leakage() {
        let input = "CREATE REINGESTOR pass_through FROM notifications TO forwarded_notifications \
                     UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR ";
        let suggestions = suggest_statement(input, input.len());

        assert!(suggestions.contains(&"SEND TO".to_string()));
        assert!(!suggestions.contains(&"DLQ".to_string()));
        assert!(!suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(!suggestions.contains(&"UNBRANCHED".to_string()));
    }

    #[test]
    fn branch_preserving_processors_accept_unbranched() {
        for statement in [
            "CREATE RELAY raw SCHEMA metric UNBRANCHED;",
            "CREATE DEDUPLICATOR dedup FROM raw DEDUPLICATE ON input.value MAX TIME 10m \
             UNBRANCHED TO projected INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE \
             ERROR LOG;",
            "CREATE REORDERER reorder FROM raw BY input.value MAX TIME 10s UNBRANCHED TO \
             projected INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
            "CREATE JUNCTION join_streams FROM left, right UNBRANCHED TO joined INHERIT ALL FLUSH \
             EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
            "CREATE WINDOW PROCESSOR window_metrics FROM raw WIDTH 2 MESSAGES STEP 2 MESSAGES \
             UNBRANCHED TO projected SET value = COUNT(input.value) ON MESSAGE ERROR LOG;",
            "CREATE INFERENCER score FROM features USING RESOURCE fraud_model VERSION 1 FILE \
             'models/simple_score.onnx' INPUTS { \"features\" DENSE TENSOR<F32>[2] = input.vector \
             } OUTPUT SCHEMA { \"score\" DENSE TENSOR<F32>[1] } UNBRANCHED TO scored SET score = \
             score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
            "CREATE WASM PROCESSOR filter_even FROM raw USING RESOURCE wasm_filter VERSION 1 FILE \
             'processors/filter_even.wasm' UNBRANCHED TO projected ON MESSAGE ERROR LOG ON GLOBAL \
             ERROR LOG;",
        ] {
            parse_statement(statement).unwrap_or_else(|error| {
                panic!("statement should parse: {statement}\n{error:?}");
            });
        }
    }

    #[test]
    fn drop_context_suggestions_do_not_leak_format_keywords() {
        let input = "DROP ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"SCHEMA".to_string()));
        assert!(suggestions.contains(&"WIRE".to_string()));
        assert!(suggestions.contains(&"CODEC".to_string()));
        assert!(suggestions.contains(&"CLIENT".to_string()));
        assert!(suggestions.contains(&"VHOST".to_string()));
        assert!(suggestions.contains(&"ENDPOINT".to_string()));
        assert!(suggestions.contains(&"NODE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn create_client_name_completion_is_not_semantic_reference_lookup() {
        let input = "CREATE CLIENT ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"client_name".to_string()));
        assert!(!suggestions.contains(&"ref:client".to_string()));
    }

    #[test]
    fn canonical_roundtrip_schema() {
        let input = r#"
            CREATE SCHEMA notification (
                user_id U32,
                created_at DATETIME,
                payload STRING
            );
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_wire_schema() {
        let input = r#"
            CREATE STRICT WIRE JSON SCHEMA notification_wire (
                user_id integer,
                created_at string,
                payload object
            );
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_transport() {
        let input = r#"
            CREATE CLIENT kafka_main
              TYPE KAFKA
              CONFIG {
                'bootstrap.servers' = 'host1:9092,host2:9092',
                'enable.auto.commit' = true
              };
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_http_client() {
        let input = r#"
            CREATE CLIENT http_main
              TYPE HTTP
              CONFIG {
                'endpoint' = 'https://api.example.com/events',
                'method' = 'POST'
              };
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_rabbitmq_transport() {
        let input = r#"
            CREATE CLIENT rabbit_main
              TYPE RABBITMQ
              CONFIG {
                'addr' = 'amqp://guest:guest@localhost:5672/%2f',
                'connection_name' = 'nervix-rabbit'
              };
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_websockets_client() {
        let input = r#"
            CREATE CLIENT ws_main
              TYPE WEBSOCKETS
              CONFIG {
                'endpoint' = 'wss://api.example.com/ws',
                'subprotocol' = 'notifications'
              };
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_redis_transport() {
        let input = r#"
            CREATE CLIENT redis_main
              TYPE REDIS
              CONFIG {
                'addr' = 'redis://127.0.0.1:6379/',
                'read_timeout_ms' = 5000
              };
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_mqtt_transport() {
        let input = r#"
            CREATE CLIENT mqtt_main
              TYPE MQTT
              CONFIG {
                'addr' = 'mqtt://127.0.0.1:1883',
                'client_id' = 'nervix-mqtt'
              };
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_prometheus_transport() {
        let input = r#"
            CREATE CLIENT prom_main
              TYPE PROMETHEUS
              CONFIG {
                'addr' = 'http://127.0.0.1:9090'
              };
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_vhost() {
        let input = r#"
            CREATE VHOST my_vhost api.example.com, foo-bar.localhost WITH TLS tls_bundle VERSION 3;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_endpoint() {
        let input = r#"
            CREATE ENDPOINT my_ws_endpoint
                ON edge
                PATH '/ws'
                TYPE WEBSOCKETS;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_http_endpoint() {
        let input = r#"
            CREATE ENDPOINT my_http_endpoint
                ON edge
                PATH '/ingest'
                TYPE HTTP;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_ingestor() {
        let input = r#"
            CREATE INGESTOR kafka_notifications
                FROM
                    KAFKA kafka_main
                    TOPIC notifications
                    OFFSET BY CONSUMER GROUP nervix_consumer
                    MODE ACK PARALLEL MAX 10 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
                DECODE USING notification_kafka_message
                TO notifications
                    BRANCHED BY user_id_kind_branch
                    SET user_id = message.user_id, kind = message.kind
                    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                    ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_prometheus_ingestor() {
        let input = r#"
            CREATE INGESTOR prom_samples
                FROM PROMETHEUS prom_main
                QUERY 'label_replace(vector(42.5), "source", "local", "", "")'
                EVERY 15s
                DECODE USING sample_codec
                TO samples
                    BRANCHED BY source_branch SET source = message.source
                    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                    ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_stream() {
        let input = r#"
            CREATE RELAY p99_latency SCHEMA notification_schema UNBRANCHED;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_junction() {
        let input = r#"
            CREATE JUNCTION join_streams
                FROM ss1, ss2, ss3
                BRANCHED BY tenant
                TO ss10 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_preserves_conditional_surface_forms() {
        let input = r#"
            CREATE JUNCTION conditional
                FROM source
                UNBRANCHED
                TO projected
                    INHERIT ALL
                    SET if_result = IF input.active THEN 1 ELSE 0 END,
                        simple_result = CASE input.kind
                            WHEN "primary" THEN 1
                            WHEN "secondary" THEN 2
                            ELSE 0
                        END,
                        searched_result = CASE
                            WHEN input.active THEN 1
                        END
                    FLUSH IMMEDIATE
                    ON MESSAGE ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        assert!(canonical.contains("IF input.active THEN 1 ELSE 0 END"));
        assert!(canonical.contains("CASE input.kind WHEN"));
        assert!(canonical.contains("CASE WHEN input.active THEN 1 END"));
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_deduplicator() {
        let input = r#"
            CREATE DEDUPLICATOR dedup_txns
                FROM ss1
                DEDUPLICATE ON input.transaction_id
                MAX TIME 10m
                BRANCHED BY tenant
                TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_emitter() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO KAFKA broker1 TOPIC topic FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_pulsar_emitter() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO PULSAR pulsar_main TOPIC topic FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_rabbitmq_emitter() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO RABBITMQ broker1 QUEUE outbox FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn canonical_roundtrip_redis_emitter() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO REDIS PUBSUB broker1 CHANNEL outbox FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_statement(input).expect("parse should succeed");
        let Statement::Create(parsed) = parsed else {
            panic!("expected create statement");
        };
        let canonical = parsed.to_canonical_nspl().expect("must render canonical");
        let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
        assert_eq!(Statement::Create(parsed), reparsed);
    }

    #[test]
    fn bolero_model_roundtrip_canonical_nspl() {
        check!()
            .with_test_time(std::time::Duration::from_millis(150))
            .for_each(|bytes: &[u8]| {
                let model = gen_model(bytes);
                let canonical = model
                    .to_canonical_nspl()
                    .expect("generator output must be renderable");
                let reparsed =
                    parse_statement(&canonical).expect("canonical NSPL must be parseable");
                assert_eq!(
                    Statement::Create(nervix_models::CreateStatement::new(Box::new(model), false)),
                    reparsed
                );
            });
    }
}
