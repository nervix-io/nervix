use std::fmt::{Display, Formatter};

use crate::{
    AssignmentTargetScope, AvroType, AzureBlobConfigEntry, BinaryOperator, BranchEviction,
    BranchSelection, ClickHouseConfigEntry, ClickHouseValueMapping, CodecEncoding,
    CodecEncodingRule, CodecJaqTransformations, CodecWireFormat, CorrelationTimeoutAction,
    CreateBranch, CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs, CreateClientHttp,
    CreateClientIcebergRest, CreateClientKafka, CreateClientKinesis, CreateClientMongoDb,
    CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientPostgres,
    CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
    CreateClientS3, CreateClientSqs, CreateClientWebsockets, CreateClientZeroMq, CreateCodec,
    CreateCorrelator, CreateDeduplicator, CreateEmitter, CreateEndpoint, CreateGenerator,
    CreateInferencer, CreateIngestor, CreateJunction, CreateLookup, CreateMaterializer,
    CreateReingestor, CreateRelay, CreateReorderer, CreateSchema, CreateSignalingProtocol,
    CreateUdf, CreateVhost, CreateWasmProcessor, CreateWindowProcessor, CreateWireSchema,
    CreateWireSchemaStmt, EmitSink, EndpointIngestMode, EndpointType, Expression, FieldScope,
    GcsConfigEntry, GeneralErrorPolicy, HttpConfigEntry, IcebergCatalog, Identifier,
    InferencerTensorDeclaration, InferencerTensorDimension, InferencerTensorMapping, IngestSource,
    IngestTimestampSource, Inheritance, JsonType, KafkaConfigEntry, KafkaIngestMode,
    KafkaOffsetMode, KinesisConfigEntry, KinesisIngestMode, Literal, MaterializedRelayState,
    MaterializedStateDependency, MaterializedStatePolicy, MessageErrorPolicy, Model,
    MongoDbConfigEntry, MongoDbConflictAction, MqttConfigEntry, MqttIngestMode, MqttQos,
    MqttSession, MySqlConfigEntry, MySqlConflictAction, NatsConfigEntry, NatsIngestMode,
    OutputBranch, ParseAsType, PostgresConfigEntry, PostgresConflictAction, ProcessorInputWhere,
    ProcessorInputs, ProcessorOutputs, PrometheusConfigEntry, PulsarConfigEntry, PulsarIngestMode,
    RabbitMqConfigEntry, RabbitMqIngestMode, RedisConfigEntry, RedisPubSubIngestMode,
    RelayBranching, RetryPolicy, RouteConstruction, S3ConfigEntry, SchemaField, SqsConfigEntry,
    SqsIngestMode, UnaryOperator, WebsocketsConfigEntry, WebsocketsIngestMode, WindowBound,
    WireSchemaField, ZeroMqConfigEntry, ZeroMqIngestMode,
};

pub fn expression_to_nspl(expression: &Expression) -> Result<String, CanonicalNsplError> {
    match expression {
        Expression::Literal(Literal::I64(value)) => Ok(value.to_string()),
        Expression::Literal(Literal::F64(value)) => Ok(value.value().to_string()),
        Expression::Literal(Literal::Bool(value)) => Ok(value.to_string().to_ascii_uppercase()),
        Expression::Literal(Literal::String(value)) => string_literal(value),
        Expression::Literal(Literal::Null) => Ok("NULL".to_string()),
        Expression::Field(reference) => {
            let prefix = match &reference.scope {
                FieldScope::Bare => None,
                FieldScope::Message => Some("message".to_string()),
                FieldScope::Input => Some("input".to_string()),
                FieldScope::Output => Some("output".to_string()),
                FieldScope::Branch => Some("branch".to_string()),
                FieldScope::Left => Some("left".to_string()),
                FieldScope::Right => Some("right".to_string()),
                FieldScope::RelayState { relay } => Some(format!("relay_state.{}", relay.as_str())),
                FieldScope::Metadata => Some("metadata".to_string()),
                FieldScope::PartialOutput => Some("partial_output".to_string()),
                FieldScope::Error => Some("error".to_string()),
            };
            Ok(prefix.map_or_else(
                || reference.field.as_str().to_string(),
                |prefix| format!("{prefix}.{}", reference.field.as_str()),
            ))
        }
        Expression::Unary {
            operator,
            expression,
        } => Ok(format!(
            "{}({})",
            match operator {
                UnaryOperator::Negate => "-",
                UnaryOperator::Not => "NOT ",
            },
            expression_to_nspl(expression)?
        )),
        Expression::Binary {
            operator,
            left,
            right,
        } => Ok(format!(
            "({} {} {})",
            expression_to_nspl(left)?,
            match operator {
                BinaryOperator::Add => "+",
                BinaryOperator::Subtract => "-",
                BinaryOperator::Multiply => "*",
                BinaryOperator::Divide => "/",
                BinaryOperator::Remainder => "%",
                BinaryOperator::Equal => "=",
                BinaryOperator::NotEqual => "!=",
                BinaryOperator::GreaterThan => ">",
                BinaryOperator::LessThan => "<",
                BinaryOperator::GreaterThanOrEqual => ">=",
                BinaryOperator::LessThanOrEqual => "<=",
                BinaryOperator::And => "AND",
                BinaryOperator::Or => "OR",
            },
            expression_to_nspl(right)?
        )),
        Expression::Cast { expression, target } => Ok(format!(
            "({} AS {})",
            expression_to_nspl(expression)?,
            parse_as_to_keyword(target)
        )),
        Expression::Call {
            function,
            arguments,
        } => Ok(format!(
            "{}({})",
            function.as_str(),
            arguments
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        Expression::UdfCall {
            function,
            arguments,
        } => Ok(format!(
            "udf::{}({})",
            function.as_str(),
            arguments
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        Expression::Array(items) => Ok(format!(
            "[{}]",
            items
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        Expression::If {
            condition,
            then_result,
            else_result,
        } => Ok(format!(
            "IF {} THEN {} ELSE {} END",
            expression_to_nspl(condition)?,
            expression_to_nspl(then_result)?,
            expression_to_nspl(else_result)?
        )),
        Expression::Case {
            operand,
            branches,
            else_result,
        } => {
            let mut rendered = "CASE".to_string();
            if let Some(operand) = operand {
                rendered.push(' ');
                rendered.push_str(&expression_to_nspl(operand)?);
            }
            for branch in branches {
                rendered.push_str(" WHEN ");
                rendered.push_str(&expression_to_nspl(&branch.when)?);
                rendered.push_str(" THEN ");
                rendered.push_str(&expression_to_nspl(&branch.result)?);
            }
            if let Some(else_result) = else_result {
                rendered.push_str(" ELSE ");
                rendered.push_str(&expression_to_nspl(else_result)?);
            }
            rendered.push_str(" END");
            Ok(rendered)
        }
    }
}

fn route_construction_to_nspl(
    construction: &RouteConstruction,
) -> Result<String, CanonicalNsplError> {
    let mut clauses = Vec::new();
    if let Some(inherit) = &construction.inherit {
        let clause = match inherit {
            Inheritance::All => "INHERIT ALL".to_string(),
            Inheritance::AllExcept(fields) => format!(
                "INHERIT ALL EXCEPT {}",
                fields
                    .iter()
                    .map(Identifier::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Inheritance::Fields(fields) => format!(
                "INHERIT {}",
                fields
                    .iter()
                    .map(|field| format!(
                        "{}{}",
                        field.field.as_str(),
                        if field.leak_sensitive {
                            " LEAK SENSITIVE"
                        } else {
                            ""
                        }
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
        clauses.push(clause);
    }
    if !construction.assignments.is_empty() {
        clauses.push(format!(
            "SET {}",
            construction
                .assignments
                .iter()
                .map(|assignment| {
                    let prefix = match assignment.target.scope {
                        AssignmentTargetScope::Bare => "",
                        AssignmentTargetScope::Message => "message.",
                        AssignmentTargetScope::Output => "output.",
                        AssignmentTargetScope::Branch => "branch.",
                    };
                    Ok(format!(
                        "{prefix}{} = {}",
                        assignment.target.field.as_str(),
                        expression_to_nspl(&assignment.value)?
                    ))
                })
                .collect::<Result<Vec<_>, CanonicalNsplError>>()?
                .join(", ")
        ));
    }
    if let Some(where_clause) = &construction.where_clause {
        clauses.push(format!("WHERE {}", expression_to_nspl(where_clause)?));
    }
    if !construction.invocations.is_empty() {
        clauses.push(format!(
            "INVOKE {}",
            construction
                .invocations
                .iter()
                .map(|invocation| Ok(format!(
                    "{}({})",
                    invocation.function.as_str(),
                    invocation
                        .arguments
                        .iter()
                        .map(expression_to_nspl)
                        .collect::<Result<Vec<_>, _>>()?
                        .join(", ")
                )))
                .collect::<Result<Vec<_>, CanonicalNsplError>>()?
                .join(", ")
        ));
    }
    Ok(clauses.join(" "))
}

fn route_construction_suffix(
    construction: &RouteConstruction,
) -> Result<String, CanonicalNsplError> {
    let rendered = route_construction_to_nspl(construction)?;
    Ok(if rendered.is_empty() {
        String::new()
    } else {
        format!(" {rendered}")
    })
}

fn value_mappings_to_nspl(values: &[ClickHouseValueMapping]) -> Result<String, CanonicalNsplError> {
    values
        .iter()
        .map(|mapping| {
            Ok(format!(
                "{} = {}",
                string_literal(&mapping.column)?,
                expression_to_nspl(&mapping.expression)?
            ))
        })
        .collect::<Result<Vec<_>, CanonicalNsplError>>()
        .map(|mappings| mappings.join(", "))
}

fn branch_selection_to_nspl(branching: &BranchSelection) -> String {
    match branching {
        BranchSelection::BranchedBy { branch } => {
            format!("BRANCHED BY {}", branch.as_str())
        }
        BranchSelection::Unbranched => "UNBRANCHED".to_string(),
    }
}

fn output_branch_to_nspl(branching: &OutputBranch) -> Result<String, CanonicalNsplError> {
    match branching {
        OutputBranch::BranchedBy {
            branch,
            assignments,
        } => {
            let mut rendered = format!("BRANCHED BY {}", branch.as_str());
            if !assignments.is_empty() {
                let construction = RouteConstruction {
                    assignments: assignments.clone(),
                    ..RouteConstruction::default()
                };
                rendered.push(' ');
                rendered.push_str(&route_construction_to_nspl(&construction)?);
            }
            Ok(rendered)
        }
        OutputBranch::Unbranched => Ok("UNBRANCHED".to_string()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalNsplError {
    UnrepresentableStringLiteral { value: String },
    InvalidCodec { reason: String },
}

impl Display for CanonicalNsplError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnrepresentableStringLiteral { value } => write!(
                f,
                "cannot represent string literal in NSPL without escapes: {value:?}"
            ),
            Self::InvalidCodec { reason } => write!(f, "invalid codec: {reason}"),
        }
    }
}

impl std::error::Error for CanonicalNsplError {}

impl Model {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        match self {
            Self::Schema(schema) => schema.to_canonical_nspl(),
            Self::WireSchema(schema) => schema.to_canonical_nspl(),
            Self::Codec(codec) => codec.to_canonical_nspl(),
            Self::ClientKafka(client) => client.to_canonical_nspl(),
            Self::ClientPulsar(client) => client.to_canonical_nspl(),
            Self::ClientKinesis(client) => client.to_canonical_nspl(),
            Self::ClientHttp(client) => client.to_canonical_nspl(),
            Self::ClientPrometheus(client) => client.to_canonical_nspl(),
            Self::ClientMqtt(client) => client.to_canonical_nspl(),
            Self::ClientNats(client) => client.to_canonical_nspl(),
            Self::ClientRabbitMq(client) => client.to_canonical_nspl(),
            Self::ClientRedis(client) => client.to_canonical_nspl(),
            Self::ClientZeroMq(client) => client.to_canonical_nspl(),
            Self::ClientSqs(client) => client.to_canonical_nspl(),
            Self::ClientWebsockets(client) => client.to_canonical_nspl(),
            Self::ClientClickHouse(client) => client.to_canonical_nspl(),
            Self::ClientPostgres(client) => client.to_canonical_nspl(),
            Self::ClientMySql(client) => client.to_canonical_nspl(),
            Self::ClientMongoDb(client) => client.to_canonical_nspl(),
            Self::ClientS3(client) => client.to_canonical_nspl(),
            Self::ClientGcs(client) => client.to_canonical_nspl(),
            Self::ClientAzureBlob(client) => client.to_canonical_nspl(),
            Self::ClientIcebergRest(client) => client.to_canonical_nspl(),
            Self::Vhost(vhost) => vhost.to_canonical_nspl(),
            Self::Branch(branch) => branch.to_canonical_nspl(),
            Self::Endpoint(endpoint) => endpoint.to_canonical_nspl(),
            Self::SignalingProtocol(protocol) => protocol.to_canonical_nspl(),
            Self::Generator(generator) => generator.to_canonical_nspl(),
            Self::Inferencer(inference) => inference.to_canonical_nspl(),
            Self::WasmProcessor(processor) => processor.to_canonical_nspl(),
            Self::Ingestor(ingestor) => ingestor.to_canonical_nspl(),
            Self::Reingestor(reingestor) => reingestor.to_canonical_nspl(),
            Self::Relay(relay) => relay.to_canonical_nspl(),
            Self::Materializer(materializer) => materializer.to_canonical_nspl(),
            Self::Lookup(lookup) => lookup.to_canonical_nspl(),
            Self::Junction(junction) => junction.to_canonical_nspl(),
            Self::Deduplicator(deduplicator) => deduplicator.to_canonical_nspl(),
            Self::Correlator(correlator) => correlator.to_canonical_nspl(),
            Self::Reorderer(reorderer) => reorderer.to_canonical_nspl(),
            Self::WindowProcessor(window_processor) => window_processor.to_canonical_nspl(),
            Self::Emitter(emitter) => emitter.to_canonical_nspl(),
            Self::Udf(udf) => udf.to_canonical_nspl(),
        }
    }
}

impl CreateUdf {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let arguments = self
            .arguments
            .iter()
            .map(|argument| {
                format!(
                    "{} {}{}",
                    argument.name.as_str(),
                    parse_as_to_keyword(&argument.ty),
                    if argument.optional { " OPTIONAL" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let returns = format!(
            "{}{}",
            parse_as_to_keyword(&self.returns.ty),
            if self.returns.optional {
                " OPTIONAL"
            } else {
                ""
            }
        );
        let mut delimiter = "$roto$".to_string();
        let mut suffix = 1_u64;
        while self.code.contains(&delimiter) {
            delimiter = format!("$roto_{suffix}$");
            suffix += 1;
        }

        Ok(format!(
            "CREATE UDF {}\n  WITH {}\n  ARGS ({arguments})\n  RETURNS {returns}{}\n  CODE \
             {delimiter}{}{delimiter};",
            self.name.as_str(),
            self.language.as_ref(),
            if self.volatile { "\n  VOLATILE" } else { "" },
            self.code,
        ))
    }
}

impl CreateSchema {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let fields = self
            .fields
            .iter()
            .map(schema_field_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE SCHEMA {} ({});",
            self.name.as_str(),
            fields
        ))
    }
}

impl CreateWireSchemaStmt {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        match self {
            Self::Json(schema) => wire_schema_to_nspl("JSON", schema),
            Self::Cbor(schema) => wire_schema_to_nspl("CBOR", schema),
            Self::Avro(schema) => wire_schema_to_nspl("AVRO", schema),
        }
    }
}

impl CreateClientKafka {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(kafka_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE KAFKA{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientKinesis {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(kinesis_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE KINESIS{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientHttp {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(http_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE HTTP{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientPulsar {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(pulsar_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE PULSAR{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientMqtt {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(mqtt_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE MQTT{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientNats {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(nats_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE NATS{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientPrometheus {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(prometheus_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE PROMETHEUS{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientRabbitMq {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(rabbitmq_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE RABBITMQ{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientRedis {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(redis_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE REDIS{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientZeroMq {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(zeromq_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE ZEROMQ{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientSqs {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(sqs_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE SQS{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientS3 {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(s3_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE S3{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientGcs {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(gcs_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE GCS{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientAzureBlob {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(azure_blob_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE AZURE_BLOB{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientIcebergRest {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(kafka_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE ICEBERG_REST{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientWebsockets {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(websockets_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE WEBSOCKETS{}{} CONFIG {{{}}};",
            self.name.as_str(),
            signaling_protocol_clause(self.signaling_protocol.as_ref()),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientClickHouse {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(clickhouse_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE CLICKHOUSE{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientPostgres {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(postgres_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE POSTGRES{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientMySql {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(mysql_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE MYSQL{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl CreateClientMongoDb {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(mongodb_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE CLIENT {} TYPE MONGODB{} CONFIG {{{}}};",
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

fn client_mount_clause(mount: Option<&Identifier>) -> String {
    mount
        .map(|mount| format!(" MOUNT {}", mount.as_str()))
        .unwrap_or_default()
}

fn signaling_protocol_clause(signaling_protocol: Option<&Identifier>) -> String {
    signaling_protocol
        .map(|protocol| format!(" WITH SIGNALING PROTOCOL {}", protocol.as_str()))
        .unwrap_or_default()
}

impl CreateVhost {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let tls = self
            .tls
            .as_ref()
            .map(|tls| {
                let mut rendered = format!(" WITH TLS {}", tls.resource.as_str());
                if let Some(version) = tls.version {
                    rendered.push_str(&format!(" VERSION {version}"));
                }
                rendered
            })
            .unwrap_or_default();
        Ok(format!(
            "CREATE VHOST {} {}{};",
            self.name.as_str(),
            self.hostnames.join(", "),
            tls,
        ))
    }
}

impl CreateEndpoint {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE ENDPOINT {} ON {} PATH {} TYPE {}{};",
            self.name.as_str(),
            self.on_vhost.as_str(),
            string_literal(&self.path)?,
            endpoint_type_to_nspl(self.endpoint_type),
            signaling_protocol_clause(self.signaling_protocol.as_ref())
        ))
    }
}

impl CreateSignalingProtocol {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let send_bodies = self
            .on_connect
            .send_bodies
            .iter()
            .map(|body| string_literal(body))
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        let wait_bodies = self
            .on_connect
            .wait_bodies
            .iter()
            .map(|body| string_literal(body))
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(format!(
            "CREATE SIGNALING PROTOCOL {} ON CONNECT SEND BODY {} WAIT BODY {} TIMEOUT {};",
            self.name.as_str(),
            send_bodies,
            wait_bodies,
            self.on_connect.timeout
        ))
    }
}

impl CreateCodec {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let (wire, transformations) =
            match &self.wire_format {
                CodecWireFormat::Json => {
                    let wire_schema = self.wire_schema.as_ref().ok_or_else(|| {
                        CanonicalNsplError::InvalidCodec {
                            reason: "JSON codec is missing wire schema reference".to_string(),
                        }
                    })?;
                    (
                        format!("WIRE JSON SCHEMA {}", wire_schema.as_str()),
                        String::new(),
                    )
                }
                CodecWireFormat::Cbor => {
                    let wire_schema = self.wire_schema.as_ref().ok_or_else(|| {
                        CanonicalNsplError::InvalidCodec {
                            reason: "CBOR codec is missing wire schema reference".to_string(),
                        }
                    })?;
                    (
                        format!("WIRE CBOR SCHEMA {}", wire_schema.as_str()),
                        String::new(),
                    )
                }
                CodecWireFormat::Avro => {
                    let wire_schema = self.wire_schema.as_ref().ok_or_else(|| {
                        CanonicalNsplError::InvalidCodec {
                            reason: "AVRO codec is missing wire schema reference".to_string(),
                        }
                    })?;
                    (
                        format!("WIRE AVRO SCHEMA {}", wire_schema.as_str()),
                        String::new(),
                    )
                }
                CodecWireFormat::JaqNative {
                    format,
                    transformations,
                } => (
                    format.as_ref().to_string(),
                    codec_jaq_transformations_to_nspl(transformations)?,
                ),
                CodecWireFormat::Protobuf(config) => {
                    let version = config
                        .resource_version
                        .map(|version| format!(" VERSION {version}"))
                        .unwrap_or_default();
                    let protobuf_config = config
                        .config
                        .iter()
                        .map(kafka_entry_to_nspl)
                        .collect::<Result<Vec<_>, _>>()?
                        .join(", ");
                    (
                        format!(
                            "PROTOBUF USING RESOURCE {}{} CONFIG {{{}}} MESSAGE {}",
                            config.resource.as_str(),
                            version,
                            protobuf_config,
                            string_literal(&config.message)?
                        ),
                        codec_jaq_transformations_to_nspl(&config.transformations)?,
                    )
                }
            };
        let encoding_rules = if self.encoding_rules.is_empty() {
            String::new()
        } else {
            format!(
                " ENCODE {}",
                self.encoding_rules
                    .iter()
                    .map(codec_encoding_rule_to_nspl)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        Ok(format!(
            "CREATE CODEC {} FROM {} TO SCHEMA {}{}{};",
            self.name.as_str(),
            wire,
            self.schema.as_str(),
            transformations,
            encoding_rules
        ))
    }
}

fn codec_jaq_transformations_to_nspl(
    transformations: &CodecJaqTransformations,
) -> Result<String, CanonicalNsplError> {
    if !transformations.has_any() {
        return Err(CanonicalNsplError::InvalidCodec {
            reason: "codec is missing JAQ transformation".to_string(),
        });
    }
    let mut rendered = String::from(" WITH JAQ TRANSFORMATIONS");
    if let Some(program) = transformations.on_ingestion.as_deref() {
        rendered.push_str(" ON INGESTION ");
        rendered.push_str(&string_literal(program)?);
    }
    if let Some(program) = transformations.on_emitting.as_deref() {
        rendered.push_str(" ON EMITTING ");
        rendered.push_str(&string_literal(program)?);
    }
    Ok(rendered)
}

fn codec_encoding_rule_to_nspl(rule: &CodecEncodingRule) -> String {
    format!(
        "{} AS {}",
        rule.field.as_str(),
        codec_encoding_to_nspl(rule.encoding)
    )
}

fn codec_encoding_to_nspl(encoding: CodecEncoding) -> &'static str {
    match encoding {
        CodecEncoding::Rfc3339 => "RFC3339",
    }
}

impl CreateBranch {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut rendered = format!(
            "CREATE BRANCH {} SCHEMA {} TTL {}",
            self.name.as_str(),
            self.schema.as_str(),
            self.ttl
        );
        if let Some(eviction) = &self.eviction {
            match eviction {
                BranchEviction::Lru { max_instances } => {
                    rendered.push_str(&format!(" MAX INSTANCES {max_instances} EVICT LRU"));
                }
            }
        }
        rendered.push(';');
        Ok(rendered)
    }
}

impl CreateIngestor {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let timestamp = self
            .timestamp_source
            .as_ref()
            .map(|source| match source {
                IngestTimestampSource::Now => " TIMESTAMP NOW".to_string(),
                IngestTimestampSource::At(field) => {
                    format!(" TIMESTAMP AT {}", field.as_str())
                }
            })
            .unwrap_or_default();
        let source = ingest_source_to_nspl(&self.source);
        Ok(format!(
            "CREATE INGESTOR {} FROM {} DECODE USING {}{}{}{} {};",
            self.name.as_str(),
            source,
            self.decode_using_codec.as_str(),
            timestamp,
            filter_where_suffix(&self.filter_where)?,
            processor_outputs_to_nspl(&self.output_routes)?,
            general_error_policy_to_nspl(&self.general_error_policy)
        ))
    }
}

impl CreateGenerator {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE GENERATOR {} USING MATERIALIZED STATE {} EACH {} {}{};",
            self.name.as_str(),
            self.materialized_relay.as_str(),
            self.each,
            branch_selection_to_nspl(&self.branched_by),
            processor_outputs_to_nspl(&self.output_routes)?
        ))
    }
}

impl CreateRelay {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut rendered = format!(
            "CREATE RELAY {} SCHEMA {}",
            self.name.as_str(),
            self.schema.as_str()
        );
        match &self.branching {
            RelayBranching::BranchedBy { branch } => {
                rendered.push_str(&format!(" BRANCHED BY {}", branch.as_str()));
                rendered.push_str(&format!(" CAPACITY {}", self.buffer));
            }
            RelayBranching::Unbranched => {
                rendered.push_str(&format!(" UNBRANCHED CAPACITY {}", self.buffer));
            }
        }
        if let Some(state) = &self.materialized_state {
            rendered.push(' ');
            rendered.push_str(materialized_relay_state_to_nspl(state));
        }
        rendered.push(';');
        Ok(rendered)
    }
}

impl CreateMaterializer {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "-- MATERIALIZER {} {}",
            self.relay.as_str(),
            materialized_relay_state_to_nspl(&self.state)
        ))
    }
}

impl CreateLookup {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE HASH MAP {} KEY {} FROM RESOURCE {} PATH {} DECODE USING {};",
            self.name.as_str(),
            self.key_field.as_str(),
            self.resource.as_str(),
            string_literal(&self.path)?,
            self.decode_using_codec.as_str()
        ))
    }
}

fn materialized_relay_state_to_nspl(state: &MaterializedRelayState) -> &'static str {
    match state {
        MaterializedRelayState::LastByTimestamp => "WITH MATERIALIZED STATE LAST BY TIMESTAMP",
    }
}

fn flush_policy_to_nspl_with_max(policy: &str, max_batch_size: Option<&str>) -> String {
    if policy.eq_ignore_ascii_case("IMMEDIATE") {
        "FLUSH IMMEDIATE".to_string()
    } else {
        format!(
            "FLUSH EACH {policy} MAX BATCH SIZE {}",
            max_batch_size.unwrap_or("1MiB")
        )
    }
}

fn commit_policy_to_nspl(policy: &str, max_size: &str) -> String {
    format!("COMMIT EACH {policy} MAX SIZE {max_size}")
}

fn message_error_policy_to_nspl(policy: &MessageErrorPolicy) -> Result<String, CanonicalNsplError> {
    Ok(match policy {
        MessageErrorPolicy::Ignore => "ON MESSAGE ERROR IGNORE".to_string(),
        MessageErrorPolicy::Log => "ON MESSAGE ERROR LOG".to_string(),
        MessageErrorPolicy::Dlq { relay, assignments } => {
            let assignments = assignments
                .iter()
                .map(|assignment| {
                    Ok(format!(
                        "{} = {}",
                        assignment.target.field.as_str(),
                        expression_to_nspl(&assignment.value)?
                    ))
                })
                .collect::<Result<Vec<_>, CanonicalNsplError>>()?
                .join(", ");
            format!(
                "ON MESSAGE ERROR SEND TO {} SET {}",
                relay.as_str(),
                assignments
            )
        }
    })
}

fn general_error_policy_to_nspl(policy: &GeneralErrorPolicy) -> &'static str {
    match policy {
        GeneralErrorPolicy::Ignore => "ON GENERAL ERROR IGNORE",
        GeneralErrorPolicy::Log => "ON GENERAL ERROR LOG",
    }
}

fn materialized_state_dependencies_suffix(
    dependencies: &[MaterializedStateDependency],
) -> Result<String, CanonicalNsplError> {
    dependencies
        .iter()
        .map(|dependency| {
            let policy = match &dependency.policy {
                MaterializedStatePolicy::RequiredSkip => "REQUIRED SKIP".to_string(),
                MaterializedStatePolicy::RequiredWait => "REQUIRED WAIT".to_string(),
                MaterializedStatePolicy::Default(assignments) => format!(
                    "DEFAULT {{ {} }}",
                    assignments
                        .iter()
                        .map(|assignment| {
                            Ok(format!(
                                "{} = {}",
                                assignment.target.field.as_str(),
                                expression_to_nspl(&assignment.value)?
                            ))
                        })
                        .collect::<Result<Vec<_>, CanonicalNsplError>>()?
                        .join(", ")
                ),
            };
            Ok(format!(
                " USING MATERIALIZED STATE {} {policy}",
                dependency.relay.as_str()
            ))
        })
        .collect::<Result<String, CanonicalNsplError>>()
}

impl CreateJunction {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE {} JUNCTION {} FROM {}{} {}{}{};",
            self.mode.as_ref(),
            self.name.as_str(),
            processor_inputs_to_nspl(&self.from)?,
            filter_where_suffix(&self.filter_where)?,
            branch_selection_to_nspl(&self.branched_by),
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            processor_outputs_to_nspl(&self.output_routes)?
        ))
    }
}

impl CreateDeduplicator {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE {} DEDUPLICATOR {} FROM {}{} DEDUPLICATE ON {} MAX TIME {} {}{}{};",
            self.mode.as_ref(),
            self.name.as_str(),
            processor_inputs_to_nspl(&self.from)?,
            filter_where_suffix(&self.filter_where)?,
            self.deduplicate_on
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", "),
            self.max_time,
            branch_selection_to_nspl(&self.branched_by),
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            processor_outputs_to_nspl(&self.output_routes)?
        ))
    }
}

impl CreateCorrelator {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE {} CORRELATOR {} {} {} CORRELATE WHERE {} MATCH {} MAX TIME {} ON CORRELATION \
             TIMEOUT {}, {} {}{}{};",
            self.mode.as_ref(),
            self.name.as_str(),
            prefixed_processor_inputs_to_nspl("LEFT", &self.left)?,
            prefixed_processor_inputs_to_nspl("RIGHT", &self.right)?,
            expression_to_nspl(&self.correlate_where)?,
            self.match_policy.as_ref(),
            self.max_time,
            correlation_timeout_action_to_nspl(&self.timeout_policy.left),
            correlation_timeout_action_to_nspl(&self.timeout_policy.right),
            branch_selection_to_nspl(&self.branched_by),
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            processor_outputs_to_nspl(&self.output_routes)?
        ))
    }
}

fn correlation_timeout_action_to_nspl(action: &CorrelationTimeoutAction) -> String {
    match action {
        CorrelationTimeoutAction::Drop => "DROP".to_string(),
        CorrelationTimeoutAction::SendTo { relay } => format!("SEND TO {}", relay.as_str()),
    }
}

impl CreateReorderer {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE {} REORDERER {} FROM {}{} BY {} MAX TIME {} {}{}{};",
            self.mode.as_ref(),
            self.name.as_str(),
            processor_inputs_to_nspl(&self.from)?,
            filter_where_suffix(&self.filter_where)?,
            self.order_by
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", "),
            self.max_time,
            branch_selection_to_nspl(&self.branched_by),
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            processor_outputs_to_nspl(&self.output_routes)?
        ))
    }
}

impl CreateWindowProcessor {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE {} WINDOW PROCESSOR {} FROM {}{} WIDTH {} STEP {} {}{}{};",
            self.mode.as_ref(),
            self.name.as_str(),
            processor_inputs_to_nspl(&self.from)?,
            filter_where_suffix(&self.filter_where)?,
            window_bound_to_nspl(&self.width),
            window_bound_to_nspl(&self.step),
            branch_selection_to_nspl(&self.branched_by),
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            processor_outputs_to_nspl(&self.output_routes)?
        ))
    }
}

impl CreateEmitter {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let (flush_each, max_batch_size) = self.flush_policy();
        let flush_policy = format!(
            " {}",
            flush_policy_to_nspl_with_max(flush_each, max_batch_size)
        );
        let commit_policy = self
            .sink
            .commit_policy()
            .map(|(policy, max_size)| format!(" {}", commit_policy_to_nspl(policy, max_size)))
            .unwrap_or_default();
        Ok(format!(
            "CREATE {} EMITTER {} FROM {}{}{} TO {}{}{}{} {} {};",
            self.mode.as_ref(),
            self.name.as_str(),
            self.from_relay.as_str(),
            self.encode_using_codec
                .as_ref()
                .map(|codec| format!(" ENCODE USING {}", codec.as_str()))
                .unwrap_or_default(),
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            emit_sink_to_nspl(&self.sink)?,
            route_construction_suffix(&self.construction)?,
            flush_policy,
            commit_policy,
            message_error_policy_to_nspl(&self.error_policies.message)?,
            general_error_policy_to_nspl(&self.error_policies.general)
        ))
    }
}

fn window_bound_to_nspl(bound: &WindowBound) -> String {
    let mut parts = Vec::new();
    if let Some(messages) = bound.messages {
        parts.push(format!("{messages} MESSAGES"));
    }
    if let Some(duration) = &bound.duration {
        parts.push(format!("{duration} DURATION"));
    }
    parts.join(" ")
}

impl CreateReingestor {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE {} REINGESTOR {} FROM {}{}{}{};",
            self.mode.as_ref(),
            self.name.as_str(),
            processor_inputs_to_nspl(&self.from)?,
            filter_where_suffix(&self.filter_where)?,
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            processor_outputs_to_nspl(&self.output_routes)?
        ))
    }
}

impl CreateInferencer {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let version = self
            .resource_version
            .map(|version| format!(" VERSION {version}"))
            .unwrap_or_default();
        Ok(format!(
            "CREATE {} INFERENCER {} FROM {}{} USING RESOURCE {}{} FILE {} INPUTS {{ {} }} OUTPUT \
             SCHEMA {{ {} }} {}{}{};",
            self.mode.as_ref(),
            self.name.as_str(),
            processor_inputs_to_nspl(&self.from)?,
            filter_where_suffix(&self.filter_where)?,
            self.resource.as_str(),
            version,
            string_literal(&self.file)?,
            inference_mappings_to_nspl(&self.inputs)?,
            inference_output_schema_to_nspl(&self.output_schema)?,
            branch_selection_to_nspl(&self.branched_by),
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            processor_outputs_to_nspl(&self.output_routes)?
        ))
    }
}

impl CreateWasmProcessor {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let version = self
            .resource_version
            .map(|version| format!(" VERSION {version}"))
            .unwrap_or_default();
        Ok(format!(
            "CREATE {} WASM PROCESSOR {} FROM {}{} USING RESOURCE {}{} FILE {} {}{}{} {};",
            self.mode.as_ref(),
            self.name.as_str(),
            processor_inputs_to_nspl(&self.from)?,
            filter_where_suffix(&self.filter_where)?,
            self.resource.as_str(),
            version,
            string_literal(&self.file)?,
            branch_selection_to_nspl(&self.branched_by),
            materialized_state_dependencies_suffix(&self.materialized_state)?,
            processor_outputs_to_nspl(&self.output_routes)?,
            general_error_policy_to_nspl(&self.global_error_policy).replace("GENERAL", "GLOBAL")
        ))
    }
}

fn inference_mappings_to_nspl(
    mappings: &[InferencerTensorMapping],
) -> Result<String, CanonicalNsplError> {
    mappings
        .iter()
        .map(|mapping| {
            Ok(format!(
                "{} {} TENSOR<{}>[{}] = {}",
                string_literal(&mapping.tensor)?,
                mapping.schema.representation.as_ref(),
                mapping.schema.element_type.as_ref(),
                mapping
                    .schema
                    .dimensions
                    .iter()
                    .map(|dimension| match dimension {
                        InferencerTensorDimension::Fixed(size) => size.to_string(),
                        InferencerTensorDimension::Dynamic => "DYNAMIC".to_string(),
                        InferencerTensorDimension::Batch => "BATCH".to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                expression_to_nspl(&mapping.expression)?
            ))
        })
        .collect::<Result<Vec<_>, CanonicalNsplError>>()
        .map(|items| items.join(", "))
}

fn inference_output_schema_to_nspl(
    declarations: &[InferencerTensorDeclaration],
) -> Result<String, CanonicalNsplError> {
    declarations
        .iter()
        .map(|declaration| {
            Ok(format!(
                "{} {} TENSOR<{}>[{}]",
                string_literal(&declaration.tensor)?,
                declaration.schema.representation.as_ref(),
                declaration.schema.element_type.as_ref(),
                declaration
                    .schema
                    .dimensions
                    .iter()
                    .map(|dimension| match dimension {
                        InferencerTensorDimension::Fixed(size) => size.to_string(),
                        InferencerTensorDimension::Dynamic => "DYNAMIC".to_string(),
                        InferencerTensorDimension::Batch => "BATCH".to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            ))
        })
        .collect::<Result<Vec<_>, CanonicalNsplError>>()
        .map(|items| items.join(", "))
}

fn filter_where_suffix(filter_where: &Option<Expression>) -> Result<String, CanonicalNsplError> {
    filter_where
        .as_ref()
        .map(|condition| Ok(format!(" FILTER WHERE {}", expression_to_nspl(condition)?)))
        .unwrap_or_else(|| Ok(String::new()))
}

fn from_relay_to_nspl(
    relay: &Identifier,
    from_where: &[ProcessorInputWhere],
) -> Result<String, CanonicalNsplError> {
    let where_suffix = from_where
        .iter()
        .find(|item| item.relay == *relay)
        .map(|item| {
            expression_to_nspl(&item.where_clause).map(|condition| format!(" WHERE {condition}"))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(format!("{}{where_suffix}", relay.as_str()))
}

fn processor_inputs_to_nspl(inputs: &ProcessorInputs) -> Result<String, CanonicalNsplError> {
    inputs
        .from
        .iter()
        .map(|relay| from_relay_to_nspl(relay, &inputs.r#where))
        .collect::<Result<Vec<_>, _>>()
        .map(|items| items.join(", "))
}

fn prefixed_processor_inputs_to_nspl(
    prefix: &str,
    inputs: &ProcessorInputs,
) -> Result<String, CanonicalNsplError> {
    inputs
        .from
        .iter()
        .map(|relay| {
            Ok(format!(
                "{prefix} FROM {}",
                from_relay_to_nspl(relay, &inputs.r#where)?
            ))
        })
        .collect::<Result<Vec<_>, CanonicalNsplError>>()
        .map(|items| items.join(" "))
}

fn processor_outputs_to_nspl(outputs: &ProcessorOutputs) -> Result<String, CanonicalNsplError> {
    outputs
        .routes
        .iter()
        .map(|output| {
            let flush = output
                .flush_policy
                .as_ref()
                .map(|policy| {
                    format!(
                        " {}",
                        flush_policy_to_nspl_with_max(
                            &policy.flush_each,
                            policy.max_batch_size.as_deref(),
                        )
                    )
                })
                .unwrap_or_default();
            let construction = route_construction_to_nspl(&output.construction)?;
            let construction = if construction.is_empty() {
                String::new()
            } else {
                format!(" {construction}")
            };
            let branch = output
                .branch
                .as_ref()
                .map(output_branch_to_nspl)
                .transpose()?
                .map(|branch| format!(" {branch}"))
                .unwrap_or_default();
            Ok(format!(
                " TO {}{}{}{} {}",
                output.relay.as_str(),
                construction,
                branch,
                flush,
                message_error_policy_to_nspl(&output.message_error_policy)?
            ))
        })
        .collect::<Result<String, CanonicalNsplError>>()
}

fn schema_field_to_nspl(field: &SchemaField) -> Result<String, CanonicalNsplError> {
    Ok(format!(
        "{} {}{}{}",
        field.name.as_str(),
        parse_as_to_keyword(&field.ty),
        optional_suffix(field.optional),
        sensitive_suffix(field.sensitive)
    ))
}

fn sensitive_suffix(sensitive: bool) -> &'static str {
    if sensitive { " SENSITIVE" } else { "" }
}

fn wire_schema_to_nspl<T>(
    format_kw: &str,
    schema: &CreateWireSchema<T>,
) -> Result<String, CanonicalNsplError>
where
    T: NativeTypeToNspl,
{
    let fields = schema
        .fields
        .iter()
        .map(wire_schema_field_to_nspl::<T>)
        .collect::<Result<Vec<_>, CanonicalNsplError>>()?
        .join(", ");

    Ok(format!(
        "CREATE {} WIRE {format_kw} SCHEMA {} ({});",
        schema.strictness.as_ref(),
        schema.name.as_str(),
        fields
    ))
}

fn wire_schema_field_to_nspl<T>(field: &WireSchemaField<T>) -> Result<String, CanonicalNsplError>
where
    T: NativeTypeToNspl,
{
    Ok(format!(
        "{} {}{}",
        field.name.as_str(),
        field.ty.to_nspl_keyword(),
        optional_suffix(field.optional)
    ))
}

fn optional_suffix(optional: bool) -> &'static str {
    if optional { " OPTIONAL" } else { "" }
}

fn kafka_entry_to_nspl(entry: &KafkaConfigEntry) -> Result<String, CanonicalNsplError> {
    let key = string_literal(&entry.key)?;
    let value = string_literal(&entry.value)?;
    Ok(format!("{key} = {value}"))
}

fn http_entry_to_nspl(entry: &HttpConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn pulsar_entry_to_nspl(entry: &PulsarConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn kinesis_entry_to_nspl(entry: &KinesisConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn ingest_source_to_nspl(source: &IngestSource) -> String {
    match source {
        IngestSource::Http { client, every } => format!("HTTP {} EVERY {}", client.as_str(), every),
        IngestSource::Kinesis {
            client,
            relay,
            instances,
            mode,
        } => format!(
            "KINESIS {} RELAY {}{} MODE {}",
            client.as_str(),
            relay.as_str(),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            kinesis_mode_to_nspl(mode)
        ),
        IngestSource::Kafka {
            client,
            topic,
            offset_mode,
            instances,
            mode,
        } => format!(
            "KAFKA {} TOPIC {} OFFSET BY {}{} MODE {}",
            client.as_str(),
            topic.as_str(),
            kafka_offset_mode_to_nspl(offset_mode),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            kafka_mode_to_nspl(mode)
        ),
        IngestSource::Pulsar {
            client,
            topic,
            subscription,
            instances,
            mode,
        } => format!(
            "PULSAR {} TOPIC {} SUBSCRIPTION {}{} MODE {}",
            client.as_str(),
            topic.as_str(),
            subscription.as_str(),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            pulsar_mode_to_nspl(mode)
        ),
        IngestSource::Mqtt {
            client,
            topic,
            instances,
            mode,
        } => {
            let instances = if *instances > 1 {
                format!(" INSTANCES {instances}")
            } else {
                String::new()
            };
            format!(
                "MQTT {} TOPIC {}{} {}",
                client.as_str(),
                mqtt_topic_to_nspl(topic).expect("validated canonical MQTT topic"),
                instances,
                mqtt_mode_to_nspl(mode)
            )
        }
        IngestSource::Nats {
            client,
            subject,
            queue_group,
            instances,
            mode,
        } => format!(
            "NATS {} SUBJECT {} QUEUE GROUP {} INSTANCES {} MODE {}",
            client.as_str(),
            subject.as_str(),
            queue_group.as_str(),
            instances,
            nats_mode_to_nspl(mode)
        ),
        IngestSource::RabbitMq {
            client,
            queue,
            instances,
            mode,
        } => format!(
            "RABBITMQ {} QUEUE {}{} MODE {}",
            client.as_str(),
            queue.as_str(),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            rabbitmq_mode_to_nspl(mode)
        ),
        IngestSource::RedisPubSub {
            client,
            channel,
            mode,
        } => format!(
            "REDIS PUBSUB {} CHANNEL {} MODE {}",
            client.as_str(),
            channel.as_str(),
            redis_pubsub_mode_to_nspl(mode)
        ),
        IngestSource::Prometheus {
            client,
            query,
            every,
        } => format!(
            "PROMETHEUS {} QUERY {} EVERY {}",
            client.as_str(),
            string_literal(query).expect("validated canonical query string"),
            every
        ),
        IngestSource::ZeroMq { client, mode } => format!(
            "ZEROMQ {} MODE {}",
            client.as_str(),
            zeromq_mode_to_nspl(mode)
        ),
        IngestSource::Sqs {
            client,
            queue,
            instances,
            mode,
        } => format!(
            "SQS {} QUEUE {}{} MODE {}",
            client.as_str(),
            queue.as_str(),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            sqs_mode_to_nspl(mode)
        ),
        IngestSource::Endpoint { endpoint, mode } => format!(
            "ENDPOINT {} MODE {}",
            endpoint.as_str(),
            endpoint_mode_to_nspl(mode)
        ),
        IngestSource::Websockets { client, mode } => format!(
            "WEBSOCKETS {} MODE {}",
            client.as_str(),
            websockets_mode_to_nspl(mode)
        ),
    }
}

fn pulsar_mode_to_nspl(mode: &PulsarIngestMode) -> String {
    kafka_mode_to_nspl(mode)
}

fn kafka_offset_mode_to_nspl(offset_mode: &KafkaOffsetMode) -> String {
    match offset_mode {
        KafkaOffsetMode::ConsumerGroup(group) => {
            format!("CONSUMER GROUP {}", group.as_str())
        }
        KafkaOffsetMode::Domain => "DOMAIN".to_string(),
    }
}

fn kafka_mode_to_nspl(mode: &KafkaIngestMode) -> String {
    match mode {
        KafkaIngestMode::AckParallel {
            max,
            batch_timeout,
            timeout,
            retry_policy,
        } => {
            format!(
                "ACK PARALLEL MAX {max} BATCH TIMEOUT {batch_timeout} ACK TIMEOUT {timeout} RETRY \
                 POLICY {}",
                retry_policy_to_nspl(retry_policy)
            )
        }
        KafkaIngestMode::AckSequential {
            timeout,
            retry_policy,
        } => format!(
            "ACK SEQUENTIAL ACK TIMEOUT {timeout} RETRY POLICY {}",
            retry_policy_to_nspl(retry_policy)
        ),
        KafkaIngestMode::NoAckParallel { max } => format!("NO_ACK PARALLEL MAX {max}"),
    }
}

fn kinesis_mode_to_nspl(mode: &KinesisIngestMode) -> String {
    match mode {
        KinesisIngestMode::AckSequential {
            timeout,
            retry_policy,
        } => format!(
            "ACK SEQUENTIAL ACK TIMEOUT {timeout} RETRY POLICY {}",
            retry_policy_to_nspl(retry_policy)
        ),
    }
}

fn mqtt_mode_to_nspl(mode: &MqttIngestMode) -> String {
    match mode {
        MqttIngestMode::NoAckSequential { session, qos } => {
            format!(
                "{}MODE NO_ACK SEQUENTIAL",
                mqtt_delivery_to_nspl(*session, *qos)
            )
        }
        MqttIngestMode::NoAckParallel { max, session, qos } => {
            format!(
                "{}MODE NO_ACK PARALLEL MAX {max}",
                mqtt_delivery_to_nspl(*session, *qos)
            )
        }
        MqttIngestMode::AckSequential {
            timeout,
            retry_policy,
        } => format!(
            "SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT {timeout} RETRY POLICY {}",
            retry_policy_to_nspl(retry_policy)
        ),
        MqttIngestMode::AckParallel {
            max,
            batch_timeout,
            timeout,
            retry_policy,
        } => format!(
            "SESSION PERSISTENT QOS 1 MODE ACK PARALLEL MAX {max} BATCH TIMEOUT {batch_timeout} \
             ACK TIMEOUT {timeout} RETRY POLICY {}",
            retry_policy_to_nspl(retry_policy)
        ),
    }
}

fn mqtt_delivery_to_nspl(session: MqttSession, qos: MqttQos) -> String {
    if session == MqttSession::Clean && qos == MqttQos::AtMostOnce {
        String::new()
    } else {
        format!("SESSION {} QOS {} ", session.as_ref(), qos.level())
    }
}

fn mqtt_topic_to_nspl(topic: &str) -> Result<String, CanonicalNsplError> {
    if Identifier::parse(topic).is_ok() {
        Ok(topic.to_string())
    } else {
        string_literal(topic)
    }
}

fn nats_mode_to_nspl(mode: &NatsIngestMode) -> String {
    match mode {
        NatsIngestMode::NoAckSequential => "NO_ACK SEQUENTIAL".to_string(),
    }
}

fn rabbitmq_mode_to_nspl(mode: &RabbitMqIngestMode) -> String {
    match mode {
        RabbitMqIngestMode::AckSequential {
            timeout,
            retry_policy,
        } => {
            format!(
                "ACK SEQUENTIAL ACK TIMEOUT {timeout} RETRY POLICY {}",
                retry_policy_to_nspl(retry_policy)
            )
        }
    }
}

fn redis_pubsub_mode_to_nspl(mode: &RedisPubSubIngestMode) -> String {
    match mode {
        RedisPubSubIngestMode::NoAckSequential => "NO_ACK SEQUENTIAL".to_string(),
    }
}

fn endpoint_mode_to_nspl(mode: &EndpointIngestMode) -> String {
    match mode {
        EndpointIngestMode::NoAckSequential => "NO_ACK SEQUENTIAL".to_string(),
    }
}

fn websockets_mode_to_nspl(mode: &WebsocketsIngestMode) -> String {
    match mode {
        WebsocketsIngestMode::NoAckSequential => "NO_ACK SEQUENTIAL".to_string(),
    }
}

fn zeromq_mode_to_nspl(mode: &ZeroMqIngestMode) -> String {
    match mode {
        ZeroMqIngestMode::NoAckSequential => "NO_ACK SEQUENTIAL".to_string(),
    }
}

fn sqs_mode_to_nspl(mode: &SqsIngestMode) -> String {
    match mode {
        SqsIngestMode::AckSequential {
            timeout,
            retry_policy,
        } => {
            format!(
                "ACK SEQUENTIAL ACK TIMEOUT {timeout} RETRY POLICY {}",
                retry_policy_to_nspl(retry_policy)
            )
        }
    }
}

fn retry_policy_to_nspl(policy: &RetryPolicy) -> String {
    format!("BACKOFF {} MAX {}", policy.backoff, policy.max_backoff)
}

fn endpoint_type_to_nspl(endpoint_type: EndpointType) -> &'static str {
    match endpoint_type {
        EndpointType::Websockets => "WEBSOCKETS",
        EndpointType::Http => "HTTP",
    }
}

fn rabbitmq_entry_to_nspl(entry: &RabbitMqConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn redis_entry_to_nspl(entry: &RedisConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn mqtt_entry_to_nspl(entry: &MqttConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn nats_entry_to_nspl(entry: &NatsConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn prometheus_entry_to_nspl(entry: &PrometheusConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn zeromq_entry_to_nspl(entry: &ZeroMqConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn sqs_entry_to_nspl(entry: &SqsConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn s3_entry_to_nspl(entry: &S3ConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn gcs_entry_to_nspl(entry: &GcsConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn azure_blob_entry_to_nspl(entry: &AzureBlobConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn websockets_entry_to_nspl(entry: &WebsocketsConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn clickhouse_entry_to_nspl(entry: &ClickHouseConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn postgres_entry_to_nspl(entry: &PostgresConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn mysql_entry_to_nspl(entry: &MySqlConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn mongodb_entry_to_nspl(entry: &MongoDbConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn emit_sink_to_nspl(sink: &EmitSink) -> Result<String, CanonicalNsplError> {
    match sink {
        EmitSink::Kafka { client, topic } => Ok(format!(
            "KAFKA {} TOPIC {}",
            client.as_str(),
            topic.as_str()
        )),
        EmitSink::Pulsar { client, topic } => Ok(format!(
            "PULSAR {} TOPIC {}",
            client.as_str(),
            topic.as_str()
        )),
        EmitSink::Kinesis { client, relay } => Ok(format!(
            "KINESIS {} RELAY {}",
            client.as_str(),
            relay.as_str()
        )),
        EmitSink::RabbitMq { client, queue } => Ok(format!(
            "RABBITMQ {} QUEUE {}",
            client.as_str(),
            queue.as_str()
        )),
        EmitSink::Redis { client, channel } => Ok(format!(
            "REDIS PUBSUB {} CHANNEL {}",
            client.as_str(),
            channel.as_str()
        )),
        EmitSink::Mqtt { client, topic } => {
            Ok(format!("MQTT {} TOPIC {}", client.as_str(), topic.as_str()))
        }
        EmitSink::Nats { client, subject } => Ok(format!(
            "NATS {} SUBJECT {}",
            client.as_str(),
            subject.as_str()
        )),
        EmitSink::ZeroMq { client } => Ok(format!("ZEROMQ {}", client.as_str())),
        EmitSink::Sqs { client, queue } => {
            Ok(format!("SQS {} QUEUE {}", client.as_str(), queue.as_str()))
        }
        EmitSink::ClickHouse {
            client,
            table,
            values,
            ..
        } => {
            let mappings = value_mappings_to_nspl(values)?;
            Ok(format!(
                "CLICKHOUSE {} INSERT TO TABLE {} VALUES {{{}}}",
                client.as_str(),
                table.as_str(),
                mappings
            ))
        }
        EmitSink::Postgres {
            client,
            table,
            values,
            conflict_action,
            max_batch,
            ..
        } => {
            let mappings = value_mappings_to_nspl(values)?;
            let conflict_action = postgres_conflict_action_to_nspl(conflict_action);
            Ok(format!(
                "POSTGRES {} INSERT TO TABLE {} VALUES {{{}}}{} WITH MAX BATCH {}",
                client.as_str(),
                table.as_str(),
                mappings,
                conflict_action,
                max_batch
            ))
        }
        EmitSink::MySql {
            client,
            table,
            values,
            conflict_action,
            max_batch,
            ..
        } => {
            let mappings = value_mappings_to_nspl(values)?;
            let conflict_action = mysql_conflict_action_to_nspl(conflict_action);
            Ok(format!(
                "MYSQL {} INSERT TO TABLE {} VALUES {{{}}}{} WITH MAX BATCH {}",
                client.as_str(),
                table.as_str(),
                mappings,
                conflict_action,
                max_batch
            ))
        }
        EmitSink::MongoDb {
            client,
            collection,
            values,
            conflict_action,
            max_batch,
            ..
        } => {
            let mappings = value_mappings_to_nspl(values)?;
            let conflict_action = mongodb_conflict_action_to_nspl(conflict_action);
            Ok(format!(
                "MONGODB {} INSERT TO COLLECTION {} VALUES {{{}}}{} WITH MAX BATCH {}",
                client.as_str(),
                collection.as_str(),
                mappings,
                conflict_action,
                max_batch
            ))
        }
        EmitSink::Iceberg {
            backend,
            client,
            table,
            values,
            location,
            catalog,
            ..
        } => {
            let mappings = value_mappings_to_nspl(values)?;
            let catalog = match catalog {
                IcebergCatalog::Rest { client } => format!("CATALOG {}", client.as_str()),
            };
            Ok(format!(
                "ICEBERG ON {} {} TABLE {} VALUES {{{}}} LOCATION {} {}",
                backend.as_ref(),
                client.as_str(),
                table.as_str(),
                mappings,
                string_literal(location)?,
                catalog
            ))
        }
    }
}

fn postgres_conflict_action_to_nspl(action: &PostgresConflictAction) -> String {
    match action {
        PostgresConflictAction::None => String::new(),
        PostgresConflictAction::DoNothing { target } => {
            let target = conflict_target_to_nspl(target);
            format!(" ON CONFLICT{target} DO NOTHING")
        }
        PostgresConflictAction::DoUpdate { target } => {
            let target = conflict_target_to_nspl(target);
            format!(" ON CONFLICT{target} DO UPDATE")
        }
    }
}

fn mysql_conflict_action_to_nspl(action: &MySqlConflictAction) -> String {
    match action {
        MySqlConflictAction::None => String::new(),
        MySqlConflictAction::DoNothing => " ON CONFLICT DO NOTHING".to_string(),
        MySqlConflictAction::DoUpdate => " ON CONFLICT DO UPDATE".to_string(),
    }
}

fn mongodb_conflict_action_to_nspl(action: &MongoDbConflictAction) -> String {
    match action {
        MongoDbConflictAction::None => String::new(),
        MongoDbConflictAction::DoNothing { target } => {
            let target = conflict_target_to_nspl(target);
            format!(" ON CONFLICT{target} DO NOTHING")
        }
        MongoDbConflictAction::DoUpdate { target } => {
            let target = conflict_target_to_nspl(target);
            format!(" ON CONFLICT{target} DO UPDATE")
        }
    }
}

fn conflict_target_to_nspl(target: &[String]) -> String {
    if target.is_empty() {
        String::new()
    } else {
        let columns = target
            .iter()
            .map(|column| string_literal(column).expect("validated canonical conflict column"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" ({columns})")
    }
}

fn parse_as_to_keyword(parse_as: &ParseAsType) -> String {
    match parse_as {
        ParseAsType::U8 => "U8".to_string(),
        ParseAsType::I8 => "I8".to_string(),
        ParseAsType::U16 => "U16".to_string(),
        ParseAsType::I16 => "I16".to_string(),
        ParseAsType::U32 => "U32".to_string(),
        ParseAsType::I32 => "I32".to_string(),
        ParseAsType::U64 => "U64".to_string(),
        ParseAsType::I64 => "I64".to_string(),
        ParseAsType::Bool => "BOOL".to_string(),
        ParseAsType::String => "STRING".to_string(),
        ParseAsType::Datetime => "DATETIME".to_string(),
        ParseAsType::F32 => "F32".to_string(),
        ParseAsType::F64 => "F64".to_string(),
        ParseAsType::Array { .. } => {
            let mut element = parse_as;
            let mut lengths = Vec::new();
            while let ParseAsType::Array {
                element: nested,
                len,
            } = element
            {
                lengths.push(len.to_string());
                element = nested;
            }
            format!(
                "ARRAY<{}, {}>",
                parse_as_to_keyword(element),
                lengths.join(", ")
            )
        }
        ParseAsType::Vec { element } => format!("VEC<{}>", parse_as_to_keyword(element)),
    }
}

fn string_literal(value: &str) -> Result<String, CanonicalNsplError> {
    let has_single = value.contains('\'');
    let has_double = value.contains('"');
    let has_newline = value.contains('\n') || value.contains('\r');

    if has_newline || (has_single && has_double) {
        return Err(CanonicalNsplError::UnrepresentableStringLiteral {
            value: value.to_string(),
        });
    }

    if has_single {
        Ok(format!("\"{value}\""))
    } else {
        Ok(format!("'{value}'"))
    }
}

trait NativeTypeToNspl {
    fn to_nspl_keyword(&self) -> &'static str;
}

impl NativeTypeToNspl for JsonType {
    fn to_nspl_keyword(&self) -> &'static str {
        match self {
            Self::String => "STRING",
            Self::Number => "NUMBER",
            Self::Integer => "INTEGER",
            Self::Object => "OBJECT",
            Self::Array => "ARRAY",
            Self::Boolean => "BOOLEAN",
            Self::Null => "NULL",
            Self::U8 => "U8",
            Self::I8 => "I8",
            Self::U16 => "U16",
            Self::I16 => "I16",
            Self::U32 => "U32",
            Self::I32 => "I32",
            Self::U64 => "U64",
            Self::I64 => "I64",
            Self::Datetime => "DATETIME",
            Self::F32 => "F32",
            Self::F64 => "F64",
        }
    }
}

impl NativeTypeToNspl for AvroType {
    fn to_nspl_keyword(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Boolean => "BOOLEAN",
            Self::Int => "INT",
            Self::Long => "LONG",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::Bytes => "BYTES",
            Self::String => "STRING",
            Self::Record => "RECORD",
            Self::Enum => "ENUM",
            Self::Array => "ARRAY",
            Self::Map => "MAP",
            Self::Fixed => "FIXED",
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AckMode, AvroType, BinaryOperator, BranchSelection, CodecEncoding, CodecEncodingRule,
        CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig, CodecWireFormat,
        CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
        CreateClientHttp, CreateClientKafka, CreateClientKinesis, CreateClientMqtt,
        CreateClientNats, CreateClientPrometheus, CreateClientRabbitMq, CreateClientRedis,
        CreateClientSqs, CreateClientWebsockets, CreateClientZeroMq, CreateCodec, CreateCorrelator,
        CreateDeduplicator, CreateEmitter, CreateEndpoint, CreateIngestor, CreateJunction,
        CreateReingestor, CreateRelay, CreateSchema, CreateSignalingProtocol, CreateUdf,
        CreateVhost, CreateWindowProcessor, CreateWireSchema, CreateWireSchemaStmt, EmitSink,
        EndpointIngestMode, EndpointType, ErrorPolicies, Expression, FieldScope,
        GeneralErrorPolicy, HttpConfigEntry, Identifier, IngestSource, JsonType, KafkaConfigEntry,
        KafkaIngestMode, KafkaOffsetMode, KinesisIngestMode, Literal, MessageErrorPolicy, Model,
        MongoDbConflictAction, MongoDbValueMapping, MqttIngestMode, MqttQos, MqttSession,
        MySqlConflictAction, MySqlValueMapping, NatsIngestMode, OutputBranch, ParseAsType,
        PostgresConflictAction, PostgresValueMapping, ProcessorInputs, ProcessorOutput,
        ProcessorOutputs, PrometheusConfigEntry, RabbitMqIngestMode, RedisPubSubIngestMode,
        RelayBranching, RetryPolicy, RouteConstruction, SchemaField, SqsIngestMode, UdfArgument,
        UdfLanguage, UdfReturn, WebsocketsIngestMode, WindowBound, WireSchemaField,
        ZeroMqIngestMode,
    };

    fn identifier(raw: &str) -> Identifier {
        Identifier::try_from(raw).expect("valid identifier")
    }

    fn flushed_output(relay: &str, construction: Option<RouteConstruction>) -> ProcessorOutput {
        let mut output = ProcessorOutput::with_flush_policy(
            identifier(relay),
            "100ms".to_string(),
            Some("1MiB".to_string()),
        );
        output.construction = construction.unwrap_or_default();
        output
    }

    fn bare_field(name: &str) -> Expression {
        Expression::Field(crate::FieldReference::bare(identifier(name)))
    }

    fn scoped_field(scope: FieldScope, name: &str) -> Expression {
        Expression::Field(crate::FieldReference::scoped(scope, identifier(name)))
    }

    fn string_value(value: &str) -> Expression {
        Expression::Literal(Literal::String(value.to_string()))
    }

    fn call(name: &str, arguments: Vec<Expression>) -> Expression {
        Expression::Call {
            function: identifier(name),
            arguments,
        }
    }

    fn equals(left: Expression, right: Expression) -> Expression {
        Expression::Binary {
            operator: BinaryOperator::Equal,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn route_where(predicate: Expression) -> RouteConstruction {
        RouteConstruction {
            where_clause: Some(predicate),
            ..RouteConstruction::default()
        }
    }

    fn route_set(field: &str, value: Expression) -> RouteConstruction {
        RouteConstruction {
            assignments: vec![crate::Assignment {
                target: crate::AssignmentTarget::bare(identifier(field)),
                value,
            }],
            ..RouteConstruction::default()
        }
    }

    fn flushed_outputs(relay: &str) -> ProcessorOutputs {
        ProcessorOutputs::new(vec![flushed_output(relay, None)])
    }

    fn flushed_ingestor_outputs(relay: &str) -> ProcessorOutputs {
        flushed_outputs(relay).with_branch(OutputBranch::Unbranched)
    }

    fn processor_branched_by(schema: &str) -> BranchSelection {
        BranchSelection::branched_by(identifier(&format!("by_{schema}")))
    }

    fn config_entry(key: &str, value: &str) -> KafkaConfigEntry {
        KafkaConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn renders_wire_schema_canonical() {
        let schema = CreateWireSchemaStmt::Avro(CreateWireSchema {
            name: identifier("latency"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: identifier("p99"),
                    ty: AvroType::Double,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("created_at"),
                    ty: AvroType::String,
                    optional: false,
                },
            ],
        });

        let nspl = schema.to_canonical_nspl().expect("must render");
        assert_eq!(
            nspl,
            "CREATE STRICT WIRE AVRO SCHEMA latency (p99 DOUBLE, created_at STRING);"
        );
    }

    #[test]
    fn renders_internal_schema_canonical() {
        let schema = CreateSchema {
            name: identifier("latency"),
            fields: vec![
                SchemaField {
                    name: identifier("p99"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("created_at"),
                    ty: ParseAsType::Datetime,
                    optional: false,
                    sensitive: false,
                },
            ],
        };

        let nspl = schema.to_canonical_nspl().expect("must render");
        assert_eq!(
            nspl,
            "CREATE SCHEMA latency (p99 F64, created_at DATETIME);"
        );
    }

    #[test]
    fn renders_multidimensional_internal_arrays_canonical() {
        let schema = CreateSchema {
            name: identifier("tensors"),
            fields: vec![SchemaField {
                name: identifier("matrix"),
                ty: ParseAsType::Array {
                    len: 2,
                    element: Box::new(ParseAsType::Array {
                        len: 3,
                        element: Box::new(ParseAsType::F32),
                    }),
                },
                optional: false,
                sensitive: false,
            }],
        };

        assert_eq!(
            schema.to_canonical_nspl().expect("must render"),
            "CREATE SCHEMA tensors (matrix ARRAY<F32, 2, 3>);"
        );
    }

    #[test]
    fn renders_transport_values_as_string_literals() {
        let model = CreateClientKafka {
            name: identifier("kafka_main"),
            mount: None,
            config: vec![
                config_entry("bootstrap.servers", "host1:9092"),
                config_entry("enable.auto.commit", "true"),
            ],
        };

        let nspl = model.to_canonical_nspl().expect("must render");
        assert_eq!(
            nspl,
            "CREATE CLIENT kafka_main TYPE KAFKA CONFIG {'bootstrap.servers' = 'host1:9092', \
             'enable.auto.commit' = 'true'};"
        );
    }

    #[test]
    fn fails_for_unrepresentable_string_literal() {
        let model = CreateClientKafka {
            name: identifier("k"),
            mount: None,
            config: vec![KafkaConfigEntry {
                key: "quoted".to_string(),
                value: "both ' and \"".to_string(),
            }],
        };

        let err = model.to_canonical_nspl().expect_err("must fail");
        assert!(matches!(
            err,
            super::CanonicalNsplError::UnrepresentableStringLiteral { .. }
        ));
    }

    #[test]
    fn canonical_error_display_includes_original_value() {
        let err = super::CanonicalNsplError::UnrepresentableStringLiteral {
            value: "line\nbreak".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "cannot represent string literal in NSPL without escapes: \"line\\nbreak\""
        );
    }

    #[test]
    fn renders_json_wire_schema_canonical() {
        let schema = CreateWireSchemaStmt::Json(CreateWireSchema {
            name: identifier("payload"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: identifier("items"),
                    ty: JsonType::Array,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("active"),
                    ty: JsonType::Boolean,
                    optional: false,
                },
            ],
        });

        assert_eq!(
            schema.to_canonical_nspl().expect("must render"),
            "CREATE STRICT WIRE JSON SCHEMA payload (items ARRAY, active BOOLEAN);"
        );
    }

    #[test]
    fn renders_loose_cbor_wire_schema_canonical() {
        let schema = CreateWireSchemaStmt::Cbor(CreateWireSchema {
            name: identifier("payload"),
            strictness: crate::WireSchemaStrictness::Loose,
            fields: vec![WireSchemaField {
                name: identifier("active"),
                ty: JsonType::Boolean,
                optional: false,
            }],
        });

        assert_eq!(
            schema.to_canonical_nspl().expect("must render"),
            "CREATE LOOSE WIRE CBOR SCHEMA payload (active BOOLEAN);"
        );
    }

    #[test]
    fn renders_optional_schema_fields_canonical() {
        let internal = CreateSchema {
            name: identifier("latency"),
            fields: vec![SchemaField {
                name: identifier("p99"),
                ty: ParseAsType::F64,
                optional: true,
                sensitive: false,
            }],
        };
        let wire = CreateWireSchemaStmt::Json(CreateWireSchema {
            name: identifier("payload"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: identifier("active"),
                ty: JsonType::Boolean,
                optional: true,
            }],
        });

        assert_eq!(
            internal.to_canonical_nspl().expect("must render"),
            "CREATE SCHEMA latency (p99 F64 OPTIONAL);"
        );
        assert_eq!(
            wire.to_canonical_nspl().expect("must render"),
            "CREATE STRICT WIRE JSON SCHEMA payload (active BOOLEAN OPTIONAL);"
        );
    }

    #[test]
    fn renders_all_client_types_canonical() {
        let expectations = [
            (
                CreateClientHttp {
                    name: identifier("http_main"),
                    mount: None,
                    config: vec![HttpConfigEntry {
                        key: "base_url".to_string(),
                        value: "https://example.com".to_string(),
                    }],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT http_main TYPE HTTP CONFIG {'base_url' = 'https://example.com'};",
            ),
            (
                CreateClientMqtt {
                    name: identifier("mqtt_main"),
                    mount: None,
                    config: vec![config_entry("host", "mqtt.internal")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT mqtt_main TYPE MQTT CONFIG {'host' = 'mqtt.internal'};",
            ),
            (
                CreateClientNats {
                    name: identifier("nats_main"),
                    mount: None,
                    config: vec![config_entry("servers", "nats://localhost:4222")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT nats_main TYPE NATS CONFIG {'servers' = 'nats://localhost:4222'};",
            ),
            (
                CreateClientPrometheus {
                    name: identifier("prom_main"),
                    mount: None,
                    config: vec![PrometheusConfigEntry {
                        key: "url".to_string(),
                        value: "http://prometheus:9090".to_string(),
                    }],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {'url' = 'http://prometheus:9090'};",
            ),
            (
                CreateClientRabbitMq {
                    name: identifier("rmq_main"),
                    mount: None,
                    config: vec![config_entry("uri", "amqp://guest:guest@localhost:5672")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT rmq_main TYPE RABBITMQ CONFIG {'uri' = 'amqp://guest:guest@localhost:5672'};",
            ),
            (
                CreateClientRedis {
                    name: identifier("redis_main"),
                    mount: None,
                    config: vec![config_entry("url", "redis://localhost:6379")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT redis_main TYPE REDIS CONFIG {'url' = 'redis://localhost:6379'};",
            ),
            (
                CreateClientZeroMq {
                    name: identifier("zmq_main"),
                    mount: None,
                    config: vec![config_entry("bind", "tcp://*:5555")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT zmq_main TYPE ZEROMQ CONFIG {'bind' = 'tcp://*:5555'};",
            ),
            (
                CreateClientSqs {
                    name: identifier("sqs_main"),
                    mount: None,
                    config: vec![config_entry("region", "us-east-1")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT sqs_main TYPE SQS CONFIG {'region' = 'us-east-1'};",
            ),
            (
                CreateClientWebsockets {
                    name: identifier("ws_main"),
                    mount: None,
                    signaling_protocol: None,
                    config: vec![config_entry("url", "wss://example.com/socket")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT ws_main TYPE WEBSOCKETS CONFIG {'url' = 'wss://example.com/socket'};",
            ),
            (
                CreateClientWebsockets {
                    name: identifier("ws_main"),
                    mount: None,
                    signaling_protocol: Some(identifier("binance_ws")),
                    config: vec![config_entry("url", "wss://example.com/socket")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT ws_main TYPE WEBSOCKETS WITH SIGNALING PROTOCOL binance_ws CONFIG {'url' = 'wss://example.com/socket'};",
            ),
        ];

        for (actual, expected) in expectations {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn renders_other_model_kinds_canonical() {
        let vhost = CreateVhost {
            name: identifier("public"),
            hostnames: vec!["example.com".to_string(), "api.example.com".to_string()],
            tls: None,
        };
        assert_eq!(
            vhost.to_canonical_nspl().expect("must render"),
            "CREATE VHOST public example.com, api.example.com;"
        );

        let tls_vhost = CreateVhost {
            name: identifier("secure"),
            hostnames: vec!["secure.example.com".to_string()],
            tls: Some(crate::VhostTlsResource {
                resource: identifier("certs"),
                version: Some(7),
            }),
        };
        assert_eq!(
            tls_vhost.to_canonical_nspl().expect("must render"),
            "CREATE VHOST secure secure.example.com WITH TLS certs VERSION 7;"
        );

        let endpoint = CreateEndpoint {
            name: identifier("orders_http"),
            on_vhost: identifier("public"),
            path: "/orders".to_string(),
            endpoint_type: EndpointType::Http,
            signaling_protocol: None,
        };
        assert_eq!(
            endpoint.to_canonical_nspl().expect("must render"),
            "CREATE ENDPOINT orders_http ON public PATH '/orders' TYPE HTTP;"
        );
        let websocket_endpoint = CreateEndpoint {
            name: identifier("orders_ws"),
            on_vhost: identifier("public"),
            path: "/ws".to_string(),
            endpoint_type: EndpointType::Websockets,
            signaling_protocol: Some(identifier("binance_ws")),
        };
        assert_eq!(
            websocket_endpoint.to_canonical_nspl().expect("must render"),
            "CREATE ENDPOINT orders_ws ON public PATH '/ws' TYPE WEBSOCKETS WITH SIGNALING \
             PROTOCOL binance_ws;"
        );

        let signaling_protocol = CreateSignalingProtocol {
            name: identifier("binance_ws"),
            on_connect: crate::SignalingProtocolOnConnect {
                send_bodies: vec![r#"{"method":"SUBSCRIBE","id":1}"#.to_string()],
                wait_bodies: vec![r#"{"id":1,"result":null}"#.to_string()],
                timeout: "5s".to_string(),
            },
        };
        assert_eq!(
            signaling_protocol.to_canonical_nspl().expect("must render"),
            r#"CREATE SIGNALING PROTOCOL binance_ws ON CONNECT SEND BODY '{"method":"SUBSCRIBE","id":1}' WAIT BODY '{"id":1,"result":null}' TIMEOUT 5s;"#
        );

        let codec = CreateCodec {
            name: identifier("orders_codec"),
            wire_format: CodecWireFormat::Json,
            wire_schema: Some(identifier("orders_wire")),
            schema: identifier("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            codec.to_canonical_nspl().expect("must render"),
            "CREATE CODEC orders_codec FROM WIRE JSON SCHEMA orders_wire TO SCHEMA orders;"
        );

        let codec_with_encoding = CreateCodec {
            name: identifier("orders_codec"),
            wire_format: CodecWireFormat::Json,
            wire_schema: Some(identifier("orders_wire")),
            schema: identifier("orders"),
            encoding_rules: vec![CodecEncodingRule {
                field: identifier("created_at"),
                encoding: CodecEncoding::Rfc3339,
            }],
        };
        assert_eq!(
            codec_with_encoding
                .to_canonical_nspl()
                .expect("must render"),
            "CREATE CODEC orders_codec FROM WIRE JSON SCHEMA orders_wire TO SCHEMA orders ENCODE \
             created_at AS RFC3339;"
        );

        let codec_with_jaq = CreateCodec {
            name: identifier("orders_codec"),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: Some("{payload: .}".to_string()),
                },
            },
            wire_schema: None,
            schema: identifier("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            codec_with_jaq.to_canonical_nspl().expect("must render"),
            "CREATE CODEC orders_codec FROM JSON TO SCHEMA orders WITH JAQ TRANSFORMATIONS ON \
             INGESTION '.payload' ON EMITTING '{payload: .}';"
        );

        let cbor_codec = CreateCodec {
            name: identifier("orders_cbor"),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Cbor,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".".to_string()),
                    on_emitting: Some(".".to_string()),
                },
            },
            wire_schema: None,
            schema: identifier("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            cbor_codec.to_canonical_nspl().expect("must render"),
            "CREATE CODEC orders_cbor FROM CBOR TO SCHEMA orders WITH JAQ TRANSFORMATIONS ON \
             INGESTION '.' ON EMITTING '.';"
        );

        let protobuf_codec = CreateCodec {
            name: identifier("orders_proto"),
            wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: identifier("proto_bundle"),
                resource_version: Some(3),
                config: vec![crate::ClientConfigEntry {
                    key: "file".to_string(),
                    value: "order.proto".to_string(),
                }],
                message: "nervix.test.Order".to_string(),
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: Some("{payload: .}".to_string()),
                },
            }),
            wire_schema: None,
            schema: identifier("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            protobuf_codec.to_canonical_nspl().expect("must render"),
            "CREATE CODEC orders_proto FROM PROTOBUF USING RESOURCE proto_bundle VERSION 3 CONFIG \
             {'file' = 'order.proto'} MESSAGE 'nervix.test.Order' TO SCHEMA orders WITH JAQ \
             TRANSFORMATIONS ON INGESTION '.payload' ON EMITTING '{payload: .}';"
        );

        let relay = CreateRelay {
            name: identifier("orders_stream"),
            schema: identifier("orders"),
            buffer: 1,
            branching: RelayBranching::branched_by(identifier("by_orders")),
            materialized_state: None,
        };
        assert_eq!(
            relay.to_canonical_nspl().expect("must render"),
            "CREATE RELAY orders_stream SCHEMA orders BRANCHED BY by_orders CAPACITY 1;"
        );

        let relay = CreateRelay {
            name: identifier("orders_stream"),
            schema: identifier("orders"),
            buffer: 1,
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        };
        assert_eq!(
            relay.to_canonical_nspl().expect("must render"),
            "CREATE RELAY orders_stream SCHEMA orders UNBRANCHED CAPACITY 1;"
        );

        let junction = CreateJunction {
            name: identifier("orders_junction"),
            from: ProcessorInputs::new(
                vec![identifier("orders_a"), identifier("orders_b")],
                Vec::new(),
            ),
            output_routes: flushed_outputs("orders_all"),
            branched_by: processor_branched_by("tenant_branch"),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            junction.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED JUNCTION orders_junction FROM orders_a, orders_b BRANCHED BY \
             by_tenant_branch TO orders_all FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR \
             LOG;"
        );

        let deduplicator = CreateDeduplicator {
            name: identifier("orders_dedup"),
            from: ProcessorInputs::single(identifier("orders_in")),
            output_routes: flushed_outputs("orders_out"),
            branched_by: processor_branched_by("tenant_branch"),
            deduplicate_on: vec![scoped_field(FieldScope::Input, "transaction_id")],
            max_time: "10m".to_string(),
            mode: AckMode::Detached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            deduplicator.to_canonical_nspl().expect("must render"),
            "CREATE DETACHED DEDUPLICATOR orders_dedup FROM orders_in DEDUPLICATE ON \
             input.transaction_id MAX TIME 10m BRANCHED BY by_tenant_branch TO orders_out FLUSH \
             EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"
        );

        let correlator = CreateCorrelator {
            name: identifier("orders_correlator"),
            left: ProcessorInputs::single(identifier("orders_left")),
            right: ProcessorInputs::single(identifier("orders_right")),
            output_routes: ProcessorOutputs::new(vec![flushed_output(
                "orders_matched",
                Some(route_set(
                    "id",
                    Expression::Field(crate::FieldReference::scoped(
                        FieldScope::Left,
                        identifier("id"),
                    )),
                )),
            )]),
            branched_by: processor_branched_by("tenant_branch"),
            correlate_where: equals(
                scoped_field(FieldScope::Left, "id"),
                scoped_field(FieldScope::Right, "id"),
            ),
            match_policy: CorrelatorMatchPolicy::Earliest,
            max_time: "5s".to_string(),
            timeout_policy: CorrelationTimeoutPolicy {
                left: CorrelationTimeoutAction::Drop,
                right: CorrelationTimeoutAction::Drop,
            },
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            correlator.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED CORRELATOR orders_correlator LEFT FROM orders_left RIGHT FROM \
             orders_right CORRELATE WHERE (left.id = right.id) MATCH EARLIEST MAX TIME 5s ON \
             CORRELATION TIMEOUT DROP, DROP BRANCHED BY by_tenant_branch TO orders_matched SET id \
             = left.id FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"
        );

        let window_processor = CreateWindowProcessor {
            name: identifier("latency_window"),
            from: ProcessorInputs::single(identifier("orders_in")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                relay: identifier("orders_p99"),
                construction: route_set(
                    "latency_p99",
                    Expression::Call {
                        function: identifier("percentile_linear_histogram"),
                        arguments: vec![
                            scoped_field(FieldScope::Input, "latency"),
                            Expression::Literal(Literal::I64(99)),
                            Expression::Literal(Literal::I64(2048)),
                            Expression::Literal(Literal::I64(0)),
                            Expression::Literal(Literal::I64(10000)),
                            string_value("2s"),
                        ],
                    },
                ),
                flush_policy: None,
                message_error_policy: MessageErrorPolicy::Log,
                branch: None,
            }]),
            branched_by: processor_branched_by("tenant_branch"),
            width: WindowBound {
                messages: Some(100),
                duration: Some("10s".to_string()),
            },
            step: WindowBound {
                messages: Some(10),
                duration: Some("1s".to_string()),
            },
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            window_processor.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED WINDOW PROCESSOR latency_window FROM orders_in WIDTH 100 MESSAGES \
             10s DURATION STEP 10 MESSAGES 1s DURATION BRANCHED BY by_tenant_branch TO orders_p99 \
             SET latency_p99 = percentile_linear_histogram(input.latency, 99, 2048, 0, 10000, \
             '2s') ON MESSAGE ERROR LOG;"
        );

        let reingestor = CreateReingestor {
            name: identifier("orders_repartition"),
            from: ProcessorInputs::single(identifier("orders_in")),
            output_routes: flushed_outputs("orders_out").with_branch(OutputBranch::Unbranched),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            reingestor.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED REINGESTOR orders_repartition FROM orders_in TO orders_out \
             UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"
        );

        let route_reingestor = CreateReingestor {
            name: identifier("orders_splitter"),
            from: ProcessorInputs::single(identifier("orders_in")),
            output_routes: ProcessorOutputs::new(vec![
                flushed_output(
                    "orders_errors",
                    Some(route_where(equals(
                        bare_field("level"),
                        string_value("error"),
                    ))),
                ),
                flushed_output(
                    "orders_warn",
                    Some(RouteConstruction {
                        assignments: route_set("severity", string_value("warning")).assignments,
                        where_clause: Some(equals(bare_field("level"), string_value("warn"))),
                        ..RouteConstruction::default()
                    }),
                ),
                flushed_output("orders_info", None),
            ])
            .with_branch(OutputBranch::Unbranched),
            mode: AckMode::Detached,
            filter_where: Some(bare_field("active")),
            materialized_state: Vec::new(),
        };
        assert_eq!(
            route_reingestor.to_canonical_nspl().expect("must render"),
            r#"CREATE DETACHED REINGESTOR orders_splitter FROM orders_in FILTER WHERE active TO orders_errors WHERE (level = 'error') UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG TO orders_warn SET severity = 'warning' WHERE (level = 'warn') UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG TO orders_info UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"#
        );
    }

    #[test]
    fn renders_emitters_for_all_sink_variants() {
        let sinks = [
            (
                EmitSink::Kafka {
                    client: identifier("kafka_main"),
                    topic: identifier("orders"),
                },
                "KAFKA kafka_main TOPIC orders",
            ),
            (
                EmitSink::Pulsar {
                    client: identifier("pulsar_main"),
                    topic: identifier("orders"),
                },
                "PULSAR pulsar_main TOPIC orders",
            ),
            (
                EmitSink::Kinesis {
                    client: identifier("kinesis_main"),
                    relay: identifier("orders_stream_out"),
                },
                "KINESIS kinesis_main RELAY orders_stream_out",
            ),
            (
                EmitSink::RabbitMq {
                    client: identifier("rmq_main"),
                    queue: identifier("orders_q"),
                },
                "RABBITMQ rmq_main QUEUE orders_q",
            ),
            (
                EmitSink::Redis {
                    client: identifier("redis_main"),
                    channel: identifier("orders_ch"),
                },
                "REDIS PUBSUB redis_main CHANNEL orders_ch",
            ),
            (
                EmitSink::Mqtt {
                    client: identifier("mqtt_main"),
                    topic: identifier("orders_topic"),
                },
                "MQTT mqtt_main TOPIC orders_topic",
            ),
            (
                EmitSink::Nats {
                    client: identifier("nats_main"),
                    subject: identifier("orders_subject"),
                },
                "NATS nats_main SUBJECT orders_subject",
            ),
            (
                EmitSink::ZeroMq {
                    client: identifier("zmq_main"),
                },
                "ZEROMQ zmq_main",
            ),
            (
                EmitSink::Sqs {
                    client: identifier("sqs_main"),
                    queue: identifier("orders_queue"),
                },
                "SQS sqs_main QUEUE orders_queue",
            ),
        ];

        for (sink, rendered_sink) in sinks {
            let emitter = CreateEmitter {
                name: identifier("emit_orders"),
                from_relay: identifier("orders_stream"),
                encode_using_codec: Some(identifier("orders_codec")),
                sink,
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Attached,
                error_policies: ErrorPolicies::handled_by_log(),

                construction: RouteConstruction::default(),
                materialized_state: Vec::new(),
            };
            assert_eq!(
                emitter.to_canonical_nspl().expect("must render"),
                format!(
                    "CREATE ATTACHED EMITTER emit_orders FROM orders_stream ENCODE USING \
                     orders_codec TO {rendered_sink} FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                     MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
                )
            );
        }
    }

    #[test]
    fn renders_postgres_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: identifier("emit_notifications"),
            from_relay: identifier("notifications"),
            encode_using_codec: None,
            sink: EmitSink::Postgres {
                client: identifier("postgres_main"),
                table: identifier("notification_rows"),
                values: vec![
                    PostgresValueMapping {
                        column: "postgres_user_id".to_string(),
                        expression: scoped_field(FieldScope::Input, "user_id"),
                    },
                    PostgresValueMapping {
                        column: "postgres_action".to_string(),
                        expression: call("lower", vec![scoped_field(FieldScope::Input, "action")]),
                    },
                ],
                conflict_action: PostgresConflictAction::DoUpdate {
                    target: vec!["postgres_user_id".to_string()],
                },
                max_batch: 500,
                flush_each: "10s".to_string(),
            },
            flush_each: "10s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED EMITTER emit_notifications FROM notifications TO POSTGRES \
             postgres_main INSERT TO TABLE notification_rows VALUES {'postgres_user_id' = \
             input.user_id, 'postgres_action' = lower(input.action)} ON CONFLICT \
             ('postgres_user_id') DO UPDATE WITH MAX BATCH 500 FLUSH EACH 10s MAX BATCH SIZE 1MiB \
             ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
    }

    #[test]
    fn renders_mysql_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: identifier("emit_notifications"),
            from_relay: identifier("notifications"),
            encode_using_codec: None,
            sink: EmitSink::MySql {
                client: identifier("mysql_main"),
                table: identifier("notification_rows"),
                values: vec![
                    MySqlValueMapping {
                        column: "mysql_user_id".to_string(),
                        expression: scoped_field(FieldScope::Input, "user_id"),
                    },
                    MySqlValueMapping {
                        column: "mysql_action".to_string(),
                        expression: call("lower", vec![scoped_field(FieldScope::Input, "action")]),
                    },
                ],
                conflict_action: MySqlConflictAction::DoNothing,
                max_batch: 500,
                flush_each: "10s".to_string(),
            },
            flush_each: "10s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED EMITTER emit_notifications FROM notifications TO MYSQL mysql_main \
             INSERT TO TABLE notification_rows VALUES {'mysql_user_id' = input.user_id, \
             'mysql_action' = lower(input.action)} ON CONFLICT DO NOTHING WITH MAX BATCH 500 \
             FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
    }

    #[test]
    fn renders_mongodb_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: identifier("emit_notifications"),
            from_relay: identifier("notifications"),
            encode_using_codec: None,
            sink: EmitSink::MongoDb {
                client: identifier("mongodb_main"),
                collection: identifier("notification_rows"),
                values: vec![
                    MongoDbValueMapping {
                        column: "mongodb_user_id".to_string(),
                        expression: scoped_field(FieldScope::Input, "user_id"),
                    },
                    MongoDbValueMapping {
                        column: "mongodb_action".to_string(),
                        expression: call("lower", vec![scoped_field(FieldScope::Input, "action")]),
                    },
                ],
                conflict_action: MongoDbConflictAction::DoUpdate {
                    target: vec!["mongodb_user_id".to_string()],
                },
                max_batch: 500,
                flush_each: "10s".to_string(),
            },
            flush_each: "10s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED EMITTER emit_notifications FROM notifications TO MONGODB \
             mongodb_main INSERT TO COLLECTION notification_rows VALUES {'mongodb_user_id' = \
             input.user_id, 'mongodb_action' = lower(input.action)} ON CONFLICT \
             ('mongodb_user_id') DO UPDATE WITH MAX BATCH 500 FLUSH EACH 10s MAX BATCH SIZE 1MiB \
             ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
    }

    #[test]
    fn renders_ingestors_for_all_source_variants() {
        let retry = RetryPolicy {
            backoff: "1s".to_string(),
            max_backoff: "30s".to_string(),
        };
        let expectations = [
            (
                CreateIngestor {
                    name: identifier("http_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Http {
                        client: identifier("http_main"),
                        every: "30s".to_string(),
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR http_ingestor FROM HTTP http_main EVERY 30s DECODE USING \
                 orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("kinesis_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Kinesis {
                        client: identifier("kinesis_main"),
                        relay: identifier("orders_stream"),
                        instances: 2,
                        mode: KinesisIngestMode::AckSequential {
                            timeout: "12s".to_string(),
                            retry_policy: retry.clone(),
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR kinesis_ingestor FROM KINESIS kinesis_main RELAY orders_stream \
                 INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 12s RETRY POLICY BACKOFF 1s MAX 30s \
                 DECODE USING orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE \
                 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("kafka_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Kafka {
                        client: identifier("kafka_main"),
                        topic: identifier("orders_topic"),
                        offset_mode: KafkaOffsetMode::ConsumerGroup(identifier("orders_group")),
                        instances: 3,
                        mode: KafkaIngestMode::AckParallel {
                            max: 8,
                            batch_timeout: "100ms".to_string(),
                            timeout: "5s".to_string(),
                            retry_policy: retry.clone(),
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR kafka_ingestor FROM KAFKA kafka_main TOPIC orders_topic OFFSET \
                 BY CONSUMER GROUP orders_group INSTANCES 3 MODE ACK PARALLEL MAX 8 BATCH TIMEOUT \
                 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s DECODE USING orders_codec \
                 TO orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG \
                 ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("mqtt_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Mqtt {
                        client: identifier("mqtt_main"),
                        topic: "orders_topic".to_string(),
                        instances: 1,
                        mode: MqttIngestMode::NoAckSequential {
                            session: MqttSession::Clean,
                            qos: MqttQos::AtMostOnce,
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR mqtt_ingestor FROM MQTT mqtt_main TOPIC orders_topic MODE NO_ACK \
                 SEQUENTIAL DECODE USING orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX \
                 BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("nats_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Nats {
                        client: identifier("nats_main"),
                        subject: identifier("orders_subject"),
                        queue_group: identifier("orders_workers"),
                        instances: 2,
                        mode: NatsIngestMode::NoAckSequential,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR nats_ingestor FROM NATS nats_main SUBJECT orders_subject QUEUE \
                 GROUP orders_workers INSTANCES 2 MODE NO_ACK SEQUENTIAL DECODE USING \
                 orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("rabbit_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::RabbitMq {
                        client: identifier("rmq_main"),
                        queue: identifier("orders_q"),
                        instances: 2,
                        mode: RabbitMqIngestMode::AckSequential {
                            timeout: "10s".to_string(),
                            retry_policy: retry.clone(),
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR rabbit_ingestor FROM RABBITMQ rmq_main QUEUE orders_q INSTANCES \
                 2 MODE ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 1s MAX 30s DECODE \
                 USING orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("redis_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::RedisPubSub {
                        client: identifier("redis_main"),
                        channel: identifier("orders_channel"),
                        mode: RedisPubSubIngestMode::NoAckSequential,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR redis_ingestor FROM REDIS PUBSUB redis_main CHANNEL \
                 orders_channel MODE NO_ACK SEQUENTIAL DECODE USING orders_codec TO orders \
                 UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL \
                 ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("prom_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Prometheus {
                        client: identifier("prom_main"),
                        query: "sum(rate(http_requests_total[5m]))".to_string(),
                        every: "15s".to_string(),
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR prom_ingestor FROM PROMETHEUS prom_main QUERY \
                 'sum(rate(http_requests_total[5m]))' EVERY 15s DECODE USING orders_codec TO \
                 orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON \
                 GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("zmq_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_main"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR zmq_ingestor FROM ZEROMQ zmq_main MODE NO_ACK SEQUENTIAL DECODE \
                 USING orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("sqs_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Sqs {
                        client: identifier("sqs_main"),
                        queue: identifier("orders_queue"),
                        instances: 1,
                        mode: SqsIngestMode::AckSequential {
                            timeout: "20s".to_string(),
                            retry_policy: retry.clone(),
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR sqs_ingestor FROM SQS sqs_main QUEUE orders_queue MODE ACK \
                 SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s DECODE USING \
                 orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("endpoint_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Endpoint {
                        endpoint: identifier("orders_endpoint"),
                        mode: EndpointIngestMode::NoAckSequential,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR endpoint_ingestor FROM ENDPOINT orders_endpoint MODE NO_ACK \
                 SEQUENTIAL DECODE USING orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX \
                 BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("ws_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::Websockets {
                        client: identifier("ws_main"),
                        mode: WebsocketsIngestMode::NoAckSequential,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR ws_ingestor FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL \
                 DECODE USING orders_codec TO orders UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE \
                 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            ),
        ];

        for (actual, expected) in expectations {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn model_dispatches_to_variant_specific_canonicalization() {
        let model = Model::ClientKafka(CreateClientKafka {
            name: identifier("kafka_main"),
            mount: None,
            config: vec![config_entry("bootstrap.servers", "localhost:9092")],
        });

        assert_eq!(
            model.to_canonical_nspl().expect("must render"),
            "CREATE CLIENT kafka_main TYPE KAFKA CONFIG {'bootstrap.servers' = 'localhost:9092'};"
        );

        let kinesis = Model::ClientKinesis(CreateClientKinesis {
            name: identifier("kinesis_main"),
            mount: None,
            config: vec![config_entry("region", "us-east-1")],
        });
        assert_eq!(
            kinesis.to_canonical_nspl().expect("must render"),
            "CREATE CLIENT kinesis_main TYPE KINESIS CONFIG {'region' = 'us-east-1'};"
        );
    }

    #[test]
    fn string_literals_choose_safe_quote_style_and_reject_newlines() {
        assert_eq!(
            super::string_literal("can't fail").expect("must render"),
            "\"can't fail\""
        );
        assert_eq!(
            super::string_literal("plain").expect("must render"),
            "'plain'"
        );
        assert!(matches!(
            super::string_literal("line\nbreak"),
            Err(super::CanonicalNsplError::UnrepresentableStringLiteral { .. })
        ));
    }

    #[test]
    fn udf_canonicalization_preserves_source_bytes_and_avoids_delimiter_collisions() {
        let code = "fn redact(value: StringColumn) -> StringColumn {\n    // $roto$\n    \
                    value\n}\n"
            .to_string();
        let udf = CreateUdf::new(
            identifier("redact"),
            UdfLanguage::Roto0_11,
            vec![UdfArgument {
                name: identifier("value"),
                ty: ParseAsType::String,
                optional: true,
            }],
            UdfReturn {
                ty: ParseAsType::String,
                optional: false,
            },
            false,
            code.clone(),
        );

        let rendered = udf.to_canonical_nspl().expect("must render");

        assert!(rendered.contains("CODE $roto_1$"));
        assert!(rendered.contains(&code));
    }
}
