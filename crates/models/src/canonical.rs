use std::fmt::{Display, Formatter};

use crate::{
    AlterDeduplicator, AlterDeduplicatorOperation, AlterEmitter, AlterEmitterOperation,
    AlterGenerator, AlterGeneratorOperation, AlterIngestor, AlterIngestorOperation, AlterJunction,
    AlterPlacement, AlterPlacementOperation, AlterProcessorOperation, AlterReingestor, AlterRelay,
    AlterRelayOperation, AlterReorderer, AlterReordererOperation, AlterSchema,
    AlterSchemaOperation, AlterWireSchema, AlterWireSchemaOperation, AssignmentTargetScope,
    AvroType, AzureBlobConfigEntry, BinaryOperator, BranchEviction, BranchSelection,
    ClickHouseConfigEntry, ClickHouseValueMapping, CodecEncoding, CodecEncodingRule,
    CodecJaqTransformations, CodecWireFormat, CorrelationTimeoutAction, CreateBranch,
    CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs, CreateClientHttp,
    CreateClientIcebergRest, CreateClientKafka, CreateClientMongoDb, CreateClientMqtt,
    CreateClientMySql, CreateClientNats, CreateClientOtel, CreateClientPostgres,
    CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
    CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
    CreateClientWebsockets, CreateClientZeroMq, CreateCodec, CreateCorrelator, CreateDeduplicator,
    CreateEmitter, CreateEndpoint, CreateGenerator, CreateInferencer, CreateIngestor,
    CreateJunction, CreateLookup, CreateMaterializer, CreatePlacement, CreateReingestor,
    CreateRelay, CreateReorderer, CreateSchema, CreateSignalingProtocol, CreateUdf, CreateVhost,
    CreateWasmProcessor, CreateWindowProcessor, CreateWireSchema, DomainPace, DomainStartPoint,
    EmitSink, EmitterAckWindow, EmitterPublishingMode, EndpointIngestMode, EndpointType,
    Expression, FieldScope, GcsConfigEntry, GeneralErrorPolicy, HttpConfigEntry, IcebergCatalog,
    Identifier, InferencerTensorDeclaration, InferencerTensorDimension, InferencerTensorMapping,
    IngestSource, IngestTimestampSource, Inheritance, InputCollectPolicy, JsonType,
    KafkaConfigEntry, KafkaIngestMode, KafkaOffsetMode, Literal, MaterializedRelayState,
    MaterializedStateDependency, MaterializedStatePolicy, MessageErrorPolicy, Model, ModelKind,
    MongoDbConfigEntry, MongoDbConflictAction, MqttConfigEntry, MqttIngestMode, MqttQos,
    MqttSession, MySqlConfigEntry, MySqlConflictAction, NatsConfigEntry, NatsIngestMode,
    OtelConfigEntry, OtelMetricKind, OtelSignal, OutputBranch, ParseAsType, PlacementPolicy,
    PostgresConfigEntry, PostgresConflictAction, ProcessorInputWhere, ProcessorInputs,
    ProcessorOutputs, PrometheusConfigEntry, PulsarConfigEntry, PulsarIngestMode,
    RabbitMqConfigEntry, RabbitMqIngestMode, RedisConfigEntry, RedisPubSubIngestMode,
    RelayBranching, RetryPolicy, RouteConstruction, S3ConfigEntry, SchemaField, SentryConfigEntry,
    SignalingStep, SignalingWaitStep, SignalingWireFormat, SqsConfigEntry, SqsFifoGroup,
    SqsIngestMode, Statement, SubscriptionLiteral, UnaryOperator, WebsocketsConfigEntry,
    WebsocketsIngestMode, WindowBound, WireSchemaDefinition, WireSchemaField, ZeroMqConfigEntry,
    ZeroMqIngestMode,
};

/// Width of one canonical indentation level.
const INDENT: usize = 2;

/// A clause in the canonical layout of a statement.
///
/// Line breaks fall between clauses and never inside one, which is what keeps an expression, a
/// quiesce mode, or a publishing mode renderable as a single line wherever it is embedded.
enum Clause {
    /// A clause occupying one line.
    Line(String),
    /// A clause that introduces further clauses one level deeper, such as a route.
    Group { head: String, nested: Vec<Clause> },
    /// A clause whose items align under the first, used only for `SET` lists.
    Aligned { head: String, items: Vec<String> },
    /// A clause introducing a delimited list, such as `CONFIG { … }`.
    Block {
        head: String,
        open: char,
        items: Vec<String>,
        close: char,
    },
}

impl Clause {
    fn line(text: impl Into<String>) -> Self {
        Self::Line(text.into())
    }

    fn group(head: impl Into<String>, nested: Vec<Clause>) -> Self {
        Self::Group {
            head: head.into(),
            nested,
        }
    }

    fn aligned(head: impl Into<String>, items: Vec<String>) -> Self {
        Self::Aligned {
            head: head.into(),
            items,
        }
    }

    fn braced(head: impl Into<String>, items: Vec<String>) -> Self {
        Self::Block {
            head: head.into(),
            open: '{',
            items,
            close: '}',
        }
    }

    fn append_to(&self, indent: usize, lines: &mut Vec<String>) {
        let pad = " ".repeat(indent);
        match self {
            Self::Line(text) => lines.push(format!("{pad}{text}")),
            Self::Group { head, nested } => {
                lines.push(format!("{pad}{head}"));
                for clause in nested {
                    clause.append_to(indent + INDENT, lines);
                }
            }
            Self::Aligned { head, items } => {
                // Continuations line up under the first item, one indentation past the keyword.
                let continuation = " ".repeat(indent + head.len() + 1);
                let last = items.len().saturating_sub(1);
                for (index, item) in items.iter().enumerate() {
                    let comma = if index == last { "" } else { "," };
                    let prefix = if index == 0 {
                        format!("{pad}{head} ")
                    } else {
                        continuation.clone()
                    };
                    lines.push(format!("{prefix}{item}{comma}"));
                }
            }
            Self::Block {
                head,
                open,
                items,
                close,
            } => {
                if items.is_empty() {
                    lines.push(format!("{pad}{head} {open}{close}"));
                    return;
                }
                lines.push(format!("{pad}{head} {open}"));
                append_list_items(items, indent + INDENT, lines);
                lines.push(format!("{pad}{close}"));
            }
        }
    }
}

/// Pushes `items` one per line, comma-separated, at `indent`.
fn append_list_items(items: &[String], indent: usize, lines: &mut Vec<String>) {
    let pad = " ".repeat(indent);
    let last = items.len() - 1;
    for (index, item) in items.iter().enumerate() {
        let comma = if index == last { "" } else { "," };
        lines.push(format!("{pad}{item}{comma}"));
    }
}

/// Lays out a statement as a header line followed by one clause per line.
///
/// A statement with no clauses stays on the header line, which is what keeps short forms such as
/// `USE demo;` and `DROP RELAY orders;` on one line.
fn clause_statement(header: impl Into<String>, clauses: Vec<Clause>) -> String {
    let mut lines = vec![header.into()];
    for clause in &clauses {
        clause.append_to(INDENT, &mut lines);
    }
    let mut rendered = lines.join("\n");
    rendered.push(';');
    rendered
}

/// Lays out a statement whose body is a delimited list, such as a schema's fields.
fn block_statement(header: impl Into<String>, items: Vec<String>) -> String {
    let header = header.into();
    if items.is_empty() {
        return format!("{header} ();");
    }

    let mut lines = vec![format!("{header} (")];
    append_list_items(&items, INDENT, &mut lines);
    lines.push(");".to_string());
    lines.join("\n")
}

/// Binding levels of the NSPL expression grammar, loosest first.
///
/// These mirror the parser's precedence ladder exactly. Rendering consults them so that an
/// expression carries only the parentheses its structure actually requires.
const PRECEDENCE_OR: u8 = 0;
const PRECEDENCE_AND: u8 = 1;
const PRECEDENCE_COMPARISON: u8 = 2;
const PRECEDENCE_ADDITIVE: u8 = 3;
const PRECEDENCE_MULTIPLICATIVE: u8 = 4;
const PRECEDENCE_UNARY: u8 = 5;
const PRECEDENCE_CAST: u8 = 6;
const PRECEDENCE_ATOM: u8 = 7;

fn binary_precedence(operator: &BinaryOperator) -> u8 {
    match operator {
        BinaryOperator::Or => PRECEDENCE_OR,
        BinaryOperator::And => PRECEDENCE_AND,
        BinaryOperator::Equal
        | BinaryOperator::NotEqual
        | BinaryOperator::GreaterThan
        | BinaryOperator::LessThan
        | BinaryOperator::GreaterThanOrEqual
        | BinaryOperator::LessThanOrEqual => PRECEDENCE_COMPARISON,
        BinaryOperator::Add | BinaryOperator::Subtract => PRECEDENCE_ADDITIVE,
        BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Remainder => {
            PRECEDENCE_MULTIPLICATIVE
        }
    }
}

/// The level at which `expression` binds when it is reparsed.
fn precedence(expression: &Expression) -> u8 {
    match expression {
        Expression::Binary { operator, .. } => binary_precedence(operator),
        Expression::Unary { .. } => PRECEDENCE_UNARY,
        Expression::Cast { .. } => PRECEDENCE_CAST,
        _ => PRECEDENCE_ATOM,
    }
}

/// Renders `expression` as an operand, parenthesizing it only when it binds more loosely than
/// `minimum` and would otherwise regroup on reparse.
///
/// Callers pass the operator's own level for a left operand and one level tighter for a right
/// operand, which is what makes the left-associative ladder round-trip.
fn operand_to_nspl(expression: &Expression, minimum: u8) -> Result<String, CanonicalNsplError> {
    let rendered = expression_to_nspl(expression)?;
    if precedence(expression) < minimum {
        Ok(format!("({rendered})"))
    } else {
        Ok(rendered)
    }
}

pub fn expression_to_nspl(expression: &Expression) -> Result<String, CanonicalNsplError> {
    match expression {
        Expression::Literal(Literal::I64(value)) => Ok(value.to_string()),
        Expression::Literal(Literal::F64(value)) => float_literal(value.value()),
        Expression::Literal(Literal::Bool(value)) => Ok(value.to_string().to_ascii_uppercase()),
        Expression::Literal(Literal::String(value)) => Ok(string_literal(value)),
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
            "{}{}",
            match operator {
                UnaryOperator::Negate => "-",
                UnaryOperator::Not => "NOT ",
            },
            operand_to_nspl(expression, PRECEDENCE_CAST)?
        )),
        Expression::Binary {
            operator,
            left,
            right,
        } => Ok(format!(
            "{} {} {}",
            operand_to_nspl(left, binary_precedence(operator))?,
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
            operand_to_nspl(right, binary_precedence(operator) + 1)?
        )),
        Expression::Cast { expression, target } => Ok(format!(
            "{} AS {}",
            operand_to_nspl(expression, PRECEDENCE_CAST)?,
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
    Ok(route_construction_clauses(construction)?
        .into_iter()
        .map(|clause| match clause {
            Clause::Aligned { head, items } => format!("{head} {}", items.join(", ")),
            Clause::Line(text) => text,
            _ => unreachable!("route construction renders only lines and aligned lists"),
        })
        .collect::<Vec<_>>()
        .join(" "))
}

/// The clauses of a route's construction, in the order they must be written.
fn route_construction_clauses(
    construction: &RouteConstruction,
) -> Result<Vec<Clause>, CanonicalNsplError> {
    let mut clauses: Vec<Clause> = Vec::new();
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
        clauses.push(Clause::line(clause));
    }
    if !construction.assignments.is_empty() {
        clauses.push(Clause::aligned(
            "SET",
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
                .collect::<Result<Vec<_>, CanonicalNsplError>>()?,
        ));
    }
    if let Some(where_clause) = &construction.where_clause {
        clauses.push(Clause::line(format!(
            "WHERE {}",
            expression_to_nspl(where_clause)?
        )));
    }
    if !construction.invocations.is_empty() {
        clauses.push(Clause::line(format!(
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
        )));
    }
    Ok(clauses)
}

fn value_mapping_items(
    values: &[ClickHouseValueMapping],
) -> Result<Vec<String>, CanonicalNsplError> {
    values
        .iter()
        .map(|mapping| {
            Ok(format!(
                "{} = {}",
                string_literal(&mapping.column),
                expression_to_nspl(&mapping.expression)?
            ))
        })
        .collect()
}

fn value_mappings_to_nspl(values: &[ClickHouseValueMapping]) -> Result<String, CanonicalNsplError> {
    Ok(value_mapping_items(values)?.join(", "))
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
    UnrepresentableFloat { value: String },
    DerivedModel { kind: ModelKind },
    InvalidCodec { reason: String },
}

impl Display for CanonicalNsplError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnrepresentableFloat { value } => {
                write!(f, "cannot represent non-finite float in NSPL: {value}")
            }
            Self::DerivedModel { kind } => write!(
                f,
                "{} is derived from its owning statement and has no NSPL form",
                kind.keyword_phrase()
            ),
            Self::InvalidCodec { reason } => write!(f, "invalid codec: {reason}"),
        }
    }
}

impl std::error::Error for CanonicalNsplError {}

impl AlterPlacement {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(|operation| match operation {
                AlterPlacementOperation::SetPolicy { policy } => {
                    format!("SET POLICY {}", policy.as_ref())
                }
                AlterPlacementOperation::SetRank { rank } => format!("SET RANK {rank}"),
                AlterPlacementOperation::DropRank => "DROP RANK".to_string(),
                AlterPlacementOperation::SetMembers { from, to } => format!(
                    "SET FROM {} TO {}",
                    identifier_list(from),
                    identifier_list(to)
                ),
                AlterPlacementOperation::RenameTo { name } => {
                    format!("RENAME TO {}", name.as_str())
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "ALTER PLACEMENT {} {operations};",
            self.placement.as_str()
        ))
    }
}

fn identifier_list(names: &[Identifier]) -> String {
    names
        .iter()
        .map(Identifier::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

impl Statement {
    /// Renders this statement as canonical NSPL.
    ///
    /// Unlike [`Model::to_canonical_nspl`], this covers the whole executable language -- the
    /// creation modifiers, the domain lifecycle, administration, and the read-only queries -- so a
    /// parsed script can be rendered back statement for statement.
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        match self {
            Self::Create(create) => {
                let rendered = create.body.to_canonical_nspl()?;
                if !create.if_not_exists {
                    return Ok(rendered);
                }
                let rest = rendered
                    .strip_prefix("CREATE ")
                    .expect("every model renders as a CREATE statement");
                Ok(format!("CREATE IF NOT EXISTS {rest}"))
            }
            Self::CreateDomain(create) => {
                let modifier = if create.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                };
                let config = &create.body.config;
                let pacing = match config.pace {
                    DomainPace::Paced => format!(
                        "PACED DOMAIN {} WITH PERIOD {} SKEW {}",
                        create.body.id.as_str(),
                        config.period,
                        config.skew
                    ),
                    DomainPace::Unpaced => {
                        format!("UNPACED DOMAIN {}", create.body.id.as_str())
                    }
                };
                Ok(format!(
                    "CREATE {modifier}{pacing}{};",
                    placement_policy_suffix(config.placement)
                ))
            }
            Self::AlterDomain(alter) => Ok(format!(
                "ALTER DOMAIN SET PLACEMENT {};",
                alter.policy.as_ref()
            )),
            Self::CreateUser(create) => {
                let modifier = if create.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                };
                Ok(format!(
                    "CREATE {modifier}USER {} WITH PASSWORD {};",
                    create.body.name.as_str(),
                    string_literal(&create.body.password)
                ))
            }
            Self::CreateResource(create) => {
                let modifier = if create.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                };
                Ok(format!(
                    "CREATE {modifier}RESOURCE {};",
                    create.body.identifier.as_str()
                ))
            }
            Self::UploadResource(upload) => Ok(format!(
                "UPLOAD RESOURCE {} VERSION {};",
                upload.identifier.as_str(),
                string_literal(&upload.source_path)
            )),
            Self::StartDomain(start) => Ok(match &start.start {
                DomainStartPoint::Resume => "START;".to_string(),
                DomainStartPoint::Now { time_rate } => {
                    format!("START AT NOW TIME RATE {time_rate};")
                }
                DomainStartPoint::At {
                    timestamp,
                    time_rate,
                } => format!(
                    "START AT {} TIME RATE {time_rate};",
                    string_literal(timestamp)
                ),
            }),
            Self::StopDomain(_) => Ok("STOP;".to_string()),
            Self::AlterSchema(alter) => alter.to_canonical_nspl(),
            Self::AlterWireJsonSchema(alter) => alter_json_wire_schema_to_canonical_nspl(alter),
            Self::AlterWireCborSchema(alter) => alter_cbor_wire_schema_to_canonical_nspl(alter),
            Self::AlterWireAvroSchema(alter) => alter_avro_wire_schema_to_canonical_nspl(alter),
            Self::AlterRelay(alter) => alter.to_canonical_nspl(),
            Self::AlterJunction(alter) => alter.to_canonical_nspl(),
            Self::AlterDeduplicator(alter) => alter.to_canonical_nspl(),
            Self::AlterReorderer(alter) => alter.to_canonical_nspl(),
            Self::AlterEmitter(alter) => alter.to_canonical_nspl(),
            Self::AlterIngestor(alter) => alter.to_canonical_nspl(),
            Self::AlterReingestor(alter) => alter.to_canonical_nspl(),
            Self::AlterGenerator(alter) => alter.to_canonical_nspl(),
            Self::AlterPlacement(alter) => alter.to_canonical_nspl(),
            Self::Drop(drop) => Ok(format!(
                "DROP {} {};",
                drop.kind.keyword_phrase(),
                drop.name.as_str()
            )),
            Self::DropNode(node) => Ok(format!("DROP NODE {};", node.node_id)),
            Self::CordonNode(node) => Ok(format!("CORDON NODE {};", node.node_id)),
            Self::UncordonNode(node) => Ok(format!("UNCORDON NODE {};", node.node_id)),
            Self::DrainNode(node) => Ok(format!("DRAIN NODE {};", node.node_id)),
            Self::DescribeRelay(describe) => {
                let bindings = if describe.bindings.is_empty() {
                    String::new()
                } else {
                    format!(
                        " WHERE ({})",
                        describe
                            .bindings
                            .iter()
                            .map(|binding| format!(
                                "{} = {}",
                                binding.field.as_str(),
                                subscription_literal_to_nspl(&binding.value)
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                Ok(format!(
                    "DESCRIBE RELAY {}{bindings};",
                    describe.relay.as_str()
                ))
            }
            Self::DescribeDomain(_) => Ok("DESCRIBE DOMAIN;".to_string()),
            Self::DescribeIngestor(describe) => {
                Ok(format!("DESCRIBE INGESTOR {};", describe.ingestor.as_str()))
            }
            Self::DescribeResource(describe) => Ok(format!(
                "DESCRIBE RESOURCE {}{};",
                describe.identifier.as_str(),
                describe
                    .version
                    .map(|version| format!(" VERSION {version}"))
                    .unwrap_or_default()
            )),
            Self::DescribeLookup(describe) => {
                Ok(format!("DESCRIBE HASH MAP {};", describe.name.as_str()))
            }
            Self::DescribeEndpoint(describe) => {
                Ok(format!("DESCRIBE ENDPOINT {};", describe.name.as_str()))
            }
            Self::DescribeJunction(describe) => {
                Ok(format!("DESCRIBE JUNCTION {};", describe.name.as_str()))
            }
            Self::DescribeDeduplicator(describe) => {
                Ok(format!("DESCRIBE DEDUPLICATOR {};", describe.name.as_str()))
            }
            Self::DescribeReingestor(describe) => {
                Ok(format!("DESCRIBE REINGESTOR {};", describe.name.as_str()))
            }
            Self::DescribeCorrelator(describe) => {
                Ok(format!("DESCRIBE CORRELATOR {};", describe.name.as_str()))
            }
            Self::DescribeReorderer(describe) => {
                Ok(format!("DESCRIBE REORDERER {};", describe.name.as_str()))
            }
            Self::DescribeEmitter(describe) => {
                Ok(format!("DESCRIBE EMITTER {};", describe.name.as_str()))
            }
            Self::DescribeWindowProcessor(describe) => Ok(format!(
                "DESCRIBE WINDOW PROCESSOR {};",
                describe.name.as_str()
            )),
            Self::DescribeWasmProcessor(describe) => Ok(format!(
                "DESCRIBE WASM PROCESSOR {};",
                describe.name.as_str()
            )),
            Self::DescribeUdf(describe) => Ok(format!("DESCRIBE UDF {};", describe.name.as_str())),
            Self::DescribePlacement(describe) => {
                Ok(format!("DESCRIBE PLACEMENT {};", describe.name.as_str()))
            }
            Self::LookupQuery(query) => Ok(format!(
                "LOOKUP {} KEY {};",
                query.name.as_str(),
                subscription_literal_to_nspl(&query.key)
            )),
            Self::ShowCreate(show) => Ok(format!(
                "SHOW CREATE {} {};",
                show.kind.keyword_phrase(),
                show.name.as_str()
            )),
            Self::ShowUdfs(_) => Ok("SHOW UDFS;".to_string()),
            Self::ShowPlacements(_) => Ok("SHOW PLACEMENTS;".to_string()),
            Self::ShowRelayMaterializedState(show) => Ok(format!(
                "SHOW RELAY {} MATERIALIZED STATE;",
                show.relay.as_str()
            )),
            Self::ShowClusterStatus(_) => Ok("SHOW CLUSTER STATUS;".to_string()),
            Self::ShowTransactions(_) => Ok("SHOW TRANSACTIONS;".to_string()),
        }
    }
}

/// Renders a trailing `PLACEMENT <policy>` clause, omitting the default policy.
fn placement_policy_suffix(policy: PlacementPolicy) -> String {
    if policy == PlacementPolicy::default() {
        String::new()
    } else {
        format!(" PLACEMENT {}", policy.as_ref())
    }
}

fn subscription_literal_to_nspl(literal: &SubscriptionLiteral) -> String {
    match literal {
        SubscriptionLiteral::String(value) => string_literal(value),
        SubscriptionLiteral::Number(value) => value.clone(),
        SubscriptionLiteral::Bool(value) => value.to_string().to_ascii_uppercase(),
    }
}

impl Model {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        match self {
            Self::Schema(schema) => schema.to_canonical_nspl(),
            Self::WireJsonSchema(schema) => wire_schema_to_nspl("JSON", schema),
            Self::WireCborSchema(schema) => wire_schema_to_nspl("CBOR", schema),
            Self::WireAvroSchema(schema) => wire_schema_to_nspl("AVRO", schema),
            Self::Codec(codec) => codec.to_canonical_nspl(),
            Self::ClientKafka(client) => client.to_canonical_nspl(),
            Self::ClientPulsar(client) => client.to_canonical_nspl(),
            Self::ClientHttp(client) => client.to_canonical_nspl(),
            Self::ClientSentry(client) => client.to_canonical_nspl(),
            Self::ClientOtel(client) => client.to_canonical_nspl(),
            Self::ClientPrometheus(client) => client.to_canonical_nspl(),
            Self::ClientMqtt(client) => client.to_canonical_nspl(),
            Self::ClientNats(client) => client.to_canonical_nspl(),
            Self::ClientRabbitMq(client) => client.to_canonical_nspl(),
            Self::ClientRedis(client) => client.to_canonical_nspl(),
            Self::ClientZeroMq(client) => client.to_canonical_nspl(),
            Self::ClientSqs(client) => client.to_canonical_nspl(),
            Self::ClientWebsockets(client) => client.to_canonical_nspl(),
            Self::ClientSyslog(client) => client.to_canonical_nspl(),
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
            Self::Placement(placement) => placement.to_canonical_nspl(),
            Self::Udf(udf) => udf.to_canonical_nspl(),
        }
    }
}

impl CreatePlacement {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut rendered = format!(
            "CREATE PLACEMENT {} FROM {} TO {} {}",
            self.name.as_str(),
            self.from
                .iter()
                .map(Identifier::as_str)
                .collect::<Vec<_>>()
                .join(", "),
            self.to
                .iter()
                .map(Identifier::as_str)
                .collect::<Vec<_>>()
                .join(", "),
            self.policy,
        );
        if let Some(rank) = self.rank {
            rendered.push_str(&format!(" RANK {rank}"));
        }
        rendered.push(';');
        Ok(rendered)
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
        let quoted_code = dollar_quote(&self.code, "roto");

        Ok(format!(
            "CREATE UDF {}\n  WITH {}\n  ARGS ({arguments})\n  RETURNS {returns}{}\n  CODE \
             {quoted_code};",
            self.name.as_str(),
            self.language.as_ref(),
            if self.volatile { "\n  VOLATILE" } else { "" },
        ))
    }
}

impl CreateSchema {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let fields = self
            .fields
            .iter()
            .map(schema_field_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?;

        Ok(block_statement(
            format!("CREATE SCHEMA {}", self.name.as_str()),
            fields,
        ))
    }
}

impl AlterSchema {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(alter_schema_operation_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        Ok(format!(
            "ALTER SCHEMA {} {operations};",
            self.schema.as_str()
        ))
    }
}

impl AlterRelay {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(|operation| match operation {
                AlterRelayOperation::SetCapacity { capacity } => {
                    format!("SET CAPACITY {capacity}")
                }
                AlterRelayOperation::SetSchema { schema } => {
                    format!("SET SCHEMA {}", schema.as_str())
                }
                AlterRelayOperation::SetBranching { branching } => match branching {
                    RelayBranching::BranchedBy { branch } => {
                        format!("SET BRANCHED BY {}", branch.as_str())
                    }
                    RelayBranching::Unbranched => "SET UNBRANCHED".to_string(),
                },
                AlterRelayOperation::SetMaterializedState => {
                    "SET MATERIALIZED STATE LAST BY TIMESTAMP".to_string()
                }
                AlterRelayOperation::DropMaterializedState => "DROP MATERIALIZED STATE".to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("ALTER RELAY {} {operations};", self.relay.as_str()))
    }
}

impl AlterJunction {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(alter_processor_operation_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        Ok(format!(
            "ALTER JUNCTION {} {operations};",
            self.junction.as_str()
        ))
    }
}

impl AlterDeduplicator {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(alter_deduplicator_operation_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        Ok(format!(
            "ALTER DEDUPLICATOR {} {operations};",
            self.deduplicator.as_str()
        ))
    }
}

impl AlterReorderer {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(alter_reorderer_operation_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        Ok(format!(
            "ALTER REORDERER {} {operations};",
            self.reorderer.as_str()
        ))
    }
}

impl AlterReingestor {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(alter_processor_operation_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        Ok(format!(
            "ALTER REINGESTOR {} {operations};",
            self.reingestor.as_str()
        ))
    }
}

impl AlterGenerator {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(alter_generator_operation_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        Ok(format!(
            "ALTER GENERATOR {} {operations};",
            self.generator.as_str()
        ))
    }
}

impl AlterEmitter {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(alter_emitter_operation_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        Ok(format!(
            "ALTER EMITTER {} {operations};",
            self.emitter.as_str()
        ))
    }
}

impl AlterIngestor {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let operations = self
            .operations
            .iter()
            .map(alter_ingestor_operation_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");
        Ok(format!(
            "ALTER INGESTOR {} {operations};",
            self.ingestor.as_str()
        ))
    }
}

impl WireSchemaDefinition {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        match self {
            Self::Json(schema) => wire_schema_to_nspl("JSON", schema),
            Self::Cbor(schema) => wire_schema_to_nspl("CBOR", schema),
            Self::Avro(schema) => wire_schema_to_nspl("AVRO", schema),
        }
    }
}

pub fn alter_json_wire_schema_to_canonical_nspl(
    alter: &AlterWireSchema<JsonType>,
) -> Result<String, CanonicalNsplError> {
    alter_wire_schema_to_nspl("JSON", alter)
}

pub fn alter_cbor_wire_schema_to_canonical_nspl(
    alter: &AlterWireSchema<JsonType>,
) -> Result<String, CanonicalNsplError> {
    alter_wire_schema_to_nspl("CBOR", alter)
}

pub fn alter_avro_wire_schema_to_canonical_nspl(
    alter: &AlterWireSchema<AvroType>,
) -> Result<String, CanonicalNsplError> {
    alter_wire_schema_to_nspl("AVRO", alter)
}

impl CreateClientKafka {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(kafka_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE KAFKA{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE HTTP{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
        ))
    }
}

impl CreateClientSentry {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(sentry_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE SENTRY{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
        ))
    }
}

impl CreateClientOtel {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(otel_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE OTEL{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE PULSAR{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE MQTT{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE NATS{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE PROMETHEUS{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE RABBITMQ{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE REDIS{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE ZEROMQ{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE SQS{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE GCS{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE AZURE_BLOB{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE ICEBERG_REST{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE WEBSOCKETS{}{}",
                    signaling_protocol_clause(self.signaling_protocol.as_ref()),
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
        ))
    }
}

impl CreateClientSyslog {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let config = self
            .config
            .iter()
            .map(kafka_entry_to_nspl)
            .collect::<Result<Vec<_>, CanonicalNsplError>>()?
            .join(", ");

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE SYSLOG{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE CLICKHOUSE{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE POSTGRES{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE MYSQL{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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

        Ok(clause_statement(
            format!("CREATE CLIENT {}", self.name.as_str()),
            vec![
                Clause::line(format!(
                    "TYPE MONGODB{}",
                    client_mount_clause(self.mount.as_ref()),
                )),
                Clause::braced("CONFIG", split_config_entries(&config)),
            ],
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
            string_literal(&self.path),
            endpoint_type_to_nspl(self.endpoint_type),
            signaling_protocol_clause(self.signaling_protocol.as_ref())
        ))
    }
}

impl CreateSignalingProtocol {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut clauses = String::new();
        if self.on_connect.accept_data {
            clauses.push_str(" ACCEPT DATA");
        }
        for step in &self.on_connect.steps {
            match step {
                SignalingStep::Send(programs) => {
                    clauses.push_str(" SEND JAQ ");
                    clauses.push_str(&jaq_program_list_to_nspl(programs));
                }
                SignalingStep::Wait(wait) => clauses.push_str(&wait.to_canonical_nspl()?),
            }
        }
        let protocol_fail = if self.on_connect.fail_matchers.is_empty() {
            String::new()
        } else {
            format!(
                " FAIL JAQ {}",
                jaq_program_list_to_nspl(&self.on_connect.fail_matchers)
            )
        };

        Ok(format!(
            "CREATE SIGNALING PROTOCOL {} FORMAT {}{} ON CONNECT{} TIMEOUT {};",
            self.name.as_str(),
            self.format.to_canonical_nspl()?,
            protocol_fail,
            clauses,
            self.on_connect.timeout
        ))
    }
}

impl SignalingWireFormat {
    fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let Self::Protobuf(config) = self else {
            return Ok(self.as_ref().to_string());
        };
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
        Ok(format!(
            "PROTOBUF USING RESOURCE {}{} CONFIG {{{}}} SEND MESSAGE {} WAIT MESSAGE {}",
            config.resource.as_str(),
            version,
            protobuf_config,
            string_literal(&config.send_message),
            string_literal(&config.wait_message)
        ))
    }
}

impl SignalingWaitStep {
    fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut rendered = format!(" WAIT JAQ {}", jaq_program_list_to_nspl(&self.matchers));
        if !self.fail_matchers.is_empty() {
            rendered.push_str(" FAIL JAQ ");
            rendered.push_str(&jaq_program_list_to_nspl(&self.fail_matchers));
        }
        if let Some(capture) = self.capture.as_deref() {
            rendered.push_str(" CAPTURE ");
            rendered.push_str(&string_literal(capture));
        }
        if self.accept_data {
            rendered.push_str(" ACCEPT DATA");
        }
        Ok(rendered)
    }
}

fn jaq_program_list_to_nspl(programs: &[String]) -> String {
    programs
        .iter()
        .map(|program| string_literal(program))
        .collect::<Vec<_>>()
        .join(", ")
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
                CodecWireFormat::Syslog => {
                    if self.wire_schema.is_some() {
                        return Err(CanonicalNsplError::InvalidCodec {
                            reason: "SYSLOG codec must not reference a wire schema".to_string(),
                        });
                    }
                    if !self.encoding_rules.is_empty() {
                        return Err(CanonicalNsplError::InvalidCodec {
                            reason: "SYSLOG codec must not declare encoding rules".to_string(),
                        });
                    }
                    ("SYSLOG".to_string(), String::new())
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
                            string_literal(&config.message)
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
        let mut clauses = Vec::new();
        // The wire description may itself carry a CONFIG map, which becomes a block of its own.
        match wire.split_once(" CONFIG {") {
            Some((before, rest)) => {
                let (entries, after) = rest
                    .rsplit_once('}')
                    .expect("a rendered CONFIG map is closed");
                clauses.push(Clause::line(format!("FROM {before}")));
                clauses.push(Clause::braced("CONFIG", split_config_entries(entries)));
                if !after.trim().is_empty() {
                    clauses.push(Clause::line(after.trim().to_string()));
                }
            }
            None => clauses.push(Clause::line(format!("FROM {wire}"))),
        }
        clauses.push(Clause::line(format!("TO SCHEMA {}", self.schema.as_str())));
        if !transformations.is_empty() {
            clauses.push(Clause::line(transformations.trim().to_string()));
        }
        if !encoding_rules.is_empty() {
            clauses.push(Clause::line(encoding_rules.trim().to_string()));
        }

        Ok(clause_statement(
            format!("CREATE CODEC {}", self.name.as_str()),
            clauses,
        ))
    }
}

/// Splits a rendered `CONFIG` map body back into its entries.
///
/// Entry values are quoted, so a comma inside one never separates entries; splitting tracks the
/// quote state rather than scanning for commas blindly.
fn split_config_entries(entries: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;

    for ch in entries.chars() {
        match quote {
            Some(open) => {
                if ch == open {
                    quote = None;
                }
                current.push(ch);
            }
            None if ch == '\'' || ch == '"' => {
                quote = Some(ch);
                current.push(ch);
            }
            None if ch == ',' => {
                items.push(current.trim().to_string());
                current.clear();
            }
            None => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
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
        rendered.push_str(&string_literal(program));
    }
    if let Some(program) = transformations.on_emitting.as_deref() {
        rendered.push_str(" ON EMITTING ");
        rendered.push_str(&string_literal(program));
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
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            ingest_source_to_nspl(&self.source)
        ))];
        clauses.push(Clause::line(format!(
            "DECODE USING {}",
            self.decode_using_codec.as_str()
        )));
        if !timestamp.is_empty() {
            clauses.push(Clause::line(timestamp.trim_start().to_string()));
        }
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.extend(processor_outputs_clauses(&self.output_routes)?);
        clauses.push(Clause::line(general_error_policy_to_nspl(
            &self.general_error_policy,
        )));

        Ok(clause_statement(
            format!("CREATE INGESTOR {}", self.name.as_str()),
            clauses,
        ))
    }
}

impl CreateGenerator {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut clauses = vec![
            Clause::line(format!(
                "USING MATERIALIZED STATE {}",
                self.materialized_relay.as_str()
            )),
            Clause::line(format!("EACH {}", self.each)),
            Clause::line(branch_selection_to_nspl(&self.branched_by)),
        ];
        clauses.extend(processor_outputs_clauses(&self.output_routes)?);

        Ok(clause_statement(
            format!("CREATE GENERATOR {}", self.name.as_str()),
            clauses,
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
    /// Materializers have no surface syntax and cannot be rendered.
    ///
    /// A materializer is derived by the registry from a relay that declares materialized state; the
    /// owning `CREATE RELAY ... WITH MATERIALIZED STATE` statement is what users write and what
    /// renders. There is nothing valid to emit here, so rendering reports the derived kind rather
    /// than inventing text that would not parse.
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Err(CanonicalNsplError::DerivedModel {
            kind: ModelKind::Materializer,
        })
    }
}

impl CreateLookup {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        Ok(format!(
            "CREATE HASH MAP {} KEY {} FROM RESOURCE {} PATH {} DECODE USING {};",
            self.name.as_str(),
            self.key_field.as_str(),
            self.resource.as_str(),
            string_literal(&self.path),
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

fn collect_policy_to_nspl(policy: &InputCollectPolicy) -> String {
    let max_batch_size = policy
        .max_batch_size
        .as_ref()
        .map(|size| format!(" MAX BATCH SIZE {size}"))
        .unwrap_or_default();
    format!("COLLECT FOR {}{max_batch_size}", policy.collect_for)
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

fn materialized_state_policy_to_nspl(
    policy: &MaterializedStatePolicy,
) -> Result<String, CanonicalNsplError> {
    match policy {
        MaterializedStatePolicy::RequiredSkip => Ok("REQUIRED SKIP".to_string()),
        MaterializedStatePolicy::RequiredWait => Ok("REQUIRED WAIT".to_string()),
        MaterializedStatePolicy::Default(assignments) => Ok(format!(
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
        )),
    }
}

fn general_error_policy_to_nspl(policy: &GeneralErrorPolicy) -> &'static str {
    match policy {
        GeneralErrorPolicy::Ignore => "ON GENERAL ERROR IGNORE",
        GeneralErrorPolicy::Log => "ON GENERAL ERROR LOG",
    }
}

impl CreateJunction {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_nspl(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.extend(processor_tail_clauses(
            &self.branched_by,
            &self.materialized_state,
            &self.output_routes,
        )?);

        Ok(clause_statement(
            format!(
                "CREATE {} JUNCTION {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

impl CreateDeduplicator {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_nspl(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "DEDUPLICATE ON {}",
            self.deduplicate_on
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )));
        clauses.push(Clause::line(format!("MAX TIME {}", self.max_time)));
        clauses.extend(processor_tail_clauses(
            &self.branched_by,
            &self.materialized_state,
            &self.output_routes,
        )?);

        Ok(clause_statement(
            format!(
                "CREATE {} DEDUPLICATOR {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

impl CreateCorrelator {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut clauses = vec![
            Clause::line(prefixed_processor_inputs_to_nspl("LEFT", &self.left)?),
            Clause::line(prefixed_processor_inputs_to_nspl("RIGHT", &self.right)?),
            Clause::line(format!(
                "CORRELATE WHERE {}",
                expression_to_nspl(&self.correlate_where)?
            )),
            Clause::line(format!("MATCH {}", self.match_policy.as_ref())),
            Clause::line(format!("MAX TIME {}", self.max_time)),
            Clause::line(format!(
                "ON CORRELATION TIMEOUT {}, {}",
                correlation_timeout_action_to_nspl(&self.timeout_policy.left),
                correlation_timeout_action_to_nspl(&self.timeout_policy.right)
            )),
        ];
        clauses.extend(processor_tail_clauses(
            &self.branched_by,
            &self.materialized_state,
            &self.output_routes,
        )?);

        Ok(clause_statement(
            format!(
                "CREATE {} CORRELATOR {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
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
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_nspl(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "BY {}",
            self.order_by
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )));
        clauses.push(Clause::line(format!("MAX TIME {}", self.max_time)));
        clauses.extend(processor_tail_clauses(
            &self.branched_by,
            &self.materialized_state,
            &self.output_routes,
        )?);

        Ok(clause_statement(
            format!(
                "CREATE {} REORDERER {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

impl CreateWindowProcessor {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_nspl(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "WIDTH {}",
            window_bound_to_nspl(&self.width)
        )));
        clauses.push(Clause::line(format!(
            "STEP {}",
            window_bound_to_nspl(&self.step)
        )));
        clauses.extend(processor_tail_clauses(
            &self.branched_by,
            &self.materialized_state,
            &self.output_routes,
        )?);

        Ok(clause_statement(
            format!(
                "CREATE {} WINDOW PROCESSOR {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
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
        // The codec and the commit cadence are written with the sink they belong to: whether either
        // is legal is a property of the sink, so they nest under it rather than precede it.
        let mut sink_clauses = Vec::new();
        if !commit_policy.is_empty() {
            sink_clauses.push(Clause::line(commit_policy.trim_start().to_string()));
        }
        sink_clauses.push(Clause::line(format!(
            "MODE {}",
            self.publishing_mode.to_canonical_nspl()
        )));
        if let Some(codec) = &self.encode_using_codec {
            sink_clauses.push(Clause::line(format!("ENCODE USING {}", codec.as_str())));
        }

        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_nspl(&self.from)?
        ))];
        clauses.extend(materialized_state_clauses(&self.materialized_state)?);
        let (sink_head, mut own_clauses) = emit_sink_clauses(&self.sink)?;
        own_clauses.append(&mut sink_clauses);
        clauses.push(Clause::group(format!("TO {sink_head}"), own_clauses));
        clauses.extend(route_construction_clauses(&self.construction)?);
        clauses.push(Clause::line(flush_policy.trim_start().to_string()));
        clauses.push(Clause::line(message_error_policy_to_nspl(
            &self.error_policies.message,
        )?));
        clauses.push(Clause::line(general_error_policy_to_nspl(
            &self.error_policies.general,
        )));

        Ok(clause_statement(
            format!(
                "CREATE {} EMITTER {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
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
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_nspl(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.extend(materialized_state_clauses(&self.materialized_state)?);
        clauses.extend(processor_outputs_clauses(&self.output_routes)?);

        Ok(clause_statement(
            format!(
                "CREATE {} REINGESTOR {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

impl CreateInferencer {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let version = self
            .resource_version
            .map(|version| format!(" VERSION {version}"))
            .unwrap_or_default();
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_nspl(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "USING RESOURCE {}{version}",
            self.resource.as_str()
        )));
        clauses.push(Clause::line(format!("FILE {}", string_literal(&self.file))));
        clauses.push(Clause::braced(
            "INPUTS",
            inference_mapping_items(&self.inputs)?,
        ));
        clauses.push(Clause::braced(
            "OUTPUT SCHEMA",
            inference_output_schema_items(&self.output_schema)?,
        ));
        clauses.extend(processor_tail_clauses(
            &self.branched_by,
            &self.materialized_state,
            &self.output_routes,
        )?);

        Ok(clause_statement(
            format!(
                "CREATE {} INFERENCER {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

impl CreateWasmProcessor {
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        let version = self
            .resource_version
            .map(|version| format!(" VERSION {version}"))
            .unwrap_or_default();
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_nspl(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "USING RESOURCE {}{version}",
            self.resource.as_str()
        )));
        clauses.push(Clause::line(format!("FILE {}", string_literal(&self.file))));
        clauses.push(Clause::line(format!("MAX FUEL {}", self.limits.max_fuel)));
        clauses.push(Clause::line(format!(
            "MAX MEMORY {}",
            byte_size_literal(self.limits.max_memory_bytes)
        )));
        clauses.extend(processor_tail_clauses(
            &self.branched_by,
            &self.materialized_state,
            &self.output_routes,
        )?);
        clauses.push(Clause::line(
            general_error_policy_to_nspl(&self.global_error_policy).replace("GENERAL", "GLOBAL"),
        ));

        Ok(clause_statement(
            format!(
                "CREATE {} WASM PROCESSOR {}",
                self.mode.as_ref(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

fn inference_mapping_items(
    mappings: &[InferencerTensorMapping],
) -> Result<Vec<String>, CanonicalNsplError> {
    mappings
        .iter()
        .map(|mapping| {
            Ok(format!(
                "{} {} TENSOR<{}>[{}] = {}",
                string_literal(&mapping.tensor),
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
        .collect()
}

fn inference_output_schema_items(
    declarations: &[InferencerTensorDeclaration],
) -> Result<Vec<String>, CanonicalNsplError> {
    declarations
        .iter()
        .map(|declaration| {
            Ok(format!(
                "{} {} TENSOR<{}>[{}]",
                string_literal(&declaration.tensor),
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
        .collect()
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
    let relays = inputs
        .from
        .iter()
        .map(|relay| from_relay_to_nspl(relay, &inputs.r#where))
        .collect::<Result<Vec<_>, _>>()
        .map(|items| items.join(", "))?;
    let collect = inputs
        .collect_policy
        .as_ref()
        .map(|policy| format!(" {}", collect_policy_to_nspl(policy)))
        .unwrap_or_default();
    Ok(format!("{relays}{collect}"))
}

fn prefixed_processor_inputs_to_nspl(
    prefix: &str,
    inputs: &ProcessorInputs,
) -> Result<String, CanonicalNsplError> {
    Ok(format!(
        "{prefix} FROM {}",
        processor_inputs_to_nspl(inputs)?
    ))
}

/// The optional `FILTER WHERE` clause of a processor.
fn filter_where_clause(
    filter_where: &Option<Expression>,
) -> Result<Option<Clause>, CanonicalNsplError> {
    filter_where
        .as_ref()
        .map(|condition| {
            Ok(Clause::line(format!(
                "FILTER WHERE {}",
                expression_to_nspl(condition)?
            )))
        })
        .transpose()
}

/// One clause per materialized-state dependency, in declaration order.
fn materialized_state_clauses(
    dependencies: &[MaterializedStateDependency],
) -> Result<Vec<Clause>, CanonicalNsplError> {
    dependencies
        .iter()
        .map(|dependency| {
            let policy = materialized_state_policy_to_nspl(&dependency.policy)?;
            Ok(Clause::line(format!(
                "USING MATERIALIZED STATE {} {policy}",
                dependency.relay.as_str()
            )))
        })
        .collect()
}

/// The clauses every processor writes after its own: branch, state, then routes.
fn processor_tail_clauses(
    branched_by: &BranchSelection,
    materialized_state: &[MaterializedStateDependency],
    outputs: &ProcessorOutputs,
) -> Result<Vec<Clause>, CanonicalNsplError> {
    let mut clauses = vec![Clause::line(branch_selection_to_nspl(branched_by))];
    clauses.extend(materialized_state_clauses(materialized_state)?);
    clauses.extend(processor_outputs_clauses(outputs)?);
    Ok(clauses)
}

/// The routes of a processor, each as a `TO <relay>` group.
fn processor_outputs_clauses(
    outputs: &ProcessorOutputs,
) -> Result<Vec<Clause>, CanonicalNsplError> {
    outputs.routes.iter().map(processor_output_clause).collect()
}

/// One route: the relay it targets, then the clauses that construct and emit its messages.
fn processor_output_clause(output: &crate::ProcessorOutput) -> Result<Clause, CanonicalNsplError> {
    let mut nested = route_construction_clauses(&output.construction)?;

    if let Some(branch) = &output.branch {
        nested.push(Clause::line(output_branch_to_nspl(branch)?));
    }
    if let Some(policy) = &output.flush_policy {
        nested.push(Clause::line(flush_policy_to_nspl_with_max(
            &policy.flush_each,
            policy.max_batch_size.as_deref(),
        )));
    }
    nested.push(Clause::line(message_error_policy_to_nspl(
        &output.message_error_policy,
    )?));

    Ok(Clause::group(
        format!("TO {}", output.relay.as_str()),
        nested,
    ))
}

fn processor_output_to_nspl(output: &crate::ProcessorOutput) -> Result<String, CanonicalNsplError> {
    let flush = output
        .flush_policy
        .as_ref()
        .map(|policy| {
            format!(
                " {}",
                flush_policy_to_nspl_with_max(&policy.flush_each, policy.max_batch_size.as_deref(),)
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
        "TO {}{}{}{} {}",
        output.relay.as_str(),
        construction,
        branch,
        flush,
        message_error_policy_to_nspl(&output.message_error_policy)?
    ))
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

fn alter_deduplicator_operation_to_nspl(
    operation: &AlterDeduplicatorOperation,
) -> Result<String, CanonicalNsplError> {
    match operation {
        AlterDeduplicatorOperation::Processor(operation) => {
            alter_processor_operation_to_nspl(operation)
        }
        AlterDeduplicatorOperation::SetDeduplicateOn { expressions } => Ok(format!(
            "SET DEDUPLICATE ON {}",
            expressions
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        AlterDeduplicatorOperation::SetMaxTime { max_time } => {
            Ok(format!("SET MAX TIME {max_time}"))
        }
    }
}

fn alter_reorderer_operation_to_nspl(
    operation: &AlterReordererOperation,
) -> Result<String, CanonicalNsplError> {
    match operation {
        AlterReordererOperation::Processor(operation) => {
            alter_processor_operation_to_nspl(operation)
        }
        AlterReordererOperation::SetOrderBy { expressions } => Ok(format!(
            "SET BY {}",
            expressions
                .iter()
                .map(expression_to_nspl)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        AlterReordererOperation::SetMaxTime { max_time } => Ok(format!("SET MAX TIME {max_time}")),
    }
}

fn alter_generator_operation_to_nspl(
    operation: &AlterGeneratorOperation,
) -> Result<String, CanonicalNsplError> {
    match operation {
        AlterGeneratorOperation::SetMaterializedState { relay } => {
            Ok(format!("SET MATERIALIZED STATE {}", relay.as_str()))
        }
        AlterGeneratorOperation::SetEach { each } => Ok(format!("SET EACH {each}")),
        AlterGeneratorOperation::SetBranching { branching } => {
            Ok(format!("SET {}", branch_selection_to_nspl(branching)))
        }
        AlterGeneratorOperation::AddRoute { route } => {
            Ok(format!("ADD ROUTE {}", processor_output_to_nspl(route)?))
        }
        AlterGeneratorOperation::DropRoute { relay } => {
            Ok(format!("DROP ROUTE TO {}", relay.as_str()))
        }
        AlterGeneratorOperation::ReplaceRoute { route } => Ok(format!(
            "REPLACE ROUTE {}",
            processor_output_to_nspl(route)?
        )),
    }
}

fn alter_processor_operation_to_nspl(
    operation: &AlterProcessorOperation,
) -> Result<String, CanonicalNsplError> {
    match operation {
        AlterProcessorOperation::AddFrom {
            relay,
            where_clause,
        } => Ok(format!(
            "ADD FROM {}{}",
            relay.as_str(),
            where_clause
                .as_ref()
                .map(|expression| Ok(format!(" WHERE {}", expression_to_nspl(expression)?)))
                .transpose()?
                .unwrap_or_default()
        )),
        AlterProcessorOperation::DropFrom { relay } => Ok(format!("DROP FROM {}", relay.as_str())),
        AlterProcessorOperation::AlterFromSetWhere {
            relay,
            where_clause,
        } => Ok(format!(
            "ALTER FROM {} SET WHERE {}",
            relay.as_str(),
            expression_to_nspl(where_clause)?
        )),
        AlterProcessorOperation::AlterFromDropWhere { relay } => {
            Ok(format!("ALTER FROM {} DROP WHERE", relay.as_str()))
        }
        AlterProcessorOperation::SetCollect { policy } => {
            Ok(format!("SET {}", collect_policy_to_nspl(policy)))
        }
        AlterProcessorOperation::DropCollect => Ok("DROP COLLECT".to_string()),
        AlterProcessorOperation::SetFilterWhere { where_clause } => Ok(format!(
            "SET FILTER WHERE {}",
            expression_to_nspl(where_clause)?
        )),
        AlterProcessorOperation::DropFilterWhere => Ok("DROP FILTER WHERE".to_string()),
        AlterProcessorOperation::SetMode { mode } => Ok(format!("SET {}", mode.as_ref())),
        AlterProcessorOperation::SetBranching { branching } => {
            Ok(format!("SET {}", branch_selection_to_nspl(branching)))
        }
        AlterProcessorOperation::AddMaterializedState { dependency } => Ok(format!(
            "ADD MATERIALIZED STATE {} {}",
            dependency.relay.as_str(),
            materialized_state_policy_to_nspl(&dependency.policy)?
        )),
        AlterProcessorOperation::DropMaterializedState { relay } => {
            Ok(format!("DROP MATERIALIZED STATE {}", relay.as_str()))
        }
        AlterProcessorOperation::AlterMaterializedState { relay, policy } => Ok(format!(
            "ALTER MATERIALIZED STATE {} SET {}",
            relay.as_str(),
            materialized_state_policy_to_nspl(policy)?
        )),
        AlterProcessorOperation::AddRoute { route } => {
            Ok(format!("ADD ROUTE {}", processor_output_to_nspl(route)?))
        }
        AlterProcessorOperation::DropRoute { relay } => {
            Ok(format!("DROP ROUTE TO {}", relay.as_str()))
        }
        AlterProcessorOperation::ReplaceRoute { route } => Ok(format!(
            "REPLACE ROUTE {}",
            processor_output_to_nspl(route)?
        )),
    }
}

fn alter_emitter_operation_to_nspl(
    operation: &AlterEmitterOperation,
) -> Result<String, CanonicalNsplError> {
    match operation {
        AlterEmitterOperation::AddFrom {
            relay,
            where_clause,
        } => {
            let where_clause = where_clause
                .as_ref()
                .map(expression_to_nspl)
                .transpose()?
                .map(|where_clause| format!(" WHERE {where_clause}"))
                .unwrap_or_default();
            Ok(format!("ADD FROM {}{where_clause}", relay.as_str()))
        }
        AlterEmitterOperation::DropFrom { relay } => Ok(format!("DROP FROM {}", relay.as_str())),
        AlterEmitterOperation::AlterFromSetWhere {
            relay,
            where_clause,
        } => Ok(format!(
            "ALTER FROM {} SET WHERE {}",
            relay.as_str(),
            expression_to_nspl(where_clause)?
        )),
        AlterEmitterOperation::AlterFromDropWhere { relay } => {
            Ok(format!("ALTER FROM {} DROP WHERE", relay.as_str()))
        }
        AlterEmitterOperation::SetSink {
            sink,
            publishing_mode,
        } => Ok(format!(
            "SET TO {}{} MODE {}",
            emit_sink_to_nspl(sink)?,
            sink.commit_policy()
                .map(|(policy, max_size)| format!(" {}", commit_policy_to_nspl(policy, max_size)))
                .unwrap_or_default(),
            publishing_mode.to_canonical_nspl()
        )),
        AlterEmitterOperation::SetClient { client } => {
            Ok(format!("SET CLIENT {}", client.as_str()))
        }
        AlterEmitterOperation::SetEncodeUsing { codec } => {
            Ok(format!("SET ENCODE USING {}", codec.as_str()))
        }
        AlterEmitterOperation::DropEncode => Ok("DROP ENCODE".to_string()),
        AlterEmitterOperation::SetCollect { policy } => {
            Ok(format!("SET {}", collect_policy_to_nspl(policy)))
        }
        AlterEmitterOperation::DropCollect => Ok("DROP COLLECT".to_string()),
        AlterEmitterOperation::SetAttachment { mode } => Ok(format!("SET {}", mode.as_ref())),
        AlterEmitterOperation::SetPublishingMode { mode } => {
            Ok(format!("SET MODE {}", mode.to_canonical_nspl()))
        }
        AlterEmitterOperation::SetFlush {
            flush_each,
            max_batch_size,
        } => Ok(format!(
            "SET {}",
            flush_policy_to_nspl_with_max(flush_each, max_batch_size.as_deref())
        )),
        AlterEmitterOperation::SetCommit {
            commit_each,
            max_commit_size,
        } => Ok(format!(
            "SET {}",
            commit_policy_to_nspl(commit_each, max_commit_size)
        )),
    }
}

fn alter_ingestor_operation_to_nspl(
    operation: &AlterIngestorOperation,
) -> Result<String, CanonicalNsplError> {
    match operation {
        AlterIngestorOperation::SetSource { source } => {
            Ok(format!("SET FROM {}", ingest_source_to_nspl(source)))
        }
        AlterIngestorOperation::SetQuiesce { quiesce } => {
            Ok(format!("SET QUIESCE {}", ingest_quiesce_to_nspl(quiesce)))
        }
        AlterIngestorOperation::SetDecodeUsing { codec } => {
            Ok(format!("SET DECODE USING {}", codec.as_str()))
        }
        AlterIngestorOperation::SetTimestamp { source } => Ok(match source {
            IngestTimestampSource::Now => "SET TIMESTAMP NOW".to_string(),
            IngestTimestampSource::At(field) => {
                format!("SET TIMESTAMP AT {}", field.as_str())
            }
        }),
        AlterIngestorOperation::DropTimestamp => Ok("DROP TIMESTAMP".to_string()),
        AlterIngestorOperation::SetFilterWhere { where_clause } => Ok(format!(
            "SET FILTER WHERE {}",
            expression_to_nspl(where_clause)?
        )),
        AlterIngestorOperation::DropFilterWhere => Ok("DROP FILTER WHERE".to_string()),
        AlterIngestorOperation::AddRoute { route } => {
            Ok(format!("ADD ROUTE {}", processor_output_to_nspl(route)?))
        }
        AlterIngestorOperation::DropRoute { relay } => {
            Ok(format!("DROP ROUTE TO {}", relay.as_str()))
        }
        AlterIngestorOperation::ReplaceRoute { route } => Ok(format!(
            "REPLACE ROUTE {}",
            processor_output_to_nspl(route)?
        )),
        AlterIngestorOperation::SetGeneralError { policy } => Ok(format!(
            "SET GENERAL ERROR {}",
            match policy {
                GeneralErrorPolicy::Ignore => "IGNORE",
                GeneralErrorPolicy::Log => "LOG",
            }
        )),
    }
}

fn alter_schema_operation_to_nspl(
    operation: &AlterSchemaOperation,
) -> Result<String, CanonicalNsplError> {
    match operation {
        AlterSchemaOperation::AddField { field } => {
            Ok(format!("ADD FIELD {}", schema_field_to_nspl(field)?))
        }
        AlterSchemaOperation::DropField { field } => Ok(format!("DROP FIELD {}", field.as_str())),
        AlterSchemaOperation::RenameField { field, to } => Ok(format!(
            "RENAME FIELD {} TO {}",
            field.as_str(),
            to.as_str()
        )),
        AlterSchemaOperation::SetFieldType { field, ty } => Ok(format!(
            "ALTER FIELD {} SET TYPE {}",
            field.as_str(),
            parse_as_to_keyword(ty)
        )),
        AlterSchemaOperation::SetFieldOptional { field, optional } => Ok(format!(
            "ALTER FIELD {} {} OPTIONAL",
            field.as_str(),
            if *optional { "SET" } else { "DROP" }
        )),
        AlterSchemaOperation::SetFieldSensitive { field, sensitive } => Ok(format!(
            "ALTER FIELD {} {} SENSITIVE",
            field.as_str(),
            if *sensitive { "SET" } else { "DROP" }
        )),
    }
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
        .collect::<Result<Vec<_>, CanonicalNsplError>>()?;

    Ok(block_statement(
        format!(
            "CREATE WIRE {format_kw} SCHEMA {} MODE {}",
            schema.name.as_str(),
            schema.strictness.as_ref()
        ),
        fields,
    ))
}

fn alter_wire_schema_to_nspl<T>(
    format_kw: &str,
    alter: &AlterWireSchema<T>,
) -> Result<String, CanonicalNsplError>
where
    T: NativeTypeToNspl,
{
    let operations = alter
        .operations
        .iter()
        .map(alter_wire_schema_operation_to_nspl::<T>)
        .collect::<Result<Vec<_>, CanonicalNsplError>>()?
        .join(", ");
    Ok(format!(
        "ALTER WIRE {format_kw} SCHEMA {} {operations};",
        alter.schema.as_str()
    ))
}

fn alter_wire_schema_operation_to_nspl<T>(
    operation: &AlterWireSchemaOperation<T>,
) -> Result<String, CanonicalNsplError>
where
    T: NativeTypeToNspl,
{
    match operation {
        AlterWireSchemaOperation::SetMode { mode } => Ok(format!("MODE {}", mode.as_ref())),
        AlterWireSchemaOperation::AddField { field } => {
            Ok(format!("ADD FIELD {}", wire_schema_field_to_nspl(field)?))
        }
        AlterWireSchemaOperation::DropField { field } => {
            Ok(format!("DROP FIELD {}", field.as_str()))
        }
        AlterWireSchemaOperation::RenameField { field, to } => Ok(format!(
            "RENAME FIELD {} TO {}",
            field.as_str(),
            to.as_str()
        )),
        AlterWireSchemaOperation::SetFieldType { field, ty } => Ok(format!(
            "ALTER FIELD {} SET TYPE {}",
            field.as_str(),
            ty.to_nspl_keyword()
        )),
        AlterWireSchemaOperation::SetFieldOptional { field, optional } => Ok(format!(
            "ALTER FIELD {} {} OPTIONAL",
            field.as_str(),
            if *optional { "SET" } else { "DROP" }
        )),
    }
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
    let key = string_literal(&entry.key);
    let value = string_literal(&entry.value);
    Ok(format!("{key} = {value}"))
}

fn http_entry_to_nspl(entry: &HttpConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn sentry_entry_to_nspl(entry: &SentryConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn otel_entry_to_nspl(entry: &OtelConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

fn pulsar_entry_to_nspl(entry: &PulsarConfigEntry) -> Result<String, CanonicalNsplError> {
    kafka_entry_to_nspl(entry)
}

pub fn ingest_quiesce_to_nspl(quiesce: &crate::IngestQuiesceMode) -> String {
    match quiesce {
        crate::IngestQuiesceMode::Suspend => "SUSPEND".to_string(),
        crate::IngestQuiesceMode::Buffer { max_size, overflow } => format!(
            "BUFFER MAX SIZE {} ON OVERFLOW {}",
            max_size,
            match overflow {
                crate::IngestQuiesceOverflow::DropOldest => "DROP OLDEST",
                crate::IngestQuiesceOverflow::DropNewest => "DROP NEWEST",
            }
        ),
        crate::IngestQuiesceMode::Drop => "DROP".to_string(),
        crate::IngestQuiesceMode::Reject { retry_after } => {
            format!("REJECT RETRY AFTER {retry_after}")
        }
        crate::IngestQuiesceMode::EndpointBuffer { max_size } => {
            format!("BUFFER MAX SIZE {max_size}")
        }
    }
}

fn ingest_source_to_nspl(source: &IngestSource) -> String {
    match source {
        IngestSource::Http {
            client,
            every,
            quiesce,
        } => format!(
            "HTTP {} EVERY {} ON QUIESCE {}",
            client.as_str(),
            every,
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::Kafka {
            client,
            topic,
            offset_mode,
            instances,
            mode,
            quiesce,
        } => format!(
            "KAFKA {} TOPIC {} OFFSET BY {}{} MODE {} ON QUIESCE {}",
            client.as_str(),
            topic.as_str(),
            kafka_offset_mode_to_nspl(offset_mode),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            kafka_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::Pulsar {
            client,
            topic,
            subscription,
            instances,
            mode,
            quiesce,
        } => format!(
            "PULSAR {} TOPIC {} SUBSCRIPTION {}{} MODE {} ON QUIESCE {}",
            client.as_str(),
            topic.as_str(),
            subscription.as_str(),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            pulsar_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::Mqtt {
            client,
            topic,
            instances,
            mode,
            quiesce,
        } => {
            let instances = if *instances > 1 {
                format!(" INSTANCES {instances}")
            } else {
                String::new()
            };
            format!(
                "MQTT {} TOPIC {}{} {} ON QUIESCE {}",
                client.as_str(),
                mqtt_topic_to_nspl(topic),
                instances,
                mqtt_mode_to_nspl(mode),
                ingest_quiesce_to_nspl(quiesce)
            )
        }
        IngestSource::Nats {
            client,
            subject,
            queue_group,
            instances,
            mode,
            quiesce,
        } => format!(
            "NATS {} SUBJECT {} QUEUE GROUP {} INSTANCES {} MODE {} ON QUIESCE {}",
            client.as_str(),
            subject.as_str(),
            queue_group.as_str(),
            instances,
            nats_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::RabbitMq {
            client,
            queue,
            instances,
            mode,
            quiesce,
        } => format!(
            "RABBITMQ {} QUEUE {}{} MODE {} ON QUIESCE {}",
            client.as_str(),
            queue.as_str(),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            rabbitmq_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::RedisPubSub {
            client,
            channel,
            mode,
            quiesce,
        } => format!(
            "REDIS PUBSUB {} CHANNEL {} MODE {} ON QUIESCE {}",
            client.as_str(),
            channel.as_str(),
            redis_pubsub_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::Prometheus {
            client,
            query,
            every,
            quiesce,
        } => format!(
            "PROMETHEUS {} QUERY {} EVERY {} ON QUIESCE {}",
            client.as_str(),
            string_literal(query),
            every,
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::ZeroMq {
            client,
            mode,
            quiesce,
        } => format!(
            "ZEROMQ {} MODE {} ON QUIESCE {}",
            client.as_str(),
            zeromq_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::Sqs {
            client,
            queue,
            instances,
            mode,
            quiesce,
        } => format!(
            "SQS {} QUEUE {}{} MODE {} ON QUIESCE {}",
            client.as_str(),
            queue.as_str(),
            if *instances > 1 {
                format!(" INSTANCES {}", instances)
            } else {
                String::new()
            },
            sqs_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::Endpoint {
            endpoint,
            mode,
            quiesce,
        } => format!(
            "ENDPOINT {} MODE {} ON QUIESCE {}",
            endpoint.as_str(),
            endpoint_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::Websockets {
            client,
            mode,
            quiesce,
        } => format!(
            "WEBSOCKETS {} MODE {} ON QUIESCE {}",
            client.as_str(),
            websockets_mode_to_nspl(mode),
            ingest_quiesce_to_nspl(quiesce)
        ),
        IngestSource::Syslog { client, quiesce } => format!(
            "SYSLOG {} MODE NO_ACK SEQUENTIAL ON QUIESCE {}",
            client.as_str(),
            ingest_quiesce_to_nspl(quiesce)
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
        KafkaIngestMode::NoAckParallel => "NO_ACK PARALLEL".to_string(),
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
        MqttIngestMode::NoAckParallel { session, qos } => {
            format!(
                "{}MODE NO_ACK PARALLEL",
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

fn mqtt_topic_to_nspl(topic: &str) -> String {
    if Identifier::parse(topic).is_ok() {
        topic.to_string()
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

fn emitter_ack_window_to_nspl(window: &EmitterAckWindow) -> String {
    match window {
        EmitterAckWindow::Sequential => "SEQUENTIAL".to_string(),
        EmitterAckWindow::Parallel { max } => format!("PARALLEL MAX {max}"),
    }
}

impl EmitterPublishingMode {
    pub fn to_canonical_nspl(&self) -> String {
        let retry = |policy: &RetryPolicy| format!("RETRY POLICY {}", retry_policy_to_nspl(policy));
        let confirmed =
            |prefix: &str, window: &EmitterAckWindow, ack_timeout: &str, policy: &RetryPolicy| {
                format!(
                    "{prefix} {} ACK TIMEOUT {ack_timeout} {}",
                    emitter_ack_window_to_nspl(window),
                    retry(policy)
                )
            };
        match self {
            EmitterPublishingMode::NoAck { retry_policy } => {
                format!("NO_ACK {}", retry(retry_policy))
            }
            EmitterPublishingMode::BrokerAck {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("ACK", window, ack_timeout, retry_policy),
            EmitterPublishingMode::MqttQos0 { retry_policy } => {
                format!("QOS 0 {}", retry(retry_policy))
            }
            EmitterPublishingMode::MqttQos1 {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("QOS 1 ACK", window, ack_timeout, retry_policy),
            EmitterPublishingMode::MqttQos2 {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("QOS 2 ACK", window, ack_timeout, retry_policy),
            EmitterPublishingMode::NatsJetStream {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("JETSTREAM ACK", window, ack_timeout, retry_policy),
            EmitterPublishingMode::SqsSingle { retry_policy } => {
                format!("SINGLE {}", retry(retry_policy))
            }
            EmitterPublishingMode::SqsBatch { retry_policy } => {
                format!("BATCH {}", retry(retry_policy))
            }
            EmitterPublishingMode::RequestAck { retry_policy } => {
                format!("ACK {}", retry(retry_policy))
            }
        }
    }
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

/// Splits a sink into the text that names it and the clauses that configure it.
///
/// Only sinks carrying a `VALUES` map have clauses of their own; the rest name themselves fully on
/// one line. Splitting here is what keeps a wide column mapping from becoming one enormous line.
fn emit_sink_clauses(sink: &EmitSink) -> Result<(String, Vec<Clause>), CanonicalNsplError> {
    let (head, values, mut trailing) = match sink {
        EmitSink::ClickHouse {
            client,
            table,
            values,
            max_batch,
            ..
        } => (
            format!(
                "CLICKHOUSE {} INSERT TO TABLE {}",
                client.as_str(),
                table.as_str()
            ),
            values,
            vec![Clause::line(format!("WITH MAX BATCH {max_batch}"))],
        ),
        EmitSink::Postgres {
            client,
            table,
            values,
            conflict_action,
            max_batch,
            ..
        } => (
            format!(
                "POSTGRES {} INSERT TO TABLE {}",
                client.as_str(),
                table.as_str()
            ),
            values,
            conflict_and_batch_clauses(
                postgres_conflict_action_to_nspl(conflict_action),
                *max_batch,
            ),
        ),
        EmitSink::MySql {
            client,
            table,
            values,
            conflict_action,
            max_batch,
            ..
        } => (
            format!(
                "MYSQL {} INSERT TO TABLE {}",
                client.as_str(),
                table.as_str()
            ),
            values,
            conflict_and_batch_clauses(mysql_conflict_action_to_nspl(conflict_action), *max_batch),
        ),
        EmitSink::MongoDb {
            client,
            collection,
            values,
            conflict_action,
            max_batch,
            ..
        } => (
            format!(
                "MONGODB {} INSERT TO COLLECTION {}",
                client.as_str(),
                collection.as_str()
            ),
            values,
            conflict_and_batch_clauses(
                mongodb_conflict_action_to_nspl(conflict_action),
                *max_batch,
            ),
        ),
        EmitSink::Iceberg {
            backend,
            client,
            table,
            values,
            location,
            catalog,
            ..
        } => (
            format!(
                "ICEBERG ON {} {} TABLE {}",
                backend.as_ref(),
                client.as_str(),
                table.as_str()
            ),
            values,
            vec![
                Clause::line(format!("LOCATION {}", string_literal(location))),
                Clause::line(match catalog {
                    IcebergCatalog::Rest { client } => format!("CATALOG {}", client.as_str()),
                }),
            ],
        ),
        other => return Ok((emit_sink_to_nspl(other)?, Vec::new())),
    };

    let mut clauses = vec![Clause::braced("VALUES", value_mapping_items(values)?)];
    clauses.append(&mut trailing);
    Ok((head, clauses))
}

/// The `ON CONFLICT` and `WITH MAX BATCH` clauses the row-insert sinks share.
fn conflict_and_batch_clauses(conflict_action: String, max_batch: u64) -> Vec<Clause> {
    let mut clauses = Vec::new();
    if !conflict_action.trim().is_empty() {
        clauses.push(Clause::line(conflict_action.trim().to_string()));
    }
    clauses.push(Clause::line(format!("WITH MAX BATCH {max_batch}")));
    clauses
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
        EmitSink::Sqs {
            client,
            queue,
            fifo_group,
        } => {
            let queue = if Identifier::try_from(queue.as_str()).is_ok() {
                queue.clone()
            } else {
                string_literal(queue)
            };
            let fifo_group = fifo_group
                .as_ref()
                .map(|group| match group {
                    SqsFifoGroup::FromBranch => Ok(" FIFO GROUP FROM BRANCH".to_string()),
                    SqsFifoGroup::Expression(expression) => {
                        Ok(format!(" FIFO GROUP {}", expression_to_nspl(expression)?))
                    }
                })
                .transpose()?
                .unwrap_or_default();
            Ok(format!(
                "SQS {} QUEUE {}{}",
                client.as_str(),
                queue,
                fifo_group
            ))
        }
        EmitSink::Sentry { client } => Ok(format!("SENTRY {}", client.as_str())),
        EmitSink::Syslog { client } => Ok(format!("SYSLOG {}", client.as_str())),
        EmitSink::Otel {
            client,
            signal,
            values,
            attributes,
            resource,
            scope,
        } => {
            let signal = match signal {
                OtelSignal::Logs => "LOGS".to_string(),
                OtelSignal::Traces => "TRACES".to_string(),
                OtelSignal::Metric(metric) => {
                    let description = metric
                        .description
                        .as_ref()
                        .map(|description| {
                            Ok(format!(" DESCRIPTION {}", string_literal(description)))
                        })
                        .transpose()?
                        .unwrap_or_default();
                    let kind = match &metric.kind {
                        OtelMetricKind::Gauge => "GAUGE".to_string(),
                        OtelMetricKind::Sum {
                            monotonic,
                            temporality,
                        } => format!(
                            "SUM{} {}",
                            if *monotonic { " MONOTONIC" } else { "" },
                            temporality.as_ref()
                        ),
                        OtelMetricKind::Histogram { temporality } => {
                            format!("HISTOGRAM {}", temporality.as_ref())
                        }
                    };
                    format!(
                        "METRIC {} UNIT {}{} {}",
                        string_literal(&metric.name),
                        string_literal(&metric.unit),
                        description,
                        kind
                    )
                }
            };
            let attributes = if attributes.is_empty() {
                String::new()
            } else {
                format!(" ATTRIBUTES {{{}}}", value_mappings_to_nspl(attributes)?)
            };
            let resource = if resource.is_empty() {
                String::new()
            } else {
                format!(" RESOURCE {{{}}}", value_mappings_to_nspl(resource)?)
            };
            let scope = scope
                .as_ref()
                .map(|scope| {
                    let version = scope
                        .version
                        .as_ref()
                        .map(|version| Ok(format!(" VERSION {}", string_literal(version))))
                        .transpose()?
                        .unwrap_or_default();
                    Ok(format!(" SCOPE {}{version}", string_literal(&scope.name)))
                })
                .transpose()?
                .unwrap_or_default();
            Ok(format!(
                "OTEL {} {} VALUES {{{}}}{}{}{}",
                client.as_str(),
                signal,
                value_mappings_to_nspl(values)?,
                attributes,
                resource,
                scope
            ))
        }
        EmitSink::ClickHouse {
            client,
            table,
            values,
            max_batch,
            ..
        } => {
            let mappings = value_mappings_to_nspl(values)?;
            Ok(format!(
                "CLICKHOUSE {} INSERT TO TABLE {} VALUES {{{}}} WITH MAX BATCH {}",
                client.as_str(),
                table.as_str(),
                mappings,
                max_batch
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
                string_literal(location),
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
            .map(|column| string_literal(column))
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

/// Renders an `F64` literal so that it lexes back as a float.
///
/// `f64`'s `Display` drops a zero fraction, so `80.0` would otherwise render as `80` and reparse as
/// an `I64`. NSPL decides float-versus-integer purely on the presence of a decimal point.
fn float_literal(value: f64) -> Result<String, CanonicalNsplError> {
    if !value.is_finite() {
        return Err(CanonicalNsplError::UnrepresentableFloat {
            value: value.to_string(),
        });
    }

    let rendered = value.to_string();
    if rendered.contains(['.', 'e', 'E']) {
        Ok(rendered)
    } else {
        Ok(format!("{rendered}.0"))
    }
}

/// Renders a stored byte count using the largest binary prefix that divides it exactly.
///
/// Byte sizes elsewhere in NSPL keep the author's spelling, but WASM memory limits are stored as a
/// count, so `64MiB` would otherwise come back as `67108864B`. Only exact multiples take a prefix,
/// which keeps the rendering total, deterministic, and lossless.
fn byte_size_literal(bytes: u64) -> String {
    const UNITS: [(u64, &str); 4] = [
        (1 << 40, "TiB"),
        (1 << 30, "GiB"),
        (1 << 20, "MiB"),
        (1 << 10, "KiB"),
    ];

    for (scale, suffix) in UNITS {
        if bytes >= scale && bytes.is_multiple_of(scale) {
            return format!("{}{suffix}", bytes / scale);
        }
    }

    format!("{bytes}B")
}

/// Wraps `value` in a dollar-quoted delimiter that does not occur inside it.
///
/// NSPL dollar-quoting is verbatim: the body needs no escaping, so this represents any string,
/// including one spanning several lines or mixing quote styles. The tag escalates until it is
/// absent from the body.
fn dollar_quote(value: &str, tag: &str) -> String {
    let mut delimiter = format!("${tag}$");
    let mut suffix = 1_u64;
    while value.contains(&delimiter) {
        delimiter = format!("${tag}_{suffix}$");
        suffix += 1;
    }

    format!("{delimiter}{value}{delimiter}")
}

/// Renders `value` as an NSPL string literal.
///
/// Single quotes are preferred, double quotes carry an embedded apostrophe, and anything the quoted
/// forms cannot express verbatim -- a newline, or both quote styles at once -- falls back to
/// dollar-quoting. Every string is therefore representable.
fn string_literal(value: &str) -> String {
    let has_single = value.contains('\'');
    let has_double = value.contains('"');
    let has_newline = value.contains('\n') || value.contains('\r');

    if has_newline || (has_single && has_double) {
        dollar_quote(value, "s")
    } else if has_single {
        format!("\"{value}\"")
    } else {
        format!("'{value}'")
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
        CreateClientHttp, CreateClientKafka, CreateClientMqtt, CreateClientNats,
        CreateClientPrometheus, CreateClientRabbitMq, CreateClientRedis, CreateClientSentry,
        CreateClientSqs, CreateClientSyslog, CreateClientWebsockets, CreateClientZeroMq,
        CreateCodec, CreateCorrelator, CreateDeduplicator, CreateEmitter, CreateEndpoint,
        CreateIngestor, CreateJunction, CreatePlacement, CreateReingestor, CreateRelay,
        CreateSchema, CreateSignalingProtocol, CreateUdf, CreateVhost, CreateWindowProcessor,
        CreateWireSchema, EmitSink, EmitterPublishingMode, EndpointIngestMode, EndpointType,
        ErrorPolicies, Expression, FieldScope, GeneralErrorPolicy, HttpConfigEntry, Identifier,
        IngestSource, JsonType, KafkaConfigEntry, KafkaIngestMode, KafkaOffsetMode, Literal,
        MessageErrorPolicy, Model, MongoDbConflictAction, MongoDbValueMapping, MqttIngestMode,
        MqttQos, MqttSession, MySqlConflictAction, MySqlValueMapping, NatsIngestMode, OutputBranch,
        ParseAsType, PlacementPolicy, PostgresConflictAction, PostgresValueMapping,
        ProcessorInputs, ProcessorOutput, ProcessorOutputs, PrometheusConfigEntry,
        RabbitMqIngestMode, RedisPubSubIngestMode, RelayBranching, RetryPolicy, RouteConstruction,
        SchemaField, SentryConfigEntry, SignalingProtobufConfig, SignalingStep, SignalingWaitStep,
        SignalingWireFormat, SqsIngestMode, UdfArgument, UdfLanguage, UdfReturn,
        WebsocketsIngestMode, WindowBound, WireSchemaDefinition, WireSchemaField, ZeroMqIngestMode,
        expression_to_nspl,
    };

    fn identifier(raw: &str) -> Identifier {
        Identifier::try_from(raw).expect("valid identifier")
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy {
            backoff: "250ms".to_string(),
            max_backoff: "30s".to_string(),
        }
    }

    fn request_ack_mode() -> EmitterPublishingMode {
        EmitterPublishingMode::RequestAck {
            retry_policy: retry_policy(),
        }
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
        let schema = WireSchemaDefinition::Avro(CreateWireSchema {
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
            "CREATE WIRE AVRO SCHEMA latency MODE STRICT (\n  p99 DOUBLE,\n  created_at STRING\n);"
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
            "CREATE SCHEMA latency (\n  p99 F64,\n  created_at DATETIME\n);"
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
            "CREATE SCHEMA tensors (\n  matrix ARRAY<F32, 2, 3>\n);"
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
            "CREATE CLIENT kafka_main\n  TYPE KAFKA\n  CONFIG {\n    'bootstrap.servers' = \
             'host1:9092',\n    'enable.auto.commit' = 'true'\n  };"
        );
    }

    #[test]
    fn renders_config_values_that_mix_quote_styles() {
        let model = CreateClientKafka {
            name: identifier("k"),
            mount: None,
            config: vec![KafkaConfigEntry {
                key: "quoted".to_string(),
                value: "both ' and \"".to_string(),
            }],
        };

        assert_eq!(
            model.to_canonical_nspl().expect("must render"),
            "CREATE CLIENT k\n  TYPE KAFKA\n  CONFIG {\n    'quoted' = $s$both ' and \"$s$\n  };"
        );
    }

    #[test]
    fn canonical_error_display_includes_original_value() {
        let err = super::CanonicalNsplError::UnrepresentableFloat {
            value: "inf".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "cannot represent non-finite float in NSPL: inf"
        );
    }

    #[test]
    fn renders_json_wire_schema_canonical() {
        let schema = WireSchemaDefinition::Json(CreateWireSchema {
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
            "CREATE WIRE JSON SCHEMA payload MODE STRICT (\n  items ARRAY,\n  active BOOLEAN\n);"
        );
    }

    #[test]
    fn renders_loose_cbor_wire_schema_canonical() {
        let schema = WireSchemaDefinition::Cbor(CreateWireSchema {
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
            "CREATE WIRE CBOR SCHEMA payload MODE LOOSE (\n  active BOOLEAN\n);"
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
        let wire = WireSchemaDefinition::Json(CreateWireSchema {
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
            "CREATE SCHEMA latency (\n  p99 F64 OPTIONAL\n);"
        );
        assert_eq!(
            wire.to_canonical_nspl().expect("must render"),
            "CREATE WIRE JSON SCHEMA payload MODE STRICT (\n  active BOOLEAN OPTIONAL\n);"
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
                "CREATE CLIENT http_main\n  TYPE HTTP\n  CONFIG {\n    'base_url' = 'https://example.com'\n  };",
            ),
            (
                CreateClientSentry {
                    name: identifier("sentry_main"),
                    mount: None,
                    config: vec![SentryConfigEntry {
                        key: "dsn".to_string(),
                        value: "https://key@sentry.example/42".to_string(),
                    }],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT sentry_main\n  TYPE SENTRY\n  CONFIG {\n    'dsn' = 'https://key@sentry.example/42'\n  };",
            ),
            (
                CreateClientMqtt {
                    name: identifier("mqtt_main"),
                    mount: None,
                    config: vec![config_entry("host", "mqtt.internal")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT mqtt_main\n  TYPE MQTT\n  CONFIG {\n    'host' = 'mqtt.internal'\n  };",
            ),
            (
                CreateClientNats {
                    name: identifier("nats_main"),
                    mount: None,
                    config: vec![config_entry("servers", "nats://localhost:4222")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT nats_main\n  TYPE NATS\n  CONFIG {\n    'servers' = 'nats://localhost:4222'\n  };",
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
                "CREATE CLIENT prom_main\n  TYPE PROMETHEUS\n  CONFIG {\n    'url' = 'http://prometheus:9090'\n  };",
            ),
            (
                CreateClientRabbitMq {
                    name: identifier("rmq_main"),
                    mount: None,
                    config: vec![config_entry("uri", "amqp://guest:guest@localhost:5672")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT rmq_main\n  TYPE RABBITMQ\n  CONFIG {\n    'uri' = 'amqp://guest:guest@localhost:5672'\n  };",
            ),
            (
                CreateClientRedis {
                    name: identifier("redis_main"),
                    mount: None,
                    config: vec![config_entry("url", "redis://localhost:6379")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT redis_main\n  TYPE REDIS\n  CONFIG {\n    'url' = 'redis://localhost:6379'\n  };",
            ),
            (
                CreateClientZeroMq {
                    name: identifier("zmq_main"),
                    mount: None,
                    config: vec![config_entry("bind", "tcp://*:5555")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT zmq_main\n  TYPE ZEROMQ\n  CONFIG {\n    'bind' = 'tcp://*:5555'\n  };",
            ),
            (
                CreateClientSyslog {
                    name: identifier("syslog_main"),
                    mount: Some(identifier("syslog_tls")),
                    config: vec![
                        config_entry("protocol", "tls"),
                        config_entry("addr", "logs.example.com:6514"),
                        config_entry("tls_ca_file", "{{ syslog_tls }}/ca.pem"),
                    ],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT syslog_main\n  TYPE SYSLOG MOUNT syslog_tls\n  CONFIG {\n    'protocol' = 'tls',\n    'addr' = 'logs.example.com:6514',\n    'tls_ca_file' = '{{ syslog_tls }}/ca.pem'\n  };",
            ),
            (
                CreateClientSqs {
                    name: identifier("sqs_main"),
                    mount: None,
                    config: vec![config_entry("region", "us-east-1")],
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE CLIENT sqs_main\n  TYPE SQS\n  CONFIG {\n    'region' = 'us-east-1'\n  };",
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
                "CREATE CLIENT ws_main\n  TYPE WEBSOCKETS\n  CONFIG {\n    'url' = 'wss://example.com/socket'\n  };",
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
                "CREATE CLIENT ws_main\n  TYPE WEBSOCKETS WITH SIGNALING PROTOCOL binance_ws\n  CONFIG {\n    'url' = 'wss://example.com/socket'\n  };",
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
            format: SignalingWireFormat::Json,
            on_connect: crate::SignalingProtocolOnConnect {
                accept_data: false,
                steps: vec![
                    SignalingStep::Send(vec![r#"{method: "SUBSCRIBE", id: 1}"#.to_string()]),
                    SignalingStep::Wait(SignalingWaitStep::new(vec![
                        ".id == 1 and .result == null".to_string(),
                    ])),
                ],
                fail_matchers: Vec::new(),
                timeout: "5s".to_string(),
            },
        };
        assert_eq!(
            signaling_protocol.to_canonical_nspl().expect("must render"),
            r#"CREATE SIGNALING PROTOCOL binance_ws FORMAT JSON ON CONNECT SEND JAQ '{method: "SUBSCRIBE", id: 1}' WAIT JAQ '.id == 1 and .result == null' TIMEOUT 5s;"#
        );

        let protobuf_signaling_protocol = CreateSignalingProtocol {
            name: identifier("orders_ws"),
            format: SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                resource: identifier("proto_bundle"),
                resource_version: Some(2),
                config: vec![crate::ClientConfigEntry {
                    key: "file".to_string(),
                    value: "signaling.proto".to_string(),
                }],
                send_message: "nervix.test.Subscribe".to_string(),
                wait_message: "nervix.test.Ack".to_string(),
            }),
            on_connect: crate::SignalingProtocolOnConnect {
                accept_data: false,
                steps: vec![
                    SignalingStep::Send(vec!["{id: 1}".to_string()]),
                    SignalingStep::Wait(SignalingWaitStep {
                        matchers: vec![".authed".to_string()],
                        capture: Some("{token: .token}".to_string()),
                        fail_matchers: vec![".denied".to_string()],
                        accept_data: true,
                    }),
                    SignalingStep::Send(vec!["{id: 2, token: $state.token}".to_string()]),
                    SignalingStep::Wait(SignalingWaitStep::new(vec![".id == 2".to_string()])),
                ],
                fail_matchers: vec![".error".to_string()],
                timeout: "5s".to_string(),
            },
        };
        assert_eq!(
            protobuf_signaling_protocol
                .to_canonical_nspl()
                .expect("must render"),
            "CREATE SIGNALING PROTOCOL orders_ws FORMAT PROTOBUF USING RESOURCE proto_bundle \
             VERSION 2 CONFIG {'file' = 'signaling.proto'} SEND MESSAGE 'nervix.test.Subscribe' \
             WAIT MESSAGE 'nervix.test.Ack' FAIL JAQ '.error' ON CONNECT SEND JAQ '{id: 1}' WAIT \
             JAQ '.authed' FAIL JAQ '.denied' CAPTURE '{token: .token}' ACCEPT DATA SEND JAQ \
             '{id: 2, token: $state.token}' WAIT JAQ '.id == 2' TIMEOUT 5s;"
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
            "CREATE CODEC orders_codec\n  FROM WIRE JSON SCHEMA orders_wire\n  TO SCHEMA orders;"
        );

        let syslog_codec = CreateCodec {
            name: identifier("syslog_codec"),
            wire_format: CodecWireFormat::Syslog,
            wire_schema: None,
            schema: identifier("syslog_event"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            syslog_codec.to_canonical_nspl().expect("must render"),
            "CREATE CODEC syslog_codec\n  FROM SYSLOG\n  TO SCHEMA syslog_event;"
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
            "CREATE CODEC orders_codec\n  FROM WIRE JSON SCHEMA orders_wire\n  TO SCHEMA orders\n  ENCODE created_at AS RFC3339;"
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
            "CREATE CODEC orders_codec\n  FROM JSON\n  TO SCHEMA orders\n  WITH JAQ \
             TRANSFORMATIONS ON INGESTION '.payload' ON EMITTING '{payload: .}';"
        );

        let ingestion_codec = CreateCodec {
            name: identifier("orders_ingestion"),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: None,
                },
            },
            wire_schema: None,
            schema: identifier("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            ingestion_codec.to_canonical_nspl().expect("must render"),
            "CREATE CODEC orders_ingestion\n  FROM JSON\n  TO SCHEMA orders\n  WITH JAQ \
             TRANSFORMATIONS ON INGESTION '.payload';"
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
            "CREATE CODEC orders_cbor\n  FROM CBOR\n  TO SCHEMA orders\n  WITH JAQ \
             TRANSFORMATIONS ON INGESTION '.' ON EMITTING '.';"
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
            "CREATE CODEC orders_proto\n  FROM PROTOBUF USING RESOURCE proto_bundle VERSION 3\n  \
             CONFIG {\n    'file' = 'order.proto'\n  }\n  MESSAGE 'nervix.test.Order'\n  TO \
             SCHEMA orders\n  WITH JAQ TRANSFORMATIONS ON INGESTION '.payload' ON EMITTING \
             '{payload: .}';"
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
            )
            .with_collect_policy("25ms".to_string(), Some("2MiB".to_string())),
            output_routes: flushed_outputs("orders_all"),
            branched_by: processor_branched_by("tenant_branch"),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            junction.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED JUNCTION orders_junction\n  FROM orders_a, orders_b COLLECT FOR 25ms \
             MAX BATCH SIZE 2MiB\n  BRANCHED BY by_tenant_branch\n  TO orders_all\n    FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
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
            "CREATE DETACHED DEDUPLICATOR orders_dedup\n  FROM orders_in\n  DEDUPLICATE ON \
             input.transaction_id\n  MAX TIME 10m\n  BRANCHED BY by_tenant_branch\n  TO \
             orders_out\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
        );

        let correlator = CreateCorrelator {
            name: identifier("orders_correlator"),
            left: ProcessorInputs::new(
                vec![identifier("orders_left"), identifier("orders_left_archive")],
                Vec::new(),
            )
            .with_collect_policy("10ms".to_string(), None),
            right: ProcessorInputs::single(identifier("orders_right"))
                .with_collect_policy("20ms".to_string(), Some("1MiB".to_string())),
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
            "CREATE ATTACHED CORRELATOR orders_correlator\n  LEFT FROM orders_left, \
             orders_left_archive COLLECT FOR 10ms\n  RIGHT FROM orders_right COLLECT FOR 20ms MAX \
             BATCH SIZE 1MiB\n  CORRELATE WHERE left.id = right.id\n  MATCH EARLIEST\n  MAX TIME \
             5s\n  ON CORRELATION TIMEOUT DROP, DROP\n  BRANCHED BY by_tenant_branch\n  TO \
             orders_matched\n    SET id = left.id\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    \
             ON MESSAGE ERROR LOG;"
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
            "CREATE ATTACHED WINDOW PROCESSOR latency_window\n  FROM orders_in\n  WIDTH 100 \
             MESSAGES 10s DURATION\n  STEP 10 MESSAGES 1s DURATION\n  BRANCHED BY \
             by_tenant_branch\n  TO orders_p99\n    SET latency_p99 = \
             percentile_linear_histogram(input.latency, 99, 2048, 0, 10000, '2s')\n    ON MESSAGE \
             ERROR LOG;"
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
            "CREATE ATTACHED REINGESTOR orders_repartition\n  FROM orders_in\n  TO orders_out\n    UNBRANCHED\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
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
            "CREATE DETACHED REINGESTOR orders_splitter\n  FROM orders_in\n  FILTER WHERE \
             active\n  TO orders_errors\n    WHERE level = 'error'\n    UNBRANCHED\n    FLUSH \
             EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  TO orders_warn\n    SET \
             severity = 'warning'\n    WHERE level = 'warn'\n    UNBRANCHED\n    FLUSH EACH 100ms \
             MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  TO orders_info\n    UNBRANCHED\n    \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
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
                    queue: "orders_queue".to_string(),
                    fifo_group: None,
                },
                "SQS sqs_main QUEUE orders_queue",
            ),
            (
                EmitSink::Sentry {
                    client: identifier("sentry_main"),
                },
                "SENTRY sentry_main",
            ),
            (
                EmitSink::Syslog {
                    client: identifier("syslog_main"),
                },
                "SYSLOG syslog_main",
            ),
        ];

        for (sink, rendered_sink) in sinks {
            let publishing_mode = match &sink {
                EmitSink::Mqtt { .. } => EmitterPublishingMode::MqttQos0 {
                    retry_policy: retry_policy(),
                },
                EmitSink::Sqs { .. } => EmitterPublishingMode::SqsSingle {
                    retry_policy: retry_policy(),
                },
                EmitSink::Sentry { .. } => request_ack_mode(),
                _ => EmitterPublishingMode::NoAck {
                    retry_policy: retry_policy(),
                },
            };
            let rendered_mode = publishing_mode.to_canonical_nspl();
            let emitter = CreateEmitter {
                name: identifier("emit_orders"),
                from: ProcessorInputs::single(identifier("orders_stream"))
                    .with_collect_policy("50ms".to_string(), Some("4MiB".to_string())),
                encode_using_codec: Some(identifier("orders_codec")),
                sink: Box::new(sink),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                publishing_mode,
                mode: AckMode::Attached,
                error_policies: ErrorPolicies::handled_by_log(),

                construction: RouteConstruction::default(),
                materialized_state: Vec::new(),
            };
            assert_eq!(
                emitter.to_canonical_nspl().expect("must render"),
                format!(
                    "CREATE ATTACHED EMITTER emit_orders\n  FROM orders_stream COLLECT FOR 50ms \
                     MAX BATCH SIZE 4MiB\n  TO {rendered_sink}\n    MODE {rendered_mode}\n    \
                     ENCODE USING orders_codec\n  FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n  ON \
                     MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;"
                )
            );
        }
    }

    #[test]
    fn renders_postgres_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: identifier("emit_notifications"),
            from: ProcessorInputs::single(identifier("notifications")),
            encode_using_codec: None,
            sink: Box::new(EmitSink::Postgres {
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
            }),
            flush_each: "10s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            publishing_mode: request_ack_mode(),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED EMITTER emit_notifications\n  FROM notifications\n  TO POSTGRES postgres_main INSERT TO TABLE notification_rows\n    VALUES {\n      'postgres_user_id' = input.user_id,\n      'postgres_action' = lower(input.action)\n    }\n    ON CONFLICT ('postgres_user_id') DO UPDATE\n    WITH MAX BATCH 500\n    MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s\n  FLUSH EACH 10s MAX BATCH SIZE 1MiB\n  ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;"
        );
    }

    #[test]
    fn renders_mysql_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: identifier("emit_notifications"),
            from: ProcessorInputs::single(identifier("notifications")),
            encode_using_codec: None,
            sink: Box::new(EmitSink::MySql {
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
            }),
            flush_each: "10s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            publishing_mode: request_ack_mode(),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED EMITTER emit_notifications\n  FROM notifications\n  TO MYSQL \
             mysql_main INSERT TO TABLE notification_rows\n    VALUES {\n      'mysql_user_id' = \
             input.user_id,\n      'mysql_action' = lower(input.action)\n    }\n    ON CONFLICT \
             DO NOTHING\n    WITH MAX BATCH 500\n    MODE ACK RETRY POLICY BACKOFF 250ms MAX \
             30s\n  FLUSH EACH 10s MAX BATCH SIZE 1MiB\n  ON MESSAGE ERROR LOG\n  ON GENERAL \
             ERROR LOG;"
        );
    }

    #[test]
    fn renders_mongodb_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: identifier("emit_notifications"),
            from: ProcessorInputs::single(identifier("notifications")),
            encode_using_codec: None,
            sink: Box::new(EmitSink::MongoDb {
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
            }),
            flush_each: "10s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            publishing_mode: request_ack_mode(),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_nspl().expect("must render"),
            "CREATE ATTACHED EMITTER emit_notifications\n  FROM notifications\n  TO MONGODB \
             mongodb_main INSERT TO COLLECTION notification_rows\n    VALUES {\n      \
             'mongodb_user_id' = input.user_id,\n      'mongodb_action' = lower(input.action)\n    \
             }\n    ON CONFLICT ('mongodb_user_id') DO UPDATE\n    WITH MAX BATCH 500\n    MODE \
             ACK RETRY POLICY BACKOFF 250ms MAX 30s\n  FLUSH EACH 10s MAX BATCH SIZE 1MiB\n  ON \
             MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;"
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
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR http_ingestor\n  FROM HTTP http_main EVERY 30s ON QUIESCE \
                 SUSPEND\n  DECODE USING orders_codec\n  TO orders\n    UNBRANCHED\n    FLUSH \
                 EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR kafka_ingestor\n  FROM KAFKA kafka_main TOPIC orders_topic \
                 OFFSET BY CONSUMER GROUP orders_group INSTANCES 3 MODE ACK PARALLEL MAX 8 BATCH \
                 TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE \
                 SUSPEND\n  DECODE USING orders_codec\n  TO orders\n    UNBRANCHED\n    FLUSH \
                 EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Drop,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR mqtt_ingestor\n  FROM MQTT mqtt_main TOPIC orders_topic MODE \
                 NO_ACK SEQUENTIAL ON QUIESCE DROP\n  DECODE USING orders_codec\n  TO orders\n    \
                 UNBRANCHED\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  \
                 ON GENERAL ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Drop,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR nats_ingestor\n  FROM NATS nats_main SUBJECT orders_subject \
                 QUEUE GROUP orders_workers INSTANCES 2 MODE NO_ACK SEQUENTIAL ON QUIESCE DROP\n  \
                 DECODE USING orders_codec\n  TO orders\n    UNBRANCHED\n    FLUSH EACH 100ms MAX \
                 BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR rabbit_ingestor\n  FROM RABBITMQ rmq_main QUEUE orders_q \
                 INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 1s MAX 30s \
                 ON QUIESCE SUSPEND\n  DECODE USING orders_codec\n  TO orders\n    UNBRANCHED\n    \
                 FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL \
                 ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Drop,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR redis_ingestor\n  FROM REDIS PUBSUB redis_main CHANNEL \
                 orders_channel MODE NO_ACK SEQUENTIAL ON QUIESCE DROP\n  DECODE USING \
                 orders_codec\n  TO orders\n    UNBRANCHED\n    FLUSH EACH 100ms MAX BATCH SIZE \
                 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR prom_ingestor\n  FROM PROMETHEUS prom_main QUERY \
                 'sum(rate(http_requests_total[5m]))' EVERY 15s ON QUIESCE SUSPEND\n  DECODE \
                 USING orders_codec\n  TO orders\n    UNBRANCHED\n    FLUSH EACH 100ms MAX BATCH \
                 SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR zmq_ingestor\n  FROM ZEROMQ zmq_main MODE NO_ACK SEQUENTIAL ON \
                 QUIESCE SUSPEND\n  DECODE USING orders_codec\n  TO orders\n    UNBRANCHED\n    \
                 FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL \
                 ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR sqs_ingestor\n  FROM SQS sqs_main QUEUE orders_queue MODE ACK \
                 SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE SUSPEND\n  \
                 DECODE USING orders_codec\n  TO orders\n    UNBRANCHED\n    FLUSH EACH 100ms MAX \
                 BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::EndpointBuffer {
                            max_size: "1MiB".to_string(),
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR endpoint_ingestor\n  FROM ENDPOINT orders_endpoint MODE NO_ACK \
                 SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB\n  DECODE USING orders_codec\n  TO \
                 orders\n    UNBRANCHED\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE \
                 ERROR LOG\n  ON GENERAL ERROR LOG;",
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
                        quiesce: crate::IngestQuiesceMode::Drop,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR ws_ingestor\n  FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL ON \
                 QUIESCE DROP\n  DECODE USING orders_codec\n  TO orders\n    UNBRANCHED\n    \
                 FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL \
                 ERROR LOG;",
            ),
            (
                CreateIngestor {
                    name: identifier("syslog_ingestor"),
                    output_routes: flushed_ingestor_outputs("orders"),
                    decode_using_codec: identifier("syslog_codec"),
                    timestamp_source: None,
                    source: IngestSource::Syslog {
                        client: identifier("syslog_main"),
                        quiesce: crate::IngestQuiesceMode::Buffer {
                            max_size: "1MiB".to_string(),
                            overflow: crate::IngestQuiesceOverflow::DropOldest,
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }
                .to_canonical_nspl()
                .expect("must render"),
                "CREATE INGESTOR syslog_ingestor\n  FROM SYSLOG syslog_main MODE NO_ACK \
                 SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST\n  DECODE \
                 USING syslog_codec\n  TO orders\n    UNBRANCHED\n    FLUSH EACH 100ms MAX BATCH \
                 SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;",
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
            "CREATE CLIENT kafka_main\n  TYPE KAFKA\n  CONFIG {\n    'bootstrap.servers' = \
             'localhost:9092'\n  };"
        );
    }

    #[test]
    fn placement_canonicalization_preserves_member_order_policy_and_rank() {
        let placement = CreatePlacement::new(
            identifier("latency_path"),
            vec![identifier("ingest"), identifier("enrich")],
            vec![identifier("score")],
            PlacementPolicy::RequireColocation,
            Some(1),
        )
        .expect("placement must be valid");

        assert_eq!(
            placement.to_canonical_nspl().expect("must render"),
            "CREATE PLACEMENT latency_path FROM ingest, enrich TO score REQUIRE COLOCATION RANK 1;"
        );
    }

    #[test]
    fn unranked_neutral_placement_canonicalization_omits_rank() {
        let placement = CreatePlacement::new(
            identifier("ordinary"),
            vec![identifier("ingest")],
            vec![identifier("emit")],
            PlacementPolicy::Neutral,
            None,
        )
        .expect("placement must be valid");

        assert_eq!(
            placement.to_canonical_nspl().expect("must render"),
            "CREATE PLACEMENT ordinary FROM ingest TO emit NEUTRAL;"
        );
    }

    #[test]
    fn byte_sizes_take_the_largest_prefix_that_divides_exactly() {
        assert_eq!(super::byte_size_literal(67_108_864), "64MiB");
        assert_eq!(super::byte_size_literal(1 << 10), "1KiB");
        assert_eq!(super::byte_size_literal(1 << 30), "1GiB");
        assert_eq!(super::byte_size_literal(1 << 40), "1TiB");
    }

    #[test]
    fn byte_sizes_that_divide_no_prefix_exactly_stay_counts() {
        assert_eq!(super::byte_size_literal(0), "0B");
        assert_eq!(super::byte_size_literal(1), "1B");
        assert_eq!(super::byte_size_literal(100_000), "100000B");
        assert_eq!(super::byte_size_literal((1 << 20) + 1), "1048577B");
    }

    #[test]
    fn string_literals_choose_a_quote_style_that_represents_the_value() {
        assert_eq!(super::string_literal("plain"), "'plain'");
        assert_eq!(super::string_literal("can't fail"), "\"can't fail\"");
        assert_eq!(super::string_literal("line\nbreak"), "$s$line\nbreak$s$");
        assert_eq!(
            super::string_literal("both ' and \""),
            "$s$both ' and \"$s$"
        );
    }

    #[test]
    fn dollar_quoted_literals_escalate_past_a_colliding_tag() {
        assert_eq!(
            super::string_literal("holds $s$ and\na newline"),
            "$s_1$holds $s$ and\na newline$s_1$"
        );
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

    fn float_value(value: f64) -> Expression {
        Expression::Literal(Literal::F64(crate::Float64Literal::new(value)))
    }

    #[test]
    fn renders_float_literals_with_a_fractional_part() {
        assert_eq!(
            expression_to_nspl(&float_value(80.0)).expect("must render"),
            "80.0"
        );
        assert_eq!(
            expression_to_nspl(&float_value(0.0)).expect("must render"),
            "0.0"
        );
        assert_eq!(
            expression_to_nspl(&float_value(15.0)).expect("must render"),
            "15.0"
        );
    }

    #[test]
    fn renders_float_literals_that_already_read_as_floats_unchanged() {
        assert_eq!(
            expression_to_nspl(&float_value(111.32)).expect("must render"),
            "111.32"
        );
        assert_eq!(
            expression_to_nspl(&float_value(0.017453292519943295)).expect("must render"),
            "0.017453292519943295"
        );
        let huge = expression_to_nspl(&float_value(1e300)).expect("must render");
        assert!(huge.ends_with(".0"), "{huge} must lex back as a float");
    }

    #[test]
    fn refuses_to_render_non_finite_floats() {
        for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            expression_to_nspl(&float_value(value)).expect_err("must not render");
        }
    }
}
