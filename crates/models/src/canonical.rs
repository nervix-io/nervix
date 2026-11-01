//! Canonical Plexus rendering for vocabulary models.
//!
//! Layer: vocabulary.
//! - **Owns.** Stable textual rendering of current semantic models.
//! - **Depends on.** Vocabulary types and their declared semantic order.
//! - **Must not know.** Parser recovery, registry state or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

mod entity_presentation;

use std::{fmt::Display, num::NonZeroU64};

pub use entity_presentation::EntityPresentation;
use error_stack::Report;
use meticulous::OptionExt as _;
use thiserror::Error;

use crate::{
    AckWindow, AlterDeduplicator, AlterDeduplicatorOperation, AlterEmitter, AlterEmitterOperation,
    AlterGenerator, AlterGeneratorOperation, AlterIngestor, AlterIngestorOperation, AlterJunction,
    AlterPlacement, AlterPlacementOperation, AlterProcessorOperation, AlterReingestor, AlterRelay,
    AlterRelayOperation, AlterReorderer, AlterReordererOperation, AlterSchema,
    AlterSchemaOperation, AlterTransportIngestorOperation, AlterWireSchema,
    AlterWireSchemaOperation, AssignmentTarget, AssignmentTargetScope, AvroType, Backup,
    BackupCapture, BackupResources, BackupScope, BinaryOperator, BranchEviction, BranchSelection,
    ClickHouseValueMapping, ClientConfigEntry, ClientIngestMode, ClientResourceMount,
    CodecEncoding, CodecEncodingRule, CodecJaqTransformations, CodecName, CodecWireFormat,
    CorrelationTimeoutAction, CreateBranch, CreateClientAzureBlob, CreateClientClickHouse,
    CreateClientGcs, CreateClientHttp, CreateClientIcebergRest, CreateClientKafka,
    CreateClientMongoDb, CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientOtel,
    CreateClientPostgres, CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq,
    CreateClientRedis, CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
    CreateClientWebsockets, CreateClientZeroMq, CreateCodec, CreateCorrelator, CreateDeduplicator,
    CreateEmitter, CreateEndpoint, CreateGenerator, CreateInferencer, CreateIngestor,
    CreateJunction, CreateLookup, CreatePlacement, CreateReingestor, CreateRelay, CreateReorderer,
    CreateSchema, CreateSignalingProtocol, CreateUdf, CreateVhost, CreateWasmProcessor,
    CreateWindowProcessor, CreateWireSchema, DescribeBackup, DescribeTransaction, DomainPace,
    DomainStartPoint, EmitterBatchPolicy, EmitterBody, EmitterEntity, EmitterEnvelope,
    EmitterPublishing, EnvelopeClause, ExistingUserPolicy, Expression, FieldScope, Float64Literal,
    FlushPolicy, GeneralErrorPolicy, InferencerTensorDeclaration, InferencerTensorDimension,
    InferencerTensorMapping, IngestTimestampSource, IngestorInput, Inheritance, InputCollectPolicy,
    InspectionFormat, JsonType, Literal, MaterializedStateDependency, MaterializedStatePolicy,
    MembershipOperator, MessageErrorPolicy, Model, ModelName, MongoDbConflictAction,
    MySqlConflictAction, NamePosition, OutputBranch, ParseAsType, PlacementPolicy,
    PostgresConflictAction, ProcessorInputWhere, ProcessorInputs, ProcessorOutputs, RangeOperator,
    RelayBranching, RelayName, Restore, RestoreLifecycle, RestoreMode, RestoreScope, RestoreState,
    RetryPolicy, RouteConstruction, SchemaField, SignalingProtocolName, SignalingStep,
    SignalingWaitStep, SignalingWireFormat, Statement, SubscriptionLiteral,
    TransactionInspectionTarget, UnaryOperator, WindowBound, WindowStateLimit, WireSchemaField,
};

/// The Plexus release canonical rendering writes.
///
/// Plexus is versioned with the product, so a text this crate renders is read back by the parser of
/// the same release. An archive records it beside the Plexus it holds.
pub const PLEXUS_LANGUAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

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
                let last = items.len().checked_sub(1);
                for (index, item) in items.iter().enumerate() {
                    let comma = if Some(index) == last { "" } else { "," };
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

/// Binding levels of the Plexus expression grammar, loosest first.
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
        | BinaryOperator::LessThanOrEqual
        | BinaryOperator::IsDistinctFrom
        | BinaryOperator::IsNotDistinctFrom => PRECEDENCE_COMPARISON,
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
        Expression::Membership { .. } | Expression::Range { .. } => PRECEDENCE_COMPARISON,
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
fn operand_to_plexus(
    expression: &Expression,
    minimum: u8,
) -> error_stack::Result<String, CanonicalPlexusError> {
    let rendered = expression_to_plexus(expression)?;
    if precedence(expression) < minimum {
        Ok(format!("({rendered})"))
    } else {
        Ok(rendered)
    }
}

pub fn expression_to_plexus(
    expression: &Expression,
) -> error_stack::Result<String, CanonicalPlexusError> {
    match expression {
        Expression::Literal(Literal::I64(value)) => Ok(value.to_string()),
        Expression::Literal(Literal::F64(value)) => float_literal(*value),
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
                FieldScope::RelayState { relay } => Some(format!(
                    "relay_state.{}",
                    NamePosition::Qualified.spell(relay.as_str())
                )),
                FieldScope::Metadata => Some("metadata".to_string()),
                FieldScope::PartialOutput => Some("partial_output".to_string()),
                FieldScope::Error => Some("error".to_string()),
            };
            let field = reference.field.as_str();
            Ok(match prefix {
                Some(prefix) => format!("{prefix}.{}", NamePosition::Qualified.spell(field)),
                None => NamePosition::Bare.spell(field).into_owned(),
            })
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
            operand_to_plexus(expression, PRECEDENCE_CAST)?
        )),
        Expression::Binary {
            operator,
            left,
            right,
        } => Ok(format!(
            "{} {} {}",
            operand_to_plexus(left, binary_precedence(operator))?,
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
                BinaryOperator::IsDistinctFrom => "IS DISTINCT FROM",
                BinaryOperator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
            },
            operand_to_plexus(right, binary_precedence(operator) + 1)?
        )),
        // The operand is a left operand of the comparison level, which folds left, and each bound
        // is read one level tighter, so a bound holding `AND` or a comparison keeps its parentheses.
        Expression::Membership {
            operator,
            operand,
            set,
        } => Ok(format!(
            "{} {} ({})",
            operand_to_plexus(operand, PRECEDENCE_COMPARISON)?,
            match operator {
                MembershipOperator::In => "IN",
                MembershipOperator::NotIn => "NOT IN",
            },
            set.iter()
                .map(expression_to_plexus)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        Expression::Range {
            operator,
            operand,
            low,
            high,
        } => Ok(format!(
            "{} {} {} AND {}",
            operand_to_plexus(operand, PRECEDENCE_COMPARISON)?,
            match operator {
                RangeOperator::Between => "BETWEEN",
                RangeOperator::NotBetween => "NOT BETWEEN",
            },
            operand_to_plexus(low, PRECEDENCE_ADDITIVE)?,
            operand_to_plexus(high, PRECEDENCE_ADDITIVE)?
        )),
        Expression::Cast { expression, target } => Ok(format!(
            "{} AS {}",
            operand_to_plexus(expression, PRECEDENCE_CAST)?,
            parse_as_to_keyword(target)
        )),
        // The operand is the whole expression before the final `AS`, so it needs no parentheses.
        Expression::TryCast { expression, target } => Ok(format!(
            "TRY_CAST({} AS {})",
            expression_to_plexus(expression)?,
            parse_as_to_keyword(target)
        )),
        Expression::JsonValue {
            document,
            path,
            target,
        } => Ok(format!(
            "JSON_VALUE({}, {} AS {})",
            expression_to_plexus(document)?,
            string_literal(&path.to_string()),
            parse_as_to_keyword(target)
        )),
        Expression::TryJsonValue {
            document,
            path,
            target,
        } => Ok(format!(
            "TRY_JSON_VALUE({}, {} AS {})",
            expression_to_plexus(document)?,
            string_literal(&path.to_string()),
            parse_as_to_keyword(target)
        )),
        Expression::JsonExists { document, path } => Ok(format!(
            "JSON_EXISTS({}, {})",
            expression_to_plexus(document)?,
            string_literal(&path.to_string())
        )),
        Expression::Call {
            function,
            arguments,
        } => Ok(format!(
            "{}({})",
            NamePosition::Called.spell(function.as_str()),
            arguments
                .iter()
                .map(expression_to_plexus)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        Expression::UdfCall {
            function,
            arguments,
        } => Ok(format!(
            "udf::{}({})",
            NamePosition::Qualified.spell(function.as_str()),
            arguments
                .iter()
                .map(expression_to_plexus)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        Expression::Array(items) => Ok(format!(
            "[{}]",
            items
                .iter()
                .map(expression_to_plexus)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        Expression::If {
            condition,
            then_result,
            else_result,
        } => Ok(format!(
            "IF {} THEN {} ELSE {} END",
            expression_to_plexus(condition)?,
            expression_to_plexus(then_result)?,
            expression_to_plexus(else_result)?
        )),
        Expression::Case {
            operand,
            branches,
            else_result,
        } => {
            let mut rendered = "CASE".to_string();
            if let Some(operand) = operand {
                rendered.push(' ');
                rendered.push_str(&expression_to_plexus(operand)?);
            }
            for branch in branches {
                rendered.push_str(" WHEN ");
                rendered.push_str(&expression_to_plexus(&branch.when)?);
                rendered.push_str(" THEN ");
                rendered.push_str(&expression_to_plexus(&branch.result)?);
            }
            if let Some(else_result) = else_result {
                rendered.push_str(" ELSE ");
                rendered.push_str(&expression_to_plexus(else_result)?);
            }
            rendered.push_str(" END");
            Ok(rendered)
        }
    }
}

fn route_construction_to_plexus(
    construction: &RouteConstruction,
) -> error_stack::Result<String, CanonicalPlexusError> {
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

/// The field an assignment writes, as `SET` and `DEFAULT` spell it: the bare field, or its scope
/// and the field.
fn assignment_target_to_plexus(target: &AssignmentTarget) -> String {
    let field = target.field.as_str();
    let scope = match target.scope {
        AssignmentTargetScope::Bare => None,
        AssignmentTargetScope::Message => Some("message"),
        AssignmentTargetScope::Output => Some("output"),
        AssignmentTargetScope::Branch => Some("branch"),
    };
    match scope {
        Some(scope) => format!("{scope}.{}", NamePosition::Qualified.spell(field)),
        None => NamePosition::Bare.spell(field).into_owned(),
    }
}

/// The clauses of a route's construction, in the order they must be written.
fn route_construction_clauses(
    construction: &RouteConstruction,
) -> error_stack::Result<Vec<Clause>, CanonicalPlexusError> {
    let mut clauses: Vec<Clause> = Vec::new();
    if let Some(inherit) = &construction.inherit {
        let clause = match inherit {
            Inheritance::All => "INHERIT ALL".to_string(),
            Inheritance::AllExcept(fields) => format!(
                "INHERIT ALL EXCEPT {}",
                fields
                    .iter()
                    .map(|field| NamePosition::Bare.spell(field.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Inheritance::Fields(fields) => format!(
                "INHERIT {}",
                fields
                    .iter()
                    .map(|field| format!(
                        "{}{}",
                        NamePosition::Bare.spell(field.field.as_str()),
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
                    Ok(format!(
                        "{} = {}",
                        assignment_target_to_plexus(&assignment.target),
                        expression_to_plexus(&assignment.value)?
                    ))
                })
                .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?,
        ));
    }
    if let Some(where_clause) = &construction.where_clause {
        clauses.push(Clause::line(format!(
            "WHERE {}",
            expression_to_plexus(where_clause)?
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
                    NamePosition::Called.spell(invocation.function.as_str()),
                    invocation
                        .arguments
                        .iter()
                        .map(expression_to_plexus)
                        .collect::<Result<Vec<_>, _>>()?
                        .join(", ")
                )))
                .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
                .join(", ")
        )));
    }
    Ok(clauses)
}

fn value_mapping_items(
    values: &[ClickHouseValueMapping],
) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
    values
        .iter()
        .map(|mapping| {
            Ok(format!(
                "{} = {}",
                string_literal(&mapping.column),
                expression_to_plexus(&mapping.expression)?
            ))
        })
        .collect()
}

fn branch_selection_to_plexus(branching: &BranchSelection) -> String {
    match branching {
        BranchSelection::BranchedBy { branch } => {
            format!("BRANCHED BY {}", branch.as_str())
        }
        BranchSelection::Unbranched => "UNBRANCHED".to_string(),
    }
}

fn output_branch_to_plexus(
    branching: &OutputBranch,
) -> error_stack::Result<String, CanonicalPlexusError> {
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
                rendered.push_str(&route_construction_to_plexus(&construction)?);
            }
            Ok(rendered)
        }
        OutputBranch::Unbranched => Ok("UNBRANCHED".to_string()),
    }
}

/// Why a Model has no canonical Plexus rendering.
///
/// Each variant is a value Plexus has no spelling for. It carries that typed value or names the Model
/// holding it, so a caller can locate the refusal without reading its message.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CanonicalPlexusError {
    /// An `F64` literal that is NaN or infinite. Plexus spells a float only as a finite decimal.
    #[error("cannot represent non-finite float in Plexus: {}", .value.value())]
    UnrepresentableFloat { value: Float64Literal },
    /// A `SYSLOG` codec that declares encoding rules. Its record shape is fixed, so it has no
    /// fields a rule could encode.
    #[error("invalid codec `{codec}`: SYSLOG codec must not declare encoding rules")]
    SyslogEncodingRules { codec: CodecName },
    /// A codec whose wire format runs JAQ transformations but declares no program to run.
    #[error("invalid codec `{codec}`: codec is missing JAQ transformation")]
    MissingJaqTransformation { codec: CodecName },
}

impl AlterPlacement {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
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
                    model_name_list(from),
                    model_name_list(to)
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

fn model_name_list(names: &[ModelName]) -> String {
    names
        .iter()
        .map(ModelName::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

impl Statement {
    /// Renders this statement as canonical Plexus.
    ///
    /// Unlike [`Model::to_canonical_plexus`], this covers the whole executable language -- the
    /// creation modifiers, the domain lifecycle, administration, and the read-only queries -- so a
    /// parsed script can be rendered back statement for statement.
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        match self {
            Self::Create(create) => {
                let rendered = create.body.to_canonical_plexus()?;
                if !create.if_not_exists {
                    return Ok(rendered);
                }
                let rest = rendered.strip_prefix("CREATE ").verified(
                    "this branch renders a create statement, whose rendering starts with the \
                     CREATE keyword",
                );
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
                    DomainPace::Paced { period, skew } => format!(
                        "PACED DOMAIN {} WITH PERIOD {} SKEW {}",
                        create.body.id.as_str(),
                        duration_literal(period.as_nanos()),
                        duration_literal(skew.as_nanos())
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
            Self::RebindResource(rebind) => {
                let latest = match rebind.version {
                    crate::RequestedResourceVersion::Number(version) => version.to_string(),
                    crate::RequestedResourceVersion::Latest => "LATEST".to_string(),
                };
                let members = match &rebind.selection {
                    crate::RebindResourceSelection::All => None,
                    crate::RebindResourceSelection::Members(members) => Some(
                        members
                            .iter()
                            .map(|member| {
                                format!(
                                    "{} {}",
                                    member.kind.keyword_phrase(),
                                    member.identifier.as_str()
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", "),
                    ),
                };
                let for_clause = match members {
                    Some(members) => format!(" FOR {members}"),
                    None => String::new(),
                };
                Ok(format!(
                    "REBIND RESOURCE {} TO VERSION {latest}{for_clause};",
                    rebind.resource.as_str()
                ))
            }
            Self::ResetWasmState(reset) => {
                let scope = match &reset.scope {
                    crate::ResetWasmStateScope::Unbranched => "UNBRANCHED".to_string(),
                    crate::ResetWasmStateScope::AllBranches => "ALL BRANCHES".to_string(),
                    crate::ResetWasmStateScope::Branch(fields) => {
                        let mut rendered = Vec::with_capacity(fields.len());
                        for field in fields {
                            let value =
                                expression_to_plexus(&Expression::Literal(field.value.clone()))?;
                            rendered.push(format!("{} = {value}", field.name.as_str()));
                        }
                        format!("BRANCH VALUES {{ {} }}", rendered.join(", "))
                    }
                };
                Ok(format!(
                    "RESET WASM PROCESSOR {} STATE IN DOMAIN {} FOR {scope};",
                    reset.processor.as_str(),
                    reset.domain.as_str()
                ))
            }
            Self::UploadResource(upload) => Ok(format!(
                "UPLOAD RESOURCE {} VERSION {};",
                upload.identifier.as_str(),
                string_literal(&upload.source_path)
            )),
            Self::Backup(backup) => Ok(backup.to_canonical_plexus()),
            Self::Restore(restore) => Ok(restore.to_canonical_plexus()),
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
                    string_literal(&timestamp.as_datetime().to_rfc3339())
                ),
            }),
            Self::StopDomain(_) => Ok("STOP;".to_string()),
            Self::AlterSchema(alter) => alter.to_canonical_plexus(),
            Self::AlterWireJsonSchema(alter) => alter_json_wire_schema_to_canonical_plexus(alter),
            Self::AlterWireCborSchema(alter) => alter_cbor_wire_schema_to_canonical_plexus(alter),
            Self::AlterWireAvroSchema(alter) => alter_avro_wire_schema_to_canonical_plexus(alter),
            Self::AlterRelay(alter) => alter.to_canonical_plexus(),
            Self::AlterJunction(alter) => alter.to_canonical_plexus(),
            Self::AlterDeduplicator(alter) => alter.to_canonical_plexus(),
            Self::AlterReorderer(alter) => alter.to_canonical_plexus(),
            Self::AlterEmitter(alter) => alter.to_canonical_plexus(),
            Self::AlterIngestor(alter) => alter.to_canonical_plexus(),
            Self::AlterReingestor(alter) => alter.to_canonical_plexus(),
            Self::AlterGenerator(alter) => alter.to_canonical_plexus(),
            Self::AlterPlacement(alter) => alter.to_canonical_plexus(),
            Self::Drop(drop) => Ok(format!(
                "DROP {} {};",
                drop.kind.keyword_phrase(),
                drop.name.as_str()
            )),
            Self::DropNode(node) => Ok(format!("DROP NODE {};", node.node_id)),
            Self::CordonNode(node) => Ok(format!("CORDON NODE {};", node.node_id)),
            Self::UncordonNode(node) => Ok(format!("UNCORDON NODE {};", node.node_id)),
            Self::DrainNode(node) => Ok(format!("DRAIN NODE {};", node.node_id)),
            Self::Relocate(relocation) => {
                Ok(format!("RELOCATE {};", relocation.to_plexus_clauses()))
            }
            Self::DescribeRelocation(relocation) => Ok(format!(
                "DESCRIBE RELOCATION {};",
                relocation.to_plexus_clauses()
            )),
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
                                subscription_literal_to_plexus(&binding.value)
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
            Self::DescribeResource(describe) => {
                let version = match describe.version {
                    Some(version) => format!(" VERSION {version}"),
                    None => String::new(),
                };
                Ok(format!(
                    "DESCRIBE RESOURCE {}{version};",
                    describe.identifier.as_str()
                ))
            }
            Self::DescribeLookup(describe) => {
                Ok(format!("DESCRIBE HASH MAP {};", describe.name.as_str()))
            }
            Self::DescribeEndpoint(describe) => {
                Ok(format!("DESCRIBE ENDPOINT {};", describe.name.as_str()))
            }
            Self::DescribeClient(describe) => {
                Ok(format!("DESCRIBE CLIENT {};", describe.name.as_str()))
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
            Self::DescribeWasmProcessor(describe) => {
                let mut statement = format!("DESCRIBE WASM PROCESSOR {}", describe.name.as_str());
                if describe.format != InspectionFormat::Text {
                    statement.push_str(" FORMAT JSON");
                }
                statement.push(';');
                Ok(statement)
            }
            Self::DescribeUdf(describe) => Ok(format!("DESCRIBE UDF {};", describe.name.as_str())),
            Self::DescribePlacement(describe) => {
                Ok(format!("DESCRIBE PLACEMENT {};", describe.name.as_str()))
            }
            Self::LookupQuery(query) => Ok(format!(
                "LOOKUP {} KEY {};",
                query.name.as_str(),
                subscription_literal_to_plexus(&query.key)
            )),
            Self::ShowCreate(show) => Ok(format!(
                "SHOW CREATE {} {};",
                show.kind.keyword_phrase(),
                show.name.as_str()
            )),
            Self::ShowUdfs(_) => Ok("SHOW UDFS;".to_string()),
            Self::ShowIngestors(_) => Ok("SHOW INGESTORS;".to_string()),
            Self::ShowEmitters(_) => Ok("SHOW EMITTERS;".to_string()),
            Self::ShowPlacements(_) => Ok("SHOW PLACEMENTS;".to_string()),
            Self::ShowRelayMaterializedState(show) => Ok(format!(
                "SHOW RELAY {} MATERIALIZED STATE;",
                show.relay.as_str()
            )),
            Self::ShowClusterStatus(_) => Ok("SHOW CLUSTER STATUS;".to_string()),
            Self::DescribeCluster(_) => Ok("DESCRIBE CLUSTER;".to_string()),
            Self::ShowTransactions(_) => Ok("SHOW TRANSACTIONS;".to_string()),
            Self::DescribeTransaction(describe) => Ok(describe.to_canonical_plexus()),
        }
    }
}

impl Backup {
    /// Renders the statement with the clauses it needs, omitting the default of including resource
    /// bytes.
    pub fn to_canonical_plexus(&self) -> String {
        let scope = match &self.scope {
            BackupScope::Cluster => "CLUSTER".to_string(),
            BackupScope::Domain(Some(domain)) => format!("DOMAIN {}", domain.as_str()),
            BackupScope::Domain(None) => "DOMAIN".to_string(),
        };
        let resources = match self.resources {
            BackupResources::Included => "",
            BackupResources::Omitted => " WITHOUT RESOURCES",
        };
        let capture = match self.capture {
            BackupCapture::Quiesced { timeout: None } => String::new(),
            BackupCapture::Quiesced {
                timeout: Some(timeout),
            } => format!(" TIMEOUT {}", duration_literal(timeout.as_nanos())),
            BackupCapture::Live => " WITHOUT PAUSE".to_string(),
            BackupCapture::ConfigurationOnly => " WITHOUT STATE".to_string(),
        };
        format!(
            "BACKUP {scope} TO {}{resources}{capture};",
            string_literal(&self.destination)
        )
    }
}

impl Restore {
    /// Renders the statement with the clauses it needs, omitting the default `ON EXISTING USER
    /// FAIL`.
    pub fn to_canonical_plexus(&self) -> String {
        let source = string_literal(&self.source);
        let resume = match self.lifecycle {
            RestoreLifecycle::Stopped => "",
            RestoreLifecycle::Resume => " RESUME",
        };
        let mut statement = match &self.scope {
            RestoreScope::Cluster { existing_users } => {
                let mut statement = format!("RESTORE CLUSTER FROM {source}{resume}");
                if *existing_users != ExistingUserPolicy::default() {
                    statement.push_str(&format!(" ON EXISTING USER {}", existing_users.as_ref()));
                }
                statement
            }
            RestoreScope::Domain { domain, target } => {
                let mut statement = format!("RESTORE DOMAIN {}", domain.as_str());
                if let Some(target) = target {
                    statement.push_str(&format!(" AS {}", target.as_str()));
                }
                statement.push_str(&format!(" FROM {source}{resume}"));
                statement
            }
        };
        match self.mode {
            RestoreMode::Apply => {}
            RestoreMode::DryRun => statement.push_str(" DRY RUN"),
        }
        match self.state {
            RestoreState::All => {}
            RestoreState::WithoutSourceOffsets => statement.push_str(" WITHOUT SOURCE OFFSETS"),
            RestoreState::ConfigurationOnly => statement.push_str(" WITHOUT STATE"),
        }
        statement.push(';');
        statement
    }
}

impl DescribeBackup {
    /// Renders the statement, omitting the default `FORMAT TEXT`.
    pub fn to_canonical_plexus(&self) -> String {
        let mut statement = format!("DESCRIBE BACKUP {}", string_literal(&self.source));
        if self.format != InspectionFormat::default() {
            statement.push_str(&format!(" FORMAT {}", self.format.as_ref()));
        }
        statement.push(';');
        statement
    }
}

impl DescribeTransaction {
    /// Renders the statement with each clause it needs, omitting the default `FORMAT TEXT`.
    pub fn to_canonical_plexus(&self) -> String {
        let mut statement = "DESCRIBE TRANSACTION".to_string();
        if let TransactionInspectionTarget::Transaction { transaction_id } = &self.request.target {
            statement.push(' ');
            statement.push_str(&string_literal(transaction_id));
        }
        if let Some(operation) = self.request.operation {
            statement.push_str(&format!(" OPERATION {operation}"));
        }
        if self.format != InspectionFormat::default() {
            statement.push_str(&format!(" FORMAT {}", self.format.as_ref()));
        }
        statement.push(';');
        statement
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

fn subscription_literal_to_plexus(literal: &SubscriptionLiteral) -> String {
    match literal {
        SubscriptionLiteral::String(value) => string_literal(value),
        SubscriptionLiteral::Number(value) => value.clone(),
        SubscriptionLiteral::Bool(value) => value.to_string().to_ascii_uppercase(),
    }
}

/// Renders `models` as one canonical Plexus document: each model's `CREATE` statement in the order
/// given, separated by a blank line and ending with a newline.
///
/// A parser reads the document back statement by statement, so the order is the order the models
/// have to be created in; the caller decides it.
pub fn canonical_plexus_document<'model, Version: Display + 'model>(
    models: impl IntoIterator<Item = &'model Model<Version>>,
) -> error_stack::Result<String, CanonicalPlexusError> {
    let mut document = String::new();
    for model in models {
        if !document.is_empty() {
            document.push('\n');
        }
        document.push_str(&model.to_canonical_plexus()?);
        document.push('\n');
    }
    Ok(document)
}

impl<Version: Display + Clone> Model<Version> {
    /// The Plexus shown by public inspection, with declared client secrets redacted.
    /// Complete canonical serialization remains the representation used by archives.
    pub fn to_public_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut presented = self.clone();
        if let Some(client) = self.configured_client() {
            let declaration = client
                .connector
                .client()
                .assured("a configured client has a declaration on its connector");
            *presented
                .client_config_mut()
                .assured("the cloned configured client has the same variant") =
                declaration.config.inspection_entries(client.config);
        }
        presented.to_canonical_plexus()
    }
}

impl<Version: Display> Model<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        match self {
            Self::Schema(schema) => schema.to_canonical_plexus(),
            Self::WireJsonSchema(schema) => wire_schema_to_plexus("JSON", schema),
            Self::WireCborSchema(schema) => wire_schema_to_plexus("CBOR", schema),
            Self::WireAvroSchema(schema) => wire_schema_to_plexus("AVRO", schema),
            Self::Codec(codec) => codec.to_canonical_plexus(),
            Self::ClientKafka(client) => client.to_canonical_plexus(),
            Self::ClientPulsar(client) => client.to_canonical_plexus(),
            Self::ClientHttp(client) => client.to_canonical_plexus(),
            Self::ClientSentry(client) => client.to_canonical_plexus(),
            Self::ClientOtel(client) => client.to_canonical_plexus(),
            Self::ClientPrometheus(client) => client.to_canonical_plexus(),
            Self::ClientMqtt(client) => client.to_canonical_plexus(),
            Self::ClientNats(client) => client.to_canonical_plexus(),
            Self::ClientRabbitMq(client) => client.to_canonical_plexus(),
            Self::ClientRedis(client) => client.to_canonical_plexus(),
            Self::ClientZeroMq(client) => client.to_canonical_plexus(),
            Self::ClientSqs(client) => client.to_canonical_plexus(),
            Self::ClientWebsockets(client) => client.to_canonical_plexus(),
            Self::ClientSyslog(client) => client.to_canonical_plexus(),
            Self::ClientClickHouse(client) => client.to_canonical_plexus(),
            Self::ClientPostgres(client) => client.to_canonical_plexus(),
            Self::ClientMySql(client) => client.to_canonical_plexus(),
            Self::ClientMongoDb(client) => client.to_canonical_plexus(),
            Self::ClientS3(client) => client.to_canonical_plexus(),
            Self::ClientGcs(client) => client.to_canonical_plexus(),
            Self::ClientAzureBlob(client) => client.to_canonical_plexus(),
            Self::ClientIcebergRest(client) => client.to_canonical_plexus(),
            Self::Vhost(vhost) => vhost.to_canonical_plexus(),
            Self::Branch(branch) => branch.to_canonical_plexus(),
            Self::Endpoint(endpoint) => endpoint.to_canonical_plexus(),
            Self::SignalingProtocol(protocol) => protocol.to_canonical_plexus(),
            Self::Generator(generator) => generator.to_canonical_plexus(),
            Self::Inferencer(inference) => inference.to_canonical_plexus(),
            Self::WasmProcessor(processor) => processor.to_canonical_plexus(),
            Self::Ingestor(ingestor) => ingestor.to_canonical_plexus(),
            Self::Reingestor(reingestor) => reingestor.to_canonical_plexus(),
            Self::Relay(relay) => relay.to_canonical_plexus(),
            Self::Lookup(lookup) => lookup.to_canonical_plexus(),
            Self::Junction(junction) => junction.to_canonical_plexus(),
            Self::Deduplicator(deduplicator) => deduplicator.to_canonical_plexus(),
            Self::Correlator(correlator) => correlator.to_canonical_plexus(),
            Self::Reorderer(reorderer) => reorderer.to_canonical_plexus(),
            Self::WindowProcessor(window_processor) => window_processor.to_canonical_plexus(),
            Self::Emitter(emitter) => emitter.to_canonical_plexus(),
            Self::Placement(placement) => placement.to_canonical_plexus(),
            Self::Udf(udf) => udf.to_canonical_plexus(),
        }
    }
}

impl CreatePlacement {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut rendered = format!(
            "CREATE PLACEMENT {} FROM {} TO {} {}",
            self.name.as_str(),
            self.from
                .iter()
                .map(ModelName::as_str)
                .collect::<Vec<_>>()
                .join(", "),
            self.to
                .iter()
                .map(ModelName::as_str)
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let fields = self
            .fields
            .iter()
            .map(schema_field_to_plexus)
            .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?;

        Ok(block_statement(
            format!("CREATE SCHEMA {}", self.name.as_str()),
            fields,
        ))
    }
}

impl AlterSchema {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let operations = self
            .operations
            .iter()
            .map(alter_schema_operation_to_plexus)
            .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
            .join(", ");
        Ok(format!(
            "ALTER SCHEMA {} {operations};",
            self.schema.as_str()
        ))
    }
}

impl AlterRelay {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let operations = self
            .operations
            .iter()
            .map(alter_processor_operation_to_plexus)
            .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
            .join(", ");
        Ok(format!(
            "ALTER JUNCTION {} {operations};",
            self.junction.as_str()
        ))
    }
}

impl AlterDeduplicator {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let operations = self
            .operations
            .iter()
            .map(alter_deduplicator_operation_to_plexus)
            .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
            .join(", ");
        Ok(format!(
            "ALTER DEDUPLICATOR {} {operations};",
            self.deduplicator.as_str()
        ))
    }
}

impl AlterReorderer {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let operations = self
            .operations
            .iter()
            .map(alter_reorderer_operation_to_plexus)
            .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
            .join(", ");
        Ok(format!(
            "ALTER REORDERER {} {operations};",
            self.reorderer.as_str()
        ))
    }
}

impl AlterReingestor {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let operations = self
            .operations
            .iter()
            .map(alter_processor_operation_to_plexus)
            .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
            .join(", ");
        Ok(format!(
            "ALTER REINGESTOR {} {operations};",
            self.reingestor.as_str()
        ))
    }
}

impl AlterGenerator {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let operations = self
            .operations
            .iter()
            .map(alter_generator_operation_to_plexus)
            .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
            .join(", ");
        Ok(format!(
            "ALTER GENERATOR {} {operations};",
            self.generator.as_str()
        ))
    }
}

impl AlterEmitter {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let operations = self
            .operations
            .iter()
            .map(alter_emitter_operation_to_plexus)
            .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
            .join(", ");
        Ok(format!(
            "ALTER {} EMITTER {} {operations};",
            self.kind.phrase(),
            self.emitter.as_str()
        ))
    }
}

impl AlterIngestor {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let operations = self.alteration.operations_to_plexus()?.join(", ");
        Ok(format!(
            "ALTER {} INGESTOR {} {operations};",
            self.kind().phrase(),
            self.ingestor.as_str()
        ))
    }
}

pub fn alter_json_wire_schema_to_canonical_plexus(
    alter: &AlterWireSchema<JsonType>,
) -> error_stack::Result<String, CanonicalPlexusError> {
    alter_wire_schema_to_plexus("JSON", alter)
}

pub fn alter_cbor_wire_schema_to_canonical_plexus(
    alter: &AlterWireSchema<JsonType>,
) -> error_stack::Result<String, CanonicalPlexusError> {
    alter_wire_schema_to_plexus("CBOR", alter)
}

pub fn alter_avro_wire_schema_to_canonical_plexus(
    alter: &AlterWireSchema<AvroType>,
) -> error_stack::Result<String, CanonicalPlexusError> {
    alter_wire_schema_to_plexus("AVRO", alter)
}

/// Pool clauses after a connector-headed client name, before its mount and configuration.
///
/// A pool-capable client always declares both bounds in the order its grammar accepts.
macro_rules! client_pool_clauses {
    ($client:expr, unpooled) => {
        Vec::new()
    };
    ($client:expr, pooled) => {
        vec![Clause::line(format!(
            "POOL SIZE MIN {} MAX {}",
            $client.pool.minimum(),
            $client.pool.maximum(),
        ))]
    };
}

macro_rules! impl_standard_client_canonical {
    ($($Client:ident => $pooling:ident;)+) => {
        $(
            impl<Version: Display> $Client<Version> {
                pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
                    let mut clauses = client_pool_clauses!(self, $pooling);
                    if let Some(mount) = self.mount.as_ref() {
                        clauses.push(Clause::line(format!(
                            "MOUNT {} VERSION {}",
                            mount.resource.as_str(),
                            mount.version,
                        )));
                    }
                    clauses.push(Clause::braced("CONFIG", config_entry_items(&self.config)?));

                    Ok(clause_statement(
                        format!("CREATE {} CLIENT {}", Self::CONNECTOR.keyword(), self.name.as_str()),
                        clauses,
                    ))
                }
            }
        )+
    };
}

impl_standard_client_canonical! {
    CreateClientKafka => unpooled;
    CreateClientPulsar => unpooled;
    CreateClientHttp => unpooled;
    CreateClientSentry => unpooled;
    CreateClientOtel => unpooled;
    CreateClientPrometheus => unpooled;
    CreateClientMqtt => unpooled;
    CreateClientNats => unpooled;
    CreateClientRabbitMq => unpooled;
    CreateClientZeroMq => unpooled;
    CreateClientSqs => unpooled;
    CreateClientGcs => unpooled;
    CreateClientAzureBlob => unpooled;
    CreateClientIcebergRest => unpooled;
    CreateClientSyslog => unpooled;
    CreateClientClickHouse => unpooled;
    CreateClientRedis => pooled;
    CreateClientPostgres => pooled;
    CreateClientMySql => pooled;
    CreateClientMongoDb => pooled;
}

impl<Version: Display> CreateClientS3<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let config = config_entry_items(&self.config)?.join(", ");

        Ok(format!(
            "CREATE {} CLIENT {}{} CONFIG {{{}}};",
            Self::CONNECTOR.keyword(),
            self.name.as_str(),
            client_mount_clause(self.mount.as_ref()),
            config
        ))
    }
}

impl<Version: Display> CreateClientWebsockets<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = Vec::new();
        if let Some(protocol) = self.signaling_protocol.as_ref() {
            clauses.push(Clause::line(format!(
                "WITH SIGNALING PROTOCOL {}",
                protocol.as_str(),
            )));
        }
        if let Some(mount) = self.mount.as_ref() {
            clauses.push(Clause::line(format!(
                "MOUNT {} VERSION {}",
                mount.resource.as_str(),
                mount.version,
            )));
        }
        clauses.push(Clause::braced("CONFIG", config_entry_items(&self.config)?));
        Ok(clause_statement(
            format!(
                "CREATE {} CLIENT {}",
                Self::CONNECTOR.keyword(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

fn client_mount_clause<Version: Display>(mount: Option<&ClientResourceMount<Version>>) -> String {
    match mount {
        Some(mount) => format!(
            " MOUNT {} VERSION {}",
            mount.resource.as_str(),
            mount.version
        ),
        None => String::new(),
    }
}

fn signaling_protocol_clause(signaling_protocol: Option<&SignalingProtocolName>) -> String {
    match signaling_protocol {
        Some(protocol) => format!(" WITH SIGNALING PROTOCOL {}", protocol.as_str()),
        None => String::new(),
    }
}

impl<Version: Display> CreateVhost<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let tls = match &self.tls {
            Some(tls) => format!(
                " WITH TLS {} VERSION {}",
                tls.resource.as_str(),
                tls.version
            ),
            None => String::new(),
        };
        Ok(format!(
            "CREATE VHOST {} {}{};",
            self.name.as_str(),
            self.hostnames.join(", "),
            tls,
        ))
    }
}

impl CreateEndpoint {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        Ok(format!(
            "CREATE ENDPOINT {} ON {} PATH {} TYPE {}{};",
            self.name.as_str(),
            self.on_vhost.as_str(),
            string_literal(&self.path),
            self.endpoint_type.as_ref(),
            signaling_protocol_clause(self.signaling_protocol.as_ref())
        ))
    }
}

impl<Version: Display> CreateSignalingProtocol<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = String::new();
        if self.on_connect.accept_data {
            clauses.push_str(" ACCEPT DATA");
        }
        for step in &self.on_connect.steps {
            match step {
                SignalingStep::Send(programs) => {
                    clauses.push_str(" SEND JAQ ");
                    clauses.push_str(&jaq_program_list_to_plexus(programs));
                }
                SignalingStep::Wait(wait) => clauses.push_str(&wait.to_canonical_plexus()?),
            }
        }
        let protocol_fail = if self.on_connect.fail_matchers.is_empty() {
            String::new()
        } else {
            format!(
                " FAIL JAQ {}",
                jaq_program_list_to_plexus(&self.on_connect.fail_matchers)
            )
        };

        Ok(format!(
            "CREATE SIGNALING PROTOCOL {} FORMAT {}{} ON CONNECT{} TIMEOUT {};",
            self.name.as_str(),
            self.format.to_canonical_plexus()?,
            protocol_fail,
            clauses,
            self.on_connect.timeout
        ))
    }
}

impl<Version: Display> SignalingWireFormat<Version> {
    fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let Self::Protobuf(config) = self else {
            return Ok(self.as_ref().to_string());
        };
        let protobuf_config = config_entry_items(&config.config)?.join(", ");
        Ok(format!(
            "PROTOBUF USING RESOURCE {} VERSION {} CONFIG {{{}}} SEND MESSAGE {} WAIT MESSAGE {}",
            config.resource.as_str(),
            config.resource_version,
            protobuf_config,
            string_literal(&config.send_message),
            string_literal(&config.wait_message)
        ))
    }
}

impl SignalingWaitStep {
    fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut rendered = format!(" WAIT JAQ {}", jaq_program_list_to_plexus(&self.matchers));
        if !self.fail_matchers.is_empty() {
            rendered.push_str(" FAIL JAQ ");
            rendered.push_str(&jaq_program_list_to_plexus(&self.fail_matchers));
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

fn jaq_program_list_to_plexus(programs: &[String]) -> String {
    programs
        .iter()
        .map(|program| string_literal(program))
        .collect::<Vec<_>>()
        .join(", ")
}

impl<Version: Display> CreateCodec<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = Vec::new();
        let transformations = match &self.wire_format {
            CodecWireFormat::Json { wire_schema } => {
                clauses.push(Clause::line(format!(
                    "FROM WIRE JSON SCHEMA {}",
                    wire_schema.as_str()
                )));
                String::new()
            }
            CodecWireFormat::Cbor { wire_schema } => {
                clauses.push(Clause::line(format!(
                    "FROM WIRE CBOR SCHEMA {}",
                    wire_schema.as_str()
                )));
                String::new()
            }
            CodecWireFormat::Avro { wire_schema } => {
                clauses.push(Clause::line(format!(
                    "FROM WIRE AVRO SCHEMA {}",
                    wire_schema.as_str()
                )));
                String::new()
            }
            CodecWireFormat::Syslog => {
                if !self.encoding_rules.is_empty() {
                    return Err(Report::new(CanonicalPlexusError::SyslogEncodingRules {
                        codec: self.name.clone(),
                    }));
                }
                clauses.push(Clause::line("FROM SYSLOG"));
                String::new()
            }
            CodecWireFormat::JaqNative {
                format,
                transformations,
            } => {
                clauses.push(Clause::line(format!("FROM {}", format.as_ref())));
                codec_jaq_transformations_to_plexus(&self.name, transformations)?
            }
            CodecWireFormat::Protobuf(config) => {
                // The configuration map is a block of its own; the messages follow it on one line.
                clauses.push(Clause::line(format!(
                    "FROM PROTOBUF USING RESOURCE {} VERSION {}",
                    config.resource.as_str(),
                    config.resource_version,
                )));
                clauses.push(Clause::braced(
                    "CONFIG",
                    config_entry_items(&config.config)?,
                ));
                let batch_message = match &config.batch_message {
                    Some(message) => format!(" BATCH MESSAGE {}", string_literal(message)),
                    None => String::new(),
                };
                clauses.push(Clause::line(format!(
                    "MESSAGE {}{batch_message}",
                    string_literal(&config.message)
                )));
                codec_jaq_transformations_to_plexus(&self.name, &config.transformations)?
            }
        };
        let encoding_rules = if self.encoding_rules.is_empty() {
            String::new()
        } else {
            format!(
                " ENCODE {}",
                self.encoding_rules
                    .iter()
                    .map(codec_encoding_rule_to_plexus)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
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

/// Renders configuration entries as the items of a `CONFIG` block, in declaration order.
fn config_entry_items(
    entries: &[ClientConfigEntry],
) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
    entries
        .iter()
        .map(ClientConfigEntry::to_canonical_plexus)
        .collect()
}

/// Renders the JAQ programs a codec runs; `codec` names the codec a refusal reports.
fn codec_jaq_transformations_to_plexus(
    codec: &CodecName,
    transformations: &CodecJaqTransformations,
) -> error_stack::Result<String, CanonicalPlexusError> {
    if !transformations.has_any() {
        return Err(Report::new(
            CanonicalPlexusError::MissingJaqTransformation {
                codec: codec.clone(),
            },
        ));
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
    if let Some(program) = transformations.on_emitting_batch.as_deref() {
        rendered.push_str(" ON EMITTING BATCH ");
        rendered.push_str(&string_literal(program));
    }
    Ok(rendered)
}

fn codec_encoding_rule_to_plexus(rule: &CodecEncodingRule) -> String {
    format!(
        "{} AS {}",
        rule.field.as_str(),
        codec_encoding_to_plexus(rule.encoding)
    )
}

fn codec_encoding_to_plexus(encoding: CodecEncoding) -> &'static str {
    match encoding {
        CodecEncoding::Rfc3339 => "RFC3339",
    }
}

impl CreateBranch {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let timestamp = match &self.timestamp_source {
            Some(IngestTimestampSource::Now) => " TIMESTAMP NOW".to_string(),
            Some(IngestTimestampSource::At(field)) => format!(" TIMESTAMP AT {}", field.as_str()),
            Some(IngestTimestampSource::AtMetadata(field)) => {
                format!(" TIMESTAMP AT metadata.{}", field.as_str())
            }
            None => String::new(),
        };
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            ingestor_input_body_to_plexus(&self.input)
        ))];
        if let Some(codec) = self.input.codec() {
            clauses.push(Clause::line(format!("DECODE USING {}", codec.as_str())));
        }
        if !timestamp.is_empty() {
            clauses.push(Clause::line(timestamp.trim_start().to_string()));
        }
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.extend(processor_outputs_clauses(&self.output_routes)?);
        clauses.push(Clause::line(general_error_policy_to_plexus(
            &self.general_error_policy,
        )));

        Ok(clause_statement(
            format!(
                "CREATE {} INGESTOR {}",
                self.input.source_phrase(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

impl CreateGenerator {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![
            Clause::line(format!(
                "USING MATERIALIZED STATE {}",
                self.materialized_relay.as_str()
            )),
            Clause::line(format!("EACH {}", duration_literal(self.each.as_nanos()))),
            Clause::line(branch_selection_to_plexus(&self.branched_by)),
        ];
        clauses.extend(processor_outputs_clauses(&self.output_routes)?);

        Ok(clause_statement(
            format!("CREATE GENERATOR {}", self.name.as_str()),
            clauses,
        ))
    }
}

impl CreateRelay {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
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
            rendered.push_str(" WITH MATERIALIZED STATE ");
            rendered.push_str(state.as_ref());
        }
        rendered.push(';');
        Ok(rendered)
    }
}

impl<Version: Display> CreateLookup<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        Ok(format!(
            "CREATE HASH MAP {} KEY {} FROM RESOURCE {} VERSION {} PATH {} DECODE USING {};",
            self.name.as_str(),
            self.key_field.as_str(),
            self.resource.as_str(),
            self.resource_version,
            string_literal(&self.path),
            self.decode_using_codec.as_str()
        ))
    }
}

impl FlushPolicy {
    pub fn to_canonical_plexus(&self) -> String {
        match self {
            Self::Immediate => "FLUSH IMMEDIATE".to_string(),
            Self::Each {
                interval,
                max_batch_size,
            } => format!("FLUSH EACH {interval} MAX BATCH SIZE {max_batch_size}"),
        }
    }
}

fn commit_policy_to_plexus(policy: &str, max_size: &crate::PayloadSizeLimit) -> String {
    format!("COMMIT EACH {policy} MAX SIZE {max_size}")
}

fn collect_policy_to_plexus(policy: &InputCollectPolicy) -> String {
    let max_batch_size = match &policy.max_batch_size {
        Some(size) => format!(" MAX BATCH SIZE {size}"),
        None => String::new(),
    };
    format!("COLLECT FOR {}{max_batch_size}", policy.collect_for)
}

impl MessageErrorPolicy {
    /// The complete error clause shared by canonical Plexus and inspection.
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        message_error_policy_to_plexus(self)
    }
}

impl GeneralErrorPolicy {
    /// The general failure clause shared by canonical Plexus and inspection.
    pub fn to_canonical_plexus(&self) -> &'static str {
        general_error_policy_to_plexus(self)
    }
}

impl crate::ErrorPolicies {
    /// Both configured failure clauses in their canonical order.
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        Ok(format!(
            "{} {}",
            self.message.to_canonical_plexus()?,
            self.general.to_canonical_plexus()
        ))
    }
}

fn message_error_policy_to_plexus(
    policy: &MessageErrorPolicy,
) -> error_stack::Result<String, CanonicalPlexusError> {
    Ok(match policy {
        MessageErrorPolicy::Ignore => "ON MESSAGE ERROR IGNORE".to_string(),
        MessageErrorPolicy::Log => "ON MESSAGE ERROR LOG".to_string(),
        MessageErrorPolicy::Dlq { relay, assignments } => {
            let assignments = assignments
                .iter()
                .map(|assignment| {
                    Ok(format!(
                        "{} = {}",
                        assignment_target_to_plexus(&assignment.target),
                        expression_to_plexus(&assignment.value)?
                    ))
                })
                .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
                .join(", ");
            format!(
                "ON MESSAGE ERROR SEND TO {} SET {}",
                relay.as_str(),
                assignments
            )
        }
    })
}

fn materialized_state_policy_to_plexus(
    policy: &MaterializedStatePolicy,
) -> error_stack::Result<String, CanonicalPlexusError> {
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
                        assignment_target_to_plexus(&assignment.target),
                        expression_to_plexus(&assignment.value)?
                    ))
                })
                .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
                .join(", ")
        )),
    }
}

fn general_error_policy_to_plexus(policy: &GeneralErrorPolicy) -> &'static str {
    match policy {
        GeneralErrorPolicy::Ignore => "ON GENERAL ERROR IGNORE",
        GeneralErrorPolicy::Log => "ON GENERAL ERROR LOG",
    }
}

impl CreateJunction {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_plexus(&self.from)?
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_plexus(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "DEDUPLICATE ON {}",
            self.deduplicate_on
                .iter()
                .map(expression_to_plexus)
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![
            Clause::line(prefixed_processor_inputs_to_plexus("LEFT", &self.left)?),
            Clause::line(prefixed_processor_inputs_to_plexus("RIGHT", &self.right)?),
            Clause::line(format!(
                "CORRELATE WHERE {}",
                expression_to_plexus(&self.correlate_where)?
            )),
            Clause::line(format!("MATCH {}", self.match_policy.as_ref())),
            Clause::line(format!("MAX TIME {}", self.max_time)),
            Clause::line(format!(
                "ON CORRELATION TIMEOUT {}, {}",
                correlation_timeout_action_to_plexus(&self.timeout_policy.left),
                correlation_timeout_action_to_plexus(&self.timeout_policy.right)
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

fn correlation_timeout_action_to_plexus(action: &CorrelationTimeoutAction) -> String {
    match action {
        CorrelationTimeoutAction::Drop => "DROP".to_string(),
        CorrelationTimeoutAction::SendTo { relay } => format!("SEND TO {}", relay.as_str()),
    }
}

impl CreateReorderer {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_plexus(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "BY {}",
            self.order_by
                .iter()
                .map(expression_to_plexus)
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_plexus(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "WIDTH {}",
            window_bound_to_plexus(&self.width)
        )));
        clauses.push(Clause::line(format!(
            "STEP {}",
            window_bound_to_plexus(&self.step)
        )));
        if let WindowStateLimit::MaxBytes(bytes) = self.state_limit {
            clauses.push(Clause::line(format!("MAX STATE SIZE {}B", bytes.get())));
        }
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_plexus(&self.from)?
        ))];
        clauses.extend(materialized_state_clauses(&self.materialized_state)?);
        let (reference, body) =
            emitter_destination_parts(&self.connector, &self.body, &self.envelope, None)?;
        let to = if reference.is_empty() {
            "TO".to_string()
        } else {
            format!("TO {reference}")
        };
        clauses.push(Clause::group(to, body));
        clauses.extend(route_construction_clauses(&self.construction)?);
        if let Some(batch) = &self.batch {
            clauses.push(Clause::line(emitter_batch_policy_to_plexus(batch)));
        }
        clauses.push(Clause::line(self.flush_policy.to_canonical_plexus()));
        clauses.push(Clause::line(message_error_policy_to_plexus(
            &self.error_policies.message,
        )?));
        clauses.push(Clause::line(general_error_policy_to_plexus(
            &self.error_policies.general,
        )));
        Ok(clause_statement(
            format!(
                "CREATE {} {} EMITTER {}",
                self.mode.as_ref(),
                self.connector.kind().phrase(),
                self.name.as_str()
            ),
            clauses,
        ))
    }
}

/// `BATCH MAX MESSAGES <n> MAX SIZE <bytes>`, with the size in its canonical unit.
fn emitter_batch_policy_to_plexus(policy: &EmitterBatchPolicy) -> String {
    format!("BATCH {policy}")
}

fn window_bound_to_plexus(bound: &WindowBound) -> String {
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
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_plexus(&self.from)?
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

impl<Version: Display> CreateInferencer<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_plexus(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "USING RESOURCE {} VERSION {}",
            self.resource.as_str(),
            self.resource_version
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

impl<Version: Display> CreateWasmProcessor<Version> {
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut clauses = vec![Clause::line(format!(
            "FROM {}",
            processor_inputs_to_plexus(&self.from)?
        ))];
        clauses.extend(filter_where_clause(&self.filter_where)?);
        clauses.push(Clause::line(format!(
            "USING RESOURCE {} VERSION {}",
            self.resource.as_str(),
            self.resource_version
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
        clauses.push(Clause::line(format!(
            "ON REJECTED STATE {}",
            self.rejected_state_policy.as_ref()
        )));
        clauses.push(Clause::line(
            general_error_policy_to_plexus(&self.global_error_policy).replace("GENERAL", "GLOBAL"),
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
) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
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
                expression_to_plexus(&mapping.expression)?
            ))
        })
        .collect()
}

fn inference_output_schema_items(
    declarations: &[InferencerTensorDeclaration],
) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
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

fn from_relay_to_plexus(
    relay: &RelayName,
    from_where: &[ProcessorInputWhere],
) -> error_stack::Result<String, CanonicalPlexusError> {
    let where_suffix = from_where
        .iter()
        .find(|item| item.relay == *relay)
        .map(|item| {
            expression_to_plexus(&item.where_clause).map(|condition| format!(" WHERE {condition}"))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(format!("{}{where_suffix}", relay.as_str()))
}

fn processor_inputs_to_plexus(
    inputs: &ProcessorInputs,
) -> error_stack::Result<String, CanonicalPlexusError> {
    let relays = inputs
        .from
        .iter()
        .map(|relay| from_relay_to_plexus(relay, &inputs.r#where))
        .collect::<Result<Vec<_>, _>>()
        .map(|items| items.join(", "))?;
    let collect = match &inputs.collect_policy {
        Some(policy) => format!(" {}", collect_policy_to_plexus(policy)),
        None => String::new(),
    };
    Ok(format!("{relays}{collect}"))
}

fn prefixed_processor_inputs_to_plexus(
    prefix: &str,
    inputs: &ProcessorInputs,
) -> error_stack::Result<String, CanonicalPlexusError> {
    Ok(format!(
        "{prefix} FROM {}",
        processor_inputs_to_plexus(inputs)?
    ))
}

/// The optional `FILTER WHERE` clause of a processor.
fn filter_where_clause(
    filter_where: &Option<Expression>,
) -> error_stack::Result<Option<Clause>, CanonicalPlexusError> {
    filter_where
        .as_ref()
        .map(|condition| {
            Ok(Clause::line(format!(
                "FILTER WHERE {}",
                expression_to_plexus(condition)?
            )))
        })
        .transpose()
}

/// One clause per materialized-state dependency, in declaration order.
fn materialized_state_clauses(
    dependencies: &[MaterializedStateDependency],
) -> error_stack::Result<Vec<Clause>, CanonicalPlexusError> {
    dependencies
        .iter()
        .map(|dependency| {
            let policy = materialized_state_policy_to_plexus(&dependency.policy)?;
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
) -> error_stack::Result<Vec<Clause>, CanonicalPlexusError> {
    let mut clauses = vec![Clause::line(branch_selection_to_plexus(branched_by))];
    clauses.extend(materialized_state_clauses(materialized_state)?);
    clauses.extend(processor_outputs_clauses(outputs)?);
    Ok(clauses)
}

/// The routes of a processor, each as a `TO <relay>` group.
fn processor_outputs_clauses(
    outputs: &ProcessorOutputs,
) -> error_stack::Result<Vec<Clause>, CanonicalPlexusError> {
    outputs.routes.iter().map(processor_output_clause).collect()
}

/// One route: the relay it targets, then the clauses that construct and emit its messages.
fn processor_output_clause(
    output: &crate::ProcessorOutput,
) -> error_stack::Result<Clause, CanonicalPlexusError> {
    let mut nested = route_construction_clauses(&output.construction)?;

    if let Some(branch) = &output.branch {
        nested.push(Clause::line(output_branch_to_plexus(branch)?));
    }
    if let Some(policy) = &output.flush_policy {
        nested.push(Clause::line(policy.to_canonical_plexus()));
    }
    nested.push(Clause::line(message_error_policy_to_plexus(
        &output.message_error_policy,
    )?));

    Ok(Clause::group(
        format!("TO {}", output.relay.as_str()),
        nested,
    ))
}

fn processor_output_to_plexus(
    output: &crate::ProcessorOutput,
) -> error_stack::Result<String, CanonicalPlexusError> {
    let flush = match &output.flush_policy {
        Some(policy) => format!(" {}", policy.to_canonical_plexus()),
        None => String::new(),
    };
    let construction = route_construction_to_plexus(&output.construction)?;
    let construction = if construction.is_empty() {
        String::new()
    } else {
        format!(" {construction}")
    };
    let branch = match &output.branch {
        Some(branch) => format!(" {}", output_branch_to_plexus(branch)?),
        None => String::new(),
    };
    Ok(format!(
        "TO {}{}{}{} {}",
        output.relay.as_str(),
        construction,
        branch,
        flush,
        message_error_policy_to_plexus(&output.message_error_policy)?
    ))
}

fn schema_field_to_plexus(
    field: &SchemaField,
) -> error_stack::Result<String, CanonicalPlexusError> {
    Ok(format!(
        "{} {}{}{}",
        field.name.as_str(),
        parse_as_to_keyword(&field.ty),
        optional_suffix(field.optional),
        sensitive_suffix(field.sensitive)
    ))
}

fn alter_deduplicator_operation_to_plexus(
    operation: &AlterDeduplicatorOperation,
) -> error_stack::Result<String, CanonicalPlexusError> {
    match operation {
        AlterDeduplicatorOperation::Processor(operation) => {
            alter_processor_operation_to_plexus(operation)
        }
        AlterDeduplicatorOperation::SetDeduplicateOn { expressions } => Ok(format!(
            "SET DEDUPLICATE ON {}",
            expressions
                .iter()
                .map(expression_to_plexus)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        AlterDeduplicatorOperation::SetMaxTime { max_time } => {
            Ok(format!("SET MAX TIME {max_time}"))
        }
    }
}

fn alter_reorderer_operation_to_plexus(
    operation: &AlterReordererOperation,
) -> error_stack::Result<String, CanonicalPlexusError> {
    match operation {
        AlterReordererOperation::Processor(operation) => {
            alter_processor_operation_to_plexus(operation)
        }
        AlterReordererOperation::SetOrderBy { expressions } => Ok(format!(
            "SET BY {}",
            expressions
                .iter()
                .map(expression_to_plexus)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        AlterReordererOperation::SetMaxTime { max_time } => Ok(format!("SET MAX TIME {max_time}")),
    }
}

fn alter_generator_operation_to_plexus(
    operation: &AlterGeneratorOperation,
) -> error_stack::Result<String, CanonicalPlexusError> {
    match operation {
        AlterGeneratorOperation::SetMaterializedState { relay } => {
            Ok(format!("SET MATERIALIZED STATE {}", relay.as_str()))
        }
        AlterGeneratorOperation::SetEach { each } => {
            Ok(format!("SET EACH {}", duration_literal(each.as_nanos())))
        }
        AlterGeneratorOperation::SetBranching { branching } => {
            Ok(format!("SET {}", branch_selection_to_plexus(branching)))
        }
        AlterGeneratorOperation::AddRoute { route } => {
            Ok(format!("ADD ROUTE {}", processor_output_to_plexus(route)?))
        }
        AlterGeneratorOperation::DropRoute { relay } => {
            Ok(format!("DROP ROUTE TO {}", relay.as_str()))
        }
        AlterGeneratorOperation::ReplaceRoute { route } => Ok(format!(
            "REPLACE ROUTE {}",
            processor_output_to_plexus(route)?
        )),
    }
}

fn alter_processor_operation_to_plexus(
    operation: &AlterProcessorOperation,
) -> error_stack::Result<String, CanonicalPlexusError> {
    match operation {
        AlterProcessorOperation::AddFrom {
            relay,
            where_clause,
        } => {
            let where_suffix = match where_clause {
                Some(expression) => format!(" WHERE {}", expression_to_plexus(expression)?),
                None => String::new(),
            };
            Ok(format!("ADD FROM {}{where_suffix}", relay.as_str()))
        }
        AlterProcessorOperation::DropFrom { relay } => Ok(format!("DROP FROM {}", relay.as_str())),
        AlterProcessorOperation::AlterFromSetWhere {
            relay,
            where_clause,
        } => Ok(format!(
            "ALTER FROM {} SET WHERE {}",
            relay.as_str(),
            expression_to_plexus(where_clause)?
        )),
        AlterProcessorOperation::AlterFromDropWhere { relay } => {
            Ok(format!("ALTER FROM {} DROP WHERE", relay.as_str()))
        }
        AlterProcessorOperation::SetCollect { policy } => {
            Ok(format!("SET {}", collect_policy_to_plexus(policy)))
        }
        AlterProcessorOperation::DropCollect => Ok("DROP COLLECT".to_string()),
        AlterProcessorOperation::SetFilterWhere { where_clause } => Ok(format!(
            "SET FILTER WHERE {}",
            expression_to_plexus(where_clause)?
        )),
        AlterProcessorOperation::DropFilterWhere => Ok("DROP FILTER WHERE".to_string()),
        AlterProcessorOperation::SetMode { mode } => Ok(format!("SET {}", mode.as_ref())),
        AlterProcessorOperation::SetBranching { branching } => {
            Ok(format!("SET {}", branch_selection_to_plexus(branching)))
        }
        AlterProcessorOperation::AddMaterializedState { dependency } => Ok(format!(
            "ADD MATERIALIZED STATE {} {}",
            dependency.relay.as_str(),
            materialized_state_policy_to_plexus(&dependency.policy)?
        )),
        AlterProcessorOperation::DropMaterializedState { relay } => {
            Ok(format!("DROP MATERIALIZED STATE {}", relay.as_str()))
        }
        AlterProcessorOperation::AlterMaterializedState { relay, policy } => Ok(format!(
            "ALTER MATERIALIZED STATE {} SET {}",
            relay.as_str(),
            materialized_state_policy_to_plexus(policy)?
        )),
        AlterProcessorOperation::AddRoute { route } => {
            Ok(format!("ADD ROUTE {}", processor_output_to_plexus(route)?))
        }
        AlterProcessorOperation::DropRoute { relay } => {
            Ok(format!("DROP ROUTE TO {}", relay.as_str()))
        }
        AlterProcessorOperation::ReplaceRoute { route } => Ok(format!(
            "REPLACE ROUTE {}",
            processor_output_to_plexus(route)?
        )),
    }
}

fn alter_emitter_operation_to_plexus(
    operation: &AlterEmitterOperation,
) -> error_stack::Result<String, CanonicalPlexusError> {
    match operation {
        AlterEmitterOperation::AddFrom {
            relay,
            where_clause,
        } => {
            let where_clause = match where_clause {
                Some(where_clause) => format!(" WHERE {}", expression_to_plexus(where_clause)?),
                None => String::new(),
            };
            Ok(format!("ADD FROM {}{where_clause}", relay.as_str()))
        }
        AlterEmitterOperation::DropFrom { relay } => Ok(format!("DROP FROM {}", relay.as_str())),
        AlterEmitterOperation::AlterFromSetWhere {
            relay,
            where_clause,
        } => Ok(format!(
            "ALTER FROM {} SET WHERE {}",
            relay.as_str(),
            expression_to_plexus(where_clause)?
        )),
        AlterEmitterOperation::AlterFromDropWhere { relay } => {
            Ok(format!("ALTER FROM {} DROP WHERE", relay.as_str()))
        }
        AlterEmitterOperation::SetTo {
            destination,
            body,
            envelope,
            construction,
        } => {
            let (reference, clauses) =
                emitter_destination_parts(destination, body, envelope, construction.as_ref())?;
            let text = clause_statement(format!("SET TO {reference}"), clauses);
            Ok(text.trim_end_matches(';').to_string())
        }
        AlterEmitterOperation::SetClient { client } => {
            Ok(format!("SET CLIENT {}", client.as_str()))
        }
        AlterEmitterOperation::SetEncodeUsing { codec } => {
            Ok(format!("SET ENCODE USING {}", codec.as_str()))
        }
        AlterEmitterOperation::DropEncode => Ok("DROP ENCODE".to_string()),
        AlterEmitterOperation::SetCollect { policy } => {
            Ok(format!("SET {}", collect_policy_to_plexus(policy)))
        }
        AlterEmitterOperation::DropCollect => Ok("DROP COLLECT".to_string()),
        AlterEmitterOperation::SetAttachment { mode } => Ok(format!("SET {}", mode.as_ref())),
        AlterEmitterOperation::SetPublishingMode { mode } => Ok(format!(
            "SET MODE {}",
            mode.publishing().to_canonical_plexus()
        )),
        AlterEmitterOperation::SetBatch { policy } => {
            Ok(format!("SET {}", emitter_batch_policy_to_plexus(policy)))
        }
        AlterEmitterOperation::DropBatch => Ok("DROP BATCH".to_string()),
        AlterEmitterOperation::SetFlush { flush_policy } => {
            Ok(format!("SET {}", flush_policy.to_canonical_plexus()))
        }
        AlterEmitterOperation::SetCommit {
            commit_each,
            max_commit_size,
        } => Ok(format!(
            "SET {}",
            commit_policy_to_plexus(commit_each, max_commit_size)
        )),
    }
}

/// The body an ingestor's statement writes after `FROM` for one entity's source, without the
/// connector its head already names.
pub(crate) trait IngestorBodyPlexus {
    fn body_to_plexus(&self) -> String;
}

/// The canonical Plexus of every operation of an alteration, in order.
pub(crate) fn alter_ingestor_operations_to_plexus<Source: IngestorBodyPlexus>(
    operations: &[AlterIngestorOperation<Source>],
) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
    let mut rendered = Vec::with_capacity(operations.len());
    for operation in operations {
        rendered.push(alter_ingestor_operation_to_plexus(operation)?);
    }
    Ok(rendered)
}

/// The canonical Plexus of every operation of an alteration of a transport's ingestor, in order.
pub(crate) fn alter_transport_ingestor_operations_to_plexus<Source: IngestorBodyPlexus>(
    operations: &[AlterTransportIngestorOperation<Source>],
) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
    let mut rendered = Vec::with_capacity(operations.len());
    for operation in operations {
        let operation = match operation {
            AlterTransportIngestorOperation::Ingestor(operation) => {
                alter_ingestor_operation_to_plexus(operation)?
            }
            AlterTransportIngestorOperation::SetDecodeUsing { codec } => {
                format!("SET DECODE USING {}", codec.as_str())
            }
        };
        rendered.push(operation);
    }
    Ok(rendered)
}

fn alter_ingestor_operation_to_plexus<Source: IngestorBodyPlexus>(
    operation: &AlterIngestorOperation<Source>,
) -> error_stack::Result<String, CanonicalPlexusError> {
    match operation {
        AlterIngestorOperation::SetSource { source } => {
            Ok(format!("SET FROM {}", source.body_to_plexus()))
        }
        AlterIngestorOperation::SetQuiesce { quiesce } => {
            Ok(format!("SET QUIESCE {}", ingest_quiesce_to_plexus(quiesce)))
        }
        AlterIngestorOperation::SetTimestamp { source } => Ok(match source {
            IngestTimestampSource::Now => "SET TIMESTAMP NOW".to_string(),
            IngestTimestampSource::At(field) => {
                format!("SET TIMESTAMP AT {}", field.as_str())
            }
            IngestTimestampSource::AtMetadata(field) => {
                format!("SET TIMESTAMP AT metadata.{}", field.as_str())
            }
        }),
        AlterIngestorOperation::DropTimestamp => Ok("DROP TIMESTAMP".to_string()),
        AlterIngestorOperation::SetFilterWhere { where_clause } => Ok(format!(
            "SET FILTER WHERE {}",
            expression_to_plexus(where_clause)?
        )),
        AlterIngestorOperation::DropFilterWhere => Ok("DROP FILTER WHERE".to_string()),
        AlterIngestorOperation::AddRoute { route } => {
            Ok(format!("ADD ROUTE {}", processor_output_to_plexus(route)?))
        }
        AlterIngestorOperation::DropRoute { relay } => {
            Ok(format!("DROP ROUTE TO {}", relay.as_str()))
        }
        AlterIngestorOperation::ReplaceRoute { route } => Ok(format!(
            "REPLACE ROUTE {}",
            processor_output_to_plexus(route)?
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

fn alter_schema_operation_to_plexus(
    operation: &AlterSchemaOperation,
) -> error_stack::Result<String, CanonicalPlexusError> {
    match operation {
        AlterSchemaOperation::AddField { field } => {
            Ok(format!("ADD FIELD {}", schema_field_to_plexus(field)?))
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

fn wire_schema_to_plexus<T>(
    format_kw: &str,
    schema: &CreateWireSchema<T>,
) -> error_stack::Result<String, CanonicalPlexusError>
where
    T: NativeTypeToPlexus,
{
    let fields = schema
        .fields
        .iter()
        .map(wire_schema_field_to_plexus::<T>)
        .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?;

    Ok(block_statement(
        format!(
            "CREATE WIRE {format_kw} SCHEMA {} MODE {}",
            schema.name.as_str(),
            schema.strictness.as_ref()
        ),
        fields,
    ))
}

fn alter_wire_schema_to_plexus<T>(
    format_kw: &str,
    alter: &AlterWireSchema<T>,
) -> error_stack::Result<String, CanonicalPlexusError>
where
    T: NativeTypeToPlexus,
{
    let operations = alter
        .operations
        .iter()
        .map(alter_wire_schema_operation_to_plexus::<T>)
        .collect::<error_stack::Result<Vec<_>, CanonicalPlexusError>>()?
        .join(", ");
    Ok(format!(
        "ALTER WIRE {format_kw} SCHEMA {} {operations};",
        alter.schema.as_str()
    ))
}

fn alter_wire_schema_operation_to_plexus<T>(
    operation: &AlterWireSchemaOperation<T>,
) -> error_stack::Result<String, CanonicalPlexusError>
where
    T: NativeTypeToPlexus,
{
    match operation {
        AlterWireSchemaOperation::SetMode { mode } => Ok(format!("MODE {}", mode.as_ref())),
        AlterWireSchemaOperation::AddField { field } => {
            Ok(format!("ADD FIELD {}", wire_schema_field_to_plexus(field)?))
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
            ty.to_plexus_keyword()
        )),
        AlterWireSchemaOperation::SetFieldOptional { field, optional } => Ok(format!(
            "ALTER FIELD {} {} OPTIONAL",
            field.as_str(),
            if *optional { "SET" } else { "DROP" }
        )),
    }
}

fn wire_schema_field_to_plexus<T>(
    field: &WireSchemaField<T>,
) -> error_stack::Result<String, CanonicalPlexusError>
where
    T: NativeTypeToPlexus,
{
    Ok(format!(
        "{} {}{}",
        field.name.as_str(),
        field.ty.to_plexus_keyword(),
        optional_suffix(field.optional)
    ))
}

fn optional_suffix(optional: bool) -> &'static str {
    if optional { " OPTIONAL" } else { "" }
}

impl ClientConfigEntry {
    fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let key = string_literal(&self.key);
        let value = string_literal(&self.value);
        Ok(format!("{key} = {value}"))
    }
}

pub fn ingest_quiesce_to_plexus(quiesce: &crate::IngestQuiesceMode) -> String {
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

impl ClientIngestMode {
    /// The mode clause of a client source, after its `MODE` keyword.
    pub fn to_canonical_plexus(&self) -> String {
        let Self {
            window,
            ack_timeout,
            retry_policy,
        } = self;
        format!(
            "ACK {} ACK TIMEOUT {ack_timeout} RETRY POLICY {}",
            ack_window_to_plexus(window),
            retry_policy_to_plexus(retry_policy)
        )
    }
}

/// The body an ingestor writes after FROM, projected from its current entity Model.
pub fn ingestor_input_body_to_plexus(input: &IngestorInput) -> String {
    input.entity_presentation().ingestor_body_to_plexus()
}

fn retry_policy_to_plexus(policy: &RetryPolicy) -> String {
    format!("BACKOFF {} MAX {}", policy.backoff, policy.max_backoff)
}

fn ack_window_to_plexus(window: &AckWindow) -> String {
    match window {
        AckWindow::Sequential => "SEQUENTIAL".to_string(),
        AckWindow::Parallel { max } => format!("PARALLEL MAX {max}"),
    }
}

impl EmitterPublishing<'_> {
    pub fn to_canonical_plexus(&self) -> String {
        let retry =
            |policy: &RetryPolicy| format!("RETRY POLICY {}", retry_policy_to_plexus(policy));
        let confirmed =
            |prefix: &str, window: &AckWindow, ack_timeout: &str, policy: &RetryPolicy| {
                format!(
                    "{prefix} {} ACK TIMEOUT {ack_timeout} {}",
                    ack_window_to_plexus(window),
                    retry(policy)
                )
            };
        match self {
            EmitterPublishing::ClientAck {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("ACK", window, ack_timeout, retry_policy),
            EmitterPublishing::NoAck { retry_policy } => {
                format!("NO ACK {}", retry(retry_policy))
            }
            EmitterPublishing::BrokerAck {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("ACK", window, ack_timeout, retry_policy),
            EmitterPublishing::MqttQos0 { retry_policy } => {
                format!("QOS 0 {}", retry(retry_policy))
            }
            EmitterPublishing::MqttQos1 {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("QOS 1 ACK", window, ack_timeout, retry_policy),
            EmitterPublishing::MqttQos2 {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("QOS 2 ACK", window, ack_timeout, retry_policy),
            EmitterPublishing::NatsJetStream {
                window,
                ack_timeout,
                retry_policy,
            } => confirmed("JETSTREAM ACK", window, ack_timeout, retry_policy),
            EmitterPublishing::RequestAck { retry_policy } => {
                format!("ACK {}", retry(retry_policy))
            }
        }
    }
}

/// The entity's reference and options, followed by its typed MODE, complete payload and envelope.
fn emitter_destination_parts(
    destination: &EmitterEntity,
    body: &EmitterBody,
    envelope: &EmitterEnvelope,
    construction: Option<&RouteConstruction>,
) -> error_stack::Result<(String, Vec<Clause>), CanonicalPlexusError> {
    let presentation = destination.entity_presentation()?;
    let reference = presentation.reference_to_plexus();
    let mut clauses = vec![Clause::line(format!(
        "MODE {}",
        destination.publishing().to_canonical_plexus()
    ))];
    match body {
        EmitterBody::Codec { codec } => {
            clauses.push(Clause::line(format!("ENCODE USING {}", codec.as_str())))
        }
        EmitterBody::WithoutBody => clauses.push(Clause::line("WITHOUT BODY".to_string())),
        EmitterBody::Client => {}
        EmitterBody::Values => {
            let mappings = destination.value_mappings();
            clauses.push(Clause::braced(
                "VALUES",
                value_mapping_items(mappings.values)?,
            ));
            clauses.extend(presentation.payload_clauses());
        }
    }
    clauses.extend(envelope_clauses(envelope)?);
    if let Some(construction) = construction {
        clauses.extend(route_construction_clauses(construction)?);
    }
    Ok((reference, clauses))
}

/// The envelope clauses an emitter writes and the properties it sets, in the order a statement
/// writes them: each clause on its own line, then the `PROPERTIES` block.
fn envelope_clauses(
    envelope: &EmitterEnvelope,
) -> error_stack::Result<Vec<Clause>, CanonicalPlexusError> {
    let mut clauses = Vec::new();
    for text in envelope_clause_texts(envelope)? {
        clauses.push(Clause::line(text));
    }
    if !envelope.properties.is_empty() {
        clauses.push(Clause::braced(
            "PROPERTIES",
            envelope_property_items(envelope)?,
        ));
    }
    Ok(clauses)
}

/// Each clause an envelope writes, as canonical Plexus, in the order a statement writes them.
fn envelope_clause_texts(
    envelope: &EmitterEnvelope,
) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
    let mut texts = Vec::new();
    for clause in envelope.clauses() {
        let keywords = clause.kind().keywords();
        let text = match clause {
            EnvelopeClause::Persistent | EnvelopeClause::Mandatory | EnvelopeClause::Retain => {
                keywords.to_string()
            }
            EnvelopeClause::DeliverAfter(duration) | EnvelopeClause::Delay(duration) => {
                format!("{keywords} {duration}")
            }
            EnvelopeClause::Timestamp(expression)
            | EnvelopeClause::ReplyTo(expression)
            | EnvelopeClause::MessageId(expression)
            | EnvelopeClause::DeduplicationId(expression) => {
                format!("{keywords} {}", expression_to_plexus(expression)?)
            }
        };
        texts.push(text);
    }
    Ok(texts)
}

/// Each property an envelope sets, as `'<key>' = <expression>`, in written order.
fn envelope_property_items(
    envelope: &EmitterEnvelope,
) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
    let mut items = Vec::with_capacity(envelope.properties.len());
    for property in &envelope.properties {
        items.push(format!(
            "{} = {}",
            string_literal(&property.name),
            expression_to_plexus(&property.expression)?
        ));
    }
    Ok(items)
}

impl EmitterEnvelope {
    /// The envelope section as one line of canonical Plexus, as `DESCRIBE EMITTER` prints it: its
    /// clauses in the order a statement writes them, then its `PROPERTIES` block. The empty
    /// envelope is the empty text.
    pub fn to_canonical_plexus(&self) -> error_stack::Result<String, CanonicalPlexusError> {
        let mut parts = envelope_clause_texts(self)?;
        if !self.properties.is_empty() {
            let items = envelope_property_items(self)?;
            parts.push(format!("PROPERTIES {{{}}}", items.join(", ")));
        }
        Ok(parts.join(" "))
    }
}

impl PostgresConflictAction {
    /// The `ON CONFLICT` clause the action writes after the emitter's `VALUES`, absent for an
    /// emitter that writes none.
    fn conflict_clause(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::DoNothing { target } => {
                let target = conflict_target_to_plexus(target);
                Some(format!("ON CONFLICT{target} DO NOTHING"))
            }
            Self::DoUpdate { target } => {
                let target = conflict_target_to_plexus(target);
                Some(format!("ON CONFLICT{target} DO UPDATE"))
            }
        }
    }
}

impl MySqlConflictAction {
    /// The `ON CONFLICT` clause the action writes after the emitter's `VALUES`, absent for an
    /// emitter that writes none.
    fn conflict_clause(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::DoNothing => Some("ON CONFLICT DO NOTHING".to_string()),
            Self::DoUpdate => Some("ON CONFLICT DO UPDATE".to_string()),
        }
    }
}

impl MongoDbConflictAction {
    /// The `ON CONFLICT` clause the action writes after the emitter's `VALUES`, absent for an
    /// emitter that writes none.
    fn conflict_clause(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::DoNothing { target } => {
                let target = conflict_target_to_plexus(target);
                Some(format!("ON CONFLICT{target} DO NOTHING"))
            }
            Self::DoUpdate { target } => {
                let target = conflict_target_to_plexus(target);
                Some(format!("ON CONFLICT{target} DO UPDATE"))
            }
        }
    }
}

fn conflict_target_to_plexus(target: &[String]) -> String {
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
        ParseAsType::Bytes => "BYTES".to_string(),
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
/// an `I64`. Plexus decides float-versus-integer purely on the presence of a decimal point.
fn float_literal(literal: Float64Literal) -> error_stack::Result<String, CanonicalPlexusError> {
    let value = literal.value();
    if !value.is_finite() {
        return Err(Report::new(CanonicalPlexusError::UnrepresentableFloat {
            value: literal,
        }));
    }

    let rendered = value.to_string();
    if rendered.contains(['.', 'e', 'E']) {
        Ok(rendered)
    } else {
        Ok(format!("{rendered}.0"))
    }
}

/// Renders a duration as one whole number of the largest unit that divides it
/// exactly, the only form a Plexus duration literal reads.
///
/// A duration's `Display` spells every component, as in `1s 500ms`, which a duration literal cannot
/// hold, so `1500ms` would otherwise render as text that no longer parses. Zero is written in seconds.
fn duration_literal(nanos: impl Into<u128>) -> String {
    let nanos = nanos.into();
    const UNITS: [(u128, &str); 6] = [
        (86_400_000_000_000, "d"),
        (3_600_000_000_000, "h"),
        (60_000_000_000, "m"),
        (1_000_000_000, "s"),
        (1_000_000, "ms"),
        (1_000, "us"),
    ];

    if nanos == 0 {
        return "0s".to_string();
    }
    for (scale, unit) in UNITS {
        if nanos.is_multiple_of(scale) {
            return format!("{}{unit}", nanos / scale);
        }
    }
    format!("{nanos}ns")
}

/// Renders a stored byte count using the largest binary prefix that divides it exactly.
///
/// Byte sizes elsewhere in Plexus keep the author's spelling, but WASM memory limits are stored as a
/// count, so `64MiB` would otherwise come back as `67108864B`. Only exact multiples take a prefix,
/// which keeps the rendering total, deterministic, and lossless.
fn byte_size_literal(bytes: NonZeroU64) -> String {
    let bytes = bytes.get();
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

/// Wraps `value` in a dollar-quoted delimiter that first occurs where the closing delimiter
/// starts.
///
/// Plexus dollar-quoting is verbatim: the body needs no escaping, so this represents any string,
/// including one spanning several lines or mixing quote styles. A lexer ends the body at the first
/// delimiter it reads, so the tag escalates until neither the body nor the body running into the
/// closing delimiter completes one early: a body ending in `$s` would otherwise close on the `$s$`
/// that its end and the closing `s$` form.
fn dollar_quote(value: &str, tag: &str) -> String {
    let mut delimiter = format!("${tag}$");
    let mut suffix = 1_u64;
    while format!("{value}{delimiter}").find(&delimiter) != Some(value.len()) {
        delimiter = format!("${tag}_{suffix}$");
        suffix = suffix
            .checked_add(1)
            .assured("a tag escalates at most once per byte of the body it quotes");
    }

    format!("{delimiter}{value}{delimiter}")
}

/// Renders `value` as a Plexus string literal.
///
/// Single quotes are preferred, double quotes carry an embedded apostrophe, and anything the quoted
/// forms cannot express verbatim -- a newline, or both quote styles at once -- falls back to
/// dollar-quoting. Every string is therefore representable, and Plexus reads a string literal
/// verbatim wherever it appears, so the result reads back as exactly `value`.
pub fn string_literal(value: &str) -> String {
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

trait NativeTypeToPlexus {
    fn to_plexus_keyword(&self) -> String;
}

impl<T> NativeTypeToPlexus for T
where
    T: AsRef<str>,
{
    fn to_plexus_keyword(&self) -> String {
        self.as_ref().to_ascii_uppercase()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use meticulous::ResultExt as _;
    use nonzero_ext::nonzero;

    use crate::{
        AckMode, AckWindow, AvroType, BinaryOperator, BranchSelection, ClientIngestMode,
        ClientPoolBounds, ClientProducerIngestor, ClientResourceMount, CodecEncoding,
        CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
        CodecWireFormat, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
        CreateClientHttp, CreateClientKafka, CreateClientMqtt, CreateClientNats,
        CreateClientPostgres, CreateClientPrometheus, CreateClientRabbitMq, CreateClientRedis,
        CreateClientSentry, CreateClientSqs, CreateClientSyslog, CreateClientWebsockets,
        CreateClientZeroMq, CreateCodec, CreateCorrelator, CreateDeduplicator, CreateEmitter,
        CreateEndpoint, CreateIngestor, CreateJunction, CreatePlacement, CreateReingestor,
        CreateRelay, CreateSchema, CreateSignalingProtocol, CreateUdf, CreateVhost,
        CreateWindowProcessor, CreateWireSchema, EmitterBody, EmitterEntity, EndpointIngestMode,
        EndpointType, ErrorPolicies, Expression, FieldScope, FlushPolicy, GeneralErrorPolicy,
        HttpConfigEntry, HttpEndpointIngestor, HttpRequestIngestMode, HttpRequestIngestor,
        IngestorEntity, JsonType, KafkaConfigEntry, KafkaEmitter, KafkaIngestMode, KafkaOffsetMode,
        KafkaTopicEmitter, KafkaTopicIngestor, Literal, MessageErrorPolicy, Model,
        MongoDbCollectionEmitter, MongoDbConflictAction, MongoDbEmitter, MongoDbValueMapping,
        MqttEmitter, MqttIngestMode, MqttQos, MqttSession, MqttTopicEmitter, MqttTopicIngestor,
        MySqlConflictAction, MySqlEmitter, MySqlTableEmitter, MySqlValueMapping, NatsEmitter,
        NatsIngestMode, NatsSubjectEmitter, NatsSubjectIngestor, OutputBranch, ParseAsType,
        PlacementPolicy, PostgresConflictAction, PostgresEmitter, PostgresTableEmitter,
        PostgresValueMapping, ProcessorInputs, ProcessorOutput, ProcessorOutputs,
        PrometheusConfigEntry, PrometheusIngestMode, PrometheusQueryIngestor, PulsarEmitter,
        PulsarIngestMode, PulsarSubscription, PulsarSubscriptionOwnership, PulsarTopicEmitter,
        PulsarTopicIngestor, RabbitMqEmitter, RabbitMqExchangeEmitter, RabbitMqIngestMode,
        RabbitMqQueueEmitter, RabbitMqQueueIngestor, RedisEmitter, RedisPubSubEmitter,
        RedisPubSubIngestMode, RedisPubSubIngestor, RelayBranching, RetryPolicy, RouteConstruction,
        SchemaField, SentryConfigEntry, SentryEmitter, SentryEventEmitter, SignalingProtobufConfig,
        SignalingStep, SignalingWaitStep, SignalingWireFormat, SqsEmitter, SqsIngestMode,
        SqsQueueEmitter, SqsQueueIngestor, SyslogCollectorEmitter, SyslogEmitter, SyslogIngestMode,
        SyslogListenerIngestor, UdfArgument, UdfLanguage, UdfReturn, WebsocketsConnectionIngestor,
        WebsocketsEndpointIngestor, WebsocketsIngestMode, WindowBound, WindowStateLimit,
        WireSchemaField, ZeroMqEmitter, ZeroMqIngestMode, ZeroMqPullIngestor, ZeroMqPushEmitter,
        expression_to_plexus,
    };

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn batch_policy(max_messages: u32, max_size: &str) -> crate::EmitterBatchPolicy {
        crate::EmitterBatchPolicy {
            max_messages: crate::BatchMessageLimit::try_from(max_messages)
                .assured("the fixture message limit is within range"),
            max_size: max_size
                .parse()
                .assured("the fixture size is a whole number of bytes"),
        }
    }

    fn flushed_output(relay: &str, construction: Option<RouteConstruction>) -> ProcessorOutput {
        let mut output = ProcessorOutput::with_flush_policy(
            named(relay),
            FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
        );
        output.construction = construction.unwrap_or_default();
        output
    }

    fn bare_field(name: &str) -> Expression {
        Expression::Field(crate::FieldReference::bare(named(name)))
    }

    fn scoped_field(scope: FieldScope, name: &str) -> Expression {
        Expression::Field(crate::FieldReference::scoped(scope, named(name)))
    }

    fn string_value(value: &str) -> Expression {
        Expression::Literal(Literal::String(value.to_string()))
    }

    fn call(name: &str, arguments: Vec<Expression>) -> Expression {
        Expression::Call {
            function: named(name),
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
                target: crate::AssignmentTarget::bare(named(field)),
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
        BranchSelection::branched_by(named(&format!("by_{schema}")))
    }

    fn config_entry(key: &str, value: &str) -> KafkaConfigEntry {
        KafkaConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn pool_bounds(minimum: u32, maximum: u32) -> ClientPoolBounds {
        ClientPoolBounds::new(
            minimum,
            NonZeroU32::new(maximum).expect("test maximum is positive"),
        )
        .expect("test minimum does not exceed its maximum")
    }

    #[test]
    fn renders_wire_schema_canonical() {
        let schema: Model = Model::WireAvroSchema(CreateWireSchema {
            name: named("latency"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("p99"),
                    ty: AvroType::Double,
                    optional: false,
                },
                WireSchemaField {
                    name: named("created_at"),
                    ty: AvroType::String,
                    optional: false,
                },
            ],
        });

        let plexus = schema.to_canonical_plexus().expect("must render");
        assert_eq!(
            plexus,
            "CREATE WIRE AVRO SCHEMA latency MODE STRICT (\n  p99 DOUBLE,\n  created_at STRING\n);"
        );
    }

    #[test]
    fn renders_internal_schema_canonical() {
        let schema = CreateSchema {
            name: named("latency"),
            fields: vec![
                SchemaField {
                    name: named("p99"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("created_at"),
                    ty: ParseAsType::Datetime,
                    optional: false,
                    sensitive: false,
                },
            ],
        };

        let plexus = schema.to_canonical_plexus().expect("must render");
        assert_eq!(
            plexus,
            "CREATE SCHEMA latency (\n  p99 F64,\n  created_at DATETIME\n);"
        );
    }

    #[test]
    fn renders_multidimensional_internal_arrays_canonical() {
        let schema = CreateSchema {
            name: named("tensors"),
            fields: vec![SchemaField {
                name: named("matrix"),
                ty: ParseAsType::Array {
                    len: nonzero!(2u32),
                    element: Box::new(ParseAsType::Array {
                        len: nonzero!(3u32),
                        element: Box::new(ParseAsType::F32),
                    }),
                },
                optional: false,
                sensitive: false,
            }],
        };

        assert_eq!(
            schema.to_canonical_plexus().expect("must render"),
            "CREATE SCHEMA tensors (\n  matrix ARRAY<F32, 2, 3>\n);"
        );
    }

    #[test]
    fn renders_transport_values_as_string_literals() {
        let model: CreateClientKafka = CreateClientKafka {
            name: named("kafka_main"),
            mount: None,
            config: vec![
                config_entry("bootstrap.servers", "host1:9092"),
                config_entry("enable.auto.commit", "true"),
            ],
        };

        let plexus = model.to_canonical_plexus().expect("must render");
        assert_eq!(
            plexus,
            "CREATE KAFKA CLIENT kafka_main\n  CONFIG {\n    'bootstrap.servers' = \
             'host1:9092',\n    'enable.auto.commit' = 'true'\n  };"
        );
    }

    #[test]
    fn renders_config_values_that_mix_quote_styles() {
        let model: CreateClientKafka = CreateClientKafka {
            name: named("k"),
            mount: None,
            config: vec![KafkaConfigEntry {
                key: "quoted".to_string(),
                value: "both ' and \"".to_string(),
            }],
        };

        assert_eq!(
            model.to_canonical_plexus().expect("must render"),
            "CREATE KAFKA CLIENT k\n  CONFIG {\n    'quoted' = $s$both ' and \"$s$\n  };"
        );
    }

    #[test]
    fn canonical_error_display_includes_original_value() {
        let err = super::CanonicalPlexusError::UnrepresentableFloat {
            value: crate::Float64Literal::new(f64::INFINITY),
        };
        assert_eq!(
            err.to_string(),
            "cannot represent non-finite float in Plexus: inf"
        );
    }

    #[test]
    fn renders_json_wire_schema_canonical() {
        let schema: Model = Model::WireJsonSchema(CreateWireSchema {
            name: named("payload"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("items"),
                    ty: JsonType::Array,
                    optional: false,
                },
                WireSchemaField {
                    name: named("active"),
                    ty: JsonType::Boolean,
                    optional: false,
                },
            ],
        });

        assert_eq!(
            schema.to_canonical_plexus().expect("must render"),
            "CREATE WIRE JSON SCHEMA payload MODE STRICT (\n  items ARRAY,\n  active BOOLEAN\n);"
        );
    }

    #[test]
    fn renders_loose_cbor_wire_schema_canonical() {
        let schema: Model = Model::WireCborSchema(CreateWireSchema {
            name: named("payload"),
            strictness: crate::WireSchemaStrictness::Loose,
            fields: vec![WireSchemaField {
                name: named("active"),
                ty: JsonType::Boolean,
                optional: false,
            }],
        });

        assert_eq!(
            schema.to_canonical_plexus().expect("must render"),
            "CREATE WIRE CBOR SCHEMA payload MODE LOOSE (\n  active BOOLEAN\n);"
        );
    }

    #[test]
    fn renders_optional_schema_fields_canonical() {
        let internal = CreateSchema {
            name: named("latency"),
            fields: vec![SchemaField {
                name: named("p99"),
                ty: ParseAsType::F64,
                optional: true,
                sensitive: false,
            }],
        };
        let wire: Model = Model::WireJsonSchema(CreateWireSchema {
            name: named("payload"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("active"),
                ty: JsonType::Boolean,
                optional: true,
            }],
        });

        assert_eq!(
            internal.to_canonical_plexus().expect("must render"),
            "CREATE SCHEMA latency (\n  p99 F64 OPTIONAL\n);"
        );
        assert_eq!(
            wire.to_canonical_plexus().expect("must render"),
            "CREATE WIRE JSON SCHEMA payload MODE STRICT (\n  active BOOLEAN OPTIONAL\n);"
        );
    }

    #[test]
    fn renders_all_client_types_canonical() {
        let expectations = [
            (
                CreateClientHttp::<u64> {
                    name: named("http_main"),
                    mount: None,
                    config: vec![
                        HttpConfigEntry {
                            key: "endpoint".to_string(),
                            value: "https://example.com".to_string(),
                        },
                        HttpConfigEntry {
                            key: "timeout_ms".to_string(),
                            value: "5000".to_string(),
                        },
                    ],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE HTTP CLIENT http_main\n  CONFIG {\n    'endpoint' = 'https://example.com',\n    'timeout_ms' = '5000'\n  };",
            ),
            (
                CreateClientSentry::<u64> {
                    name: named("sentry_main"),
                    mount: None,
                    config: vec![SentryConfigEntry {
                        key: "dsn".to_string(),
                        value: "https://key@sentry.example/42".to_string(),
                    }],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE SENTRY CLIENT sentry_main\n  CONFIG {\n    'dsn' = 'https://key@sentry.example/42'\n  };",
            ),
            (
                CreateClientMqtt::<u64> {
                    name: named("mqtt_main"),
                    mount: None,
                    config: vec![config_entry("host", "mqtt.internal")],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE MQTT CLIENT mqtt_main\n  CONFIG {\n    'host' = 'mqtt.internal'\n  };",
            ),
            (
                CreateClientNats::<u64> {
                    name: named("nats_main"),
                    mount: None,
                    config: vec![config_entry("servers", "nats://localhost:4222")],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE NATS CLIENT nats_main\n  CONFIG {\n    'servers' = 'nats://localhost:4222'\n  };",
            ),
            (
                CreateClientPrometheus::<u64> {
                    name: named("prom_main"),
                    mount: None,
                    config: vec![PrometheusConfigEntry {
                        key: "url".to_string(),
                        value: "http://prometheus:9090".to_string(),
                    }],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE PROMETHEUS CLIENT prom_main\n  CONFIG {\n    'url' = 'http://prometheus:9090'\n  };",
            ),
            (
                CreateClientRabbitMq::<u64> {
                    name: named("rmq_main"),
                    mount: None,
                    config: vec![config_entry("uri", "amqp://guest:guest@localhost:5672")],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE RABBITMQ CLIENT rmq_main\n  CONFIG {\n    'uri' = 'amqp://guest:guest@localhost:5672'\n  };",
            ),
            (
                CreateClientRedis::<u64> {
                    name: named("redis_main"),
                    pool: pool_bounds(1, 4),
                    mount: None,
                    config: vec![config_entry("url", "redis://localhost:6379")],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE REDIS CLIENT redis_main\n  POOL SIZE MIN 1 MAX 4\n  CONFIG {\n    'url' = 'redis://localhost:6379'\n  };",
            ),
            (
                CreateClientPostgres::<u64> {
                    name: named("postgres_tls"),
                    pool: pool_bounds(0, 1),
                    mount: Some(ClientResourceMount {
                        resource: named("dev_tls"),
                        version: 3,
                    }),
                    config: vec![
                        config_entry(
                            "addr",
                            "postgresql://db.example.com/nervix?sslmode=verify-full",
                        ),
                        config_entry("tls_ca_file", "{{ dev_tls }}/ca.pem"),
                    ],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE POSTGRES CLIENT postgres_tls\n  POOL SIZE MIN 0 MAX 1\n  MOUNT dev_tls VERSION 3\n  CONFIG {\n    'addr' = 'postgresql://db.example.com/nervix?sslmode=verify-full',\n    'tls_ca_file' = '{{ dev_tls }}/ca.pem'\n  };",
            ),
            (
                CreateClientZeroMq::<u64> {
                    name: named("zmq_main"),
                    mount: None,
                    config: vec![config_entry("bind", "tcp://*:5555")],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE ZEROMQ CLIENT zmq_main\n  CONFIG {\n    'bind' = 'tcp://*:5555'\n  };",
            ),
            (
                CreateClientSyslog::<u64> {
                    name: named("syslog_main"),
                    mount: Some(ClientResourceMount {
                        resource: named("syslog_tls"),
                        version: 2,
                    }),
                    config: vec![
                        config_entry("protocol", "tls"),
                        config_entry("addr", "logs.example.com:6514"),
                        config_entry("tls_ca_file", "{{ syslog_tls }}/ca.pem"),
                    ],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE SYSLOG CLIENT syslog_main\n  MOUNT syslog_tls VERSION 2\n  CONFIG {\n    'protocol' = 'tls',\n    'addr' = 'logs.example.com:6514',\n    'tls_ca_file' = '{{ syslog_tls }}/ca.pem'\n  };",
            ),
            (
                CreateClientSqs::<u64> {
                    name: named("sqs_main"),
                    mount: None,
                    config: vec![config_entry("region", "us-east-1")],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE SQS CLIENT sqs_main\n  CONFIG {\n    'region' = 'us-east-1'\n  };",
            ),
            (
                CreateClientWebsockets::<u64> {
                    name: named("ws_main"),
                    mount: None,
                    signaling_protocol: None,
                    config: vec![config_entry("url", "wss://example.com/socket")],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE WEBSOCKETS CLIENT ws_main\n  CONFIG {\n    'url' = 'wss://example.com/socket'\n  };",
            ),
            (
                CreateClientWebsockets::<u64> {
                    name: named("ws_main"),
                    mount: None,
                    signaling_protocol: Some(named("binance_ws")),
                    config: vec![config_entry("url", "wss://example.com/socket")],
                }
                .to_canonical_plexus()
                .expect("must render"),
                "CREATE WEBSOCKETS CLIENT ws_main\n  WITH SIGNALING PROTOCOL binance_ws\n  CONFIG {\n    'url' = 'wss://example.com/socket'\n  };",
            ),
        ];

        for (actual, expected) in expectations {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn renders_other_model_kinds_canonical() {
        let vhost: CreateVhost = CreateVhost {
            name: named("public"),
            hostnames: vec!["example.com".to_string(), "api.example.com".to_string()],
            tls: None,
        };
        assert_eq!(
            vhost.to_canonical_plexus().expect("must render"),
            "CREATE VHOST public example.com, api.example.com;"
        );

        let tls_vhost: CreateVhost = CreateVhost {
            name: named("secure"),
            hostnames: vec!["secure.example.com".to_string()],
            tls: Some(crate::VhostTlsResource {
                resource: named("certs"),
                version: 7,
            }),
        };
        assert_eq!(
            tls_vhost.to_canonical_plexus().expect("must render"),
            "CREATE VHOST secure secure.example.com WITH TLS certs VERSION 7;"
        );

        let endpoint = CreateEndpoint {
            name: named("orders_http"),
            on_vhost: named("public"),
            path: "/orders".to_string(),
            endpoint_type: EndpointType::Http,
            signaling_protocol: None,
        };
        assert_eq!(
            endpoint.to_canonical_plexus().expect("must render"),
            "CREATE ENDPOINT orders_http ON public PATH '/orders' TYPE HTTP;"
        );
        let websocket_endpoint = CreateEndpoint {
            name: named("orders_ws"),
            on_vhost: named("public"),
            path: "/ws".to_string(),
            endpoint_type: EndpointType::Websockets,
            signaling_protocol: Some(named("binance_ws")),
        };
        assert_eq!(
            websocket_endpoint
                .to_canonical_plexus()
                .expect("must render"),
            "CREATE ENDPOINT orders_ws ON public PATH '/ws' TYPE WEBSOCKETS WITH SIGNALING \
             PROTOCOL binance_ws;"
        );

        let signaling_protocol: CreateSignalingProtocol = CreateSignalingProtocol {
            name: named("binance_ws"),
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
            signaling_protocol
                .to_canonical_plexus()
                .expect("must render"),
            r#"CREATE SIGNALING PROTOCOL binance_ws FORMAT JSON ON CONNECT SEND JAQ '{method: "SUBSCRIBE", id: 1}' WAIT JAQ '.id == 1 and .result == null' TIMEOUT 5s;"#
        );

        let protobuf_signaling_protocol: CreateSignalingProtocol = CreateSignalingProtocol {
            name: named("orders_ws"),
            format: SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                resource: named("proto_bundle"),
                resource_version: 2,
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
                .to_canonical_plexus()
                .expect("must render"),
            "CREATE SIGNALING PROTOCOL orders_ws FORMAT PROTOBUF USING RESOURCE proto_bundle \
             VERSION 2 CONFIG {'file' = 'signaling.proto'} SEND MESSAGE 'nervix.test.Subscribe' \
             WAIT MESSAGE 'nervix.test.Ack' FAIL JAQ '.error' ON CONNECT SEND JAQ '{id: 1}' WAIT \
             JAQ '.authed' FAIL JAQ '.denied' CAPTURE '{token: .token}' ACCEPT DATA SEND JAQ \
             '{id: 2, token: $state.token}' WAIT JAQ '.id == 2' TIMEOUT 5s;"
        );

        let codec: CreateCodec = CreateCodec {
            name: named("orders_codec"),
            wire_format: CodecWireFormat::Json {
                wire_schema: named("orders_wire"),
            },
            schema: named("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            codec.to_canonical_plexus().expect("must render"),
            "CREATE CODEC orders_codec\n  FROM WIRE JSON SCHEMA orders_wire\n  TO SCHEMA orders;"
        );

        let syslog_codec: CreateCodec = CreateCodec {
            name: named("syslog_codec"),
            wire_format: CodecWireFormat::Syslog,
            schema: named("syslog_event"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            syslog_codec.to_canonical_plexus().expect("must render"),
            "CREATE CODEC syslog_codec\n  FROM SYSLOG\n  TO SCHEMA syslog_event;"
        );

        let codec_with_encoding: CreateCodec = CreateCodec {
            name: named("orders_codec"),
            wire_format: CodecWireFormat::Json {
                wire_schema: named("orders_wire"),
            },
            schema: named("orders"),
            encoding_rules: vec![CodecEncodingRule {
                field: named("created_at"),
                encoding: CodecEncoding::Rfc3339,
            }],
        };
        assert_eq!(
            codec_with_encoding
                .to_canonical_plexus()
                .expect("must render"),
            "CREATE CODEC orders_codec\n  FROM WIRE JSON SCHEMA orders_wire\n  TO SCHEMA orders\n  ENCODE created_at AS RFC3339;"
        );

        let codec_with_jaq: CreateCodec = CreateCodec {
            name: named("orders_codec"),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: Some("{payload: .}".to_string()),
                    on_emitting_batch: None,
                },
            },
            schema: named("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            codec_with_jaq.to_canonical_plexus().expect("must render"),
            "CREATE CODEC orders_codec\n  FROM JSON\n  TO SCHEMA orders\n  WITH JAQ \
             TRANSFORMATIONS ON INGESTION '.payload' ON EMITTING '{payload: .}';"
        );

        let ingestion_codec: CreateCodec = CreateCodec {
            name: named("orders_ingestion"),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: None,
                    on_emitting_batch: None,
                },
            },
            schema: named("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            ingestion_codec.to_canonical_plexus().expect("must render"),
            "CREATE CODEC orders_ingestion\n  FROM JSON\n  TO SCHEMA orders\n  WITH JAQ \
             TRANSFORMATIONS ON INGESTION '.payload';"
        );

        let cbor_codec: CreateCodec = CreateCodec {
            name: named("orders_cbor"),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Cbor,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".".to_string()),
                    on_emitting: Some(".".to_string()),
                    on_emitting_batch: None,
                },
            },
            schema: named("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            cbor_codec.to_canonical_plexus().expect("must render"),
            "CREATE CODEC orders_cbor\n  FROM CBOR\n  TO SCHEMA orders\n  WITH JAQ \
             TRANSFORMATIONS ON INGESTION '.' ON EMITTING '.';"
        );

        let protobuf_codec: CreateCodec = CreateCodec {
            name: named("orders_proto"),
            wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: named("proto_bundle"),
                resource_version: 3,
                config: vec![crate::ClientConfigEntry {
                    key: "file".to_string(),
                    value: "order.proto".to_string(),
                }],
                message: "nervix.test.Order".to_string(),
                batch_message: None,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: Some("{payload: .}".to_string()),
                    on_emitting_batch: None,
                },
            }),
            schema: named("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            protobuf_codec.to_canonical_plexus().expect("must render"),
            "CREATE CODEC orders_proto\n  FROM PROTOBUF USING RESOURCE proto_bundle VERSION 3\n  \
             CONFIG {\n    'file' = 'order.proto'\n  }\n  MESSAGE 'nervix.test.Order'\n  TO \
             SCHEMA orders\n  WITH JAQ TRANSFORMATIONS ON INGESTION '.payload' ON EMITTING \
             '{payload: .}';"
        );

        let batching_protobuf_codec: CreateCodec = CreateCodec {
            name: named("orders_proto"),
            wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: named("proto_bundle"),
                resource_version: 3,
                config: Vec::new(),
                message: "nervix.test.Order".to_string(),
                batch_message: Some("nervix.test.OrderBatch".to_string()),
                transformations: CodecJaqTransformations {
                    on_ingestion: None,
                    on_emitting: Some("{payload: .}".to_string()),
                    on_emitting_batch: Some("{orders: .}".to_string()),
                },
            }),
            schema: named("orders"),
            encoding_rules: Vec::new(),
        };
        assert_eq!(
            batching_protobuf_codec
                .to_canonical_plexus()
                .expect("must render"),
            "CREATE CODEC orders_proto\n  FROM PROTOBUF USING RESOURCE proto_bundle VERSION 3\n  \
             CONFIG {}\n  MESSAGE 'nervix.test.Order' BATCH MESSAGE 'nervix.test.OrderBatch'\n  \
             TO SCHEMA orders\n  WITH JAQ TRANSFORMATIONS ON EMITTING '{payload: .}' ON EMITTING \
             BATCH '{orders: .}';"
        );

        let relay = CreateRelay {
            name: named("orders_stream"),
            schema: named("orders"),
            buffer: nonzero!(1usize),
            branching: RelayBranching::branched_by(named("by_orders")),
            materialized_state: None,
        };
        assert_eq!(
            relay.to_canonical_plexus().expect("must render"),
            "CREATE RELAY orders_stream SCHEMA orders BRANCHED BY by_orders CAPACITY 1;"
        );

        let relay = CreateRelay {
            name: named("orders_stream"),
            schema: named("orders"),
            buffer: nonzero!(1usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        };
        assert_eq!(
            relay.to_canonical_plexus().expect("must render"),
            "CREATE RELAY orders_stream SCHEMA orders UNBRANCHED CAPACITY 1;"
        );

        let junction = CreateJunction {
            name: named("orders_junction"),
            from: ProcessorInputs::new(vec![named("orders_a"), named("orders_b")], Vec::new())
                .with_collect_policy("25ms".to_string(), Some("2MiB".to_string())),
            output_routes: flushed_outputs("orders_all"),
            branched_by: processor_branched_by("tenant_branch"),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            junction.to_canonical_plexus().expect("must render"),
            "CREATE ATTACHED JUNCTION orders_junction\n  FROM orders_a, orders_b COLLECT FOR 25ms \
             MAX BATCH SIZE 2MiB\n  BRANCHED BY by_tenant_branch\n  TO orders_all\n    FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
        );

        let deduplicator = CreateDeduplicator {
            name: named("orders_dedup"),
            from: ProcessorInputs::single(named("orders_in")),
            output_routes: flushed_outputs("orders_out"),
            branched_by: processor_branched_by("tenant_branch"),
            deduplicate_on: vec![scoped_field(FieldScope::Input, "transaction_id")],
            max_time: "10m".to_string(),
            mode: AckMode::Detached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            deduplicator.to_canonical_plexus().expect("must render"),
            "CREATE DETACHED DEDUPLICATOR orders_dedup\n  FROM orders_in\n  DEDUPLICATE ON \
             input.transaction_id\n  MAX TIME 10m\n  BRANCHED BY by_tenant_branch\n  TO \
             orders_out\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
        );

        let correlator = CreateCorrelator {
            name: named("orders_correlator"),
            left: ProcessorInputs::new(
                vec![named("orders_left"), named("orders_left_archive")],
                Vec::new(),
            )
            .with_collect_policy("10ms".to_string(), None),
            right: ProcessorInputs::single(named("orders_right"))
                .with_collect_policy("20ms".to_string(), Some("1MiB".to_string())),
            output_routes: ProcessorOutputs::new(vec![flushed_output(
                "orders_matched",
                Some(route_set(
                    "id",
                    Expression::Field(crate::FieldReference::scoped(FieldScope::Left, named("id"))),
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
            correlator.to_canonical_plexus().expect("must render"),
            "CREATE ATTACHED CORRELATOR orders_correlator\n  LEFT FROM orders_left, \
             orders_left_archive COLLECT FOR 10ms\n  RIGHT FROM orders_right COLLECT FOR 20ms MAX \
             BATCH SIZE 1MiB\n  CORRELATE WHERE left.id = right.id\n  MATCH EARLIEST\n  MAX TIME \
             5s\n  ON CORRELATION TIMEOUT DROP, DROP\n  BRANCHED BY by_tenant_branch\n  TO \
             orders_matched\n    SET id = left.id\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    \
             ON MESSAGE ERROR LOG;"
        );

        let window_processor = CreateWindowProcessor {
            name: named("latency_window"),
            from: ProcessorInputs::single(named("orders_in")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                relay: named("orders_p99"),
                construction: route_set(
                    "latency_p99",
                    Expression::Call {
                        function: named("percentile_linear_histogram"),
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
            state_limit: WindowStateLimit::Unbounded,
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            window_processor.to_canonical_plexus().expect("must render"),
            "CREATE ATTACHED WINDOW PROCESSOR latency_window\n  FROM orders_in\n  WIDTH 100 \
             MESSAGES 10s DURATION\n  STEP 10 MESSAGES 1s DURATION\n  BRANCHED BY \
             by_tenant_branch\n  TO orders_p99\n    SET latency_p99 = \
             percentile_linear_histogram(input.latency, 99, 2048, 0, 10000, '2s')\n    ON MESSAGE \
             ERROR LOG;"
        );

        let reingestor = CreateReingestor {
            name: named("orders_repartition"),
            from: ProcessorInputs::single(named("orders_in")),
            output_routes: flushed_outputs("orders_out").with_branch(OutputBranch::Unbranched),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        assert_eq!(
            reingestor.to_canonical_plexus().expect("must render"),
            "CREATE ATTACHED REINGESTOR orders_repartition\n  FROM orders_in\n  TO orders_out\n    UNBRANCHED\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
        );

        let route_reingestor = CreateReingestor {
            name: named("orders_splitter"),
            from: ProcessorInputs::single(named("orders_in")),
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
            route_reingestor.to_canonical_plexus().expect("must render"),
            "CREATE DETACHED REINGESTOR orders_splitter\n  FROM orders_in\n  FILTER WHERE \
             active\n  TO orders_errors\n    WHERE level = 'error'\n    UNBRANCHED\n    FLUSH \
             EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  TO orders_warn\n    SET \
             severity = 'warning'\n    WHERE level = 'warn'\n    UNBRANCHED\n    FLUSH EACH 100ms \
             MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  TO orders_info\n    UNBRANCHED\n    \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
        );
    }

    #[test]
    fn renders_emitters_for_all_emitter_variants() {
        let emitters = [
            (
                EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
                    mode: crate::KafkaTopicPublishingMode::NoAck {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("kafka_main"),
                    topic: named("orders"),
                })),
                "kafka_main TOPIC 'orders'",
            ),
            (
                EmitterEntity::Pulsar(PulsarEmitter::Topic(PulsarTopicEmitter {
                    mode: crate::PulsarTopicPublishingMode::NoAck {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("pulsar_main"),
                    topic: named("orders"),
                })),
                "pulsar_main TOPIC 'orders'",
            ),
            (
                EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(RabbitMqQueueEmitter {
                    mode: crate::RabbitMqQueuePublishingMode::NoAck {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("rmq_main"),
                    queue: named("orders_q"),
                })),
                "rmq_main QUEUE 'orders_q'",
            ),
            (
                EmitterEntity::RabbitMq(RabbitMqEmitter::Exchange(RabbitMqExchangeEmitter {
                    mode: crate::RabbitMqExchangePublishingMode::NoAck {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("rmq_main"),
                    exchange: named("orders_x"),
                    routing_key: scoped_field(FieldScope::Input, "category"),
                })),
                "rmq_main EXCHANGE 'orders_x' ROUTING KEY input.category",
            ),
            (
                EmitterEntity::Redis(RedisEmitter::PubSub(RedisPubSubEmitter {
                    mode: crate::RedisPubSubPublishingMode::NoAck {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("redis_main"),
                    channel: named("orders_ch"),
                })),
                "redis_main CHANNEL 'orders_ch'",
            ),
            (
                EmitterEntity::Mqtt(MqttEmitter::Topic(MqttTopicEmitter {
                    mode: crate::MqttTopicPublishingMode::Qos0 {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("mqtt_main"),
                    topic: named("orders_topic"),
                })),
                "mqtt_main TOPIC 'orders_topic'",
            ),
            (
                EmitterEntity::Nats(NatsEmitter::Subject(NatsSubjectEmitter {
                    mode: crate::NatsSubjectPublishingMode::NoAck {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("nats_main"),
                    subject: named("orders_subject"),
                })),
                "nats_main SUBJECT 'orders_subject'",
            ),
            (
                EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
                    mode: crate::ZeroMqPushPublishingMode::NoAck {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("zmq_main"),
                })),
                "zmq_main",
            ),
            (
                EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
                    mode: crate::SqsQueuePublishingMode::Ack {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("sqs_main"),
                    queue: crate::SqsQueueName::parse("orders_queue").expect("valid queue"),
                    fifo_group: None,
                    send: crate::SqsSend::Single,
                })),
                "sqs_main QUEUE 'orders_queue' SEND SINGLE",
            ),
            (
                EmitterEntity::Sentry(SentryEmitter::Event(SentryEventEmitter {
                    mode: crate::SentryEventPublishingMode::Ack {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("sentry_main"),
                    platform: None,
                })),
                "sentry_main",
            ),
            (
                EmitterEntity::Syslog(SyslogEmitter::Collector(SyslogCollectorEmitter {
                    mode: crate::SyslogCollectorPublishingMode::NoAck {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("syslog_main"),
                })),
                "syslog_main",
            ),
        ];

        for (connector, rendered_emitter) in emitters {
            let rendered_mode = connector.publishing().to_canonical_plexus();
            let rendered_kind = connector.kind().phrase();
            let emitter = CreateEmitter {
                name: named("emit_orders"),
                from: ProcessorInputs::single(named("orders_stream"))
                    .with_collect_policy("50ms".to_string(), Some("4MiB".to_string())),
                body: EmitterBody::Codec {
                    codec: named("orders_codec"),
                },
                envelope: Box::new(crate::EmitterEnvelope::default()),
                connector: Box::new(connector),
                batch: None,
                flush_policy: FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
                mode: AckMode::Attached,
                error_policies: ErrorPolicies::handled_by_log(),

                construction: RouteConstruction::default(),
                materialized_state: Vec::new(),
            };
            assert_eq!(
                emitter.to_canonical_plexus().expect("must render"),
                format!(
                    "CREATE ATTACHED {rendered_kind} EMITTER emit_orders\n  FROM orders_stream \
                     COLLECT FOR 50ms MAX BATCH SIZE 4MiB\n  TO {rendered_emitter}\n    MODE \
                     {rendered_mode}\n    ENCODE USING orders_codec\n  FLUSH EACH 100ms MAX BATCH \
                     SIZE 1MiB\n  ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;"
                )
            );

            let batching = CreateEmitter {
                batch: Some(batch_policy(500, "1MiB")),
                ..emitter
            };
            assert_eq!(
                batching.to_canonical_plexus().expect("must render"),
                format!(
                    "CREATE ATTACHED {rendered_kind} EMITTER emit_orders\n  FROM orders_stream \
                     COLLECT FOR 50ms MAX BATCH SIZE 4MiB\n  TO {rendered_emitter}\n    MODE \
                     {rendered_mode}\n    ENCODE USING orders_codec\n  BATCH MAX MESSAGES 500 MAX \
                     SIZE 1MiB\n  FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n  ON MESSAGE ERROR LOG\n  \
                     ON GENERAL ERROR LOG;"
                )
            );
        }
    }

    #[test]
    fn renders_optional_clauses_only_when_the_model_declares_them() {
        let add_from = |where_clause| crate::AlterProcessorOperation::AddFrom {
            relay: named("incoming"),
            where_clause,
        };
        let alter = crate::AlterJunction {
            junction: named("route_events"),
            operations: vec![
                add_from(None),
                add_from(Some(scoped_field(FieldScope::Input, "active"))),
            ],
        };
        assert_eq!(
            alter.to_canonical_plexus().expect("must render"),
            "ALTER JUNCTION route_events ADD FROM incoming, ADD FROM incoming WHERE input.active;"
        );

        let sqs = |fifo_group| {
            EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
                mode: crate::SqsQueuePublishingMode::Ack {
                    retry_policy: crate::RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("sqs_main"),
                queue: crate::SqsQueueName::parse("orders_queue").expect("valid queue"),
                fifo_group,
                send: crate::SqsSend::Batch,
            }))
        };
        assert_eq!(
            sqs(Some(crate::SqsFifoGroup::FromBranch))
                .entity_presentation()
                .assured("the branch group has no fallible expression")
                .reference_to_plexus(),
            "sqs_main QUEUE 'orders_queue' FIFO GROUP FROM BRANCH SEND BATCH"
        );
        assert_eq!(
            sqs(Some(crate::SqsFifoGroup::Expression(scoped_field(
                FieldScope::Input,
                "tenant"
            ))))
            .entity_presentation()
            .assured("the scoped field has a canonical representation")
            .reference_to_plexus(),
            "sqs_main QUEUE 'orders_queue' FIFO GROUP input.tenant SEND BATCH"
        );

        let otel = EmitterEntity::from(crate::OtelMetricsEmitter {
            mode: crate::OtelMetricsPublishingMode::Ack {
                retry_policy: crate::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named("otel_main"),
            metric: crate::OtelMetric {
                name: "queue.depth".to_string(),
                unit: "1".to_string(),
                description: None,
                kind: crate::OtelMetricKind::Gauge,
            },
            mappings: crate::OtelMappings {
                values: vec![crate::OtelValueMapping {
                    column: "value".to_string(),
                    expression: scoped_field(FieldScope::Input, "depth"),
                }],
                attributes: Vec::new(),
                resource: Vec::new(),
                scope: Some(crate::OtelScope {
                    name: "nervix".to_string(),
                    version: None,
                }),
            },
        });
        assert_eq!(
            otel.entity_presentation()
                .assured("the fixed metric has canonical values")
                .reference_to_plexus(),
            "otel_main METRIC 'queue.depth' UNIT '1' GAUGE"
        );

        let default_policy = crate::MaterializedStatePolicy::Default(vec![crate::Assignment {
            target: crate::AssignmentTarget::bare(named("tier")),
            value: Expression::Literal(Literal::String("basic".to_string())),
        }]);
        assert_eq!(
            super::materialized_state_policy_to_plexus(&default_policy).expect("must render"),
            "DEFAULT { tier = 'basic' }"
        );

        let cbor = crate::AlterWireSchema {
            schema: named("orders_wire"),
            operations: vec![crate::AlterWireSchemaOperation::DropField {
                field: named("note"),
            }],
        };
        assert_eq!(
            crate::alter_cbor_wire_schema_to_canonical_plexus(&cbor).expect("must render"),
            "ALTER WIRE CBOR SCHEMA orders_wire DROP FIELD note;"
        );
    }

    #[test]
    fn renders_postgres_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: named("emit_notifications"),
            from: ProcessorInputs::single(named("notifications")),
            body: EmitterBody::Values,
            envelope: Box::new(crate::EmitterEnvelope::default()),
            connector: Box::new(EmitterEntity::Postgres(PostgresEmitter::Table(
                PostgresTableEmitter {
                    mode: crate::PostgresTablePublishingMode::Ack {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("postgres_main"),
                    table: named("notification_rows"),
                    values: vec![
                        PostgresValueMapping {
                            column: "postgres_user_id".to_string(),
                            expression: scoped_field(FieldScope::Input, "user_id"),
                        },
                        PostgresValueMapping {
                            column: "postgres_action".to_string(),
                            expression: call(
                                "lower",
                                vec![scoped_field(FieldScope::Input, "action")],
                            ),
                        },
                    ],
                    conflict_action: PostgresConflictAction::DoUpdate {
                        target: vec!["postgres_user_id".to_string()],
                    },
                },
            ))),
            batch: Some(batch_policy(500, "8MiB")),
            flush_policy: FlushPolicy::Each {
                interval: "10s".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_plexus().expect("must render"),
            "CREATE ATTACHED POSTGRES TABLE EMITTER emit_notifications\n  FROM notifications\n  \
             TO postgres_main TABLE 'notification_rows'\n    MODE ACK RETRY POLICY BACKOFF 250ms \
             MAX 30s\n    VALUES {\n      'postgres_user_id' = input.user_id,\n      \
             'postgres_action' = lower(input.action)\n    }\n    ON CONFLICT ('postgres_user_id') \
             DO UPDATE\n  BATCH MAX MESSAGES 500 MAX SIZE 8MiB\n  FLUSH EACH 10s MAX BATCH SIZE \
             1MiB\n  ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;"
        );
    }

    #[test]
    fn renders_mysql_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: named("emit_notifications"),
            from: ProcessorInputs::single(named("notifications")),
            body: EmitterBody::Values,
            envelope: Box::new(crate::EmitterEnvelope::default()),
            connector: Box::new(EmitterEntity::MySql(MySqlEmitter::Table(
                MySqlTableEmitter {
                    mode: crate::MySqlTablePublishingMode::Ack {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("mysql_main"),
                    table: named("notification_rows"),
                    values: vec![
                        MySqlValueMapping {
                            column: "mysql_user_id".to_string(),
                            expression: scoped_field(FieldScope::Input, "user_id"),
                        },
                        MySqlValueMapping {
                            column: "mysql_action".to_string(),
                            expression: call(
                                "lower",
                                vec![scoped_field(FieldScope::Input, "action")],
                            ),
                        },
                    ],
                    conflict_action: MySqlConflictAction::DoNothing,
                },
            ))),
            batch: Some(batch_policy(500, "8MiB")),
            flush_policy: FlushPolicy::Each {
                interval: "10s".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_plexus().expect("must render"),
            "CREATE ATTACHED MYSQL TABLE EMITTER emit_notifications\n  FROM notifications\n  TO \
             mysql_main TABLE 'notification_rows'\n    MODE ACK RETRY POLICY BACKOFF 250ms MAX \
             30s\n    VALUES {\n      'mysql_user_id' = input.user_id,\n      'mysql_action' = \
             lower(input.action)\n    }\n    ON CONFLICT DO NOTHING\n  BATCH MAX MESSAGES 500 MAX \
             SIZE 8MiB\n  FLUSH EACH 10s MAX BATCH SIZE 1MiB\n  ON MESSAGE ERROR LOG\n  ON \
             GENERAL ERROR LOG;"
        );
    }

    #[test]
    fn renders_mongodb_conflict_action_canonical() {
        let emitter = CreateEmitter {
            name: named("emit_notifications"),
            from: ProcessorInputs::single(named("notifications")),
            body: EmitterBody::Values,
            envelope: Box::new(crate::EmitterEnvelope::default()),
            connector: Box::new(EmitterEntity::MongoDb(MongoDbEmitter::Collection(
                MongoDbCollectionEmitter {
                    mode: crate::MongoDbCollectionPublishingMode::Ack {
                        retry_policy: crate::RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("mongodb_main"),
                    collection: named("notification_rows"),
                    values: vec![
                        MongoDbValueMapping {
                            column: "mongodb_user_id".to_string(),
                            expression: scoped_field(FieldScope::Input, "user_id"),
                        },
                        MongoDbValueMapping {
                            column: "mongodb_action".to_string(),
                            expression: call(
                                "lower",
                                vec![scoped_field(FieldScope::Input, "action")],
                            ),
                        },
                    ],
                    conflict_action: MongoDbConflictAction::DoUpdate {
                        target: vec!["mongodb_user_id".to_string()],
                    },
                },
            ))),
            batch: Some(batch_policy(500, "8MiB")),
            flush_policy: FlushPolicy::Each {
                interval: "10s".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };

        assert_eq!(
            emitter.to_canonical_plexus().expect("must render"),
            "CREATE ATTACHED MONGODB COLLECTION EMITTER emit_notifications\n  FROM \
             notifications\n  TO mongodb_main COLLECTION 'notification_rows'\n    MODE ACK RETRY \
             POLICY BACKOFF 250ms MAX 30s\n    VALUES {\n      'mongodb_user_id' = \
             input.user_id,\n      'mongodb_action' = lower(input.action)\n    }\n    ON CONFLICT \
             ('mongodb_user_id') DO UPDATE\n  BATCH MAX MESSAGES 500 MAX SIZE 8MiB\n  FLUSH EACH \
             10s MAX BATCH SIZE 1MiB\n  ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;"
        );
    }

    /// An ingestor of `input` writing to `orders`, as the canonical rendering tests declare it.
    fn orders_ingestor(name: &str, input: crate::IngestorInput) -> CreateIngestor {
        CreateIngestor {
            name: named(name),
            output_routes: flushed_ingestor_outputs("orders"),
            input,
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        }
    }

    /// A transport input decoded by `orders_codec`.
    fn decoded(source: impl Into<IngestorEntity>) -> crate::IngestorInput {
        crate::IngestorInput::Transport(crate::TransportIngestorInput {
            source: source.into(),
            codec: named("orders_codec"),
        })
    }

    #[test]
    fn renders_every_ingestor_pair_under_its_kinded_head() {
        let retry = RetryPolicy {
            backoff: "1s".to_string(),
            max_backoff: "30s".to_string(),
        };
        let every = |raw: &str| -> crate::DomainClockPeriod {
            raw.parse()
                .assured("the fixture cadence is a positive duration")
        };
        let tail = "\n  DECODE USING orders_codec\n  TO orders\n    UNBRANCHED\n    FLUSH EACH \
                    100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;";
        let expectations = [
            (
                orders_ingestor(
                    "http_ingestor",
                    decoded(HttpRequestIngestor {
                        client: named("http_main"),
                        method: "GET".to_owned(),
                        headers: Vec::new(),
                        max_body_size: None,
                        every: every("30s"),
                        mode: HttpRequestIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE HTTP REQUEST INGESTOR http_ingestor\n  FROM http_main METHOD 'GET' EVERY \
                 30s MODE NO ACK ON QUIESCE SUSPEND",
            ),
            (
                orders_ingestor(
                    "endpoint_ingestor",
                    decoded(HttpEndpointIngestor {
                        endpoint: named("orders_endpoint"),
                        mode: EndpointIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Reject {
                            retry_after: "5s".to_string(),
                        },
                    }),
                ),
                "CREATE HTTP ENDPOINT INGESTOR endpoint_ingestor\n  FROM orders_endpoint MODE NO \
                 ACK ON QUIESCE REJECT RETRY AFTER 5s",
            ),
            (
                orders_ingestor(
                    "kafka_ingestor",
                    decoded(KafkaTopicIngestor {
                        client: named("kafka_main"),
                        topic: named("Orders.Topic"),
                        offset_mode: KafkaOffsetMode::ConsumerGroup(named("Orders Group")),
                        instances: nonzero!(3u64),
                        mode: KafkaIngestMode::AckParallel {
                            max: nonzero!(8u64),
                            batch_timeout: "100ms".to_string(),
                            timeout: "5s".to_string(),
                            retry_policy: retry.clone(),
                        },
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE KAFKA TOPIC INGESTOR kafka_ingestor\n  FROM kafka_main TOPIC \
                 'Orders.Topic' OFFSET BY CONSUMER GROUP 'Orders Group' INSTANCES 3 MODE ACK \
                 PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX \
                 30s ON QUIESCE SUSPEND",
            ),
            (
                orders_ingestor(
                    "pulsar_ingestor",
                    decoded(PulsarTopicIngestor {
                        client: named("pulsar_main"),
                        topic: named("persistent://public/default/Orders"),
                        subscription: PulsarSubscription {
                            name: named("Readers"),
                            ownership: PulsarSubscriptionOwnership::Provisioned,
                        },
                        instances: nonzero!(1u64),
                        mode: PulsarIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE PULSAR TOPIC INGESTOR pulsar_ingestor\n  FROM pulsar_main TOPIC \
                 'persistent://public/default/Orders' SUBSCRIPTION 'Readers' PROVISIONED MODE NO \
                 ACK ON QUIESCE SUSPEND",
            ),
            (
                orders_ingestor(
                    "mqtt_ingestor",
                    decoded(MqttTopicIngestor {
                        client: named("mqtt_main"),
                        topic: named("devices/+/Orders"),
                        instances: nonzero!(2u64),
                        session: MqttSession::Clean,
                        qos: MqttQos::Qos0,
                        mode: MqttIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Drop,
                    }),
                ),
                "CREATE MQTT TOPIC INGESTOR mqtt_ingestor\n  FROM mqtt_main TOPIC \
                 'devices/+/Orders' INSTANCES 2 SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE DROP",
            ),
            (
                orders_ingestor(
                    "mqtt_session_ingestor",
                    decoded(MqttTopicIngestor {
                        client: named("mqtt_main"),
                        topic: named("devices/#"),
                        instances: nonzero!(1u64),
                        session: MqttSession::Persistent,
                        qos: MqttQos::Qos2,
                        mode: MqttIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE MQTT TOPIC INGESTOR mqtt_session_ingestor\n  FROM mqtt_main TOPIC \
                 'devices/#' SESSION PERSISTENT QOS 2 MODE NO ACK ON QUIESCE SUSPEND",
            ),
            (
                orders_ingestor(
                    "nats_ingestor",
                    decoded(NatsSubjectIngestor {
                        client: named("nats_main"),
                        subject: named("orders.*.created"),
                        queue_group: named("Orders-Workers"),
                        instances: nonzero!(2u64),
                        mode: NatsIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Drop,
                    }),
                ),
                "CREATE NATS SUBJECT INGESTOR nats_ingestor\n  FROM nats_main SUBJECT \
                 'orders.*.created' QUEUE GROUP 'Orders-Workers' INSTANCES 2 MODE NO ACK ON \
                 QUIESCE DROP",
            ),
            (
                orders_ingestor(
                    "rabbit_ingestor",
                    decoded(RabbitMqQueueIngestor {
                        client: named("rmq_main"),
                        queue: named("Orders Queue"),
                        instances: nonzero!(1u64),
                        mode: RabbitMqIngestMode::AckSequential {
                            timeout: "20s".to_string(),
                            retry_policy: retry.clone(),
                        },
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE RABBITMQ QUEUE INGESTOR rabbit_ingestor\n  FROM rmq_main QUEUE 'Orders \
                 Queue' MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s ON \
                 QUIESCE SUSPEND",
            ),
            (
                orders_ingestor(
                    "redis_ingestor",
                    decoded(RedisPubSubIngestor {
                        client: named("redis_main"),
                        subscription: crate::RedisSubscriptionTarget::Channel(named("orders:*")),
                        mode: RedisPubSubIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Drop,
                    }),
                ),
                "CREATE REDIS PUBSUB INGESTOR redis_ingestor\n  FROM redis_main CHANNEL \
                 'orders:*' MODE NO ACK ON QUIESCE DROP",
            ),
            (
                orders_ingestor(
                    "prom_ingestor",
                    decoded(PrometheusQueryIngestor {
                        client: named("prom_main"),
                        query: "rate(orders_total[5m])".to_string(),
                        max_body_size: None,
                        every: every("15s"),
                        mode: PrometheusIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE PROMETHEUS QUERY INGESTOR prom_ingestor\n  FROM prom_main QUERY \
                 'rate(orders_total[5m])' EVERY 15s MODE NO ACK ON QUIESCE SUSPEND",
            ),
            (
                orders_ingestor(
                    "bounded_prom_ingestor",
                    decoded(PrometheusQueryIngestor {
                        client: named("prom_main"),
                        query: "up".to_string(),
                        max_body_size: Some(
                            "4MB"
                                .parse()
                                .assured("the fixture size is a whole number of megabytes"),
                        ),
                        every: every("15s"),
                        mode: PrometheusIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE PROMETHEUS QUERY INGESTOR bounded_prom_ingestor\n  FROM prom_main QUERY \
                 'up' MAX BODY SIZE 4MB EVERY 15s MODE NO ACK ON QUIESCE SUSPEND",
            ),
            (
                orders_ingestor(
                    "zmq_ingestor",
                    decoded(ZeroMqPullIngestor {
                        client: named("zmq_main"),
                        mode: ZeroMqIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE ZEROMQ PULL INGESTOR zmq_ingestor\n  FROM zmq_main MODE NO ACK ON QUIESCE \
                 SUSPEND",
            ),
            (
                orders_ingestor(
                    "sqs_ingestor",
                    decoded(SqsQueueIngestor {
                        client: named("sqs_main"),
                        queue: named("Orders.fifo"),
                        instances: nonzero!(4u64),
                        mode: SqsIngestMode::AckSequential {
                            timeout: "45s".to_string(),
                            retry_policy: retry.clone(),
                        },
                        quiesce: crate::IngestQuiesceMode::Suspend,
                    }),
                ),
                "CREATE SQS QUEUE INGESTOR sqs_ingestor\n  FROM sqs_main QUEUE 'Orders.fifo' \
                 INSTANCES 4 MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 1s MAX 30s \
                 ON QUIESCE SUSPEND",
            ),
            (
                orders_ingestor(
                    "ws_ingestor",
                    decoded(WebsocketsConnectionIngestor {
                        client: named("ws_main"),
                        headers: Vec::new(),
                        mode: WebsocketsIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Drop,
                    }),
                ),
                "CREATE WEBSOCKETS CONNECTION INGESTOR ws_ingestor\n  FROM ws_main MODE NO ACK ON \
                 QUIESCE DROP",
            ),
            (
                orders_ingestor(
                    "ws_endpoint_ingestor",
                    decoded(WebsocketsEndpointIngestor {
                        endpoint: named("orders_socket"),
                        mode: EndpointIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::EndpointBuffer {
                            max_size: "1MiB".to_string(),
                        },
                    }),
                ),
                "CREATE WEBSOCKETS ENDPOINT INGESTOR ws_endpoint_ingestor\n  FROM orders_socket \
                 MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB",
            ),
            (
                orders_ingestor(
                    "syslog_ingestor",
                    decoded(SyslogListenerIngestor {
                        client: named("syslog_main"),
                        mode: SyslogIngestMode::NoAck,
                        quiesce: crate::IngestQuiesceMode::Drop,
                    }),
                ),
                "CREATE SYSLOG LISTENER INGESTOR syslog_ingestor\n  FROM syslog_main MODE NO ACK \
                 ON QUIESCE DROP",
            ),
        ];

        for (ingestor, head) in expectations {
            assert_eq!(
                ingestor.to_canonical_plexus().expect("must render"),
                format!("{head}{tail}")
            );
        }
    }

    #[test]
    fn renders_a_client_producer_ingestor_without_a_codec() {
        let ingestor = orders_ingestor(
            "orders_in",
            crate::IngestorInput::Client(ClientProducerIngestor {
                schema: named("order"),
                mode: ClientIngestMode {
                    window: AckWindow::Parallel {
                        max: nonzero!(64u64),
                    },
                    ack_timeout: "30s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "100ms".to_string(),
                        max_backoff: "5s".to_string(),
                    },
                },
            }),
        );
        assert_eq!(
            ingestor.to_canonical_plexus().expect("must render"),
            "CREATE CLIENT PRODUCER INGESTOR orders_in\n  FROM SCHEMA order MODE ACK PARALLEL MAX \
             64 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s ON QUIESCE SUSPEND\n  TO \
             orders\n    UNBRANCHED\n    FLUSH EACH 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE \
             ERROR LOG\n  ON GENERAL ERROR LOG;"
        );
    }

    #[test]
    fn model_dispatches_to_variant_specific_canonicalization() {
        let model: Model = Model::ClientKafka(CreateClientKafka {
            name: named("kafka_main"),
            mount: None,
            config: vec![config_entry("bootstrap.servers", "localhost:9092")],
        });

        assert_eq!(
            model.to_canonical_plexus().expect("must render"),
            "CREATE KAFKA CLIENT kafka_main\n  CONFIG {\n    'bootstrap.servers' = \
             'localhost:9092'\n  };"
        );
    }

    #[test]
    fn placement_canonicalization_preserves_member_order_policy_and_rank() {
        let placement = CreatePlacement::new(
            named("latency_path"),
            vec![named("ingest"), named("enrich")],
            vec![named("score")],
            PlacementPolicy::RequireColocation,
            Some(nonzero!(1u64)),
        )
        .expect("placement must be valid");

        assert_eq!(
            placement.to_canonical_plexus().expect("must render"),
            "CREATE PLACEMENT latency_path FROM ingest, enrich TO score REQUIRE COLOCATION RANK 1;"
        );
    }

    #[test]
    fn unranked_neutral_placement_canonicalization_omits_rank() {
        let placement = CreatePlacement::new(
            named("ordinary"),
            vec![named("ingest")],
            vec![named("emit")],
            PlacementPolicy::Neutral,
            None,
        )
        .expect("placement must be valid");

        assert_eq!(
            placement.to_canonical_plexus().expect("must render"),
            "CREATE PLACEMENT ordinary FROM ingest TO emit NEUTRAL;"
        );
    }

    #[test]
    fn byte_sizes_take_the_largest_prefix_that_divides_exactly() {
        assert_eq!(super::byte_size_literal(nonzero!(67_108_864u64)), "64MiB");
        assert_eq!(super::byte_size_literal(nonzero!(1_024u64)), "1KiB");
        assert_eq!(super::byte_size_literal(nonzero!(1_073_741_824u64)), "1GiB");
        assert_eq!(
            super::byte_size_literal(nonzero!(1_099_511_627_776u64)),
            "1TiB"
        );
    }

    #[test]
    fn byte_sizes_that_divide_no_prefix_exactly_stay_counts() {
        assert_eq!(super::byte_size_literal(nonzero!(1u64)), "1B");
        assert_eq!(super::byte_size_literal(nonzero!(100_000u64)), "100000B");
        assert_eq!(super::byte_size_literal(nonzero!(1_048_577u64)), "1048577B");
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
    fn dollar_quoted_literals_escalate_past_a_tag_the_body_ends_with() {
        assert_eq!(super::string_literal("line\n$s"), "$s_1$line\n$s$s_1$");
        assert_eq!(
            super::string_literal("both ' and \"$"),
            "$s$both ' and \"$$s$"
        );
    }

    #[test]
    fn udf_canonicalization_preserves_source_bytes_and_avoids_delimiter_collisions() {
        let code = "fn redact(value: StringColumn) -> StringColumn {\n    // $roto$\n    \
                    value\n}\n"
            .to_string();
        let udf = CreateUdf::new(
            named("redact"),
            UdfLanguage::Roto0_13,
            vec![UdfArgument {
                name: named("value"),
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

        let rendered = udf.to_canonical_plexus().expect("must render");

        assert!(rendered.contains("CODE $roto_1$"));
        assert!(rendered.contains(&code));
    }

    fn float_value(value: f64) -> Expression {
        Expression::Literal(Literal::F64(crate::Float64Literal::new(value)))
    }

    #[test]
    fn renders_float_literals_with_a_fractional_part() {
        assert_eq!(
            expression_to_plexus(&float_value(80.0)).expect("must render"),
            "80.0"
        );
        assert_eq!(
            expression_to_plexus(&float_value(0.0)).expect("must render"),
            "0.0"
        );
        assert_eq!(
            expression_to_plexus(&float_value(15.0)).expect("must render"),
            "15.0"
        );
    }

    #[test]
    fn renders_float_literals_that_already_read_as_floats_unchanged() {
        assert_eq!(
            expression_to_plexus(&float_value(111.32)).expect("must render"),
            "111.32"
        );
        assert_eq!(
            expression_to_plexus(&float_value(0.017453292519943295)).expect("must render"),
            "0.017453292519943295"
        );
        let huge = expression_to_plexus(&float_value(1e300)).expect("must render");
        assert!(huge.ends_with(".0"), "{huge} must lex back as a float");
    }

    #[test]
    fn refuses_to_render_non_finite_floats() {
        for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let error = expression_to_plexus(&float_value(value)).expect_err("must not render");
            assert_eq!(
                error.current_context(),
                &super::CanonicalPlexusError::UnrepresentableFloat {
                    value: crate::Float64Literal::new(value),
                }
            );
        }
    }

    #[test]
    fn a_statement_holding_a_non_finite_float_reports_the_literal_it_cannot_spell() {
        let junction = CreateJunction {
            name: named("scored_orders"),
            from: ProcessorInputs::single(named("orders")),
            output_routes: flushed_outputs("scored"),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: Some(Expression::Binary {
                operator: BinaryOperator::LessThan,
                left: Box::new(scoped_field(FieldScope::Input, "score")),
                right: Box::new(float_value(f64::NEG_INFINITY)),
            }),
            materialized_state: Vec::new(),
        };
        let statement = crate::Statement::Create(crate::CreateStatement::new(
            Box::new(Model::Junction(junction)),
            false,
        ));

        let error = statement
            .to_canonical_plexus()
            .expect_err("an infinite literal has no Plexus spelling");

        assert_eq!(
            error.current_context(),
            &super::CanonicalPlexusError::UnrepresentableFloat {
                value: crate::Float64Literal::new(f64::NEG_INFINITY),
            }
        );
        assert_eq!(
            error.to_string(),
            "cannot represent non-finite float in Plexus: -inf"
        );
    }

    #[test]
    fn a_codec_without_a_spelling_names_itself_and_the_rule_it_breaks() {
        let syslog: CreateCodec = CreateCodec {
            name: named("syslog_codec"),
            wire_format: CodecWireFormat::Syslog,
            schema: named("syslog_event"),
            encoding_rules: vec![CodecEncodingRule {
                field: named("timestamp"),
                encoding: CodecEncoding::Rfc3339,
            }],
        };
        let error = syslog
            .to_canonical_plexus()
            .expect_err("a SYSLOG codec has no fields to encode");
        assert_eq!(
            error.current_context(),
            &super::CanonicalPlexusError::SyslogEncodingRules {
                codec: named("syslog_codec"),
            }
        );
        assert_eq!(
            error.to_string(),
            "invalid codec `syslog_codec`: SYSLOG codec must not declare encoding rules"
        );

        let no_programs = CodecJaqTransformations::default();
        let jaq: CreateCodec = CreateCodec {
            name: named("jaq_codec"),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: no_programs.clone(),
            },
            schema: named("orders"),
            encoding_rules: Vec::new(),
        };
        let protobuf: CreateCodec = CreateCodec {
            name: named("proto_codec"),
            wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: named("proto_bundle"),
                resource_version: 3,
                config: Vec::new(),
                message: "nervix.test.Order".to_string(),
                batch_message: None,
                transformations: no_programs,
            }),
            schema: named("orders"),
            encoding_rules: Vec::new(),
        };
        for (codec, name) in [(jaq, "jaq_codec"), (protobuf, "proto_codec")] {
            let error = codec
                .to_canonical_plexus()
                .expect_err("a JAQ-transformed codec needs a program");
            assert_eq!(
                error.current_context(),
                &super::CanonicalPlexusError::MissingJaqTransformation { codec: named(name) }
            );
            assert_eq!(
                error.to_string(),
                format!("invalid codec `{name}`: codec is missing JAQ transformation")
            );
        }
    }

    #[test]
    fn renders_json_extractions_with_their_canonical_path_and_declared_type() {
        let path = |text: &str| crate::JsonPath::parse(text).expect("test paths are valid");
        let document = || Box::new(scoped_field(FieldScope::Input, "doc"));
        let vector = ParseAsType::Vec {
            element: Box::new(ParseAsType::Array {
                element: Box::new(ParseAsType::F32),
                len: nonzero_ext::nonzero!(2_u32),
            }),
        };
        assert_eq!(
            expression_to_plexus(&Expression::JsonValue {
                document: document(),
                path: path(r#"$["plain"][0]"#),
                target: ParseAsType::I64,
            })
            .expect("must render"),
            "JSON_VALUE(input.doc, '$.plain[0]' AS I64)"
        );
        assert_eq!(
            expression_to_plexus(&Expression::TryJsonValue {
                document: document(),
                path: path(r#"$["odd key"]"#),
                target: vector,
            })
            .expect("must render"),
            r#"TRY_JSON_VALUE(input.doc, '$["odd key"]' AS VEC<ARRAY<F32, 2>>)"#
        );
        assert_eq!(
            expression_to_plexus(&Expression::JsonExists {
                document: document(),
                path: path(r#"$["it's \"quoted\""]"#),
            })
            .expect("must render"),
            r#"JSON_EXISTS(input.doc, $s$$["it's \"quoted\""]$s$)"#
        );
    }

    #[test]
    fn renders_a_tolerant_conversion_around_its_whole_operand() {
        let sum = Expression::Binary {
            operator: BinaryOperator::Add,
            left: Box::new(scoped_field(FieldScope::Input, "low")),
            right: Box::new(scoped_field(FieldScope::Input, "high")),
        };
        let converted_sum = Expression::TryCast {
            expression: Box::new(sum),
            target: ParseAsType::String,
        };
        assert_eq!(
            expression_to_plexus(&converted_sum).expect("must render"),
            "TRY_CAST(input.low + input.high AS STRING)"
        );

        let reparsed = Expression::TryCast {
            expression: Box::new(Expression::Cast {
                expression: Box::new(scoped_field(FieldScope::Input, "raw")),
                target: ParseAsType::I64,
            }),
            target: ParseAsType::String,
        };
        assert_eq!(
            expression_to_plexus(&reparsed).expect("must render"),
            "TRY_CAST(input.raw AS I64 AS STRING)"
        );

        let narrowed = Expression::Cast {
            expression: Box::new(Expression::TryCast {
                expression: Box::new(scoped_field(FieldScope::Input, "raw")),
                target: ParseAsType::I64,
            }),
            target: ParseAsType::U8,
        };
        assert_eq!(
            expression_to_plexus(&narrowed).expect("must render"),
            "TRY_CAST(input.raw AS I64) AS U8"
        );
    }
}
