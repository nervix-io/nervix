use std::ops::Range;

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    BuiltinFunctionScope, CanonicalNsplError, CreateSubscription, DeleteSubscription, DomainName,
    EmitSinkKind, IngestSourceKind, ModelKind, RelayName, SchemaName, SemanticReference, Statement,
    UploadResource, WireSchemaName,
};

use crate::{
    lexer::{Identifier as Keyword, Token, Word},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, ack_mode, completion_context,
        completion_tokens, domain_ref, if_not_exists_clause, into_parse_error, junction_name, kw,
        lex_input, relay_ref, schema_name, suggestions_from_errors, tok, wire_schema_name,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientStatement {
    UseDomain(DomainName),
    ListDomains,
    BeginTransaction,
    CommitTransaction,
    RevertTransaction,
    UploadResource(UploadResource),
    CreateSubscription(CreateSubscription),
    DeleteSubscription(DeleteSubscription),
    Server(Statement),
}

impl ClientStatement {
    /// Renders this statement as canonical NSPL.
    ///
    /// Server statements delegate to [`Statement::to_canonical_nspl`]; the session-local forms are
    /// rendered here because they belong to the client protocol rather than to a stored model.
    pub fn to_canonical_nspl(&self) -> Result<String, CanonicalNsplError> {
        match self {
            Self::UseDomain(domain) => Ok(format!("USE {};", domain.as_str())),
            Self::ListDomains => Ok("LIST DOMAINS;".to_string()),
            Self::BeginTransaction => Ok("BEGIN;".to_string()),
            Self::CommitTransaction => Ok("COMMIT;".to_string()),
            Self::RevertTransaction => Ok("REVERT;".to_string()),
            Self::UploadResource(upload) => {
                Statement::UploadResource(upload.clone()).to_canonical_nspl()
            }
            Self::CreateSubscription(subscription) => {
                Ok(crate::subscribe::create_subscription_query(
                    subscription.name.as_str(),
                    subscription.relay.as_str(),
                    subscription.delivery_behavior,
                    subscription.batch_sample_rate.as_deref(),
                    subscription.where_clause.as_ref(),
                ))
            }
            Self::DeleteSubscription(subscription) => Ok(
                crate::subscribe::delete_subscription_query(subscription.name.as_str()),
            ),
            Self::Server(statement) => statement.to_canonical_nspl(),
        }
    }

    pub fn requires_local_handling(&self) -> bool {
        match self {
            Self::UseDomain(_) | Self::ListDomains | Self::UploadResource(_) => true,
            Self::BeginTransaction
            | Self::CommitTransaction
            | Self::RevertTransaction
            | Self::CreateSubscription(_)
            | Self::DeleteSubscription(_)
            | Self::Server(_) => false,
        }
    }

    /// Whether this statement reads a transaction's impact report instead of changing anything.
    ///
    /// An inspection is answered before transaction queueing: it never becomes transaction
    /// content, never takes a queue position, and never shares a request with statements that
    /// change the transaction it reads.
    pub fn inspects_transaction(&self) -> bool {
        matches!(self, Self::Server(Statement::DescribeTransaction(_)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedClientStatement {
    /// Byte range of the statement in the original input, from its first token through the byte
    /// after its terminating semicolon.
    ///
    /// Ending past the semicolon means the gaps between consecutive statements hold only
    /// whitespace and comments, which is what lets a caller recover comments by scanning them.
    pub span: Range<usize>,
    pub statement: ClientStatement,
}

impl ParsedClientStatement {
    /// The original source text of this statement.
    ///
    /// `input` must be the same string the statement was parsed from.
    pub fn source<'a>(&self, input: &'a str) -> &'a str {
        &input[self.span.clone()]
    }
}

pub fn use_domain_parser<'src>()
-> impl Parser<'src, &'src [Token], DomainName, extra::Err<ParseError<'src>>> + Clone {
    kw(Keyword::Use)
        .ignore_then(domain_ref())
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn list_domains_parser<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw(Keyword::List)
        .ignore_then(kw(Keyword::Domains))
        .then_ignore(tok(Token::Semicolon).or_not())
        .to(())
}

pub fn begin_transaction_parser<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw(Keyword::Begin)
        .then_ignore(tok(Token::Semicolon).or_not())
        .to(())
}

pub fn commit_transaction_parser<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw(Keyword::Commit)
        .then_ignore(tok(Token::Semicolon).or_not())
        .to(())
}

pub fn revert_transaction_parser<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw(Keyword::Revert)
        .then_ignore(tok(Token::Semicolon).or_not())
        .to(())
}

pub fn client_command_parser<'src>()
-> impl Parser<'src, &'src [Token], ClientStatement, extra::Err<ParseError<'src>>> + Clone {
    choice((
        use_domain_parser().map(ClientStatement::UseDomain),
        list_domains_parser().to(ClientStatement::ListDomains),
        begin_transaction_parser().to(ClientStatement::BeginTransaction),
        commit_transaction_parser().to(ClientStatement::CommitTransaction),
        revert_transaction_parser().to(ClientStatement::RevertTransaction),
        crate::upload_resource::upload_resource_parser().map(ClientStatement::UploadResource),
        crate::subscribe::create_subscription_parser().map(ClientStatement::CreateSubscription),
        crate::subscribe::delete_subscription_parser().map(ClientStatement::DeleteSubscription),
    ))
}

/// The one grammar the public client uses for both local commands and server statements.
pub fn client_statement_parser<'src>()
-> impl Parser<'src, &'src [Token], ClientStatement, extra::Err<ParseError<'src>>> + Clone {
    choice((
        client_command_parser(),
        crate::statement::statement_parser().map(ClientStatement::Server),
    ))
}

/// A grammar expectation that a semantic candidate owner can resolve without reading labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionExpectation {
    Literal(String),
    Semantic(SemanticReference),
}

impl CompletionExpectation {
    fn from_label(label: String, schema: Option<&SchemaFieldContext>) -> Option<Self> {
        if let Some(kind) = ModelKind::from_completion_label(&label) {
            return Some(Self::Semantic(SemanticReference::Model(kind)));
        }
        let expectation = match label.as_str() {
            "ref:resource" => Self::Semantic(SemanticReference::Resource),
            "resource_version" => Self::Semantic(SemanticReference::ResourceVersion),
            "completed_resource_version" => {
                Self::Semantic(SemanticReference::CompletedResourceVersion)
            }
            "ref:session_subscription" => Self::Semantic(SemanticReference::SessionSubscription),
            "ref:runtime_node" => Self::Semantic(SemanticReference::RuntimeNode),
            "ref:domain" => Self::Semantic(SemanticReference::Domain),
            "ref:schema_field" => {
                return match schema {
                    Some(SchemaFieldContext::Schema(name)) => {
                        Some(Self::Semantic(SemanticReference::SchemaField(name.clone())))
                    }
                    Some(SchemaFieldContext::Wire(kind, name)) => Some(Self::Semantic(
                        SemanticReference::WireSchemaField(*kind, name.clone()),
                    )),
                    None => None,
                };
            }
            _ if label
                .chars()
                .next()
                .is_some_and(|first| !first.is_ascii_lowercase()) =>
            {
                Self::Literal(label)
            }
            _ => return None,
        };
        Some(expectation)
    }
}

pub fn suggest_client_expectations(input: &str, cursor: usize) -> Vec<CompletionExpectation> {
    let (labels, tokens) = client_completion(input, cursor);
    let schema = alter_schema_context(&tokens);
    let route_references = route_expression_references(&tokens);
    if !route_references.is_empty() {
        return route_references
            .into_iter()
            .map(CompletionExpectation::Semantic)
            .collect();
    }
    let mut expectations = Vec::new();
    for label in labels {
        if let Some(expectation) = CompletionExpectation::from_label(label, schema.as_ref()) {
            expectations.push(expectation);
        }
    }
    expectations
}

fn route_expression_references(tokens: &[Token]) -> Vec<SemanticReference> {
    let output_route_start = tokens.iter().rposition(|token| {
        matches!(
            token,
            Token::Word(Word::KnownWord {
                iden: Keyword::To,
                ..
            })
        )
    });
    let Some(output_route_start) = output_route_start else {
        return Vec::new();
    };
    let has_construction = tokens[output_route_start..].iter().any(|token| {
        matches!(
            token,
            Token::Word(Word::KnownWord {
                iden: Keyword::Set | Keyword::Where | Keyword::Invoke | Keyword::Inherit,
                ..
            })
        )
    });
    if !has_construction {
        return Vec::new();
    }
    match tokens.last() {
        Some(Token::Word(Word::KnownWord {
            iden: Keyword::Set, ..
        })) if junction_input_relay(tokens).is_some() => junction_output_relay(tokens)
            .map(SemanticReference::RelayField)
            .into_iter()
            .collect(),
        Some(Token::Dot) => {
            let Some(previous_index) = tokens.len().checked_sub(2) else {
                return Vec::new();
            };
            if matches!(
                tokens.get(previous_index),
                Some(Token::Word(crate::lexer::Word::KnownWord {
                    iden: Keyword::Input,
                    ..
                }))
            ) {
                junction_input_relay(tokens)
                    .map(SemanticReference::RelayField)
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            }
        }
        Some(Token::DoubleColon) => {
            let Some(previous_index) = tokens.len().checked_sub(2) else {
                return Vec::new();
            };
            if matches!(
                tokens.get(previous_index),
                Some(Token::Word(crate::lexer::Word::KnownWord {
                    iden: Keyword::Udf,
                    ..
                }))
            ) {
                vec![SemanticReference::Model(ModelKind::Udf)]
            } else {
                Vec::new()
            }
        }
        Some(Token::Word(Word::KnownWord {
            iden: Keyword::Invoke,
            ..
        })) => {
            let scope = match emitter_sink_kind(tokens) {
                Some(kind) => BuiltinFunctionScope::EmitterInvocation(kind),
                None => BuiltinFunctionScope::Ordinary,
            };
            vec![SemanticReference::BuiltinFunction(scope)]
        }
        Some(Token::Eq | Token::LParen | Token::Comma | Token::Plus | Token::Star) => {
            let invocation =
                tokens[output_route_start..]
                    .iter()
                    .rev()
                    .find_map(|token| match token {
                        Token::Word(Word::KnownWord {
                            iden: Keyword::Set | Keyword::Where,
                            ..
                        }) => Some(false),
                        Token::Word(Word::KnownWord {
                            iden: Keyword::Invoke,
                            ..
                        }) => Some(true),
                        _ => None,
                    });
            let scope = if invocation == Some(true) {
                match emitter_sink_kind(tokens) {
                    Some(kind) => BuiltinFunctionScope::EmitterInvocation(kind),
                    None => BuiltinFunctionScope::Ordinary,
                }
            } else {
                match ingestor_source_kind(tokens) {
                    Some(kind) => BuiltinFunctionScope::IngestSource(kind),
                    None => BuiltinFunctionScope::Ordinary,
                }
            };
            vec![SemanticReference::BuiltinFunction(scope)]
        }
        _ => Vec::new(),
    }
}

fn junction_input_relay(tokens: &[Token]) -> Option<RelayName> {
    kw(Keyword::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Keyword::Junction))
        .then_ignore(junction_name())
        .then_ignore(kw(Keyword::From))
        .ignore_then(relay_ref())
        .then_ignore(any().repeated())
        .then_ignore(end())
        .parse(tokens)
        .into_output()
}

fn junction_output_relay(tokens: &[Token]) -> Option<RelayName> {
    let to_index = tokens.iter().rposition(|token| {
        matches!(
            token,
            Token::Word(Word::KnownWord {
                iden: Keyword::To,
                ..
            })
        )
    })?;
    let relay_index = to_index.checked_add(1)?;
    let relay = tokens.get(relay_index)?;
    relay_ref()
        .then_ignore(end())
        .parse(std::slice::from_ref(relay))
        .into_output()
}

fn known_keyword(token: &Token) -> Option<Keyword> {
    match token {
        Token::Word(Word::KnownWord { iden, .. }) => Some(*iden),
        _ => None,
    }
}

fn ingestor_source_kind(tokens: &[Token]) -> Option<IngestSourceKind> {
    let from = tokens
        .iter()
        .position(|token| known_keyword(token) == Some(Keyword::From))?;
    let ingestor = tokens[..from]
        .iter()
        .any(|token| known_keyword(token) == Some(Keyword::Ingestor));
    if !ingestor {
        return None;
    }
    match known_keyword(tokens.get(from.checked_add(1)?)?)? {
        Keyword::Http => Some(IngestSourceKind::Http),
        Keyword::Kafka => Some(IngestSourceKind::Kafka),
        Keyword::Pulsar => Some(IngestSourceKind::Pulsar),
        Keyword::Mqtt => Some(IngestSourceKind::Mqtt),
        Keyword::Nats => Some(IngestSourceKind::Nats),
        Keyword::Rabbitmq => Some(IngestSourceKind::RabbitMq),
        Keyword::Redis => Some(IngestSourceKind::RedisPubSub),
        Keyword::Prometheus => Some(IngestSourceKind::Prometheus),
        Keyword::Zeromq => Some(IngestSourceKind::ZeroMq),
        Keyword::Sqs => Some(IngestSourceKind::Sqs),
        Keyword::Endpoint => Some(IngestSourceKind::Endpoint),
        Keyword::Websockets => Some(IngestSourceKind::Websockets),
        Keyword::Syslog => Some(IngestSourceKind::Syslog),
        _ => None,
    }
}

fn emitter_sink_kind(tokens: &[Token]) -> Option<EmitSinkKind> {
    let to = tokens
        .iter()
        .position(|token| known_keyword(token) == Some(Keyword::To))?;
    let emitter = tokens[..to]
        .iter()
        .any(|token| known_keyword(token) == Some(Keyword::Emitter));
    if !emitter {
        return None;
    }
    match known_keyword(tokens.get(to.checked_add(1)?)?)? {
        Keyword::Http => Some(EmitSinkKind::Http),
        Keyword::Kafka => Some(EmitSinkKind::Kafka),
        Keyword::Pulsar => Some(EmitSinkKind::Pulsar),
        Keyword::Rabbitmq => Some(EmitSinkKind::RabbitMq),
        Keyword::Redis => Some(EmitSinkKind::Redis),
        Keyword::Mqtt => Some(EmitSinkKind::Mqtt),
        Keyword::Nats => Some(EmitSinkKind::Nats),
        Keyword::Zeromq => Some(EmitSinkKind::ZeroMq),
        Keyword::Sqs => Some(EmitSinkKind::Sqs),
        Keyword::Sentry => Some(EmitSinkKind::Sentry),
        Keyword::Syslog => Some(EmitSinkKind::Syslog),
        Keyword::Otel => Some(EmitSinkKind::Otel),
        Keyword::Clickhouse => Some(EmitSinkKind::ClickHouse),
        Keyword::Postgres => Some(EmitSinkKind::Postgres),
        Keyword::Mysql => Some(EmitSinkKind::MySql),
        Keyword::Mongodb => Some(EmitSinkKind::MongoDb),
        Keyword::Iceberg => Some(EmitSinkKind::Iceberg),
        _ => None,
    }
}

enum SchemaFieldContext {
    Schema(SchemaName),
    Wire(ModelKind, WireSchemaName),
}

fn alter_schema_context(tokens: &[Token]) -> Option<SchemaFieldContext> {
    let internal = kw(Keyword::Schema)
        .ignore_then(schema_name())
        .map(SchemaFieldContext::Schema);
    let wire_kind = choice((
        kw(Keyword::Json).to(ModelKind::WireJsonSchema),
        kw(Keyword::Cbor).to(ModelKind::WireCborSchema),
        kw(Keyword::Avro).to(ModelKind::WireAvroSchema),
    ));
    let wire = kw(Keyword::Wire)
        .ignore_then(wire_kind)
        .then_ignore(kw(Keyword::Schema))
        .then(wire_schema_name())
        .map(|(kind, name)| SchemaFieldContext::Wire(kind, name));
    kw(Keyword::Alter)
        .ignore_then(choice((internal, wire)))
        .then_ignore(any().repeated())
        .then_ignore(end())
        .parse(tokens)
        .into_output()
}

pub fn parse_use_domain(input: &str) -> Result<DomainName, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    let out = use_domain_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if out.has_errors() {
        return Err(into_parse_error(
            source,
            &spanned_tokens,
            input.len(),
            out.into_errors(),
        ));
    }
    Ok(out
        .into_output()
        .verified("has_errors returned false above, so this parse produced output"))
}

pub fn parse_upload_resource_query(input: &str) -> Result<UploadResource, ParseFromSourceError> {
    crate::upload_resource::parse_upload_resource(input)
}

pub fn parse_client_statement(input: &str) -> Result<ClientStatement, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    let out = client_statement_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if out.has_errors() {
        return Err(into_parse_error(
            source,
            &spanned_tokens,
            input.len(),
            out.into_errors(),
        ));
    }
    Ok(out
        .into_output()
        .verified("has_errors returned false above, so this parse produced output"))
}

pub fn parse_client_statements(input: &str) -> Result<Vec<ClientStatement>, ParseFromSourceError> {
    parse_client_statement_sources(input).map(|statements| {
        statements
            .into_iter()
            .map(|parsed| parsed.statement)
            .collect()
    })
}

pub fn parse_client_statement_sources(
    input: &str,
) -> Result<Vec<ParsedClientStatement>, ParseFromSourceError> {
    let LexedInput { spanned_tokens, .. } = lex_input(input)?;
    let mut statements = Vec::new();
    let mut segment_start: Option<usize> = None;

    for token in &spanned_tokens {
        if token.token == Token::Semicolon {
            // A segment is a statement only when it actually contains tokens, so a stray
            // semicolon or a trailing comment does not become an empty statement.
            if let Some(start) = segment_start.take() {
                statements.push(ParsedClientStatement {
                    span: start..token.span.end,
                    statement: parse_client_statement(&input[start..token.span.start])?,
                });
            }
        } else if segment_start.is_none() {
            segment_start = Some(token.span.start);
        }
    }

    if let Some(start) = segment_start {
        let end = match spanned_tokens.last() {
            Some(token) => token.span.end,
            None => input.len(),
        };
        statements.push(ParsedClientStatement {
            span: start..end,
            statement: parse_client_statement(&input[start..])?,
        });
    }

    Ok(statements)
}

pub fn suggest_client_statement(input: &str, cursor: usize) -> Vec<String> {
    client_completion(input, cursor).0
}

fn client_completion(input: &str, cursor: usize) -> (Vec<String>, Vec<Token>) {
    let (source, prefix) = completion_context(input, cursor);

    let Some(tokens) = completion_tokens(&source) else {
        return (Vec::new(), Vec::new());
    };

    let out = client_statement_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    let labels = if out.has_errors() {
        suggestions_from_errors(out.into_errors(), &prefix)
    } else {
        match out.into_output() {
            Some(ClientStatement::Server(statement)) => {
                crate::statement::statement_tail(&statement, &tokens, &source, &prefix)
            }
            _ => Vec::new(),
        }
    };
    (labels, tokens)
}

pub fn upload_resource_path_fragment(input: &str, cursor: usize) -> Option<&str> {
    let safe_cursor = cursor.min(input.len());
    let raw_prefix = &input[..safe_cursor];
    let upper = raw_prefix.to_ascii_uppercase();
    let version_index = upper.find(" VERSION ")?;
    let before_version = &raw_prefix[..version_index];
    if !before_version
        .trim_end()
        .to_ascii_uppercase()
        .starts_with("UPLOAD RESOURCE ")
    {
        return None;
    }
    let after_version = &raw_prefix[version_index + " VERSION ".len()..];
    if after_version.is_empty() {
        return Some("");
    }
    let quote = after_version.chars().next()?;
    if quote != '\'' && quote != '"' {
        return Some("");
    }
    let fragment = &after_version[quote.len_utf8()..];
    if fragment.contains(quote) || fragment.contains('\n') {
        return None;
    }
    Some(fragment)
}

/// Source bytes replaced when accepting a local upload path candidate.
pub fn upload_resource_path_range(input: &str, cursor: usize) -> Option<Range<usize>> {
    let fragment = upload_resource_path_fragment(input, cursor)?;
    let start = cursor.checked_sub(fragment.len())?;
    let quote = input.get(..start)?.chars().last();
    let end = match quote {
        Some(quote @ ('\'' | '"')) => {
            let suffix = input.get(cursor..)?;
            let after_path = suffix.find(quote).unwrap_or(suffix.len());
            cursor.checked_add(after_path)?
        }
        _ => cursor,
    };
    Some(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_expression_expectations_keep_their_semantic_scope() {
        let prefix = "CREATE JUNCTION normalizer FROM incoming UNBRANCHED TO outgoing SET value = ";
        let source = format!("{prefix}co");
        let functions = suggest_client_expectations(&source, source.len());
        assert!(functions.contains(&CompletionExpectation::Semantic(
            SemanticReference::BuiltinFunction(BuiltinFunctionScope::Ordinary),
        )));

        let source = format!("{prefix}input.va");
        let fields = suggest_client_expectations(&source, source.len());
        assert!(fields.iter().any(|expectation| matches!(
            expectation,
            CompletionExpectation::Semantic(SemanticReference::RelayField(relay))
                if relay.as_str() == "incoming"
        )));

        let source = "CREATE JUNCTION normalizer FROM incoming UNBRANCHED TO outgoing SET va";
        let output_fields = suggest_client_expectations(source, source.len());
        assert!(output_fields.iter().any(|expectation| matches!(
            expectation,
            CompletionExpectation::Semantic(SemanticReference::RelayField(relay))
                if relay.as_str() == "outgoing"
        )));

        let source = format!("{prefix}udf::plus");
        let udfs = suggest_client_expectations(&source, source.len());
        assert!(
            udfs.contains(&CompletionExpectation::Semantic(SemanticReference::Model(
                ModelKind::Udf
            ),))
        );

        let required = "ALTER GENERATOR source ADD ROUTE TO outgoing ";
        assert!(
            suggest_client_expectations(required, required.len())
                .contains(&CompletionExpectation::Literal("SET".to_string()))
        );
    }

    #[test]
    fn header_function_expectations_carry_the_transport_kind() {
        let endpoint = "CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON \
                        QUIESCE SUSPEND DECODE USING codec TO outgoing SET value = read_";
        assert!(
            suggest_client_expectations(endpoint, endpoint.len()).contains(
                &CompletionExpectation::Semantic(SemanticReference::BuiltinFunction(
                    BuiltinFunctionScope::IngestSource(IngestSourceKind::Endpoint),
                ))
            )
        );

        let mqtt = "CREATE INGESTOR source FROM MQTT broker TOPIC events MODE NO_ACK SEQUENTIAL \
                    ON QUIESCE SUSPEND DECODE USING codec TO outgoing SET value = read_";
        assert!(suggest_client_expectations(mqtt, mqtt.len()).contains(
            &CompletionExpectation::Semantic(SemanticReference::BuiltinFunction(
                BuiltinFunctionScope::IngestSource(IngestSourceKind::Mqtt),
            ))
        ));

        let kafka = "CREATE EMITTER sink FROM incoming TO KAFKA broker TOPIC events MODE NO_ACK \
                     RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING codec INVOKE write_";
        assert!(suggest_client_expectations(kafka, kafka.len()).contains(
            &CompletionExpectation::Semantic(SemanticReference::BuiltinFunction(
                BuiltinFunctionScope::EmitterInvocation(EmitSinkKind::Kafka),
            ))
        ));

        let sentry = "CREATE EMITTER sink FROM incoming TO SENTRY client MODE ACK RETRY POLICY \
                      BACKOFF 250ms MAX 30s ENCODE USING codec INVOKE write_";
        assert!(suggest_client_expectations(sentry, sentry.len()).contains(
            &CompletionExpectation::Semantic(SemanticReference::BuiltinFunction(
                BuiltinFunctionScope::EmitterInvocation(EmitSinkKind::Sentry),
            ))
        ));
    }

    #[test]
    fn parses_use_domain() {
        assert_eq!(
            parse_use_domain("USE prod;").expect("parse should succeed"),
            DomainName::try_from("prod").expect("valid domain")
        );
        assert_eq!(
            parse_use_domain(" use tenant_a ; ").expect("parse should succeed"),
            DomainName::try_from("tenant_a").expect("valid domain")
        );
        assert!(parse_use_domain("USE two words;").is_err());
    }

    #[test]
    fn parses_client_upload_resource_query() {
        let parsed = parse_upload_resource_query("UPLOAD RESOURCE proto VERSION '/tmp/proto';")
            .expect("parse should succeed");
        assert_eq!(parsed.identifier.as_str(), "proto");
        assert_eq!(parsed.source_path, "/tmp/proto");
    }

    #[test]
    fn parses_list_domains() {
        let parsed = parse_client_statement("LIST DOMAINS;").expect("parse should succeed");
        assert!(matches!(parsed, ClientStatement::ListDomains));
    }

    #[test]
    fn parses_transaction_controls() {
        assert!(matches!(
            parse_client_statement("BEGIN;").expect("parse should succeed"),
            ClientStatement::BeginTransaction
        ));
        assert!(matches!(
            parse_client_statement("COMMIT;").expect("parse should succeed"),
            ClientStatement::CommitTransaction
        ));
        assert!(matches!(
            parse_client_statement("REVERT;").expect("parse should succeed"),
            ClientStatement::RevertTransaction
        ));
    }

    #[test]
    fn parses_create_subscription_as_client_statement() {
        let parsed =
            parse_client_statement("CREATE SUBSCRIPTION live_notifications TO notifications;")
                .expect("parse should succeed");
        match parsed {
            ClientStatement::CreateSubscription(subscription) => {
                assert_eq!(subscription.name.as_str(), "live_notifications");
                assert_eq!(subscription.relay.as_str(), "notifications");
            }
            other => panic!("unexpected statement: {other:?}"),
        }
    }

    #[test]
    fn parses_server_statement_inside_client_statement() {
        let parsed = parse_client_statement("SHOW CLUSTER STATUS;").expect("parse should succeed");
        assert!(matches!(parsed, ClientStatement::Server(_)));
    }

    #[test]
    fn parses_server_statement_without_trailing_semicolon() {
        let parsed = parse_client_statement("CREATE DOMAIN prod").expect("parse should succeed");
        assert!(matches!(parsed, ClientStatement::Server(_)));
    }

    #[test]
    fn parses_semicolon_separated_client_statement_batch() {
        let parsed = parse_client_statements(
            "CREATE DOMAIN prod; CREATE SCHEMA notification ( user_id U32 )",
        )
        .expect("parse should succeed");
        assert_eq!(parsed.len(), 2);
        assert!(
            parsed
                .iter()
                .all(|statement| matches!(statement, ClientStatement::Server(_)))
        );
    }

    #[test]
    fn parsed_client_statement_sources_preserve_upload_segments() {
        let input = "CREATE RESOURCE proto; UPLOAD RESOURCE proto VERSION '/tmp/proto'; DESCRIBE \
                     RESOURCE proto;";
        let parsed = parse_client_statement_sources(input).expect("parse should succeed");

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].source(input), "CREATE RESOURCE proto;");
        assert_eq!(
            parsed[1].source(input),
            "UPLOAD RESOURCE proto VERSION '/tmp/proto';"
        );
        assert!(matches!(
            parsed[1].statement,
            ClientStatement::UploadResource(_)
        ));
        assert_eq!(parsed[2].source(input), "DESCRIBE RESOURCE proto;");
    }

    #[test]
    fn client_statement_batch_ignores_semicolon_inside_strings() {
        let parsed = parse_client_statements(
            "CREATE CLIENT http_main TYPE HTTP CONFIG { 'url' = 'http://localhost/a;b' }; CREATE \
             DOMAIN prod;",
        )
        .expect("parse should succeed");
        assert_eq!(parsed.len(), 2);
    }

    fn parse_example_script(name: &str, source: &str) {
        let statements = parse_client_statement_sources(source)
            .unwrap_or_else(|error| panic!("{name} example should parse: {error:?}"));
        for statement in &statements {
            if let ClientStatement::Server(nervix_models::Statement::Create(create)) =
                &statement.statement
                && let nervix_models::Model::WindowProcessor(window_processor) =
                    create.body.as_ref()
            {
                for output in window_processor.output_routes.outputs() {
                    nervix_vm::window::lower_window_assignments(&output.construction)
                        .unwrap_or_else(|error| {
                            panic!("{name} window aggregate should lower: {error}")
                        });
                }
            }
        }
    }

    #[test]
    fn parses_runnable_example_scripts() {
        parse_example_script("iot", include_str!("../../../examples/iot/iot.nspl"));
        parse_example_script(
            "nats_factory_windows",
            include_str!("../../../examples/nats-factory-windows/nats_factory_windows.nspl"),
        );
        parse_example_script(
            "datalake",
            include_str!("../../../examples/datalake/datalake.nspl"),
        );
        parse_example_script(
            "wasm_dual",
            include_str!("../../../examples/wasm-processors/wasm-dual.nspl"),
        );
    }

    /// Renders every statement of `source` and asserts each one reparses to the same statement.
    fn roundtrip_example_script(name: &str, source: &str) {
        let statements =
            parse_client_statements(source).unwrap_or_else(|error| panic!("{name}: {error:?}"));

        for statement in statements {
            let canonical = statement
                .to_canonical_nspl()
                .unwrap_or_else(|error| panic!("{name}: must render {statement:?}: {error}"));
            let reparsed = parse_client_statement(&canonical)
                .unwrap_or_else(|error| panic!("{name}: {canonical} must reparse: {error:?}"));
            assert_eq!(statement, reparsed, "{name}: {canonical} changed meaning");
        }
    }

    #[test]
    fn canonical_roundtrip_of_statements_outside_the_example_scripts() {
        // Kinds the runnable examples never use: the lifecycle, administration, and query forms.
        const STATEMENTS: &[&str] = &[
            "USE demo;",
            "LIST DOMAINS;",
            "BEGIN;",
            "COMMIT;",
            "REVERT;",
            "CREATE UNPACED DOMAIN demo;",
            "CREATE IF NOT EXISTS UNPACED DOMAIN demo;",
            "CREATE PACED DOMAIN sim WITH PERIOD 100ms SKEW 10ms;",
            "CREATE PACED DOMAIN sim WITH PERIOD 100ms SKEW 10ms PLACEMENT REQUIRE COLOCATION;",
            "CREATE UNPACED DOMAIN demo PLACEMENT SUGGEST SEPARATION;",
            "ALTER DOMAIN SET PLACEMENT NEUTRAL;",
            "ALTER DOMAIN SET PLACEMENT PREFER COLOCATION;",
            "CREATE USER alice WITH PASSWORD 'secret';",
            "CREATE RESOURCE refdata;",
            "UPLOAD RESOURCE refdata VERSION './reference-data';",
            "START;",
            "START AT NOW TIME RATE 1.0;",
            "START AT '2026-01-01T00:00:00Z' TIME RATE 2.0;",
            "STOP;",
            "DROP RELAY orders;",
            "DROP WIRE JSON SCHEMA orders_wire;",
            "DROP NODE node3;",
            "CORDON NODE node3;",
            "UNCORDON NODE node3;",
            "DRAIN NODE node3;",
            "DESCRIBE DOMAIN;",
            "DESCRIBE RELAY orders;",
            "DESCRIBE RELAY orders WHERE (tenant = 'acme');",
            "DESCRIBE INGESTOR ing;",
            "DESCRIBE RESOURCE refdata VERSION 2;",
            "DESCRIBE RESOURCE refdata;",
            "DESCRIBE HASH MAP sites;",
            "DESCRIBE UDF risk_band;",
            "DESCRIBE PLACEMENT scoring_local;",
            "DESCRIBE WINDOW PROCESSOR windows;",
            "DESCRIBE WASM PROCESSOR guest;",
            "SHOW CREATE RELAY orders;",
            "SHOW CREATE WIRE AVRO SCHEMA orders_wire;",
            "SHOW CREATE HASH MAP sites;",
            "SHOW UDFS;",
            "SHOW PLACEMENTS;",
            "SHOW CLUSTER STATUS;",
            "SHOW TRANSACTIONS;",
            "SHOW RELAY orders MATERIALIZED STATE;",
            "LOOKUP sites KEY 'edge-7';",
            "ALTER PLACEMENT scoring SET RANK 2;",
            "ALTER PLACEMENT scoring SET POLICY NEUTRAL, DROP RANK;",
            "ALTER PLACEMENT scoring SET FROM a, b TO c, d;",
            "ALTER PLACEMENT scoring RENAME TO scoring_local;",
            "CREATE SUBSCRIPTION alerts TO critical_alerts;",
            "DELETE SUBSCRIPTION alerts;",
        ];

        for source in STATEMENTS {
            let statement = parse_client_statement(source)
                .unwrap_or_else(|error| panic!("{source} must parse: {error:?}"));
            let canonical = statement
                .to_canonical_nspl()
                .unwrap_or_else(|error| panic!("{source} must render: {error}"));
            let reparsed = parse_client_statement(&canonical)
                .unwrap_or_else(|error| panic!("{canonical} must reparse: {error:?}"));
            assert_eq!(statement, reparsed, "{source} rendered as {canonical}");
        }
    }

    #[test]
    fn canonical_roundtrip_of_every_runnable_example_statement() {
        roundtrip_example_script("iot", include_str!("../../../examples/iot/iot.nspl"));
        roundtrip_example_script(
            "nats_factory_windows",
            include_str!("../../../examples/nats-factory-windows/nats_factory_windows.nspl"),
        );
        roundtrip_example_script(
            "datalake",
            include_str!("../../../examples/datalake/datalake.nspl"),
        );
        roundtrip_example_script(
            "wasm_dual",
            include_str!("../../../examples/wasm-processors/wasm-dual.nspl"),
        );
        roundtrip_example_script(
            "binance_websocket",
            include_str!("../../../examples/binance-websocket/binance_websocket.nspl"),
        );
        roundtrip_example_script(
            "onnx_batched",
            include_str!("../../../examples/onnx-inference/batched.nspl"),
        );
        roundtrip_example_script(
            "onnx_per_message",
            include_str!("../../../examples/onnx-inference/per-message.nspl"),
        );
        roundtrip_example_script(
            "quickstart",
            include_str!("../../../scripts/console-screenshots/quickstart.nspl"),
        );
    }

    #[test]
    fn suggests_client_statement_keywords() {
        let suggestions = suggest_client_statement("UP", 2);
        assert!(suggestions.contains(&"UPLOAD".to_string()));
        let suggestions = suggest_client_statement("CR", 2);
        assert!(suggestions.contains(&"CREATE SUBSCRIPTION".to_string()));
        let suggestions = suggest_client_statement("CREATE ", "CREATE ".len());
        assert!(suggestions.contains(&"SUBSCRIPTION".to_string()));
        let suggestions = suggest_client_statement("DEL", 3);
        assert!(suggestions.contains(&"DELETE SUBSCRIPTION".to_string()));
        let suggestions = suggest_client_statement("LI", 2);
        assert!(suggestions.contains(&"LIST".to_string()));
        let suggestions = suggest_client_statement("BE", 2);
        assert!(suggestions.contains(&"BEGIN".to_string()));
        let suggestions = suggest_client_statement("RE", 2);
        assert!(suggestions.contains(&"REVERT".to_string()));
    }

    #[test]
    fn client_statement_suggestions_do_not_leak_transaction_controls_into_server_context() {
        let suggestions = suggest_client_statement("SHOW ", "SHOW ".len());
        assert!(suggestions.contains(&"CLUSTER".to_string()));
        assert!(suggestions.contains(&"CREATE".to_string()));
        assert!(!suggestions.contains(&"BEGIN".to_string()));
        assert!(!suggestions.contains(&"COMMIT".to_string()));
        assert!(!suggestions.contains(&"REVERT".to_string()));
    }

    #[test]
    fn composed_client_expectations_preserve_semantic_reference_kinds() {
        let source = "CREATE RELAY output SCHEMA ";
        let suggestions = suggest_client_expectations(source, source.len());
        assert!(
            suggestions.contains(&CompletionExpectation::Semantic(SemanticReference::Model(
                ModelKind::Schema
            )))
        );
        assert!(
            !suggestions.contains(&CompletionExpectation::Semantic(SemanticReference::Model(
                ModelKind::Relay
            )))
        );

        let source = "DROP RELAY ";
        let suggestions = suggest_client_expectations(source, source.len());
        assert!(
            suggestions.contains(&CompletionExpectation::Semantic(SemanticReference::Model(
                ModelKind::Relay
            )))
        );
        assert!(
            !suggestions.contains(&CompletionExpectation::Semantic(SemanticReference::Model(
                ModelKind::Schema
            )))
        );

        let source = "USE ";
        let suggestions = suggest_client_expectations(source, source.len());
        assert!(suggestions.contains(&CompletionExpectation::Semantic(SemanticReference::Domain)));

        let source = "CREATE DOMAIN ";
        let suggestions = suggest_client_expectations(source, source.len());
        assert!(!suggestions.contains(&CompletionExpectation::Semantic(SemanticReference::Domain)));
    }

    #[test]
    fn schema_field_expectation_carries_its_schema() {
        let source = "ALTER SCHEMA order_event DROP FIELD val";
        let suggestions = suggest_client_expectations(source, source.len());
        assert!(suggestions.iter().any(|suggestion| matches!(
            suggestion,
            CompletionExpectation::Semantic(SemanticReference::SchemaField(schema))
                if schema.as_str() == "order_event"
        )));

        let source = "ALTER WIRE JSON SCHEMA event_wire RENAME FIELD val";
        let suggestions = suggest_client_expectations(source, source.len());
        assert!(suggestions.iter().any(|suggestion| matches!(
            suggestion,
            CompletionExpectation::Semantic(SemanticReference::WireSchemaField(
                ModelKind::WireJsonSchema,
                schema,
            )) if schema.as_str() == "event_wire"
        )));
    }

    #[test]
    fn grammar_placeholders_do_not_become_text_candidates() {
        let source = "CREATE SCHEMA ";
        assert!(!suggest_client_expectations(source, source.len())
            .iter()
            .any(|candidate| matches!(candidate, CompletionExpectation::Literal(value) if value == "schema_name")));
    }

    #[test]
    fn detects_upload_resource_path_fragment() {
        let input = "UPLOAD RESOURCE proto VERSION '/tmp/pro|to';";
        let cursor = input
            .find('|')
            .assured("the test input contains a cursor marker");
        let input = input.replace('|', "");
        assert_eq!(
            upload_resource_path_range(&input, cursor),
            Some("UPLOAD RESOURCE proto VERSION '".len()..input.len() - 2)
        );
        let closed = "UPLOAD RESOURCE proto VERSION '/tmp/pro';";
        assert_eq!(upload_resource_path_fragment(closed, closed.len()), None);
        assert_eq!(
            upload_resource_path_fragment(
                "UPLOAD RESOURCE proto VERSION '/tmp/pro",
                "UPLOAD RESOURCE proto VERSION '/tmp/pro".len(),
            ),
            Some("/tmp/pro")
        );
        assert_eq!(
            upload_resource_path_fragment(
                "UPLOAD RESOURCE proto VERSION ",
                "UPLOAD RESOURCE proto VERSION ".len(),
            ),
            Some("")
        );
        assert_eq!(
            upload_resource_path_fragment(
                "DESCRIBE RESOURCE proto VERSION ",
                "DESCRIBE RESOURCE proto VERSION ".len(),
            ),
            None
        );
    }
}
