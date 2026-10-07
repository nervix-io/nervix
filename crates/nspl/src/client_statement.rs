use std::ops::Range;

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    Backup, BuiltinFunctionScope, CanonicalNsplError, CreateSubscription, DeleteSubscription,
    DescribeBackup, DomainName, EmitSinkKind, IngestSourceKind, ModelKind, RelayName, Restore,
    SchemaName, SemanticReference, Statement, UploadResource, WireSchemaName,
};

use crate::{
    lexer::{Identifier as Keyword, Token, Word},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, ack_mode, completion_context,
        completion_tokens, domain_ref, filter_by_prefix, if_not_exists_clause, into_parse_error,
        junction_name, kw, kw_phrase3, lex_input, relay_ref, schema_name, suggestions_from_errors,
        tok, wire_schema_name,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientStatement {
    UseDomain(DomainName),
    ListDomains,
    /// Attaches the session to the clock of its active domain.
    AttachDomainClock,
    /// Detaches the session from the clock of its active domain.
    DetachDomainClock,
    BeginTransaction,
    CommitTransaction,
    RevertTransaction,
    UploadResource(UploadResource),
    /// Describes a local archive file without asking a server.
    DescribeBackup(DescribeBackup),
    CreateSubscription(CreateSubscription),
    DeleteSubscription(DeleteSubscription),
    Server(Statement),
}

impl ClientStatement {
    /// Renders this statement as canonical NSPL.
    ///
    /// Server statements delegate to [`Statement::to_canonical_nspl`]; the session-local forms are
    /// rendered here because they belong to the client protocol rather than to a stored model.
    pub fn to_canonical_nspl(&self) -> error_stack::Result<String, CanonicalNsplError> {
        match self {
            Self::UseDomain(domain) => Ok(format!("USE {};", domain.as_str())),
            Self::ListDomains => Ok("LIST DOMAINS;".to_string()),
            Self::AttachDomainClock => Ok("ATTACH DOMAIN CLOCK;".to_string()),
            Self::DetachDomainClock => Ok("DETACH DOMAIN CLOCK;".to_string()),
            Self::BeginTransaction => Ok("BEGIN;".to_string()),
            Self::CommitTransaction => Ok("COMMIT;".to_string()),
            Self::RevertTransaction => Ok("REVERT;".to_string()),
            Self::UploadResource(upload) => {
                Statement::UploadResource(upload.clone()).to_canonical_nspl()
            }
            Self::DescribeBackup(describe) => Ok(describe.to_canonical_nspl()),
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

    /// Whether the client serves this statement itself, alone, rather than as one statement of a
    /// command batch.
    ///
    /// A `BACKUP` is also executed by the server, but its client writes the archive the server
    /// assembles to a local file, so it too is sent on its own. A `RESTORE` is executed by the
    /// server from an archive its client reads and streams, so it is sent on its own as well.
    pub fn requires_local_handling(&self) -> bool {
        match self {
            Self::UseDomain(_)
            | Self::ListDomains
            | Self::AttachDomainClock
            | Self::DetachDomainClock
            | Self::UploadResource(_)
            | Self::DescribeBackup(_)
            | Self::Server(Statement::Backup(_) | Statement::Restore(_)) => true,
            Self::BeginTransaction
            | Self::CommitTransaction
            | Self::RevertTransaction
            | Self::CreateSubscription(_)
            | Self::DeleteSubscription(_)
            | Self::Server(_) => false,
        }
    }

    /// Whether this statement saves, streams or reads a backup archive on its client's machine: a
    /// `BACKUP` saves the archive the server assembles, a `RESTORE` streams one to the server, and
    /// a `DESCRIBE BACKUP` reads one without asking a server.
    pub fn handles_backup_archive(&self) -> bool {
        match self {
            Self::DescribeBackup(_)
            | Self::Server(Statement::Backup(_) | Statement::Restore(_)) => true,
            Self::UseDomain(_)
            | Self::ListDomains
            | Self::AttachDomainClock
            | Self::DetachDomainClock
            | Self::BeginTransaction
            | Self::CommitTransaction
            | Self::RevertTransaction
            | Self::UploadResource(_)
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

    /// Transaction catalog reads run beside an attached transaction without taking a queue
    /// position or changing its binding.
    pub fn reads_transaction_state(&self) -> bool {
        self.inspects_transaction() || matches!(self, Self::Server(Statement::ShowTransactions(_)))
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

/// `ATTACH DOMAIN CLOCK`, one composed phrase that completion offers as one item.
pub fn attach_domain_clock_parser<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Keyword::Attach, Keyword::Domain, Keyword::Clock)
        .then_ignore(tok(Token::Semicolon).or_not())
}

/// `DETACH DOMAIN CLOCK`, one composed phrase that completion offers as one item.
pub fn detach_domain_clock_parser<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Keyword::Detach, Keyword::Domain, Keyword::Clock)
        .then_ignore(tok(Token::Semicolon).or_not())
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
        attach_domain_clock_parser().to(ClientStatement::AttachDomainClock),
        detach_domain_clock_parser().to(ClientStatement::DetachDomainClock),
        begin_transaction_parser().to(ClientStatement::BeginTransaction),
        commit_transaction_parser().to(ClientStatement::CommitTransaction),
        revert_transaction_parser().to(ClientStatement::RevertTransaction),
        crate::upload_resource::upload_resource_parser().map(ClientStatement::UploadResource),
        crate::backup::backup_parser()
            .map(|backup| ClientStatement::Server(Statement::Backup(backup))),
        crate::backup::restore_parser()
            .map(|restore| ClientStatement::Server(Statement::Restore(restore))),
        crate::backup::describe_backup_parser().map(ClientStatement::DescribeBackup),
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
        Keyword::Client => Some(IngestSourceKind::Client),
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

pub fn parse_use_domain(input: &str) -> error_stack::Result<DomainName, ParseFromSourceError> {
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

pub fn parse_upload_resource_query(
    input: &str,
) -> error_stack::Result<UploadResource, ParseFromSourceError> {
    crate::upload_resource::parse_upload_resource(input)
}

pub fn parse_client_statement(
    input: &str,
) -> error_stack::Result<ClientStatement, ParseFromSourceError> {
    let lexed = lex_input(input)?;
    let tokens = 0..lexed.tokens.len();
    parse_lexed_client_statement(&lexed, tokens, input.len())
}

/// Parses the one statement that `tokens` of `lexed` hold.
///
/// The tokens keep the spans they have in the lexed source, so a rejection is located in that whole
/// source rather than in the statement alone, and its diagnostics index the text it names.
/// `source_end` is where the statement's source ends, at its terminating semicolon or at the end of
/// the source; a rejection that expected another token points there.
fn parse_lexed_client_statement(
    lexed: &LexedInput,
    tokens: Range<usize>,
    source_end: usize,
) -> error_stack::Result<ClientStatement, ParseFromSourceError> {
    let spanned_tokens = &lexed.spanned_tokens[tokens.clone()];
    let out = client_statement_parser()
        .then_ignore(end())
        .parse(&lexed.tokens[tokens]);
    if out.has_errors() {
        return Err(into_parse_error(
            lexed.source.clone(),
            spanned_tokens,
            source_end,
            out.into_errors(),
        ));
    }
    Ok(out
        .into_output()
        .verified("has_errors returned false above, so this parse produced output"))
}

pub fn parse_client_statements(
    input: &str,
) -> error_stack::Result<Vec<ClientStatement>, ParseFromSourceError> {
    parse_client_statement_sources(input).map(|statements| {
        statements
            .into_iter()
            .map(|parsed| parsed.statement)
            .collect()
    })
}

/// Parses a batch of semicolon-separated statements, keeping where each one sits in `input`.
///
/// The batch is lexed once and every statement is parsed from its own run of those tokens. A
/// rejected statement is therefore reported against the whole batch: its diagnostics index `input`,
/// wherever in the batch that statement starts.
pub fn parse_client_statement_sources(
    input: &str,
) -> error_stack::Result<Vec<ParsedClientStatement>, ParseFromSourceError> {
    /// The statement being read: where its first token sits among the batch's tokens and in the
    /// batch's source.
    struct OpenStatement {
        first_token: usize,
        start: usize,
    }

    let lexed = lex_input(input)?;
    let mut statements = Vec::new();
    let mut open: Option<OpenStatement> = None;

    for (index, token) in lexed.spanned_tokens.iter().enumerate() {
        if token.token == Token::Semicolon {
            // A segment is a statement only when it actually contains tokens, so a stray
            // semicolon or a trailing comment does not become an empty statement.
            if let Some(statement) = open.take() {
                let tokens = statement.first_token..index;
                statements.push(ParsedClientStatement {
                    span: statement.start..token.span.end,
                    statement: parse_lexed_client_statement(&lexed, tokens, token.span.start)?,
                });
            }
        } else if open.is_none() {
            open = Some(OpenStatement {
                first_token: index,
                start: token.span.start,
            });
        }
    }

    if let Some(statement) = open {
        let end = match lexed.spanned_tokens.last() {
            Some(token) => token.span.end,
            None => input.len(),
        };
        let tokens = statement.first_token..lexed.spanned_tokens.len();
        statements.push(ParsedClientStatement {
            span: statement.start..end,
            statement: parse_lexed_client_statement(&lexed, tokens, input.len())?,
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
            Some(ClientStatement::Server(Statement::Backup(backup))) => {
                backup_tail(&backup, &source, &prefix)
            }
            Some(ClientStatement::Server(Statement::Restore(restore))) => {
                restore_tail(&restore, &tokens, &source, &prefix)
            }
            Some(ClientStatement::Server(statement)) => {
                crate::statement::statement_tail(&statement, &tokens, &source, &prefix)
            }
            Some(ClientStatement::DescribeBackup(describe)) => {
                describe_backup_tail(&describe, &tokens, &source, &prefix)
            }
            _ => Vec::new(),
        }
    };
    (labels, tokens)
}

/// The optional clause completion offers after a complete `BACKUP`.
///
/// Nothing may follow a terminated statement, and a clause is offered only once the word before
/// it has ended, so a statement still being typed is left to its own expectations.
fn backup_tail(backup: &Backup, source: &str, prefix: &str) -> Vec<String> {
    let trimmed = source.trim_end();
    let open = source.len() > trimmed.len() && !trimmed.ends_with(';');
    if !open {
        return Vec::new();
    }
    filter_by_prefix(crate::backup::backup_tail(backup), prefix)
}

/// The optional clauses completion offers after a complete `RESTORE`.
///
/// Nothing may follow a terminated statement, and a clause is offered only once the word before
/// it has ended, so a statement still being typed is left to its own expectations.
fn restore_tail(restore: &Restore, tokens: &[Token], source: &str, prefix: &str) -> Vec<String> {
    let trimmed = source.trim_end();
    let open = source.len() > trimmed.len() && !trimmed.ends_with(';');
    if !open {
        return Vec::new();
    }
    filter_by_prefix(crate::backup::restore_tail(restore, tokens), prefix)
}

/// The optional clauses completion offers after a complete `DESCRIBE BACKUP`.
///
/// Nothing may follow a terminated statement, and a clause is offered only once the word before
/// it has ended, so a statement still being typed is left to its own expectations.
fn describe_backup_tail(
    describe: &DescribeBackup,
    tokens: &[Token],
    source: &str,
    prefix: &str,
) -> Vec<String> {
    let trimmed = source.trim_end();
    let open = source.len() > trimmed.len() && !trimmed.ends_with(';');
    if !open {
        return Vec::new();
    }
    filter_by_prefix(
        crate::backup::describe_backup_tail(describe, tokens),
        prefix,
    )
}

/// A local path being written at the cursor, which a client completes from its own filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPathFragment<'input> {
    /// The part of the path already written, from its opening quote to the cursor. Empty when the
    /// cursor is where a quoted path may begin.
    pub fragment: &'input str,
    /// The source bytes a candidate path replaces: the path written so far, and the rest of it up
    /// to its closing quote.
    pub range: Range<usize>,
}

/// Finds the local path being written at `cursor`, if the grammar expects one there.
///
/// A client-local statement names a file it reads or writes as a quoted `local_path`. Inside an
/// unterminated quote the source no longer lexes, so the grammar is asked what it expects where the
/// quote opens; outside a quote it is asked at the cursor itself. Only a position that expects a
/// `local_path` answers, which is what keeps a path lookup out of every other string literal.
pub fn local_path_fragment(input: &str, cursor: usize) -> Option<LocalPathFragment<'_>> {
    let cursor = input.floor_char_boundary(cursor.min(input.len()));
    let head = &input[..cursor];
    let opening = head
        .char_indices()
        .rev()
        .find(|(_, character)| matches!(character, '\'' | '"'));
    if let Some((quote_at, quote)) = opening {
        let fragment_start = quote_at
            .checked_add(quote.len_utf8())
            .verified("a quote found in the head is followed by at least the cursor");
        let fragment = &head[fragment_start..];
        if !fragment.contains('\n') && expects_local_path(&head[..quote_at]) {
            let rest = &input[cursor..];
            let line_end = rest.find('\n').unwrap_or(rest.len());
            let closing = rest[..line_end].find(quote).unwrap_or(line_end);
            let end = cursor
                .checked_add(closing)
                .verified("the closing quote is found within the input after the cursor");
            return Some(LocalPathFragment {
                fragment,
                range: fragment_start..end,
            });
        }
    }
    if expects_local_path(head) {
        return Some(LocalPathFragment {
            fragment: "",
            range: cursor..cursor,
        });
    }
    None
}

/// Whether the client grammar expects a `local_path` right after `head`.
fn expects_local_path(head: &str) -> bool {
    suggest_client_statement(head, head.len())
        .iter()
        .any(|label| label == "local_path")
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

        let client = "CREATE INGESTOR source FROM CLIENT SCHEMA event MODE ACK SEQUENTIAL ACK \
                      TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND TO outgoing \
                      SET value = read_";
        assert!(suggest_client_expectations(client, client.len()).contains(
            &CompletionExpectation::Semantic(SemanticReference::BuiltinFunction(
                BuiltinFunctionScope::IngestSource(IngestSourceKind::Client),
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
    fn parses_domain_clock_attachment_statements_in_any_case() {
        for (source, expected) in [
            ("ATTACH DOMAIN CLOCK;", ClientStatement::AttachDomainClock),
            ("attach domain clock", ClientStatement::AttachDomainClock),
            (
                " Detach Domain Clock ; ",
                ClientStatement::DetachDomainClock,
            ),
            ("DETACH DOMAIN CLOCK", ClientStatement::DetachDomainClock),
        ] {
            let parsed = parse_client_statement(source)
                .unwrap_or_else(|error| panic!("{source:?} must parse: {error:?}"));
            assert_eq!(parsed, expected, "{source:?}");
            assert!(parsed.requires_local_handling(), "{source:?}");
        }
    }

    #[test]
    fn only_backup_restore_and_describe_backup_handle_backup_archives() {
        for (source, handles) in [
            ("BACKUP CLUSTER TO '/tmp/cluster.nvxb';", true),
            (
                "RESTORE DOMAIN prod AS prod_copy FROM './arch.nvxb' DRY RUN;",
                true,
            ),
            ("DESCRIBE BACKUP './cluster.nvxb' FORMAT JSON;", true),
            ("UPLOAD RESOURCE proto VERSION '/tmp/proto';", false),
            ("LIST DOMAINS;", false),
            ("SHOW TRANSACTIONS;", false),
        ] {
            let parsed = parse_client_statement(source)
                .unwrap_or_else(|error| panic!("{source:?} must parse: {error:?}"));
            assert_eq!(parsed.handles_backup_archive(), handles, "{source:?}");
        }
    }

    #[test]
    fn rejects_incomplete_or_qualified_domain_clock_statements() {
        for source in [
            "ATTACH;",
            "ATTACH DOMAIN;",
            "ATTACH CLOCK;",
            "DETACH DOMAIN;",
            "ATTACH DOMAIN CLOCK sim;",
            "ATTACH DOMAIN sim CLOCK;",
            "DETACH DOMAIN CLOCK NOW;",
            "ATTACH DOMAIN CLOCK; DETACH",
        ] {
            assert!(
                parse_client_statements(source).is_err(),
                "{source:?} must be rejected"
            );
        }
    }

    #[test]
    fn completion_offers_each_domain_clock_statement_as_one_phrase() {
        for (source, phrase) in [
            ("AT", "ATTACH DOMAIN CLOCK"),
            ("", "ATTACH DOMAIN CLOCK"),
            ("DET", "DETACH DOMAIN CLOCK"),
            ("", "DETACH DOMAIN CLOCK"),
        ] {
            let suggestions = suggest_client_statement(source, source.len());
            assert!(
                suggestions.contains(&phrase.to_string()),
                "{source:?} must offer {phrase:?}: {suggestions:?}"
            );
        }
        assert_eq!(
            suggest_client_statement("ATTACH ", "ATTACH ".len()),
            ["DOMAIN"]
        );
        assert_eq!(
            suggest_client_statement("DETACH DOMAIN ", "DETACH DOMAIN ".len()),
            ["CLOCK"]
        );
        assert_eq!(
            suggest_client_statement("ATTACH DOMAIN CL", "ATTACH DOMAIN CL".len()),
            ["CLOCK"]
        );
    }

    #[test]
    fn domain_clock_phrases_stay_out_of_other_statement_contexts() {
        for source in ["SHOW ", "CREATE ", "DROP ", "DESCRIBE ", "START ", "LIST "] {
            let suggestions = suggest_client_statement(source, source.len());
            for phrase in [
                "ATTACH DOMAIN CLOCK",
                "DETACH DOMAIN CLOCK",
                "ATTACH",
                "DETACH",
                "CLOCK",
            ] {
                assert!(
                    !suggestions.contains(&phrase.to_string()),
                    "{source:?} leaks {phrase:?}: {suggestions:?}"
                );
            }
        }
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
    fn a_rejected_statement_is_located_in_the_whole_source() {
        for (input, rejected) in [
            ("CREATE SCHEMA broken (id BOGUS);", 25..30),
            ("  CREATE SCHEMA broken (id BOGUS);", 27..32),
            (
                "CREATE SCHEMA valid (id STRING);\nCREATE SCHEMA broken (id BOGUS);",
                58..63,
            ),
            ("// why\nCREATE SCHEMA broken (id BOGUS)", 32..37),
        ] {
            let error = parse_client_statement_sources(input).expect_err("BOGUS is not a type");
            let ParseFromSourceError::Parse { text, diagnostics } = error.current_context() else {
                panic!("every statement lexes, so parsing rejects the batch: {error:?}");
            };
            assert_eq!(text, input, "the diagnostics index the text they name");
            assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
            assert_eq!(diagnostics[0].span, rejected, "{input:?}");
            assert_eq!(&input[diagnostics[0].span.clone()], "BOGUS");
            assert!(
                diagnostics[0].message.ends_with("found BOGUS"),
                "{}",
                diagnostics[0].message
            );
        }
    }

    #[test]
    fn an_incomplete_statement_is_located_at_its_own_end() {
        let input = "USE demo;\nCREATE RELAY;\nBEGIN;";
        let error = parse_client_statement_sources(input).expect_err("a relay needs a name");
        let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
            panic!("every statement lexes, so parsing rejects the batch: {error:?}");
        };
        assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
        assert_eq!(
            diagnostics[0].span,
            22..22,
            "the relay name is missing before the `;`"
        );
    }

    #[test]
    fn an_unlexable_batch_is_rejected_at_the_lex_stage() {
        let input = "USE demo;\nUSE 'unterminated;";
        let error = parse_client_statement_sources(input).expect_err("the string never closes");
        let ParseFromSourceError::Lex { text, diagnostics } = error.current_context() else {
            panic!("an unterminated string cannot be lexed: {error:?}");
        };
        assert_eq!(text, input);
        assert!(!diagnostics.is_empty());
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.span.end <= input.len())
        );
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
        parse_example_script(
            "paced_simulation",
            include_str!("../../../examples/paced-simulation/paced_simulation.nspl"),
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
            "ATTACH DOMAIN CLOCK;",
            "DETACH DOMAIN CLOCK;",
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
            "BACKUP CLUSTER TO './cluster.nvxb';",
            "BACKUP DOMAIN demo TO './demo.nvxb' WITHOUT RESOURCES;",
            "BACKUP DOMAIN TO './current.nvxb';",
            "DESCRIBE BACKUP './cluster.nvxb';",
            "DESCRIBE BACKUP './cluster.nvxb' FORMAT JSON;",
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
            "SHOW INGESTORS;",
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

    fn fragment_at_marker(input: &str) -> (String, Option<(String, Range<usize>)>) {
        let cursor = input
            .find('|')
            .assured("the test input contains a cursor marker");
        let source = input.replace('|', "");
        let found = local_path_fragment(&source, cursor)
            .map(|found| (found.fragment.to_string(), found.range));
        (source, found)
    }

    #[test]
    fn finds_the_local_path_of_every_client_local_statement() {
        for (input, expected_fragment) in [
            ("UPLOAD RESOURCE proto VERSION '/tmp/pro|to';", "/tmp/pro"),
            ("BACKUP CLUSTER TO '/tmp/cl|';", "/tmp/cl"),
            (
                "BACKUP DOMAIN prod TO \"~/ba|ck.nvxb\" WITHOUT RESOURCES;",
                "~/ba",
            ),
            ("BACKUP DOMAIN TO './|", "./"),
            ("DESCRIBE BACKUP 'arch|ives/c.nvxb' FORMAT JSON;", "arch"),
            ("RESTORE CLUSTER FROM '/tmp/re|';", "/tmp/re"),
            (
                "RESTORE DOMAIN prod AS prod_copy FROM './ar|ch.nvxb' DRY RUN;",
                "./ar",
            ),
        ] {
            let (source, found) = fragment_at_marker(input);
            let (fragment, range) = found.unwrap_or_else(|| panic!("{input:?} names a local path"));
            assert_eq!(fragment, expected_fragment, "{input:?}");
            let quote_at = input
                .find(['\'', '"'])
                .assured("each input quotes its path");
            assert_eq!(range.start, quote_at + 1, "{input:?}");
            let replaced = &source[range];
            assert!(
                replaced.starts_with(expected_fragment) && !replaced.contains(['\'', '"']),
                "{input:?} replaces {replaced:?}"
            );
        }
    }

    #[test]
    fn offers_a_path_lookup_where_a_quoted_path_may_begin() {
        for input in [
            "UPLOAD RESOURCE proto VERSION |",
            "BACKUP CLUSTER TO |",
            "DESCRIBE BACKUP |",
            "RESTORE CLUSTER FROM |",
            "RESTORE DOMAIN prod AS prod_copy FROM |",
        ] {
            let (_, found) = fragment_at_marker(input);
            let cursor = input
                .find('|')
                .assured("the test input contains a cursor marker");
            assert_eq!(
                found,
                Some((String::new(), cursor..cursor)),
                "{input:?} expects a local path"
            );
        }
    }

    #[test]
    fn other_strings_are_not_local_paths() {
        for input in [
            "UPLOAD RESOURCE proto VERSION '/tmp/pro';|",
            "BACKUP CLUSTER TO '/tmp/c.nvxb' |",
            "DESCRIBE RESOURCE proto VERSION |",
            "CREATE USER alice WITH PASSWORD 'sec|",
            "CREATE CLIENT http_main TYPE HTTP CONFIG { 'url' = 'http://lo|",
            "DESCRIBE TRANSACTION 'tx|",
            "BACKUP CLUSTER |",
            "RESTORE CLUSTER FROM '/tmp/c.nvxb' |",
            "RESTORE DOMAIN |",
        ] {
            let (_, found) = fragment_at_marker(input);
            assert_eq!(found, None, "{input:?} is not a local path");
        }
    }
}
