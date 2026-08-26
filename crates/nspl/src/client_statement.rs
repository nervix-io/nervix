use std::ops::Range;

use chumsky::prelude::*;
use nervix_models::{
    CanonicalNsplError, CreateSubscription, DeleteSubscription, Domain, Statement, UploadResource,
};

use crate::{
    lexer::{Identifier as Keyword, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, completion_context, domain_name, into_parse_error, kw,
        lex_input, suggestions_from_errors, tok,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientStatement {
    UseDomain(Domain),
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
-> impl Parser<'src, &'src [Token], Domain, extra::Err<ParseError<'src>>> + Clone {
    kw(Keyword::Use)
        .ignore_then(domain_name())
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

pub fn parse_use_domain(input: &str) -> Result<Domain, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
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
        .expect("successful parse must have output"))
}

pub fn parse_upload_resource_query(input: &str) -> Result<UploadResource, ParseFromSourceError> {
    crate::upload_resource::parse_upload_resource(input)
}

pub fn parse_client_statement(input: &str) -> Result<ClientStatement, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    let out = client_command_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Ok(out
            .into_output()
            .expect("successful parse must have output"));
    }
    let client_errors = out.into_errors();
    if starts_with_client_command_keyword(&tokens) {
        return Err(into_parse_error(
            source,
            &spanned_tokens,
            input.len(),
            client_errors,
        ));
    }
    crate::statement::parse_statement_tokens(&tokens)
        .map(ClientStatement::Server)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
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
    let (_, spanned_tokens, _) = lex_input(input)?;
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
        let end = spanned_tokens
            .last()
            .map_or(input.len(), |token| token.span.end);
        statements.push(ParsedClientStatement {
            span: start..end,
            statement: parse_client_statement(&input[start..])?,
        });
    }

    Ok(statements)
}

fn starts_with_client_command_keyword(tokens: &[Token]) -> bool {
    let Some(Token::Word(Word::KnownWord { iden, .. })) = tokens.first() else {
        return false;
    };
    if *iden == Keyword::Create || *iden == Keyword::Delete {
        let Some(Token::Word(Word::KnownWord { iden, .. })) = tokens.get(1) else {
            return false;
        };
        return *iden == Keyword::Subscription;
    }
    if *iden == Keyword::Use {
        return true;
    }
    if *iden == Keyword::List {
        return true;
    }
    if *iden == Keyword::Begin {
        return true;
    }
    if *iden == Keyword::Commit {
        return true;
    }
    if *iden == Keyword::Revert {
        return true;
    }
    if *iden == Keyword::Upload {
        return true;
    }
    false
}

fn starts_with_server_command_keyword(tokens: &[Token]) -> bool {
    let Some(Token::Word(Word::KnownWord { iden, .. })) = tokens.first() else {
        return false;
    };
    if *iden == Keyword::Create || *iden == Keyword::Delete {
        return if let Some(Token::Word(Word::KnownWord { iden, .. })) = tokens.get(1) {
            *iden != Keyword::Subscription
        } else {
            !matches!(
                tokens.get(1),
                Some(Token::Word(Word::UnknownWord(_))) | None
            )
        };
    }
    !starts_with_client_command_keyword(tokens)
}

pub fn suggest_client_statement(input: &str, cursor: usize) -> Vec<String> {
    let (source, prefix) = completion_context(input, cursor);

    let (_, _, tokens) = match lex_input(&source) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    if starts_with_server_command_keyword(&tokens) {
        return crate::statement::suggest_statement(input, cursor);
    }

    let out = client_command_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    let mut suggestions = if out.has_errors() {
        suggestions_from_errors(out.into_errors(), &prefix)
    } else {
        Vec::new()
    };

    if !starts_with_client_command_keyword(&tokens) {
        for suggestion in crate::statement::suggest_statement(input, cursor) {
            if !suggestions.contains(&suggestion) {
                suggestions.push(suggestion);
            }
        }
    }

    suggestions.sort();
    suggestions
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
    Some(&after_version[quote.len_utf8()..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_use_domain() {
        assert_eq!(
            parse_use_domain("USE prod;").expect("parse should succeed"),
            Domain::try_from("prod").expect("valid domain")
        );
        assert_eq!(
            parse_use_domain(" use tenant_a ; ").expect("parse should succeed"),
            Domain::try_from("tenant_a").expect("valid domain")
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
                    crate::window_processor::aggregate::lower_window_assignments(
                        &output.construction,
                    )
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
    fn detects_upload_resource_path_fragment() {
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
