use chumsky::prelude::*;
use nervix_models::{CreateSubscription, DeleteSubscription, Domain, Statement, UploadResource};

use crate::{
    lexer::{Identifier as Keyword, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, domain_name, into_parse_error, kw,
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
    pub source: String,
    pub statement: ClientStatement,
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
    let mut start = 0;

    for token in spanned_tokens
        .iter()
        .filter(|token| token.token == Token::Semicolon)
    {
        let segment = &input[start..token.span.start];
        if !segment.trim().is_empty() {
            statements.push(ParsedClientStatement {
                source: segment.trim().to_string(),
                statement: parse_client_statement(segment)?,
            });
        }
        start = token.span.end;
    }

    let tail = &input[start..];
    if !tail.trim().is_empty() {
        statements.push(ParsedClientStatement {
            source: tail.trim().to_string(),
            statement: parse_client_statement(tail)?,
        });
    }

    if statements.is_empty() {
        return parse_client_statement(input).map(|statement| {
            vec![ParsedClientStatement {
                source: input.trim().to_string(),
                statement,
            }]
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
        } else if let Some(Token::Word(Word::UnknownWord(_))) | None = tokens.get(1) {
            false
        } else {
            true
        };
    }
    !starts_with_client_command_keyword(tokens)
}

pub fn suggest_client_statement(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
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
        assert!(parsed.iter().all(|statement| {
            if let ClientStatement::Server(_) = statement {
                true
            } else {
                false
            }
        }));
    }

    #[test]
    fn parsed_client_statement_sources_preserve_upload_segments() {
        let parsed = parse_client_statement_sources(
            "CREATE RESOURCE proto; UPLOAD RESOURCE proto VERSION '/tmp/proto'; DESCRIBE RESOURCE \
             proto;",
        )
        .expect("parse should succeed");

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].source, "CREATE RESOURCE proto");
        assert_eq!(
            parsed[1].source,
            "UPLOAD RESOURCE proto VERSION '/tmp/proto'"
        );
        assert!(matches!(
            parsed[1].statement,
            ClientStatement::UploadResource(_)
        ));
        assert_eq!(parsed[2].source, "DESCRIBE RESOURCE proto");
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
                crate::window_processor::aggregate::parse_aggregate_program(
                    &window_processor.aggregate,
                )
                .unwrap_or_else(|error| panic!("{name} window aggregate should parse: {error:?}"));
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
