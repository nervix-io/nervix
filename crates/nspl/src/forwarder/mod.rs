use chumsky::prelude::*;
use nervix_models::{AckMode, CreateForwarder, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, current_word_prefix,
        filter_map_program, flush_each, forwarder_name, if_not_exists_clause, into_parse_error, kw,
        lex_input, message_error_policy, relay_ref, suggestions_from_errors, tok,
    },
};

pub fn create_forwarder_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateForwarder>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Forwarder))
        .then(forwarder_name())
        .then_ignore(kw(Identifier::From))
        .then(relay_ref())
        .then_ignore(kw(Identifier::To))
        .then(relay_ref())
        .then(branch_parameterization())
        .then(flush_each())
        .then(filter_map_program().or_not())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (
                            ((((if_not_exists, mode), name), from_relay), into_relay),
                            parameterized_by,
                        ),
                        flush_each,
                    ),
                    filter_map,
                ),
                message_error_policy,
            )| {
                let (flush_each, max_batch_size) = flush_each;
                CreateStatement::new(
                    CreateForwarder {
                        name,
                        from_relay,
                        into_relay,
                        parameterized_by,
                        flush_each,
                        max_batch_size,
                        message_error_policy,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_map,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_forwarder_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateForwarder>, Vec<ParseError<'_>>> {
    let out = create_forwarder_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_forwarder(
    input: &str,
) -> Result<CreateStatement<CreateForwarder>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_forwarder_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_forwarder(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_forwarder_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_create_forwarder_with_filter_map() {
        let input = r#"
            CREATE FORWARDER fw1
                FROM ss1
                TO ss3
                PARAMETERIZED BY tenant
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                SET ss1.normalized = lower(ss1.raw) UNSET ss1.raw WHERE ss1.active ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_forwarder_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "fw1");
        assert_eq!(parsed.from_relay.as_str(), "ss1");
        assert_eq!(parsed.into_relay.as_str(), "ss3");
        assert_eq!(parsed.flush_each, "100ms");
        assert_eq!(parsed.max_batch_size.as_deref(), Some("1MiB"));
        assert_eq!(parsed.mode, AckMode::Attached);
        assert_eq!(
            parsed.filter_map.as_deref(),
            Some("SET ss1.normalized = lower ( ss1.raw ) UNSET ss1.raw WHERE ss1.active")
        );
    }

    #[test]
    fn parses_detached_forwarder() {
        let tokens = to_tokens(
            "CREATE DETACHED FORWARDER fw1 FROM ss1 TO ss3 PARAMETERIZED BY tenant FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_forwarder_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_forwarder_flush_immediate() {
        let tokens = to_tokens(
            "CREATE FORWARDER fw1 FROM ss1 TO ss3 PARAMETERIZED BY tenant FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_forwarder_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "IMMEDIATE");
    }

    #[test]
    fn rejects_forwarder_without_target() {
        let tokens = to_tokens("CREATE FORWARDER fw1 FROM ss1 ON MESSAGE ERROR LOG;");
        assert!(parse_create_forwarder_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_forwarder_without_flush_each() {
        let tokens = to_tokens("CREATE FORWARDER fw1 FROM ss1 TO ss3 ON MESSAGE ERROR LOG;");
        assert!(parse_create_forwarder_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_forwarder_flush_each_without_max_batch_size() {
        let tokens = to_tokens(
            "CREATE FORWARDER fw1 FROM ss1 TO ss3 PARAMETERIZED BY tenant FLUSH EACH 100ms ON \
             MESSAGE ERROR LOG;",
        );
        assert!(parse_create_forwarder_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_max_batch_size_after_flush_each_duration() {
        let input =
            "CREATE FORWARDER fw1 FROM ss1 TO ss3 PARAMETERIZED BY tenant FLUSH EACH 100ms ";
        let suggestions = suggest_create_forwarder(input, input.len());
        assert!(suggestions.contains(&"MAX BATCH SIZE".to_string()));
        assert!(!suggestions.contains(&"ON".to_string()));
    }

    #[test]
    fn suggests_to_after_source_without_cross_branch_leakage() {
        let input = "CREATE FORWARDER fw1 FROM ss1 ";
        let suggestions = suggest_create_forwarder(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
