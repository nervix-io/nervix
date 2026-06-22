use chumsky::prelude::*;
use nervix_models::{AckMode, CreateStatement, CreateUnifier};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, current_word_prefix,
        filter_map_program, flush_each, if_not_exists_clause, into_parse_error, kw, lex_input,
        message_error_policy, relay_ref, suggestions_from_errors, tok, unifier_name,
    },
};

pub fn create_unifier_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateUnifier>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Unifier))
        .then(unifier_name())
        .then_ignore(kw(Identifier::From))
        .then(
            relay_ref()
                .separated_by(tok(Token::Comma))
                .at_least(2)
                .collect::<Vec<_>>(),
        )
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
                            ((((if_not_exists, mode), name), from_relays), into_relay),
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
                    CreateUnifier {
                        name,
                        from_relays,
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

pub fn parse_create_unifier_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateUnifier>, Vec<ParseError<'_>>> {
    let out = create_unifier_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_unifier(
    input: &str,
) -> Result<CreateStatement<CreateUnifier>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_unifier_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_unifier(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_unifier_parser()
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
    fn parses_create_unifier() {
        let input = r#"
            CREATE UNIFIER join_streams
                FROM ss1, ss2, ss3
                TO ss10
                PARAMETERIZED BY tenant
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_unifier_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "join_streams");
        assert_eq!(
            parsed
                .from_relays
                .iter()
                .map(|relay| relay.as_str())
                .collect::<Vec<_>>(),
            vec!["ss1", "ss2", "ss3"]
        );
        assert_eq!(parsed.into_relay.as_str(), "ss10");
        assert_eq!(parsed.mode, AckMode::Attached);
    }

    #[test]
    fn parses_create_detached_unifier() {
        let tokens = to_tokens(
            "CREATE DETACHED UNIFIER join_streams FROM ss1, ss2 TO ss10 PARAMETERIZED BY tenant \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_unifier_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_unifier_flush_each() {
        let tokens = to_tokens(
            "CREATE UNIFIER join_streams FROM ss1, ss2 TO ss10 PARAMETERIZED BY tenant FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_unifier_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "100ms");
    }

    #[test]
    fn parses_unifier_flush_immediate() {
        let tokens = to_tokens(
            "CREATE UNIFIER join_streams FROM ss1, ss2 TO ss10 PARAMETERIZED BY tenant FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_unifier_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "IMMEDIATE");
    }

    #[test]
    fn rejects_single_source_unifier() {
        let tokens =
            to_tokens("CREATE UNIFIER join_streams FROM ss1 TO ss10 ON MESSAGE ERROR LOG;");
        assert!(parse_create_unifier_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_relay_reference_after_from_comma() {
        let input = "CREATE UNIFIER join_streams FROM ss1, ";
        let suggestions = suggest_create_unifier(input, input.len());
        assert!(suggestions.contains(&"ref:relay".to_string()));
        assert!(!suggestions.contains(&"TO".to_string()));
    }

    #[test]
    fn suggests_to_after_source_list_without_schema_leakage() {
        let input = "CREATE UNIFIER join_streams FROM ss1, ss2 ";
        let suggestions = suggest_create_unifier(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_flush_after_target_without_schema_leakage() {
        let input = "CREATE UNIFIER join_streams FROM ss1, ss2 TO ss10 PARAMETERIZED BY tenant \
                     VALUES { tenant = ss1.tenant } FL";
        let suggestions = suggest_create_unifier(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
