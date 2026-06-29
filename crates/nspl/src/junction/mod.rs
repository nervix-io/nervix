use chumsky::prelude::*;
use nervix_models::{AckMode, CreateJunction, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, current_word_prefix,
        filter_where_clause, flush_each, from_relay_clauses, if_not_exists_clause,
        into_parse_error, junction_name, kw, lex_input, message_error_policy, processor_outputs,
        suggestions_from_errors, tok,
    },
};

pub fn create_junction_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateJunction>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Junction))
        .then(junction_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .then(processor_outputs())
        .then(branch_parameterization())
        .then(flush_each())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (((((if_not_exists, mode), name), from_inputs), filter_where), outputs),
                        parameterized_by,
                    ),
                    flush_each,
                ),
                message_error_policy,
            )| {
                let (flush_each, max_batch_size) = flush_each;
                CreateStatement::new(
                    CreateJunction {
                        name,
                        from: from_inputs,
                        output_routes: outputs,
                        parameterized_by,
                        flush_each,
                        max_batch_size,
                        message_error_policy,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_junction_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateJunction>, Vec<ParseError<'_>>> {
    let out = create_junction_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_junction(
    input: &str,
) -> Result<CreateStatement<CreateJunction>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_junction_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_junction(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_junction_parser()
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
    fn parses_create_junction() {
        let input = r#"
            CREATE JUNCTION join_streams
                FROM ss1, ss2, ss3
                TO ss10
                PARAMETERIZED BY tenant
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "join_streams");
        assert_eq!(
            parsed
                .from
                .from
                .iter()
                .map(|relay| relay.as_str())
                .collect::<Vec<_>>(),
            vec!["ss1", "ss2", "ss3"]
        );
        assert_eq!(
            parsed
                .output_routes
                .routes
                .first()
                .expect("output route should parse")
                .relay
                .as_str(),
            "ss10"
        );
        assert_eq!(parsed.mode, AckMode::Attached);
    }

    #[test]
    fn parses_create_detached_junction() {
        let tokens = to_tokens(
            "CREATE DETACHED JUNCTION join_streams FROM ss1, ss2 TO ss10 PARAMETERIZED BY tenant \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_junction_flush_each() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1, ss2 TO ss10 PARAMETERIZED BY tenant FLUSH \
             EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "100ms");
    }

    #[test]
    fn parses_junction_flush_immediate() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1, ss2 TO ss10 PARAMETERIZED BY tenant FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "IMMEDIATE");
    }

    #[test]
    fn parses_single_source_junction() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1 TO ss10 UNPARAMETERIZED FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.from.from.len(), 1);
        assert_eq!(parsed.from.from[0].as_str(), "ss1");
    }

    #[test]
    fn suggests_relay_reference_after_from_comma() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"ref:relay".to_string()));
        assert!(!suggestions.contains(&"TO".to_string()));
    }

    #[test]
    fn suggests_to_after_source_list_without_schema_leakage() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ss2 ";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_flush_after_target_without_schema_leakage() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ss2 TO ss10 PARAMETERIZED BY tenant \
                     VALUES { tenant = ss1.tenant } FL";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
