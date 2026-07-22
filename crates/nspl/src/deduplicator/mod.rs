use chumsky::prelude::*;
use nervix_models::{AckMode, CreateDeduplicator, CreateStatement};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_selection, current_word_prefix,
        deduplicator_name, duration_lit, filter_where_clause, flushed_processor_outputs,
        from_relay_clauses, if_not_exists_clause, into_parse_error, kw, kw_phrase2, lex_input,
        materialized_state_dependencies, render_vm_program_tokens, suggestions_from_errors, tok,
        vm_program_error_message,
    },
};

fn boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::Max,
                ..
            })
    )
}

fn deduplicate_on_exprs<'src>()
-> impl Parser<'src, &'src [Token], Vec<nervix_models::Expression>, extra::Err<ParseError<'src>>> + Clone
{
    kw_phrase2(Identifier::Deduplicate, Identifier::On)
        .ignore_then(
            any()
                .filter(|token: &Token| !boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>()
                .labelled("deduplicate_on"),
        )
        .try_map(|tokens, span| {
            crate::parse_expression_list(&render_vm_program_tokens(&tokens))
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
}

pub fn create_deduplicator_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateDeduplicator>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Deduplicator))
        .then(deduplicator_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .then(deduplicate_on_exprs())
        .then_ignore(kw(Identifier::Max))
        .then_ignore(kw(Identifier::Time))
        .then(duration_lit())
        .then(branch_selection())
        .then(materialized_state_dependencies())
        .then(flushed_processor_outputs())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (
                            (
                                ((((if_not_exists, mode), name), from_input), filter_where),
                                deduplicate_on,
                            ),
                            max_time,
                        ),
                        branched_by,
                    ),
                    materialized_state,
                ),
                outputs,
            )| {
                CreateStatement::new(
                    CreateDeduplicator {
                        name,
                        from: from_input,
                        output_routes: outputs,
                        branched_by,
                        deduplicate_on,
                        max_time,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                        materialized_state,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_deduplicator_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateDeduplicator>, Vec<ParseError<'_>>> {
    let out = create_deduplicator_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_deduplicator(
    input: &str,
) -> Result<CreateStatement<CreateDeduplicator>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_deduplicator_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_deduplicator(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_deduplicator_parser()
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
    fn parses_create_deduplicator() {
        let input = r#"
            CREATE DEDUPLICATOR dedup_txns
                FROM ss1
                DEDUPLICATE ON input.transaction_id
                MAX TIME 10m
                BRANCHED BY tenant
                TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "dedup_txns");
        assert_eq!(parsed.from.from[0].as_str(), "ss1");
        assert_eq!(
            parsed
                .output_routes
                .routes
                .first()
                .expect("output route should parse")
                .relay
                .as_str(),
            "ss2"
        );
        assert_eq!(
            parsed.deduplicate_on,
            vec![crate::parse_expression("input.transaction_id").expect("valid expression")]
        );
        assert_eq!(parsed.max_time, "10m");
        assert_eq!(parsed.mode, AckMode::Attached);
    }

    #[test]
    fn parses_source_where_after_from_relay() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 WHERE input.value = 1 DEDUPLICATE ON \
             input.transaction_id MAX TIME 10m BRANCHED BY tenant TO ss2 INHERIT ALL FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.from.from[0].as_str(), "ss1");
        assert_eq!(parsed.from.r#where.len(), 1);
        assert_eq!(parsed.from.r#where[0].relay.as_str(), "ss1");
        assert_eq!(
            parsed.from.r#where[0].where_clause,
            crate::parse_expression("input.value = 1").expect("valid expression")
        );
    }

    #[test]
    fn parses_multiple_from_relays_with_source_where() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 WHERE input.value = 1, ss2 WHERE input.value \
             != 2 DEDUPLICATE ON input.transaction_id MAX TIME 10m BRANCHED BY tenant TO ss3 \
             INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed
                .from
                .from
                .iter()
                .map(|relay| relay.as_str())
                .collect::<Vec<_>>(),
            vec!["ss1", "ss2"]
        );
        assert_eq!(parsed.from.r#where.len(), 2);
        assert_eq!(parsed.from.r#where[0].relay.as_str(), "ss1");
        assert_eq!(
            parsed.from.r#where[0].where_clause,
            crate::parse_expression("input.value = 1").expect("valid expression")
        );
        assert_eq!(parsed.from.r#where[1].relay.as_str(), "ss2");
        assert_eq!(
            parsed.from.r#where[1].where_clause,
            crate::parse_expression("input.value != 2").expect("valid expression")
        );
    }

    #[test]
    fn parses_deduplicator_expression_list() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON \
             concat(lower(input.transaction_id), '-', trim(input.source)), abs(input.amount) MAX \
             TIME 10m BRANCHED BY tenant TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB \
             ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.deduplicate_on.len(), 2);
    }

    #[test]
    fn parses_create_detached_deduplicator() {
        let tokens = to_tokens(
            "CREATE DETACHED DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.transaction_id \
             MAX TIME 10m BRANCHED BY tenant TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE \
             1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_deduplicator_flush_each() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.transaction_id MAX TIME \
             10m BRANCHED BY tenant TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
             MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "100ms"
        );
    }

    #[test]
    fn rejects_missing_time_keyword() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.transaction_id MAX 100 \
             BRANCHED BY tenant TO ss2 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        let errors = parse_create_deduplicator_tokens(&tokens).expect_err("parse must fail");
        let debug = format!("{errors:?}");
        assert!(debug.contains("TIME"));
    }

    #[test]
    fn rejects_empty_source_where() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 WHERE DEDUPLICATE ON input.transaction_id \
             MAX TIME 10m BRANCHED BY tenant TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE \
             1MiB ON MESSAGE ERROR LOG;",
        );
        let errors = parse_create_deduplicator_tokens(&tokens).expect_err("parse must fail");
        assert!(!errors.is_empty());
    }

    #[test]
    fn suggests_where_after_from_relay() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"WHERE".to_string()));
        assert!(suggestions.contains(&"DEDUPLICATE ON".to_string()));
    }

    #[test]
    fn suggests_relay_after_from_comma_without_schema_keyword_leakage() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1, ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"ref:relay".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_to_after_source_where_without_cross_branch_leakage() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 WHERE input.active ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"DEDUPLICATE ON".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_deduplicate_on_as_compound_keyword() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"DEDUPLICATE ON".to_string()));
    }

    #[test]
    fn suggests_field_name_after_deduplicate_on() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"deduplicate_on".to_string()));
        assert!(!suggestions.contains(&"ref:relay".to_string()));
    }

    #[test]
    fn suggests_time_after_max_without_schema_leakage() {
        let input =
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.transaction_id MAX ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"TIME".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_flush_on_output_without_cross_branch_leakage() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.id MAX TIME 10m \
                     UNBRANCHED TO ss2 FL";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
