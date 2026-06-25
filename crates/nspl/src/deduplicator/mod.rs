use chumsky::prelude::*;
use nervix_models::{AckMode, CreateDeduplicator, CreateStatement};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, current_word_prefix,
        deduplicator_name, duration_lit, filter_where_clause, flush_each, if_not_exists_clause,
        into_parse_error, kw, kw_phrase2, lex_input, message_error_policy, processor_outputs,
        relay_ref, suggestions_from_errors, tok,
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

fn token_to_source(token: &Token) -> String {
    match token {
        Token::Word(Word::KnownWord { raw, .. }) => raw.clone(),
        Token::Word(Word::UnknownWord(raw)) => raw.clone(),
        Token::StringLiteral(value) => {
            format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        Token::NumberLiteral(value) => value.clone(),
        Token::LBrace => "{".to_string(),
        Token::RBrace => "}".to_string(),
        Token::LBracket => "[".to_string(),
        Token::RBracket => "]".to_string(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::Comma => ",".to_string(),
        Token::Semicolon => ";".to_string(),
        Token::Colon => ":".to_string(),
        Token::Dot => ".".to_string(),
        Token::Hyphen => "-".to_string(),
        Token::Eq => "=".to_string(),
        Token::NotEq => "!=".to_string(),
        Token::Gt => ">".to_string(),
        Token::Lt => "<".to_string(),
        Token::GtEq => ">=".to_string(),
        Token::LtEq => "<=".to_string(),
        Token::Plus => "+".to_string(),
        Token::Star => "*".to_string(),
        Token::Slash => "/".to_string(),
        Token::Percent => "%".to_string(),
    }
}

fn render_tokens(tokens: &[Token]) -> String {
    let mut rendered = String::new();
    for (index, token) in tokens.iter().enumerate() {
        let needs_space = if index == 0 {
            false
        } else {
            let previous = &tokens[index - 1];
            let previous_blocks_space =
                matches!(previous, Token::Dot | Token::LParen | Token::LBracket);
            let token_blocks_space = matches!(
                token,
                Token::Dot | Token::Comma | Token::RParen | Token::RBracket
            );
            !previous_blocks_space && !token_blocks_space
        };
        if needs_space {
            rendered.push(' ');
        }
        rendered.push_str(&token_to_source(token));
    }
    rendered
}

fn deduplicate_on_exprs<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Deduplicate, Identifier::On)
        .ignore_then(
            any()
                .filter(|token: &Token| !boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>()
                .labelled("deduplicate_on"),
        )
        .map(|tokens| render_tokens(&tokens))
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
        .then(relay_ref())
        .then(filter_where_clause().or_not())
        .then(processor_outputs())
        .then(branch_parameterization())
        .then(deduplicate_on_exprs())
        .then_ignore(kw(Identifier::Max))
        .then_ignore(kw(Identifier::Time))
        .then(duration_lit())
        .then(flush_each())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (
                            (
                                (
                                    ((((if_not_exists, mode), name), from_relay), filter_where),
                                    outputs,
                                ),
                                parameterized_by,
                            ),
                            deduplicate_on,
                        ),
                        max_time,
                    ),
                    flush_each,
                ),
                message_error_policy,
            )| {
                let (flush_each, max_batch_size) = flush_each;
                CreateStatement::new(
                    CreateDeduplicator {
                        name,
                        from_relay,
                        output_routes: outputs,
                        parameterized_by,
                        deduplicate_on,
                        max_time,
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
                FROM ss1 TO ss2
                PARAMETERIZED BY tenant
                DEDUPLICATE ON ss1.transaction_id
                MAX TIME 10m
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "dedup_txns");
        assert_eq!(parsed.from_relay.as_str(), "ss1");
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
        assert_eq!(parsed.deduplicate_on, "ss1.transaction_id");
        assert_eq!(parsed.max_time, "10m");
        assert_eq!(parsed.mode, AckMode::Attached);
    }

    #[test]
    fn parses_deduplicator_expression_list() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY tenant DEDUPLICATE \
             ON concat(lower(ss1.transaction_id), '-', trim(ss1.source)), abs(ss1.amount) MAX \
             TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.deduplicate_on,
            "concat (lower (ss1.transaction_id), '-', trim (ss1.source)), abs (ss1.amount)"
        );
    }

    #[test]
    fn parses_create_detached_deduplicator() {
        let tokens = to_tokens(
            "CREATE DETACHED DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY tenant \
             DEDUPLICATE ON ss1.transaction_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB \
             ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_deduplicator_flush_each() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY tenant DEDUPLICATE \
             ON ss1.transaction_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE \
             ERROR LOG;",
        );
        let parsed = parse_create_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "100ms");
    }

    #[test]
    fn rejects_missing_time_keyword() {
        let tokens = to_tokens(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY tenant DEDUPLICATE \
             ON ss1.transaction_id MAX 100 ON MESSAGE ERROR LOG;",
        );
        let errors = parse_create_deduplicator_tokens(&tokens).expect_err("parse must fail");
        let debug = format!("{errors:?}");
        assert!(debug.contains("TIME"));
    }

    #[test]
    fn suggests_deduplicate_on_as_compound_keyword() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY tenant ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"DEDUPLICATE ON".to_string()));
    }

    #[test]
    fn suggests_field_name_after_deduplicate_on() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY tenant \
                     DEDUPLICATE ON ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"deduplicate_on".to_string()));
        assert!(!suggestions.contains(&"ref:relay".to_string()));
    }

    #[test]
    fn suggests_time_after_max_without_schema_leakage() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY tenant \
                     DEDUPLICATE ON ss1.transaction_id MAX ";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"TIME".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_flush_after_max_time_without_cross_branch_leakage() {
        let input = "CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY tenant \
                     DEDUPLICATE ON ss1.transaction_id MAX TIME 10m FL";
        let suggestions = suggest_create_deduplicator(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
