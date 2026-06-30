use chumsky::prelude::*;
use nervix_models::{
    AckMode, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
    CreateCorrelator, CreateStatement, ProcessorInputs,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, correlator_name,
        current_word_prefix, duration_lit, filter_where_clause, flush_each,
        from_relay_clause_with_boundary, from_where_boundary_token, if_not_exists_clause,
        into_parse_error, kw, kw_phrase2, kw_phrase3, lex_input, message_error_policy,
        processor_outputs, relay_ref, render_vm_program_tokens, suggestions_from_errors, tok,
        vm_program_error_message,
    },
};

fn output_boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::Max,
                ..
            })
    )
}

fn output_assignments<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Output)
        .ignore_then(
            any()
                .filter(|token: &Token| !output_boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .try_map(|tokens, span| {
            let source = render_vm_program_tokens(&tokens);
            crate::vm_program::parse_program(&format!("SET {source}"))
                .map(|_| source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
        .labelled("correlator_output")
}

fn correlate_where_boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Word(Word::KnownWord {
            iden: Identifier::Match,
            ..
        })
    )
}

fn correlate_where_clause<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Correlate, Identifier::Where)
        .ignore_then(
            any()
                .filter(|token: &Token| !correlate_where_boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .try_map(|tokens, span| {
            let source = render_vm_program_tokens(&tokens);
            let program = format!("WHERE {source}");
            crate::vm_program::parse_program(&program)
                .map(|parsed| {
                    if parsed.inner.filter.is_some()
                        && parsed.inner.set.is_empty()
                        && parsed.inner.unset.is_empty()
                        && parsed.inner.branch_filters.is_empty()
                    {
                        Ok(program)
                    } else {
                        Err(Rich::custom(
                            span,
                            "CORRELATE WHERE must contain exactly one WHERE clause",
                        ))
                    }
                })
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))?
        })
}

fn match_policy<'src>()
-> impl Parser<'src, &'src [Token], CorrelatorMatchPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Match).ignore_then(choice((
        kw(Identifier::Earliest).to(CorrelatorMatchPolicy::Earliest),
        kw(Identifier::Latest).to(CorrelatorMatchPolicy::Latest),
    )))
}

fn correlator_from_where_boundary_token(token: &Token) -> bool {
    from_where_boundary_token(token)
        || matches!(
            token,
            Token::Word(Word::KnownWord {
                iden: Identifier::Left | Identifier::Right | Identifier::Correlate,
                ..
            })
        )
}

fn side_from_clauses<'src>(
    side: Identifier,
) -> impl Parser<'src, &'src [Token], ProcessorInputs, extra::Err<ParseError<'src>>> + Clone {
    kw(side)
        .ignore_then(kw(Identifier::From))
        .ignore_then(from_relay_clause_with_boundary(
            correlator_from_where_boundary_token,
        ))
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(|inputs| {
            let mut from = Vec::with_capacity(inputs.len());
            let mut r#where = Vec::new();
            for (relay, mut relay_where) in inputs {
                from.push(relay);
                r#where.append(&mut relay_where);
            }
            ProcessorInputs::new(from, r#where)
        })
}

fn timeout_action<'src>()
-> impl Parser<'src, &'src [Token], CorrelationTimeoutAction, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        kw(Identifier::Drop).to(CorrelationTimeoutAction::Drop),
        kw_phrase2(Identifier::Send, Identifier::To)
            .ignore_then(relay_ref())
            .map(|relay| CorrelationTimeoutAction::SendTo { relay }),
    ))
}

fn timeout_policy<'src>()
-> impl Parser<'src, &'src [Token], CorrelationTimeoutPolicy, extra::Err<ParseError<'src>>> + Clone
{
    kw_phrase3(Identifier::On, Identifier::Correlation, Identifier::Timeout)
        .ignore_then(timeout_action())
        .then_ignore(tok(Token::Comma))
        .then(timeout_action())
        .map(|(left, right)| CorrelationTimeoutPolicy { left, right })
}

pub fn create_correlator_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateCorrelator>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Correlator))
        .then(correlator_name())
        .then(side_from_clauses(Identifier::Left))
        .then(side_from_clauses(Identifier::Right))
        .then(correlate_where_clause())
        .then(match_policy())
        .then(filter_where_clause().or_not())
        .then(processor_outputs())
        .then(branch_parameterization())
        .then(flush_each())
        .then(output_assignments())
        .then_ignore(kw(Identifier::Max))
        .then_ignore(kw(Identifier::Time))
        .then(duration_lit())
        .then(timeout_policy())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .try_map(|value, _span| {
            let (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (((base, correlate_where), match_policy), filter_where),
                                        output_routes,
                                    ),
                                    parameterized_by,
                                ),
                                flush_each,
                            ),
                            output,
                        ),
                        max_time,
                    ),
                    timeout_policy,
                ),
                message_error_policy,
            ) = value;
            let ((((if_not_exists, mode), name), left), right) = base;
            let (flush_each, max_batch_size) = flush_each;
            Ok(CreateStatement::new(
                CreateCorrelator {
                    name,
                    left,
                    right,
                    output_routes,
                    parameterized_by,
                    correlate_where,
                    match_policy,
                    output,
                    max_time,
                    flush_each,
                    max_batch_size,
                    timeout_policy,
                    message_error_policy,
                    mode: mode.unwrap_or(AckMode::Attached),
                    filter_where,
                },
                if_not_exists,
            ))
        })
}

pub fn parse_create_correlator_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateCorrelator>, Vec<ParseError<'_>>> {
    let out = create_correlator_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_correlator(
    input: &str,
) -> Result<CreateStatement<CreateCorrelator>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_correlator_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_correlator(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_correlator_parser()
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
            .map(|token| token.token)
            .collect()
    }

    #[test]
    fn parses_correlator_with_earliest_match() {
        let tokens = to_tokens(
            "CREATE CORRELATOR correlate LEFT FROM relay1 WHERE relay1.name != '' LEFT FROM \
             relay1_extra RIGHT FROM relay2 WHERE relay2.first_name != '' RIGHT FROM relay2_extra \
             CORRELATE WHERE lower(relay1.name) = lower(relay2.first_name) MATCH EARLIEST TO \
             relay3 BRANCHED BY tenant_branch FLUSH EACH 100ms MAX BATCH SIZE 1MiB OUTPUT \
             relay3.name = lower(relay1.name), relay3.surname = upper(relay2.surname) MAX TIME 1s \
             ON CORRELATION TIMEOUT DROP, SEND TO relay4 ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_correlator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "correlate");
        assert_eq!(
            parsed
                .left
                .relays()
                .iter()
                .map(|relay| relay.as_str())
                .collect::<Vec<_>>(),
            vec!["relay1", "relay1_extra"]
        );
        assert_eq!(
            parsed
                .right
                .relays()
                .iter()
                .map(|relay| relay.as_str())
                .collect::<Vec<_>>(),
            vec!["relay2", "relay2_extra"]
        );
        assert_eq!(parsed.left.where_clauses().len(), 1);
        assert_eq!(parsed.right.where_clauses().len(), 1);
        assert_eq!(
            parsed
                .output_routes
                .routes
                .first()
                .expect("output route should parse")
                .relay
                .as_str(),
            "relay3"
        );
        assert_eq!(
            parsed.correlate_where,
            "WHERE lower ( relay1.name ) = lower ( relay2.first_name )"
        );
        assert_eq!(parsed.match_policy, CorrelatorMatchPolicy::Earliest);
        assert!(
            parsed
                .output
                .contains("relay3.name = lower ( relay1.name )")
        );
        assert_eq!(parsed.max_time, "1s");
    }

    #[test]
    fn parses_compound_predicate_and_latest_match() {
        let tokens = to_tokens(
            "CREATE DETACHED CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE \
             WHERE lower(relay1.name) = lower(relay2.first_name) AND relay1.tenant = \
             relay2.tenant MATCH LATEST TO relay3 UNBRANCHED FLUSH IMMEDIATE OUTPUT relay3.name = \
             lower(relay1.name), relay3.surname = upper(relay2.surname) MAX TIME 1s ON \
             CORRELATION TIMEOUT DROP, DROP ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_correlator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
        assert_eq!(
            parsed.correlate_where,
            "WHERE lower ( relay1.name ) = lower ( relay2.first_name ) AND relay1.tenant = \
             relay2.tenant"
        );
        assert_eq!(parsed.match_policy, CorrelatorMatchPolicy::Latest);
        assert_eq!(parsed.flush_each, "IMMEDIATE");
    }

    #[test]
    fn rejects_legacy_comma_separated_inputs() {
        let tokens = to_tokens(
            "CREATE CORRELATOR correlate FROM relay1, relay2 CORRELATE WHERE relay1.name = \
             relay2.first_name MATCH LATEST TO relay3 UNBRANCHED FLUSH IMMEDIATE OUTPUT \
             relay3.name = relay1.name MAX TIME 1s ON CORRELATION TIMEOUT DROP, DROP ON MESSAGE \
             ERROR LOG;",
        );
        assert!(parse_create_correlator_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_missing_right_inputs() {
        let tokens = to_tokens(
            "CREATE CORRELATOR correlate LEFT FROM relay1 CORRELATE WHERE relay1.name = \
             relay2.first_name MATCH LATEST TO relay3 UNBRANCHED FLUSH IMMEDIATE OUTPUT \
             relay3.name = relay1.name MAX TIME 1s ON CORRELATION TIMEOUT DROP, DROP ON MESSAGE \
             ERROR LOG;",
        );
        assert!(parse_create_correlator_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_left_after_correlator_name() {
        let input = "CREATE CORRELATOR correlate ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"LEFT".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }

    #[test]
    fn suggests_correlate_where_without_schema_keyword_leakage() {
        let input = "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"CORRELATE WHERE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }

    #[test]
    fn suggests_right_after_left_input() {
        let input = "CREATE CORRELATOR correlate LEFT FROM relay1 ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"RIGHT".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }

    #[test]
    fn suggests_where_after_correlate() {
        let input = "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"WHERE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }

    #[test]
    fn suggests_match_policy_without_schema_keyword_leakage() {
        let input = "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE \
                     WHERE relay1.name = relay2.first_name MATCH ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"EARLIEST".to_string()));
        assert!(suggestions.contains(&"LATEST".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }

    #[test]
    fn suggests_correlation_timeout_phrase() {
        let input = "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE \
                     WHERE relay1.name = relay2.first_name MATCH LATEST TO relay3 UNBRANCHED \
                     FLUSH IMMEDIATE OUTPUT relay3.name = relay1.name MAX TIME 1s ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"ON CORRELATION TIMEOUT".to_string()));
        assert!(!suggestions.contains(&"ON MESSAGE ERROR".to_string()));
    }
}
