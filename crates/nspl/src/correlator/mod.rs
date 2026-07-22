use chumsky::prelude::*;
use nervix_models::{
    AckMode, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
    CreateCorrelator, CreateStatement, ProcessorInputs,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_selection, correlator_name,
        current_word_prefix, duration_lit, flushed_explicit_processor_outputs,
        from_relay_clause_with_boundary, from_where_boundary_token, if_not_exists_clause,
        into_parse_error, kw, kw_phrase2, kw_phrase3, lex_input, materialized_state_dependencies,
        relay_ref, render_vm_program_tokens, suggestions_from_errors, tok,
        vm_program_error_message,
    },
};

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
-> impl Parser<'src, &'src [Token], nervix_models::Expression, extra::Err<ParseError<'src>>> + Clone
{
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
            crate::parse_expression(&source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
}

fn match_policy<'src>()
-> impl Parser<'src, &'src [Token], CorrelatorMatchPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Match).ignore_then(choice((
        kw(Identifier::Earliest).to(CorrelatorMatchPolicy::Earliest),
        kw(Identifier::Latest).to(CorrelatorMatchPolicy::Latest),
    )))
}

fn left_from_where_boundary_token(token: &Token) -> bool {
    from_where_boundary_token(token)
        || matches!(
            token,
            Token::Word(Word::KnownWord {
                iden: Identifier::Right | Identifier::Correlate,
                ..
            })
        )
}

fn right_from_where_boundary_token(token: &Token) -> bool {
    from_where_boundary_token(token)
        || matches!(
            token,
            Token::Word(Word::KnownWord {
                iden: Identifier::Correlate,
                ..
            })
        )
}

fn side_from_clauses<'src>(
    side: Identifier,
) -> impl Parser<'src, &'src [Token], ProcessorInputs, extra::Err<ParseError<'src>>> + Clone {
    let boundary = match side {
        Identifier::Left => left_from_where_boundary_token as fn(&Token) -> bool,
        Identifier::Right => right_from_where_boundary_token as fn(&Token) -> bool,
        _ => unreachable!("correlator inputs expose only left and right sides"),
    };
    kw(side)
        .ignore_then(kw(Identifier::From))
        .ignore_then(
            from_relay_clause_with_boundary(boundary)
                .separated_by(tok(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>(),
        )
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
        .then_ignore(kw(Identifier::Max))
        .then_ignore(kw(Identifier::Time))
        .then(duration_lit())
        .then(timeout_policy())
        .then(branch_selection())
        .then(materialized_state_dependencies())
        .then(flushed_explicit_processor_outputs())
        .then_ignore(tok(Token::Semicolon).or_not())
        .try_map(|value, _span| {
            let (
                (
                    (
                        ((((base, correlate_where), match_policy), max_time), timeout_policy),
                        branched_by,
                    ),
                    materialized_state,
                ),
                output_routes,
            ) = value;
            let ((((if_not_exists, mode), name), left), right) = base;
            Ok(CreateStatement::new(
                CreateCorrelator {
                    name,
                    left,
                    right,
                    output_routes,
                    branched_by,
                    correlate_where,
                    match_policy,
                    max_time,
                    timeout_policy,
                    mode: mode.unwrap_or(AckMode::Attached),
                    filter_where: None,
                    materialized_state,
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
            "CREATE CORRELATOR correlate LEFT FROM relay1 WHERE left.name != '', relay1_extra \
             RIGHT FROM relay2 WHERE right.first_name != '', relay2_extra CORRELATE WHERE \
             lower(left.name) = lower(right.first_name) MATCH EARLIEST MAX TIME 1s ON CORRELATION \
             TIMEOUT DROP, SEND TO relay4 BRANCHED BY tenant_branch TO relay3 SET name = \
             lower(left.name), surname = upper(right.surname) FLUSH EACH 100ms MAX BATCH SIZE \
             1MiB ON MESSAGE ERROR LOG TO relay5 SET display_name = right.first_name WHERE \
             output.display_name != '' FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
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
            crate::parse_expression("lower(left.name) = lower(right.first_name)")
                .expect("valid expression")
        );
        assert_eq!(parsed.match_policy, CorrelatorMatchPolicy::Earliest);
        assert!(
            !parsed.output_routes.routes[0]
                .construction
                .assignments
                .is_empty()
        );
        assert_eq!(parsed.output_routes.routes.len(), 2);
        assert!(
            !parsed.output_routes.routes[1]
                .construction
                .assignments
                .is_empty()
        );
        assert!(
            parsed.output_routes.routes[1]
                .construction
                .where_clause
                .is_some()
        );
        assert_eq!(parsed.max_time, "1s");
    }

    #[test]
    fn parses_compound_predicate_and_latest_match() {
        let tokens = to_tokens(
            "CREATE DETACHED CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE \
             WHERE lower(left.name) = lower(right.first_name) AND left.tenant = right.tenant \
             MATCH LATEST MAX TIME 1s ON CORRELATION TIMEOUT DROP, DROP UNBRANCHED TO relay3 SET \
             name = lower(left.name), surname = upper(right.surname) FLUSH IMMEDIATE ON MESSAGE \
             ERROR LOG;",
        );
        let parsed = parse_create_correlator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
        assert_eq!(
            parsed.correlate_where,
            crate::parse_expression(
                "lower(left.name) = lower(right.first_name) AND left.tenant = right.tenant",
            )
            .expect("valid expression")
        );
        assert_eq!(parsed.match_policy, CorrelatorMatchPolicy::Latest);
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "IMMEDIATE"
        );
    }

    #[test]
    fn rejects_legacy_comma_separated_inputs() {
        let tokens = to_tokens(
            "CREATE CORRELATOR correlate FROM relay1, relay2 CORRELATE WHERE relay1.name = \
             relay2.first_name MATCH LATEST MAX TIME 1s ON CORRELATION TIMEOUT DROP, DROP \
             UNBRANCHED TO relay3 SET name = left.name FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        assert!(parse_create_correlator_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_missing_right_inputs() {
        let tokens = to_tokens(
            "CREATE CORRELATOR correlate LEFT FROM relay1 CORRELATE WHERE left.name = \
             right.first_name MATCH LATEST MAX TIME 1s ON CORRELATION TIMEOUT DROP, DROP \
             UNBRANCHED TO relay3 SET name = left.name FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        assert!(parse_create_correlator_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_output_route_without_set_assignments() {
        let tokens = to_tokens(
            "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE WHERE \
             left.name = right.first_name MATCH LATEST MAX TIME 1s ON CORRELATION TIMEOUT DROP, \
             DROP UNBRANCHED TO relay3 FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        assert!(parse_create_correlator_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_legacy_output_block() {
        let tokens = to_tokens(
            "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE WHERE \
             left.name = right.first_name MATCH LATEST MAX TIME 1s ON CORRELATION TIMEOUT DROP, \
             DROP UNBRANCHED OUTPUT name = left.name TO relay3 SET name = left.name FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG;",
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
                     WHERE left.name = right.first_name MATCH ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"EARLIEST".to_string()));
        assert!(suggestions.contains(&"LATEST".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }

    #[test]
    fn suggests_set_at_the_start_of_each_output_route() {
        let input = "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE \
                     WHERE left.name = right.first_name MATCH LATEST MAX TIME 1s ON CORRELATION \
                     TIMEOUT DROP, DROP UNBRANCHED TO relay3 ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"SET".to_string()));
        assert!(!suggestions.contains(&"WHERE".to_string()));
        assert!(!suggestions.contains(&"UNSET".to_string()));
        assert!(!suggestions.contains(&"ON".to_string()));
    }

    #[test]
    fn suggests_correlation_timeout_phrase() {
        let input = "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE \
                     WHERE left.name = right.first_name MATCH LATEST MAX TIME 1s ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"ON CORRELATION TIMEOUT".to_string()));
        assert!(!suggestions.contains(&"ON MESSAGE ERROR".to_string()));
    }

    #[test]
    fn suggests_branch_after_timeout_without_output_leakage() {
        let input = "CREATE CORRELATOR correlate LEFT FROM relay1 RIGHT FROM relay2 CORRELATE \
                     WHERE left.name = right.first_name MATCH LATEST MAX TIME 1s ON CORRELATION \
                     TIMEOUT DROP, DROP ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
        assert!(!suggestions.contains(&"OUTPUT".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }
}
