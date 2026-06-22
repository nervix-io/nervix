use chumsky::prelude::*;
use nervix_models::{
    AckMode, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
    CreateCorrelator, CreateStatement,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, correlator_name,
        current_word_prefix, duration_lit, flush_each, if_not_exists_clause, into_parse_error, kw,
        kw_phrase2, kw_phrase3, lex_input, message_error_policy, relay_ref,
        render_vm_program_tokens, suggestions_from_errors, tok, vm_program_error_message,
    },
};

fn split_key_group_tokens(tokens: &[Token]) -> Result<Vec<String>, String> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0usize;
    for token in tokens {
        match token {
            Token::LParen | Token::LBracket | Token::LBrace => {
                depth = depth.saturating_add(1);
                current.push(token.clone());
            }
            Token::RParen | Token::RBracket | Token::RBrace => {
                depth = depth.saturating_sub(1);
                current.push(token.clone());
            }
            Token::Comma if depth == 0 => {
                if current.is_empty() {
                    return Err("correlator key group contains an empty expression".to_string());
                }
                parts.push(render_vm_program_tokens(&current));
                current.clear();
            }
            _ => current.push(token.clone()),
        }
    }

    if current.is_empty() {
        return Err("correlator key group contains an empty expression".to_string());
    }
    parts.push(render_vm_program_tokens(&current));
    Ok(parts)
}

fn balanced_group_body<'src>()
-> impl Parser<'src, &'src [Token], Vec<Token>, extra::Err<ParseError<'src>>> + Clone {
    recursive(|body| {
        let atom = any()
            .filter(|token| !matches!(token, Token::LParen | Token::RParen))
            .map(|token| vec![token]);
        let nested = body
            .delimited_by(tok(Token::LParen), tok(Token::RParen))
            .map(|inner: Vec<Token>| {
                let mut out = Vec::with_capacity(inner.len() + 2);
                out.push(Token::LParen);
                out.extend(inner);
                out.push(Token::RParen);
                out
            });

        choice((nested, atom))
            .repeated()
            .collect::<Vec<_>>()
            .map(|chunks| chunks.into_iter().flatten().collect())
    })
}

fn key_group<'src>()
-> impl Parser<'src, &'src [Token], Vec<String>, extra::Err<ParseError<'src>>> + Clone {
    balanced_group_body()
        .delimited_by(tok(Token::LParen), tok(Token::RParen))
        .try_map(|tokens, span| {
            split_key_group_tokens(&tokens).map_err(|error| Rich::custom(span, error))
        })
        .labelled("correlator_key_group")
}

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

fn match_policy<'src>()
-> impl Parser<'src, &'src [Token], CorrelatorMatchPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Match).ignore_then(choice((
        kw(Identifier::Earliest).to(CorrelatorMatchPolicy::Earliest),
        kw(Identifier::Latest).to(CorrelatorMatchPolicy::Latest),
    )))
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
        .then_ignore(kw(Identifier::From))
        .then(relay_ref())
        .then_ignore(tok(Token::Comma))
        .then(relay_ref())
        .then_ignore(kw(Identifier::On))
        .then(key_group())
        .then_ignore(tok(Token::Comma))
        .then(key_group())
        .then(match_policy())
        .then_ignore(kw(Identifier::To))
        .then(relay_ref())
        .then(branch_parameterization())
        .then(flush_each())
        .then(output_assignments())
        .then_ignore(kw(Identifier::Max))
        .then_ignore(kw(Identifier::Time))
        .then(duration_lit())
        .then(timeout_policy())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .try_map(|value, span| {
            let (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (((if_not_exists, mode), name), left_relay),
                                                        right_relay,
                                                    ),
                                                    left_on,
                                                ),
                                                right_on,
                                            ),
                                            match_policy,
                                        ),
                                        into_relay,
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
            if left_on.len() != right_on.len() {
                return Err(Rich::custom(
                    span,
                    format!(
                        "correlator ON groups must have the same expression count, found {} and {}",
                        left_on.len(),
                        right_on.len()
                    ),
                ));
            }
            let (flush_each, max_batch_size) = flush_each;
            Ok(CreateStatement::new(
                CreateCorrelator {
                    name,
                    left_relay,
                    right_relay,
                    into_relay,
                    parameterized_by,
                    left_on,
                    right_on,
                    match_policy,
                    output,
                    max_time,
                    flush_each,
                    max_batch_size,
                    timeout_policy,
                    message_error_policy,
                    mode: mode.unwrap_or(AckMode::Attached),
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
            "CREATE CORRELATOR correlate FROM relay1, relay2 ON (lower(relay1.name)), \
             (lower(relay2.first_name)) MATCH EARLIEST TO relay3 PARAMETERIZED BY tenant_branch \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB OUTPUT relay3.name = lower(relay1.name), \
             relay3.surname = upper(relay2.surname) MAX TIME 1s ON CORRELATION TIMEOUT DROP, SEND \
             TO relay4 ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_correlator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "correlate");
        assert_eq!(parsed.left_relay.as_str(), "relay1");
        assert_eq!(parsed.right_relay.as_str(), "relay2");
        assert_eq!(parsed.into_relay.as_str(), "relay3");
        assert_eq!(parsed.left_on, vec!["lower ( relay1.name )"]);
        assert_eq!(parsed.right_on, vec!["lower ( relay2.first_name )"]);
        assert_eq!(parsed.match_policy, CorrelatorMatchPolicy::Earliest);
        assert!(
            parsed
                .output
                .contains("relay3.name = lower ( relay1.name )")
        );
        assert_eq!(parsed.max_time, "1s");
    }

    #[test]
    fn parses_compound_keys_and_latest_match() {
        let tokens = to_tokens(
            "CREATE DETACHED CORRELATOR correlate FROM relay1, relay2 ON (lower(relay1.name), \
             relay1.tenant), (lower(relay2.first_name), relay2.tenant) MATCH LATEST TO relay3 \
             UNPARAMETERIZED FLUSH IMMEDIATE OUTPUT relay3.name = lower(relay1.name), \
             relay3.surname = upper(relay2.surname) MAX TIME 1s ON CORRELATION TIMEOUT DROP, DROP \
             ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_correlator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
        assert_eq!(
            parsed.left_on,
            vec!["lower ( relay1.name )", "relay1.tenant"]
        );
        assert_eq!(
            parsed.right_on,
            vec!["lower ( relay2.first_name )", "relay2.tenant"]
        );
        assert_eq!(parsed.match_policy, CorrelatorMatchPolicy::Latest);
        assert_eq!(parsed.flush_each, "IMMEDIATE");
    }

    #[test]
    fn rejects_mismatched_key_group_lengths() {
        let tokens = to_tokens(
            "CREATE CORRELATOR correlate FROM relay1, relay2 ON (relay1.name, relay1.tenant), \
             (relay2.first_name) MATCH LATEST TO relay3 UNPARAMETERIZED FLUSH IMMEDIATE OUTPUT \
             relay3.name = relay1.name MAX TIME 1s ON CORRELATION TIMEOUT DROP, DROP ON MESSAGE \
             ERROR LOG;",
        );
        assert!(parse_create_correlator_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_match_policy_without_schema_keyword_leakage() {
        let input = "CREATE CORRELATOR correlate FROM relay1, relay2 ON (relay1.name), \
                     (relay2.first_name) MATCH ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"EARLIEST".to_string()));
        assert!(suggestions.contains(&"LATEST".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }

    #[test]
    fn suggests_correlation_timeout_phrase() {
        let input = "CREATE CORRELATOR correlate FROM relay1, relay2 ON (relay1.name), \
                     (relay2.first_name) MATCH LATEST TO relay3 UNPARAMETERIZED FLUSH IMMEDIATE \
                     OUTPUT relay3.name = relay1.name MAX TIME 1s ";
        let suggestions = suggest_create_correlator(input, input.len());
        assert!(suggestions.contains(&"ON CORRELATION TIMEOUT".to_string()));
        assert!(!suggestions.contains(&"ON MESSAGE ERROR".to_string()));
    }
}
