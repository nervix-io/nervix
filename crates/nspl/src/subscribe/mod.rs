use chumsky::prelude::*;
use nervix_models::{
    SubscribeSession, SubscriptionBinding, SubscriptionDeliveryBehavior, SubscriptionLiteral,
    UnsubscribeSession,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, field_ref, filter_map_program,
        into_parse_error, kw, kw_phrase2, lex_input, relay_ref, string_lit,
        suggestions_from_errors, tok, word_raw,
    },
};

pub(crate) fn subscription_literal_parser<'src>()
-> impl Parser<'src, &'src [Token], SubscriptionLiteral, extra::Err<ParseError<'src>>> + Clone {
    choice((
        string_lit().map(SubscriptionLiteral::String),
        select! { Token::NumberLiteral(value) => SubscriptionLiteral::Number(value) }
            .labelled("number_literal"),
        word_raw().try_map(|raw, span| match raw.to_ascii_lowercase().as_str() {
            "true" => Ok(SubscriptionLiteral::Bool(true)),
            "false" => Ok(SubscriptionLiteral::Bool(false)),
            _ => Err(Rich::custom(
                span,
                format!("unsupported subscription literal '{raw}'"),
            )),
        }),
    ))
}

fn subscription_binding<'src>()
-> impl Parser<'src, &'src [Token], SubscriptionBinding, extra::Err<ParseError<'src>>> + Clone {
    field_ref()
        .then_ignore(tok(Token::Eq))
        .then(subscription_literal_parser())
        .map(|(field, value)| SubscriptionBinding { field, value })
}

pub(crate) fn subscription_bindings_parser<'src>()
-> impl Parser<'src, &'src [Token], Vec<SubscriptionBinding>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Where)
        .ignore_then(tok(Token::LParen))
        .ignore_then(
            subscription_binding()
                .separated_by(tok(Token::Comma))
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::RParen))
}

fn subscription_delivery_behavior<'src>()
-> impl Parser<'src, &'src [Token], SubscriptionDeliveryBehavior, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        kw(Identifier::Blocking).to(SubscriptionDeliveryBehavior::Blocking),
        kw(Identifier::Dropping).to(SubscriptionDeliveryBehavior::Dropping),
    ))
}

fn batch_sample_rate<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Batch)
        .ignore_then(kw_phrase2(Identifier::Sample, Identifier::Rate))
        .ignore_then(
            select! { Token::NumberLiteral(value) => value }
                .labelled("number_literal")
                .try_map(|value, span| match value.parse::<f64>() {
                    Ok(rate) if (0.0..=1.0).contains(&rate) => Ok(value),
                    Ok(_) => Err(Rich::custom(
                        span,
                        "batch sample rate must be between 0.0 and 1.0",
                    )),
                    Err(error) => Err(Rich::custom(
                        span,
                        format!("invalid batch sample rate: {error}"),
                    )),
                }),
        )
}

pub fn subscribe_session_parser<'src>()
-> impl Parser<'src, &'src [Token], SubscribeSession, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Subscribe, Identifier::Session)
        .ignore_then(kw(Identifier::To))
        .ignore_then(relay_ref())
        .then(subscription_delivery_behavior().or_not())
        .then(batch_sample_rate().or_not())
        .then(filter_map_program().or_not())
        .map(
            |(((relay, delivery_behavior), batch_sample_rate), filter_map)| SubscribeSession {
                relay,
                delivery_behavior: delivery_behavior
                    .unwrap_or(SubscriptionDeliveryBehavior::Blocking),
                batch_sample_rate,
                filter_map,
            },
        )
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn subscribe_session_query(
    relay: &str,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<&str>,
    filter_map: Option<&str>,
) -> String {
    let mut query = format!("SUBSCRIBE SESSION TO {relay}");
    if delivery_behavior != SubscriptionDeliveryBehavior::Blocking {
        query.push(' ');
        query.push_str(delivery_behavior.as_ref());
    }
    if let Some(batch_sample_rate) = batch_sample_rate {
        query.push_str(" BATCH SAMPLE RATE ");
        query.push_str(batch_sample_rate);
    }
    if let Some(filter_map) = filter_map {
        query.push(' ');
        query.push_str(filter_map);
    }
    query.push(';');
    query
}

pub fn unsubscribe_session_parser<'src>()
-> impl Parser<'src, &'src [Token], UnsubscribeSession, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Unsubscribe, Identifier::Session)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .then(subscription_delivery_behavior().or_not())
        .then(batch_sample_rate().or_not())
        .then(filter_map_program().or_not())
        .map(
            |(((relay, delivery_behavior), batch_sample_rate), filter_map)| UnsubscribeSession {
                relay,
                delivery_behavior: delivery_behavior
                    .unwrap_or(SubscriptionDeliveryBehavior::Blocking),
                batch_sample_rate,
                filter_map,
            },
        )
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_subscribe_session_tokens(
    tokens: &[Token],
) -> Result<SubscribeSession, Vec<ParseError<'_>>> {
    let out = subscribe_session_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_subscribe_session(input: &str) -> Result<SubscribeSession, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_subscribe_session_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_subscribe_session(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = subscribe_session_parser()
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
    fn parses_subscribe_session() {
        let tokens = to_tokens("SUBSCRIBE SESSION TO notifications;");
        let parsed = parse_subscribe_session_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "notifications");
        assert_eq!(parsed.filter_map, None);
    }

    #[test]
    fn parses_subscribe_session_with_filter_map_program() {
        let tokens = to_tokens(
            "SUBSCRIBE SESSION TO notifications SET notifications.normalized = \
             lower(notifications.name) UNSET notifications.raw WHERE notifications.active;",
        );
        let parsed = parse_subscribe_session_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "notifications");
        assert_eq!(
            parsed.filter_map.as_deref(),
            Some(
                "SET notifications.normalized = lower ( notifications.name ) UNSET \
                 notifications.raw WHERE notifications.active"
            )
        );
    }

    #[test]
    fn parses_subscribe_session_with_delivery_options() {
        let tokens = to_tokens(
            "SUBSCRIBE SESSION TO telemetry DROPPING BATCH SAMPLE RATE 0.1 WHERE telemetry.active;",
        );
        let parsed = parse_subscribe_session_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "telemetry");
        assert_eq!(parsed.delivery_behavior.as_ref(), "DROPPING");
        assert_eq!(parsed.batch_sample_rate.as_deref(), Some("0.1"));
        assert_eq!(parsed.filter_map.as_deref(), Some("WHERE telemetry.active"));
    }

    #[test]
    fn rejects_subscribe_session_sample_rate_outside_probability_range() {
        let tokens = to_tokens("SUBSCRIBE SESSION TO telemetry BATCH SAMPLE RATE 1.1;");
        let result = subscribe_session_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result();
        assert!(result.is_err(), "sample rate must be between 0.0 and 1.0");
    }

    #[test]
    fn renders_subscribe_session_query_with_delivery_options() {
        assert_eq!(
            subscribe_session_query(
                "telemetry",
                SubscriptionDeliveryBehavior::Dropping,
                Some("0.1"),
                Some("WHERE telemetry.active"),
            ),
            "SUBSCRIBE SESSION TO telemetry DROPPING BATCH SAMPLE RATE 0.1 WHERE telemetry.active;"
        );
    }

    #[test]
    fn suggests_delivery_options_after_subscribe_session_relay() {
        let input = "SUBSCRIBE SESSION TO telemetry D";
        let suggestions = suggest_subscribe_session(input, input.len());
        assert!(suggestions.contains(&"DROPPING".to_string()));

        let input = "SUBSCRIBE SESSION TO telemetry B";
        let suggestions = suggest_subscribe_session(input, input.len());
        assert!(suggestions.contains(&"BLOCKING".to_string()));
        assert!(suggestions.contains(&"BATCH".to_string()));
    }

    #[test]
    fn suggests_relay_reference_after_subscribe_session_source() {
        let input = "SUBSCRIBE SESSION TO ";
        let suggestions = suggest_subscribe_session(input, input.len());
        assert!(suggestions.contains(&"ref:relay".to_string()));
    }

    #[test]
    fn subscribe_session_after_stream_has_no_cross_branch_suggestions() {
        let input = "SUBSCRIBE SESSION TO notifications ";
        let suggestions = suggest_subscribe_session(input, input.len());
        assert!(!suggestions.contains(&"field_name".to_string()));
        assert!(!suggestions.contains(&"ref:relay".to_string()));
    }

    #[test]
    fn suggests_subscribe_session_keyword_phrase() {
        let input = "SUB";
        let suggestions = suggest_subscribe_session(input, input.len());
        assert!(suggestions.contains(&"SUBSCRIBE SESSION".to_string()));
    }

    #[test]
    fn parses_unsubscribe_session() {
        let tokens = to_tokens("UNSUBSCRIBE SESSION FROM notifications;");
        let parsed = unsubscribe_session_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "notifications");
        assert_eq!(parsed.filter_map, None);
    }

    #[test]
    fn parses_unsubscribe_session_with_filter_map_program() {
        let tokens = to_tokens(
            "UNSUBSCRIBE SESSION FROM notifications SET notifications.normalized = \
             lower(notifications.name) WHERE notifications.active;",
        );
        let parsed = unsubscribe_session_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "notifications");
        assert_eq!(
            parsed.filter_map.as_deref(),
            Some(
                "SET notifications.normalized = lower ( notifications.name ) WHERE \
                 notifications.active"
            )
        );
    }

    #[test]
    fn rejects_invalid_subscribe_session_filter_map_program() {
        let tokens =
            to_tokens("SUBSCRIBE SESSION TO notifications SET notifications.normalized =;");
        let result = subscribe_session_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result();
        assert!(
            result.is_err(),
            "invalid FILTER-MAP program must fail to parse"
        );
    }
}
