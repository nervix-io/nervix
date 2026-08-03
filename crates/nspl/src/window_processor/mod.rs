pub mod aggregate;

use chumsky::prelude::*;
use nervix_models::{AckMode, CreateStatement, CreateWindowProcessor, WindowBound};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, boxed_choice, branch_selection, duration_lit,
        explicit_processor_outputs, filter_where_clause, from_relay_clauses, if_not_exists_clause,
        into_parse_error, kw, lex_input, materialized_state_dependencies, suggest_from, tok,
        window_processor_name,
    },
};

/// The count in a `<n> MESSAGES` window bound. Narrower than the shared integer parser: a window
/// bound is always a numeric literal, never a bare word.
fn message_count<'src>()
-> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone {
    select! { Token::NumberLiteral(value) => value }
        .try_map(|raw, span| {
            raw.parse::<u64>()
                .map_err(|_| Rich::custom(span, format!("invalid integer '{raw}'")))
        })
        .labelled("message_count")
}

fn message_bound<'src>()
-> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone {
    message_count()
        .then_ignore(kw(Identifier::Messages))
        .boxed()
}

fn duration_bound<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    duration_lit().then_ignore(kw(Identifier::Duration)).boxed()
}

/// `WIDTH <bound> STEP <bound>`, checked as one unit.
///
/// A window bound is a message count, a duration, or one of each — never two of the same kind.
/// Spelling that out in the grammar rather than checking it after a `repeated()` is what stops
/// completion offering `DURATION` again once a duration bound is already present, and then
/// rejecting the statement the user just built from its own suggestion.
///
/// The step is constrained by the width that precedes it, so the two are validated where both are
/// known. Checking against the finished statement instead reports the failure at the statement's
/// first token, where every other `CREATE` alternative outranks it and the user is told
/// `expected ... found WINDOW`.
fn width_and_step<'src>()
-> impl Parser<'src, &'src [Token], (WindowBound, WindowBound), extra::Err<ParseError<'src>>> + Clone
{
    // The step is enumerated per width shape rather than parsed freely and checked afterwards. A
    // step may only use the kinds the width declared, and the width is already known by the time
    // the step is read, so the grammar can say so: after `WIDTH 10s DURATION STEP` the only thing
    // offered is a duration.
    let messages_only = message_bound()
        .then_ignore(kw(Identifier::Step))
        .then(message_bound())
        .map(|(width, step)| {
            (
                WindowBound::of_messages(width),
                WindowBound::of_messages(step),
            )
        });
    let duration_only = duration_bound()
        .then_ignore(kw(Identifier::Step))
        .then(duration_bound())
        .map(|(width, step)| {
            (
                WindowBound::of_duration(width),
                WindowBound::of_duration(step),
            )
        });
    // Either order, as before: `10 MESSAGES 1s DURATION` and `1s DURATION 10 MESSAGES` are the
    // same bound.
    let both = boxed_choice!(
        message_bound()
            .then(duration_bound())
            .map(|(messages, duration)| (messages, duration)),
        duration_bound()
            .then(message_bound())
            .map(|(duration, messages)| (messages, duration)),
    )
    .then_ignore(kw(Identifier::Step))
    .then(step_within_both())
    .map(|((messages, duration), step)| {
        (
            WindowBound {
                messages: Some(messages),
                duration: Some(duration),
            },
            step,
        )
    });

    kw(Identifier::Width)
        .ignore_then(boxed_choice!(both, messages_only, duration_only))
        .try_map(|(width, step), span| {
            validate_step(&width, &step, span)?;
            Ok((width, step))
        })
        .boxed()
}

/// A step for a width that declared both kinds: either kind, or both.
fn step_within_both<'src>()
-> impl Parser<'src, &'src [Token], WindowBound, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        message_bound()
            .then(duration_bound())
            .map(|(messages, duration)| WindowBound {
                messages: Some(messages),
                duration: Some(duration),
            }),
        duration_bound()
            .then(message_bound())
            .map(|(duration, messages)| WindowBound {
                messages: Some(messages),
                duration: Some(duration),
            }),
        message_bound().map(WindowBound::of_messages),
        duration_bound().map(WindowBound::of_duration),
    )
}

fn validate_step<'src>(
    width: &WindowBound,
    step: &WindowBound,
    span: chumsky::span::SimpleSpan,
) -> Result<(), Rich<'src, Token>> {
    if step.messages.is_some() && width.messages.is_none() {
        return Err(Rich::custom(
            span,
            "STEP MESSAGES requires WIDTH MESSAGES".to_string(),
        ));
    }
    if step.duration.is_some() && width.duration.is_none() {
        return Err(Rich::custom(
            span,
            "STEP DURATION requires WIDTH DURATION".to_string(),
        ));
    }
    if let (Some(step), Some(width)) = (step.messages, width.messages)
        && step > width
    {
        return Err(Rich::custom(
            span,
            "STEP MESSAGES must be less than or equal to WIDTH MESSAGES".to_string(),
        ));
    }
    if let (Some(step), Some(width)) = (&step.duration, &width.duration) {
        let step = humantime::parse_duration(step)
            .map_err(|err| Rich::custom(span, format!("invalid STEP duration: {err}")))?;
        let width = humantime::parse_duration(width)
            .map_err(|err| Rich::custom(span, format!("invalid WIDTH duration: {err}")))?;
        if step > width {
            return Err(Rich::custom(
                span,
                "STEP DURATION must be less than or equal to WIDTH DURATION".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn create_window_processor_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateWindowProcessor>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Window))
        .then_ignore(kw(Identifier::Processor))
        .then(window_processor_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .boxed()
        .then(width_and_step())
        .map(|(head, (width, step))| ((head, width), step))
        .boxed()
        .then(branch_selection())
        .then(materialized_state_dependencies())
        .then(explicit_processor_outputs())
        .then_ignore(tok(Token::Semicolon).or_not())
        .try_map(
            |(
                (
                    (
                        (
                            (((((if_not_exists, mode), name), from_input), filter_where), width),
                            step,
                        ),
                        branched_by,
                    ),
                    materialized_state,
                ),
                outputs,
            ),
             _span| {
                Ok(CreateStatement::new(
                    CreateWindowProcessor {
                        name,
                        from: from_input,
                        output_routes: outputs,
                        branched_by,
                        width,
                        step,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                        materialized_state,
                    },
                    if_not_exists,
                ))
            },
        )
        .boxed()
}

pub fn parse_create_window_processor_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateWindowProcessor>, Vec<ParseError<'_>>> {
    let out = create_window_processor_parser()
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

pub fn parse_create_window_processor(
    input: &str,
) -> Result<CreateStatement<CreateWindowProcessor>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_window_processor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_window_processor(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_window_processor_parser())
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
    fn parses_create_window_processor() {
        let input = r#"
            CREATE WINDOW PROCESSOR latency_window
                FROM s1
                WIDTH 100 MESSAGES 10s DURATION
                STEP 10 MESSAGES 1s DURATION
                BRANCHED BY tenant
                TO s2
                SET latency_p99 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 99, 2048, 0, 10000, '2s'),
                    time = MAX(input.timestamp),
                    started_at = FIRST(input.timestamp)
                ON MESSAGE ERROR LOG;
        "#;

        let parsed =
            parse_create_window_processor_tokens(&to_tokens(input)).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "latency_window");
        assert_eq!(parsed.from.from[0].as_str(), "s1");
        assert_eq!(
            parsed
                .output_routes
                .routes
                .first()
                .expect("output route should parse")
                .relay
                .as_str(),
            "s2"
        );
        assert_eq!(parsed.width.messages, Some(100));
        assert_eq!(parsed.width.duration.as_deref(), Some("10s"));
        assert_eq!(parsed.step.messages, Some(10));
        assert_eq!(parsed.step.duration.as_deref(), Some("1s"));
        assert!(
            !parsed.output_routes.routes[0]
                .construction
                .assignments
                .is_empty()
        );
    }

    #[test]
    fn parses_tumbling_message_window() {
        let input = r#"
            CREATE DETACHED WINDOW PROCESSOR counts
                FROM s1
                WIDTH 100 MESSAGES
                STEP 100 MESSAGES
                BRANCHED BY tenant
                TO s2 SET count = COUNT(input.value) ON MESSAGE ERROR LOG;
        "#;

        let parsed =
            parse_create_window_processor_tokens(&to_tokens(input)).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
        assert_eq!(parsed.width.messages, Some(100));
        assert_eq!(parsed.width.duration, None);
        assert_eq!(parsed.step.messages, Some(100));
        assert_eq!(parsed.step.duration, None);
    }

    #[test]
    fn rejects_step_larger_than_width() {
        let input = r#"
            CREATE WINDOW PROCESSOR bad
                FROM s1
                WIDTH 100 MESSAGES
                STEP 101 MESSAGES
                BRANCHED BY tenant
                TO s2 SET count = COUNT(input.value) ON MESSAGE ERROR LOG;
        "#;
        assert!(parse_create_window_processor_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn rejects_step_dimension_missing_from_width() {
        let input = r#"
            CREATE WINDOW PROCESSOR bad
                FROM s1
                WIDTH 100 MESSAGES
                STEP 1s DURATION
                BRANCHED BY tenant
                TO s2 SET count = COUNT(input.value) ON MESSAGE ERROR LOG;
        "#;
        assert!(parse_create_window_processor_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn suggests_window_processor_keywords() {
        let input = "CREATE WINDOW PROCESSOR p FROM s1 ";
        let suggestions = suggest_create_window_processor(input, input.len());
        assert!(suggestions.contains(&"WIDTH".to_string()));
    }
}
