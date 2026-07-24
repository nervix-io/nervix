pub mod aggregate;

use chumsky::prelude::*;
use nervix_models::{AckMode, CreateStatement, CreateWindowProcessor, WindowBound};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_selection, current_word_prefix,
        duration_lit, explicit_processor_outputs, filter_where_clause, from_relay_clauses,
        if_not_exists_clause, into_parse_error, kw, lex_input, materialized_state_dependencies,
        suggestions_from_errors, tok, window_processor_name,
    },
};

fn u64_value<'src>() -> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone
{
    select! { Token::NumberLiteral(value) => value }
        .try_map(|raw, span| {
            raw.parse::<u64>()
                .map_err(|_| Rich::custom(span, format!("invalid integer '{raw}'")))
        })
        .labelled("message_count")
}

fn bound_part<'src>()
-> impl Parser<'src, &'src [Token], WindowBound, extra::Err<ParseError<'src>>> + Clone {
    let messages = u64_value()
        .then_ignore(kw(Identifier::Messages))
        .map(|messages| WindowBound {
            messages: Some(messages),
            duration: None,
        });
    let duration = duration_lit()
        .then_ignore(kw(Identifier::Duration))
        .map(|duration| WindowBound {
            messages: None,
            duration: Some(duration),
        });
    choice((messages, duration))
}

fn merge_bound_parts<'src>(
    parts: Vec<WindowBound>,
    span: chumsky::span::SimpleSpan,
) -> Result<WindowBound, Rich<'src, Token>> {
    let mut bound = WindowBound {
        messages: None,
        duration: None,
    };
    for part in parts {
        if let Some(messages) = part.messages
            && bound.messages.replace(messages).is_some()
        {
            return Err(Rich::custom(span, "message bound may appear at most once"));
        }
        if let Some(duration) = part.duration
            && bound.duration.replace(duration).is_some()
        {
            return Err(Rich::custom(span, "duration bound may appear at most once"));
        }
    }
    if bound.is_empty() {
        return Err(Rich::custom(span, "window bound must not be empty"));
    }
    Ok(bound)
}

fn window_bound<'src>()
-> impl Parser<'src, &'src [Token], WindowBound, extra::Err<ParseError<'src>>> + Clone {
    bound_part()
        .repeated()
        .at_least(1)
        .at_most(2)
        .collect::<Vec<_>>()
        .try_map(merge_bound_parts)
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
        .then_ignore(kw(Identifier::Width))
        .then(window_bound())
        .then_ignore(kw(Identifier::Step))
        .then(window_bound())
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
             span| {
                validate_step(&width, &step, span)?;
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
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_window_processor_parser()
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
