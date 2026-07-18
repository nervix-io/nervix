pub mod aggregate;

use chumsky::prelude::*;
use nervix_models::{AckMode, CreateStatement, CreateWindowProcessor, WindowBound};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_selection, current_word_prefix,
        duration_lit, filter_where_clause, from_relay_clauses, if_not_exists_clause,
        into_parse_error, kw, lex_input, processor_outputs, suggestions_from_errors, tok,
        window_processor_name,
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

fn aggregate_boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::On,
                ..
            })
    )
}

fn token_to_source(token: &Token) -> String {
    match token {
        Token::Word(Word::KnownWord { raw, .. }) => raw.clone(),
        Token::Word(Word::UnknownWord(raw)) => raw.clone(),
        Token::StringLiteral(value) => {
            format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
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

fn render_aggregate_tokens(tokens: &[Token]) -> String {
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

fn aggregate_block<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Aggregate)
        .ignore_then(
            any()
                .filter(|token: &Token| !aggregate_boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .map(|tokens| render_aggregate_tokens(&tokens))
        .labelled("aggregate_block")
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
        .then(processor_outputs())
        .then(branch_selection())
        .then_ignore(kw(Identifier::Width))
        .then(window_bound())
        .then_ignore(kw(Identifier::Step))
        .then(window_bound())
        .then(aggregate_block())
        .then_ignore(tok(Token::Semicolon).or_not())
        .try_map(
            |(
                (
                    (
                        (
                            (((((if_not_exists, mode), name), from_input), filter_where), outputs),
                            branched_by,
                        ),
                        width,
                    ),
                    step,
                ),
                aggregate,
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
                        aggregate,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                    },
                    if_not_exists,
                ))
            },
        )
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
                TO s2 ON MESSAGE ERROR LOG
                BRANCHED BY tenant
                WIDTH 100 MESSAGES 10s DURATION
                STEP 10 MESSAGES 1s DURATION
                AGGREGATE
                    s2.latency_p99 = PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 99, 2048, 0, 10000, '2s'),
                    s2.time = MAX(s1.timestamp),
                    s2.started_at = FIRST(s1.timestamp),
                    s2.latencies = [PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 90, 2048, 0, 10000, '2s'), PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 95, 2048, 0, 10000, '2s')];
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
        assert!(parsed.aggregate.contains("PERCENTILE_LINEAR_HISTOGRAM"));
        assert!(parsed.aggregate.contains("[PERCENTILE"));
    }

    #[test]
    fn parses_tumbling_message_window() {
        let input = r#"
            CREATE DETACHED WINDOW PROCESSOR counts
                FROM s1 TO s2 ON MESSAGE ERROR LOG
                BRANCHED BY tenant
                WIDTH 100 MESSAGES
                STEP 100 MESSAGES
                AGGREGATE s2.count = COUNT(s1.value);
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
                FROM s1 TO s2 ON MESSAGE ERROR LOG
                BRANCHED BY tenant
                WIDTH 100 MESSAGES
                STEP 101 MESSAGES
                AGGREGATE s2.count = COUNT(s1.value);
        "#;
        assert!(parse_create_window_processor_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn rejects_step_dimension_missing_from_width() {
        let input = r#"
            CREATE WINDOW PROCESSOR bad
                FROM s1 TO s2 ON MESSAGE ERROR LOG
                BRANCHED BY tenant
                WIDTH 100 MESSAGES
                STEP 1s DURATION
                AGGREGATE s2.count = COUNT(s1.value);
        "#;
        assert!(parse_create_window_processor_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn suggests_window_processor_keywords() {
        let input =
            "CREATE WINDOW PROCESSOR p FROM s1 TO s2 ON MESSAGE ERROR LOG BRANCHED BY tenant ";
        let suggestions = suggest_create_window_processor(input, input.len());
        assert!(suggestions.contains(&"WIDTH".to_string()));
    }
}
