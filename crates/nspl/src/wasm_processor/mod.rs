use chumsky::prelude::*;
use nervix_models::{
    AckMode, CreateStatement, CreateWasmProcessor, GeneralErrorPolicy, ProcessorOutput,
    ProcessorOutputs,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_selection, current_word_prefix,
        filter_where_clause, from_relay_clauses, if_not_exists_clause, into_parse_error, kw,
        lex_input, message_error_policy, output_filter_map_program, relay_ref, resource_ref,
        string_lit, suggestions_from_errors, tok, wasm_processor_name,
    },
};

fn u64_value<'src>() -> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        select! { Token::NumberLiteral(v) => v },
        crate::parser_support::word_raw(),
    ))
    .try_map(|raw, span| {
        raw.parse::<u64>()
            .map_err(|_| Rich::custom(span, format!("invalid integer '{raw}'")))
    })
}

fn global_error_policy<'src>()
-> impl Parser<'src, &'src [Token], GeneralErrorPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::On)
        .ignore_then(kw(Identifier::Global))
        .then_ignore(kw(Identifier::Error))
        .ignore_then(choice((
            kw(Identifier::Ignore).to(GeneralErrorPolicy::Ignore),
            kw(Identifier::Log).to(GeneralErrorPolicy::Log),
        )))
}

fn wasm_output_filter_map_program<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    output_filter_map_program().try_map(|source, span| {
        let parsed = crate::vm_program::parse_program(&source).map_err(|error| {
            Rich::custom(span, crate::parser_support::vm_program_error_message(error))
        })?;
        if !parsed.inner.unset.is_empty() {
            return Err(Rich::custom(
                span,
                "WASM processor TO clauses may use SET and WHERE, but not UNSET",
            ));
        }
        Ok(source)
    })
}

fn wasm_processor_output_route<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(wasm_output_filter_map_program().or_not())
        .map(|(relay, filter_map)| ProcessorOutput { relay, filter_map })
}

fn wasm_processor_outputs<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutputs, extra::Err<ParseError<'src>>> + Clone {
    wasm_processor_output_route()
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(ProcessorOutputs::new)
}

pub fn create_wasm_processor_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateWasmProcessor>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Wasm))
        .then_ignore(kw(Identifier::Processor))
        .then(wasm_processor_name())
        .then_ignore(kw(Identifier::Using))
        .then_ignore(kw(Identifier::Resource))
        .then(resource_ref())
        .then(kw(Identifier::Version).ignore_then(u64_value()).or_not())
        .then_ignore(kw(Identifier::File))
        .then(string_lit())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .then(wasm_processor_outputs())
        .then(branch_selection())
        .then(message_error_policy())
        .then(global_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (((if_not_exists, mode), name), resource),
                                            resource_version,
                                        ),
                                        file,
                                    ),
                                    from_input,
                                ),
                                filter_where,
                            ),
                            outputs,
                        ),
                        branched_by,
                    ),
                    message_error_policy,
                ),
                global_error_policy,
            )| {
                CreateStatement::new(
                    CreateWasmProcessor {
                        name,
                        from: from_input,
                        output_routes: outputs,
                        branched_by,
                        resource,
                        resource_version,
                        file,
                        message_error_policy,
                        global_error_policy,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_wasm_processor_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateWasmProcessor>, Vec<ParseError<'_>>> {
    let out = create_wasm_processor_parser()
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

pub fn parse_create_wasm_processor(
    input: &str,
) -> Result<CreateStatement<CreateWasmProcessor>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_wasm_processor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_wasm_processor(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_wasm_processor_parser()
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
    fn parses_create_wasm_processor() {
        let input = r#"
            CREATE DETACHED WASM PROCESSOR filter_even
            USING RESOURCE wasm_filters VERSION 2 FILE 'processors/filter_even.wasm'
            FROM raw_orders TO filtered_orders
            BRANCHED BY tenant_branch
            ON MESSAGE ERROR LOG ON GLOBAL ERROR IGNORE;
        "#;

        let parsed = parse_create_wasm_processor_tokens(&to_tokens(input)).expect("parse works");
        assert_eq!(parsed.name.as_str(), "filter_even");
        assert_eq!(parsed.resource.as_str(), "wasm_filters");
        assert_eq!(parsed.resource_version, Some(2));
        assert_eq!(parsed.file, "processors/filter_even.wasm");
        assert_eq!(parsed.from.from[0].as_str(), "raw_orders");
        assert_eq!(
            parsed
                .output_routes
                .routes
                .first()
                .expect("output route should parse")
                .relay
                .as_str(),
            "filtered_orders"
        );
        assert_eq!(parsed.mode, AckMode::Detached);
        assert_eq!(
            parsed.message_error_policy,
            nervix_models::MessageErrorPolicy::Log
        );
        assert_eq!(parsed.global_error_policy, GeneralErrorPolicy::Ignore);
        assert_eq!(
            parsed.branched_by.branch().map(|branch| branch.as_str()),
            Some("tenant_branch")
        );
    }

    #[test]
    fn rejects_values_block() {
        let input = "CREATE WASM PROCESSOR p USING RESOURCE r FILE 'p.wasm' FROM a TO b BRANCHED \
                     BY tenant_branch VALUES { tenant = a.tenant } ON MESSAGE ERROR LOG ON GLOBAL \
                     ERROR LOG;";
        assert!(parse_create_wasm_processor_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn rejects_flush_policy() {
        let input = "CREATE WASM PROCESSOR p USING RESOURCE r FILE 'p.wasm' FROM a TO b \
                     UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;";
        assert!(parse_create_wasm_processor_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn parses_conditional_and_unconditional_output_routes() {
        let input = r#"
            CREATE WASM PROCESSOR filter_even
            USING RESOURCE wasm_filters FILE 'processors/filter_even.wasm'
            FROM raw_orders FILTER WHERE raw_orders.value >= 0
            TO even_orders WHERE even_orders.value = even_orders.value
            TO other_orders SET other_orders.bucket = "fallback"
            UNBRANCHED
            ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
        "#;

        let parsed = parse_create_wasm_processor_tokens(&to_tokens(input)).expect("parse works");
        assert_eq!(parsed.output_routes.routes[0].relay.as_str(), "even_orders");
        assert_eq!(
            parsed
                .output_routes
                .routes
                .get(1)
                .expect("second output route should parse")
                .relay
                .as_str(),
            "other_orders"
        );
    }

    #[test]
    fn parses_unconditional_output_route() {
        let input = "CREATE WASM PROCESSOR p USING RESOURCE r FILE 'p.wasm' FROM a TO b \
                     UNBRANCHED ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;";
        let tokens = to_tokens(input);
        let parsed =
            parse_create_wasm_processor_tokens(&tokens).expect("unconditional TO should parse");
        assert_eq!(parsed.output_routes.routes.len(), 1);
        assert_eq!(parsed.output_routes.routes[0].relay.as_str(), "b");
        assert_eq!(parsed.output_routes.routes[0].filter_map, None);
    }

    #[test]
    fn parses_output_route_with_set_but_no_where() {
        let input = "CREATE WASM PROCESSOR p USING RESOURCE r FILE 'p.wasm' FROM input TO out1 \
                     SET out1.name = lower(out1.name), out1.surname = input.surname UNBRANCHED ON \
                     MESSAGE ERROR LOG ON GLOBAL ERROR LOG;";
        let tokens = to_tokens(input);
        let parsed = parse_create_wasm_processor_tokens(&tokens)
            .expect("TO with SET and no WHERE should parse");
        assert_eq!(
            parsed.output_routes.routes[0].filter_map.as_deref(),
            Some("SET out1.name = lower ( out1.name ) , out1.surname = input.surname")
        );
    }

    #[test]
    fn rejects_output_route_with_unset() {
        let input = "CREATE WASM PROCESSOR p USING RESOURCE r FILE 'p.wasm' FROM input TO out1 \
                     UNSET out1.legacy UNBRANCHED ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;";
        assert!(parse_create_wasm_processor_tokens(&to_tokens(input)).is_err());
    }
}
