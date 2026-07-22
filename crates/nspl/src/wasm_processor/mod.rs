use chumsky::prelude::*;
use nervix_models::{
    AckMode, CreateStatement, CreateWasmProcessor, GeneralErrorPolicy, ProcessorOutput,
    ProcessorOutputs, RouteConstruction,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_selection, current_word_prefix,
        filter_where_clause, from_relay_clauses, if_not_exists_clause, into_parse_error, kw,
        lex_input, materialized_state_dependencies, message_error_policy, relay_ref, resource_ref,
        route_construction, string_lit, suggestions_from_errors, tok, wasm_processor_name,
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

fn wasm_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    route_construction().try_map(|construction, span| {
        if construction.inherit.is_some() {
            return Err(Rich::custom(
                span,
                "WASM processor TO clauses do not support INHERIT",
            ));
        }
        if !construction.invocations.is_empty() {
            return Err(Rich::custom(
                span,
                "WASM processor TO clauses do not support INVOKE",
            ));
        }
        Ok(construction)
    })
}

fn wasm_processor_output_route<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(wasm_route_construction().or_not())
        .then(message_error_policy())
        .map(
            |((relay, construction), message_error_policy)| ProcessorOutput {
                relay,
                construction: construction.unwrap_or_default(),
                flush_policy: None,
                message_error_policy,
                branch: None,
            },
        )
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
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .then_ignore(kw(Identifier::Using))
        .then_ignore(kw(Identifier::Resource))
        .then(resource_ref())
        .then(kw(Identifier::Version).ignore_then(u64_value()).or_not())
        .then_ignore(kw(Identifier::File))
        .then(string_lit())
        .then(branch_selection())
        .then(materialized_state_dependencies())
        .then(wasm_processor_outputs())
        .then(global_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(base, global_error_policy)| {
            let (
                (
                    (
                        (
                            (
                                (
                                    ((((if_not_exists, mode), name), from_input), filter_where),
                                    resource,
                                ),
                                resource_version,
                            ),
                            file,
                        ),
                        branched_by,
                    ),
                    materialized_state,
                ),
                outputs,
            ) = base;
            CreateStatement::new(
                CreateWasmProcessor {
                    name,
                    from: from_input,
                    output_routes: outputs,
                    branched_by,
                    resource,
                    resource_version,
                    file,
                    global_error_policy,
                    mode: mode.unwrap_or(AckMode::Attached),
                    filter_where,
                    materialized_state,
                },
                if_not_exists,
            )
        })
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
            FROM raw_orders
            USING RESOURCE wasm_filters VERSION 2 FILE 'processors/filter_even.wasm'
            BRANCHED BY tenant_branch
            TO filtered_orders SET value = value ON MESSAGE ERROR LOG
            ON GLOBAL ERROR IGNORE;
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
            parsed.output_routes.routes[0].message_error_policy,
            nervix_models::MessageErrorPolicy::Log
        );
        assert_eq!(parsed.global_error_policy, GeneralErrorPolicy::Ignore);
        assert_eq!(
            parsed.branched_by.branch().map(|branch| branch.as_str()),
            Some("tenant_branch")
        );
    }

    #[test]
    fn keeps_global_error_policy_on_node_after_route_message_policies() {
        let input = r#"
            CREATE WASM PROCESSOR route_wasm
                FROM incoming
                USING RESOURCE wasm_resource FILE 'processor.wasm'
                UNBRANCHED
                TO accepted ON MESSAGE ERROR IGNORE
                TO rejected ON MESSAGE ERROR LOG
                ON GLOBAL ERROR IGNORE;
        "#;

        let parsed = parse_create_wasm_processor(input).expect("route policies should parse");
        assert_eq!(
            parsed.output_routes.routes[0].message_error_policy,
            nervix_models::MessageErrorPolicy::Ignore
        );
        assert_eq!(
            parsed.output_routes.routes[1].message_error_policy,
            nervix_models::MessageErrorPolicy::Log
        );
        assert_eq!(parsed.global_error_policy, GeneralErrorPolicy::Ignore);
    }

    #[test]
    fn rejects_values_block() {
        let input = "CREATE WASM PROCESSOR p FROM a USING RESOURCE r FILE 'p.wasm' BRANCHED BY \
                     tenant_branch VALUES { tenant = input.tenant } TO b ON MESSAGE ERROR LOG ON \
                     GLOBAL ERROR LOG;";
        assert!(parse_create_wasm_processor_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn rejects_flush_policy() {
        let input = "CREATE WASM PROCESSOR p FROM a USING RESOURCE r FILE 'p.wasm' UNBRANCHED TO \
                     b FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;";
        assert!(parse_create_wasm_processor_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn parses_conditional_and_unconditional_output_routes() {
        let input = r#"
            CREATE WASM PROCESSOR filter_even
            FROM raw_orders FILTER WHERE input.value >= 0
            USING RESOURCE wasm_filters FILE 'processors/filter_even.wasm'
            UNBRANCHED
            TO even_orders SET value = value WHERE output.value = output.value ON MESSAGE ERROR LOG
            TO other_orders SET bucket = "fallback" ON MESSAGE ERROR LOG
            ON GLOBAL ERROR LOG;
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
        let input = "CREATE WASM PROCESSOR p FROM a USING RESOURCE r FILE 'p.wasm' UNBRANCHED TO \
                     b ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;";
        let tokens = to_tokens(input);
        let parsed =
            parse_create_wasm_processor_tokens(&tokens).expect("unconditional TO should parse");
        assert_eq!(parsed.output_routes.routes.len(), 1);
        assert_eq!(parsed.output_routes.routes[0].relay.as_str(), "b");
        assert_eq!(
            parsed.output_routes.routes[0].construction,
            RouteConstruction::default()
        );
    }

    #[test]
    fn parses_output_route_with_set_but_no_where() {
        let input = "CREATE WASM PROCESSOR p FROM source USING RESOURCE r FILE 'p.wasm' \
                     UNBRANCHED TO out1 SET name = lower(name), surname = surname ON MESSAGE \
                     ERROR LOG ON GLOBAL ERROR LOG;";
        let tokens = to_tokens(input);
        let parsed = parse_create_wasm_processor_tokens(&tokens)
            .expect("TO with SET and no WHERE should parse");
        assert_eq!(
            parsed.output_routes.routes[0]
                .construction
                .assignments
                .len(),
            2
        );
    }

    #[test]
    fn rejects_output_route_with_unset() {
        let input = "CREATE WASM PROCESSOR p FROM input USING RESOURCE r FILE 'p.wasm' UNBRANCHED \
                     TO out1 UNSET legacy ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;";
        assert!(parse_create_wasm_processor_tokens(&to_tokens(input)).is_err());
    }
}
