use chumsky::prelude::*;
use nervix_models::{AckMode, CreateRouter, CreateStatement, RouterMatchPolicy, RouterRoute};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, current_word_prefix,
        filter_map_program_until_router_clause, flush_each, if_not_exists_clause, into_parse_error,
        kw, lex_input, message_error_policy, relay_ref, router_name, suggestions_from_errors, tok,
        where_expr_until_router_clause,
    },
};

fn conditional_route_parser<'src>()
-> impl Parser<'src, &'src [Token], RouterRoute, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(where_expr_until_router_clause())
        .map(|(into_relay, condition)| RouterRoute {
            into_relay,
            condition,
        })
}

fn default_route_parser<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::Identifier, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Default)
        .ignore_then(kw(Identifier::To))
        .ignore_then(relay_ref())
}

fn match_policy_parser<'src>()
-> impl Parser<'src, &'src [Token], RouterMatchPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Match)
        .ignore_then(choice((
            kw(Identifier::First).to(RouterMatchPolicy::First),
            kw(Identifier::All).to(RouterMatchPolicy::All),
        )))
        .or_not()
        .map(|policy| policy.unwrap_or_default())
}

fn route_set_parser<'src>()
-> impl Parser<'src, &'src [Token], (Vec<RouterRoute>, RouterMatchPolicy), extra::Err<ParseError<'src>>>
+ Clone {
    conditional_route_parser()
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .then(match_policy_parser())
        .or(empty().to((Vec::new(), RouterMatchPolicy::default())))
}

pub fn create_router_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateRouter>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Router))
        .then(router_name())
        .then_ignore(kw(Identifier::From))
        .then(relay_ref())
        .then(choice((
            filter_map_program_until_router_clause().map(Some),
            empty().to(None),
        )))
        .then(route_set_parser())
        .then(default_route_parser())
        .then(branch_parameterization())
        .then(flush_each())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (
                            (
                                ((((if_not_exists, mode), name), from_relay), filter_map),
                                (routes, match_policy),
                            ),
                            default_into_relay,
                        ),
                        parameterized_by,
                    ),
                    flush_each,
                ),
                message_error_policy,
            )| {
                let (flush_each, max_batch_size) = flush_each;
                CreateStatement::new(
                    CreateRouter {
                        name,
                        from_relay,
                        routes,
                        match_policy,
                        default_into_relay,
                        parameterized_by,
                        flush_each,
                        max_batch_size,
                        message_error_policy,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_map,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_router_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateRouter>, Vec<ParseError<'_>>> {
    let out = create_router_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_router(
    input: &str,
) -> Result<CreateStatement<CreateRouter>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_router_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_router(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_router_parser()
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
    fn parses_create_router_with_filter_map_and_default() {
        let input = r#"
            CREATE ROUTER log_router
                FROM incoming_logs
                SET incoming_logs.severity = lower(incoming_logs.level) UNSET incoming_logs.legacy_field WHERE incoming_logs.active
                TO errors_ss WHERE incoming_logs.level = "error"
                TO warnings_ss WHERE incoming_logs.level = "warn"
                DEFAULT TO info_ss
                PARAMETERIZED BY tenant
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_router_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "log_router");
        assert_eq!(parsed.from_relay.as_str(), "incoming_logs");
        assert_eq!(
            parsed.filter_map.as_deref(),
            Some(
                "SET incoming_logs.severity = lower ( incoming_logs.level ) UNSET \
                 incoming_logs.legacy_field WHERE incoming_logs.active"
            )
        );
        assert_eq!(parsed.routes.len(), 2);
        assert_eq!(parsed.match_policy, RouterMatchPolicy::All);
        assert_eq!(parsed.routes[0].into_relay.as_str(), "errors_ss");
        assert_eq!(
            parsed.routes[0].condition,
            r#"incoming_logs.level = "error""#
        );
        assert_eq!(parsed.default_into_relay.as_str(), "info_ss");
        assert_eq!(parsed.flush_each, "100ms");
        assert_eq!(parsed.mode, AckMode::Attached);
    }

    #[test]
    fn parses_create_detached_router() {
        let tokens = to_tokens(
            r#"CREATE DETACHED ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.level = "error" DEFAULT TO info_ss PARAMETERIZED BY tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"#,
        );
        let parsed = parse_create_router_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_create_router_with_match_first() {
        let tokens = to_tokens(
            r#"CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.level = "error" MATCH FIRST DEFAULT TO info_ss PARAMETERIZED BY tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"#,
        );
        let parsed = parse_create_router_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.match_policy, RouterMatchPolicy::First);
    }

    #[test]
    fn parses_create_router_with_only_default_route() {
        let tokens = to_tokens(
            r#"CREATE ROUTER project_notifications FROM notifications SET notifications.normalized = lower(notifications.raw) UNSET notifications.raw WHERE notifications.active DEFAULT TO projected_notifications PARAMETERIZED BY tenant FLUSH IMMEDIATE ON MESSAGE ERROR LOG;"#,
        );
        let parsed = parse_create_router_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "project_notifications");
        assert_eq!(parsed.from_relay.as_str(), "notifications");
        assert!(parsed.routes.is_empty());
        assert_eq!(parsed.match_policy, RouterMatchPolicy::All);
        assert_eq!(
            parsed.default_into_relay.as_str(),
            "projected_notifications"
        );
        assert_eq!(parsed.flush_each, "IMMEDIATE");
        assert_eq!(
            parsed.filter_map.as_deref(),
            Some(
                "SET notifications.normalized = lower ( notifications.raw ) UNSET \
                 notifications.raw WHERE notifications.active"
            )
        );
    }

    #[test]
    fn rejects_router_match_without_policy() {
        let tokens = to_tokens(
            r#"CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.level = "error" MATCH DEFAULT TO info_ss PARAMETERIZED BY tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"#,
        );
        assert!(parse_create_router_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_router_without_default_route() {
        let tokens = to_tokens(
            r#"CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.active ON MESSAGE ERROR LOG;"#,
        );
        assert!(parse_create_router_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_router_without_flush_each() {
        let tokens = to_tokens(
            r#"CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.active DEFAULT TO info_ss ON MESSAGE ERROR LOG;"#,
        );
        assert!(parse_create_router_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_default_only_router_with_match_policy() {
        let tokens = to_tokens(
            r#"CREATE ROUTER log_router FROM incoming_logs MATCH FIRST DEFAULT TO info_ss PARAMETERIZED BY tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"#,
        );
        assert!(parse_create_router_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_mode_after_create() {
        let input = "CREATE ";
        let suggestions = suggest_create_router(input, input.len());
        assert!(suggestions.contains(&"ATTACHED".to_string()));
        assert!(suggestions.contains(&"DETACHED".to_string()));
    }

    #[test]
    fn suggests_to_after_source_stream_without_cross_branch_leakage() {
        let input = "CREATE ROUTER log_router FROM incoming_logs ";
        let suggestions = suggest_create_router(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(suggestions.contains(&"DEFAULT".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_where_after_route_target_without_cross_branch_leakage() {
        let input = "CREATE ROUTER log_router FROM incoming_logs TO errors_ss ";
        let suggestions = suggest_create_router(input, input.len());
        assert!(suggestions.contains(&"WHERE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_default_or_next_to_after_conditional_route() {
        let input = r#"CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.level = "error" "#;
        let suggestions = suggest_create_router(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(suggestions.contains(&"DEFAULT".to_string()));
        assert!(suggestions.contains(&"MATCH".to_string()));
    }

    #[test]
    fn suggests_router_match_policy() {
        let input = r#"CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.level = "error" MATCH "#;
        let suggestions = suggest_create_router(input, input.len());
        assert!(suggestions.contains(&"FIRST".to_string()));
        assert!(suggestions.contains(&"ALL".to_string()));
    }

    #[test]
    fn suggests_to_after_default_without_cross_branch_leakage() {
        let input = r#"CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.active DEFAULT "#;
        let suggestions = suggest_create_router(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
