use chumsky::prelude::*;
use nervix_models::{AckMode, CreateJunction, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_selection, current_word_prefix,
        filter_where_clause, flushed_processor_outputs, from_relay_clauses, if_not_exists_clause,
        into_parse_error, junction_name, kw, lex_input, suggestions_from_errors, tok,
    },
};

pub fn create_junction_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateJunction>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Junction))
        .then(junction_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .then(flushed_processor_outputs())
        .then(branch_selection())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (((((if_not_exists, mode), name), from_inputs), filter_where), outputs),
                branched_by,
            )| {
                CreateStatement::new(
                    CreateJunction {
                        name,
                        from: from_inputs,
                        output_routes: outputs,
                        branched_by,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_junction_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateJunction>, Vec<ParseError<'_>>> {
    let out = create_junction_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_junction(
    input: &str,
) -> Result<CreateStatement<CreateJunction>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_junction_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_junction(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_junction_parser()
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
    fn parses_create_junction() {
        let input = r#"
            CREATE JUNCTION join_streams
                FROM ss1, ss2, ss3
                TO ss10 FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
                BRANCHED BY tenant;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "join_streams");
        assert_eq!(
            parsed
                .from
                .from
                .iter()
                .map(|relay| relay.as_str())
                .collect::<Vec<_>>(),
            vec!["ss1", "ss2", "ss3"]
        );
        assert_eq!(
            parsed
                .output_routes
                .routes
                .first()
                .expect("output route should parse")
                .relay
                .as_str(),
            "ss10"
        );
        assert_eq!(parsed.mode, AckMode::Attached);
    }

    #[test]
    fn parses_message_error_policy_on_each_output_route() {
        let input = r#"
            CREATE JUNCTION route_messages
                FROM incoming
                TO accepted FLUSH IMMEDIATE ON MESSAGE ERROR IGNORE
                TO rejected FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                    ON MESSAGE ERROR SEND TO errors
                    SET reason = message_error.message
                UNBRANCHED;
        "#;

        let parsed = parse_create_junction(input).expect("route policies should parse");
        assert_eq!(
            parsed.output_routes.routes[0].message_error_policy,
            nervix_models::MessageErrorPolicy::Ignore
        );
        assert!(matches!(
            parsed.output_routes.routes[1].message_error_policy,
            nervix_models::MessageErrorPolicy::Dlq { .. }
        ));
    }

    #[test]
    fn rejects_output_route_without_message_error_policy() {
        let input = r#"
            CREATE JUNCTION route_messages
                FROM incoming
                TO accepted FLUSH IMMEDIATE ON MESSAGE ERROR IGNORE
                TO rejected FLUSH IMMEDIATE
                UNBRANCHED;
        "#;

        assert!(parse_create_junction(input).is_err());
    }

    #[test]
    fn completion_does_not_leak_branch_clause_before_output_message_policy() {
        let input = "CREATE JUNCTION route_messages FROM incoming TO accepted FLUSH IMMEDIATE ON ";
        let suggestions = suggest_create_junction(input, input.len());

        assert!(suggestions.iter().any(|suggestion| suggestion == "MESSAGE"));
        assert!(!suggestions.iter().any(|suggestion| suggestion == "TO"));
        assert!(
            !suggestions
                .iter()
                .any(|suggestion| suggestion == "UNBRANCHED")
        );
        assert!(
            !suggestions
                .iter()
                .any(|suggestion| suggestion == "BRANCHED BY")
        );
    }

    #[test]
    fn parses_create_detached_junction() {
        let tokens = to_tokens(
            "CREATE DETACHED JUNCTION join_streams FROM ss1, ss2 TO ss10 FLUSH EACH 100ms MAX \
             BATCH SIZE 1MiB ON MESSAGE ERROR LOG BRANCHED BY tenant;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_junction_flush_each() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1, ss2 TO ss10 FLUSH EACH 100ms MAX BATCH SIZE \
             1MiB ON MESSAGE ERROR LOG BRANCHED BY tenant;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "100ms"
        );
    }

    #[test]
    fn parses_junction_flush_immediate() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1, ss2 TO ss10 FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG BRANCHED BY tenant;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
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
    fn parses_distinct_flush_policy_for_each_output() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1, ss2 TO fast FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG TO slow FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG UNBRANCHED;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.output_routes.routes.len(), 2);
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("first output flush policy should parse")
                .flush_each,
            "IMMEDIATE"
        );
        assert_eq!(
            parsed.output_routes.routes[1]
                .flush_policy
                .as_ref()
                .expect("second output flush policy should parse")
                .flush_each,
            "1s"
        );
    }

    #[test]
    fn rejects_output_without_flush_policy() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1, ss2 TO fast FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG TO slow ON MESSAGE ERROR LOG UNBRANCHED;",
        );
        parse_create_junction_tokens(&tokens)
            .expect_err("every output must declare its own flush policy");
    }

    #[test]
    fn suggests_flush_for_each_output_without_branch_leakage() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ss2 TO fast FLUSH IMMEDIATE ON \
                     MESSAGE ERROR LOG TO slow FL";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
        assert!(!suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(!suggestions.contains(&"UNBRANCHED".to_string()));
    }

    #[test]
    fn parses_single_source_junction() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1 TO ss10 FLUSH IMMEDIATE ON MESSAGE ERROR LOG \
             UNBRANCHED;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.from.from.len(), 1);
        assert_eq!(parsed.from.from[0].as_str(), "ss1");
    }

    #[test]
    fn suggests_relay_reference_after_from_comma() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"ref:relay".to_string()));
        assert!(!suggestions.contains(&"TO".to_string()));
    }

    #[test]
    fn suggests_to_after_source_list_without_schema_leakage() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ss2 ";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_flush_after_target_without_schema_leakage() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ss2 TO ss10 FL";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
