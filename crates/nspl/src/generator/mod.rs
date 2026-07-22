use chumsky::prelude::*;
use nervix_models::{CreateGenerator, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, branch_selection, current_word_prefix,
        flushed_explicit_processor_outputs, generator_name, if_not_exists_clause, into_parse_error,
        kw, kw_phrase3, lex_input, relay_ref, suggestions_from_errors, tok,
    },
};

pub fn create_generator_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateGenerator>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Generator))
        .then(generator_name())
        .then_ignore(kw_phrase3(
            Identifier::Using,
            Identifier::Materialized,
            Identifier::State,
        ))
        .then(relay_ref())
        .then_ignore(kw(Identifier::Each))
        .then(crate::parser_support::duration_lit())
        .then(branch_selection())
        .then(flushed_explicit_processor_outputs())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                ((((if_not_exists, name), materialized_relay), each), branched_by),
                output_routes,
            )| {
                CreateStatement::new(
                    CreateGenerator {
                        name,
                        materialized_relay,
                        branched_by,
                        each,
                        output_routes,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_generator_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateGenerator>, Vec<ParseError<'_>>> {
    let out = create_generator_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_generator(
    input: &str,
) -> Result<CreateStatement<CreateGenerator>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_generator_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_generator(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_generator_parser()
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
    fn parses_create_generator() {
        let input = r#"
            CREATE GENERATOR synth
                USING MATERIALIZED STATE notifications
                EACH 100ms
                BRANCHED BY tenant
                TO alerts
                SET user_id = relay_state.notifications.user_id,
                    topic = relay_state.notifications.topic
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "synth");
        assert_eq!(parsed.materialized_relay.as_str(), "notifications");
        assert_eq!(parsed.each, "100ms");
        let route = &parsed.output_routes.routes[0];
        assert_eq!(route.relay.as_str(), "alerts");
        assert_eq!(route.construction.assignments.len(), 2);
        assert_eq!(
            route
                .flush_policy
                .as_ref()
                .map(|policy| policy.flush_each.as_str()),
            Some("100ms")
        );
    }

    #[test]
    fn parses_create_generator_with_flush_each() {
        let input = r#"
            CREATE GENERATOR synth
                USING MATERIALIZED STATE notifications
                EACH 100ms
                BRANCHED BY tenant
                TO alerts
                SET user_id = relay_state.notifications.user_id
                FLUSH EACH 1s MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .map(|policy| policy.flush_each.as_str()),
            Some("1s")
        );
    }

    #[test]
    fn parses_create_generator_with_flush_immediate() {
        let input = r#"
            CREATE GENERATOR synth
                USING MATERIALIZED STATE notifications
                EACH 100ms
                BRANCHED BY tenant
                TO alerts
                SET user_id = relay_state.notifications.user_id
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .map(|policy| policy.flush_each.as_str()),
            Some("IMMEDIATE")
        );
    }

    #[test]
    fn parses_create_generator_unbranched() {
        let input = r#"
            CREATE GENERATOR synth
                USING MATERIALIZED STATE notifications
                EACH 100ms
                UNBRANCHED
                TO alerts
                SET user_id = relay_state.notifications.user_id
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.branched_by,
            nervix_models::BranchSelection::unbranched()
        );
    }

    #[test]
    fn rejects_generator_route_without_set() {
        let tokens = to_tokens(
            "CREATE GENERATOR synth USING MATERIALIZED STATE notifications EACH 100ms BRANCHED BY \
             tenant TO alerts FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        assert!(parse_create_generator_tokens(&tokens).is_err());
    }

    #[test]
    fn parses_generator_where_clause_after_set() {
        let tokens = to_tokens(
            "CREATE GENERATOR synth USING MATERIALIZED STATE notifications EACH 100ms BRANCHED BY \
             tenant TO alerts SET keep = relay_state.notifications.keep WHERE output.keep FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");
        assert!(
            parsed.output_routes.routes[0]
                .construction
                .where_clause
                .is_some()
        );
    }

    #[test]
    fn suggests_materialized_state_after_generator_name_without_cross_branch_leakage() {
        let input = "CREATE GENERATOR synth ";
        let suggestions = suggest_create_generator(input, input.len());
        assert!(suggestions.contains(&"USING MATERIALIZED STATE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_branching_after_each_duration() {
        let input = "CREATE GENERATOR synth USING MATERIALIZED STATE notifications EACH 100ms ";
        let suggestions = suggest_create_generator(input, input.len());
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
    }

    #[test]
    fn suggests_each_after_materialized_relay_without_cross_branch_leakage() {
        let input = "CREATE GENERATOR synth USING MATERIALIZED STATE notifications ";
        let suggestions = suggest_create_generator(input, input.len());
        assert!(suggestions.contains(&"EACH".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }
}
