use chumsky::prelude::*;
use nervix_models::{AlterGenerator, AlterGeneratorOperation, CreateGenerator, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, alter_generator_route_body, alter_op_separator,
        branch_selection, current_word_prefix, duration_lit, flushed_explicit_processor_outputs,
        generator_name, generator_ref, if_not_exists_clause, into_parse_error, kw, kw_phrase3,
        lex_input, relay_ref, suggestions_from_errors, tok,
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

pub fn alter_generator_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterGenerator, extra::Err<ParseError<'src>>> + Clone {
    let set_materialized_state = kw(Identifier::Set)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(relay_ref())
        .map(|relay| AlterGeneratorOperation::SetMaterializedState { relay });
    let set_each = kw(Identifier::Set)
        .ignore_then(kw(Identifier::Each))
        .ignore_then(duration_lit())
        .map(|each| AlterGeneratorOperation::SetEach { each });
    let set_branching = kw(Identifier::Set)
        .ignore_then(branch_selection())
        .map(|branching| AlterGeneratorOperation::SetBranching { branching });
    let add_route = kw(Identifier::Add)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(alter_generator_route_body())
        .map(|route| AlterGeneratorOperation::AddRoute { route });
    let drop_route = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(kw(Identifier::To))
        .ignore_then(relay_ref())
        .map(|relay| AlterGeneratorOperation::DropRoute { relay });
    let replace_route = kw(Identifier::Replace)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(alter_generator_route_body())
        .map(|route| AlterGeneratorOperation::ReplaceRoute { route });
    let operation = choice((
        set_materialized_state,
        set_each,
        set_branching,
        add_route,
        drop_route,
        replace_route,
    ))
    .boxed();

    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Generator))
        .ignore_then(generator_ref())
        .then(
            operation
                .separated_by(alter_op_separator())
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(generator, operations)| AlterGenerator {
            generator,
            operations,
        })
        .boxed()
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

pub fn parse_alter_generator_tokens(
    tokens: &[Token],
) -> Result<AlterGenerator, Vec<ParseError<'_>>> {
    let out = alter_generator_parser().then_ignore(end()).parse(tokens);
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

pub fn parse_alter_generator(input: &str) -> Result<AlterGenerator, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_alter_generator_tokens(&tokens)
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

pub fn suggest_alter_generator(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);
    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };
    let out = alter_generator_parser()
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

    #[test]
    fn parses_alter_generator_operations_in_written_order() {
        let parsed = parse_alter_generator_tokens(&to_tokens(
            "ALTER GENERATOR synth SET MATERIALIZED STATE state_v2, SET EACH 250ms, SET \
             UNBRANCHED, REPLACE ROUTE TO alerts SET user_id = relay_state.state_v2.user_id FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG;",
        ))
        .expect("ALTER GENERATOR should parse");

        assert_eq!(parsed.generator.as_str(), "synth");
        assert_eq!(parsed.operations.len(), 4);
        let AlterGeneratorOperation::ReplaceRoute { route } = &parsed.operations[3] else {
            panic!("last operation should replace the route");
        };
        assert_eq!(route.relay.as_str(), "alerts");
        assert_eq!(route.construction.assignments.len(), 1);
    }

    #[test]
    fn rejects_alter_generator_inherit_route() {
        let tokens = to_tokens(
            "ALTER GENERATOR synth REPLACE ROUTE TO alerts INHERIT ALL FLUSH IMMEDIATE ON MESSAGE \
             ERROR LOG;",
        );
        let errors =
            parse_alter_generator_tokens(&tokens).expect_err("generator routes remain set-only");
        assert!(!errors.is_empty());
    }

    #[test]
    fn alter_generator_completion_exposes_operations_without_schema_leakage() {
        let input = "ALTER GENERATOR synth ";
        let suggestions = suggest_alter_generator(input, input.len());
        assert!(suggestions.contains(&"SET".to_string()));
        assert!(suggestions.contains(&"ADD".to_string()));
        assert!(suggestions.contains(&"DROP".to_string()));
        assert!(suggestions.contains(&"REPLACE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }
}
