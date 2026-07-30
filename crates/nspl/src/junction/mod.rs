use chumsky::prelude::*;
use nervix_models::{
    AckMode, AlterJunction, AlterJunctionOperation, CreateJunction, CreateStatement,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, alter_flushed_route_body, alter_op_separator,
        branch_selection, collect_for, filter_where_clause, flushed_processor_outputs,
        from_relay_clauses, if_not_exists_clause, into_parse_error, junction_name, junction_ref,
        kw, lex_input, materialized_state_dependencies, materialized_state_policy, relay_ref,
        suggest_from, tok, where_expression,
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
        .boxed()
        .then(branch_selection())
        .then(materialized_state_dependencies())
        .then(flushed_processor_outputs())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (((((if_not_exists, mode), name), from_inputs), filter_where), branched_by),
                    materialized_state,
                ),
                outputs,
            )| {
                CreateStatement::new(
                    CreateJunction {
                        name,
                        from: from_inputs,
                        output_routes: outputs,
                        branched_by,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                        materialized_state,
                    },
                    if_not_exists,
                )
            },
        )
        .boxed()
}

pub fn alter_junction_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterJunction, extra::Err<ParseError<'src>>> + Clone {
    let add_from = kw(Identifier::Add)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .then(where_expression(alter_op_separator()).or_not())
        .map(|(relay, where_clause)| AlterJunctionOperation::AddFrom {
            relay,
            where_clause,
        });
    let drop_from = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .map(|relay| AlterJunctionOperation::DropFrom { relay });
    let alter_from = kw(Identifier::Alter)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .then(choice((
            kw(Identifier::Set)
                .ignore_then(where_expression(alter_op_separator()))
                .map(Some),
            kw(Identifier::Drop)
                .ignore_then(kw(Identifier::Where))
                .to(None),
        )))
        .map(|(relay, where_clause)| match where_clause {
            Some(where_clause) => AlterJunctionOperation::AlterFromSetWhere {
                relay,
                where_clause,
            },
            None => AlterJunctionOperation::AlterFromDropWhere { relay },
        });
    let set_collect = kw(Identifier::Set)
        .ignore_then(collect_for())
        .map(|policy| AlterJunctionOperation::SetCollect { policy });
    let drop_collect = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Collect))
        .to(AlterJunctionOperation::DropCollect);
    let set_filter = kw(Identifier::Set)
        .ignore_then(kw(Identifier::Filter))
        .ignore_then(where_expression(alter_op_separator()))
        .map(|where_clause| AlterJunctionOperation::SetFilterWhere { where_clause });
    let drop_filter = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Filter))
        .ignore_then(kw(Identifier::Where))
        .to(AlterJunctionOperation::DropFilterWhere);
    let set_mode = kw(Identifier::Set)
        .ignore_then(ack_mode())
        .map(|mode| AlterJunctionOperation::SetMode { mode });
    let set_branching = kw(Identifier::Set)
        .ignore_then(branch_selection())
        .map(|branching| AlterJunctionOperation::SetBranching { branching });
    let add_materialized = kw(Identifier::Add)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(relay_ref())
        .then(materialized_state_policy())
        .map(
            |(relay, policy)| AlterJunctionOperation::AddMaterializedState {
                dependency: nervix_models::MaterializedStateDependency { relay, policy },
            },
        );
    let drop_materialized = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(relay_ref())
        .map(|relay| AlterJunctionOperation::DropMaterializedState { relay });
    let alter_materialized = kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(relay_ref())
        .then_ignore(kw(Identifier::Set))
        .then(materialized_state_policy())
        .map(|(relay, policy)| AlterJunctionOperation::AlterMaterializedState { relay, policy });
    let add_route = kw(Identifier::Add)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(alter_flushed_route_body())
        .map(|route| AlterJunctionOperation::AddRoute { route });
    let drop_route = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(kw(Identifier::To))
        .ignore_then(relay_ref())
        .map(|relay| AlterJunctionOperation::DropRoute { relay });
    let replace_route = kw(Identifier::Replace)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(alter_flushed_route_body())
        .map(|route| AlterJunctionOperation::ReplaceRoute { route });

    let operation = choice((
        add_from,
        drop_from,
        alter_from,
        set_collect,
        drop_collect,
        set_filter,
        drop_filter,
        set_mode,
        set_branching,
        add_materialized,
        drop_materialized,
        alter_materialized,
        add_route,
        drop_route,
        replace_route,
    ))
    .boxed();

    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Junction))
        .ignore_then(junction_ref())
        .then(
            operation
                .separated_by(alter_op_separator())
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(junction, operations)| AlterJunction {
            junction,
            operations,
        })
        .boxed()
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

pub fn parse_alter_junction_tokens(tokens: &[Token]) -> Result<AlterJunction, Vec<ParseError<'_>>> {
    let out = alter_junction_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_alter_junction(input: &str) -> Result<AlterJunction, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_alter_junction_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_junction(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_junction_parser())
}

pub fn suggest_alter_junction(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, alter_junction_parser())
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
    fn parses_route_local_inherit_and_structured_set_expressions() {
        let input = r#"
            CREATE JUNCTION project_events
            FROM incoming_events
            UNBRANCHED
            TO projected_events
            INHERIT ALL EXCEPT raw
            SET normalized = lower(input.raw),
                label = concat(output.normalized, ':ready')
            WHERE output.normalized != ''
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG;
        "#;

        let parsed = parse_create_junction_tokens(&to_tokens(input))
            .expect("canonical route construction must parse");
        let route = &parsed.output_routes.routes[0];

        assert!(matches!(
            route.construction.inherit,
            Some(nervix_models::Inheritance::AllExcept(ref fields))
                if fields.iter().map(|field| field.as_str()).eq(["raw"])
        ));
        assert_eq!(route.construction.assignments.len(), 2);
        assert!(route.construction.where_clause.is_some());
    }

    #[test]
    fn parses_alter_junction_expression_commas_and_dlq_tail_before_next_operation() {
        let parsed = parse_alter_junction(
            "ALTER JUNCTION project_events SET FILTER WHERE concat(input.kind, ',') != '', ADD \
             ROUTE TO projected SET normalized = concat(input.raw, ',ready') FLUSH IMMEDIATE ON \
             MESSAGE ERROR SEND TO errors SET code = concat(error.code, ',bad'), SET DETACHED;",
        )
        .expect("ALTER JUNCTION should preserve expression commas and find operation separators");

        assert_eq!(parsed.operations.len(), 3);
        assert!(matches!(
            parsed.operations[0],
            AlterJunctionOperation::SetFilterWhere { .. }
        ));
        assert!(matches!(
            parsed.operations[1],
            AlterJunctionOperation::AddRoute { .. }
        ));
        assert_eq!(
            parsed.operations[2],
            AlterJunctionOperation::SetMode {
                mode: AckMode::Detached
            }
        );
    }

    #[test]
    fn parses_alter_junction_structural_operations() {
        let parsed = parse_alter_junction(
            "ALTER JUNCTION project_events ADD FROM incoming WHERE input.kind = 'event', ALTER \
             FROM incoming DROP WHERE, SET COLLECT FOR 10ms MAX BATCH SIZE 1MiB, ADD MATERIALIZED \
             STATE profiles REQUIRED WAIT, ALTER MATERIALIZED STATE profiles SET REQUIRED SKIP, \
             DROP MATERIALIZED STATE profiles, DROP FROM incoming;",
        )
        .expect("structural operations should parse");

        assert_eq!(parsed.operations.len(), 7);
    }

    #[test]
    fn rejects_alter_junction_without_operations_or_complete_route_contracts() {
        assert!(parse_alter_junction("ALTER JUNCTION project_events;").is_err());
        assert!(
            parse_alter_junction(
                "ALTER JUNCTION project_events ADD ROUTE TO projected INHERIT ALL;"
            )
            .is_err()
        );
    }

    #[test]
    fn alter_junction_completion_comes_from_operation_grammar() {
        let suggestions = suggest_alter_junction("ALTER JUNCTION project_events ", usize::MAX);
        for expected in ["ADD", "DROP", "ALTER", "SET", "REPLACE"] {
            assert!(
                suggestions.contains(&expected.to_string()),
                "missing {expected}: {suggestions:?}"
            );
        }
        assert!(!suggestions.contains(&"SCHEMA".to_string()));
        assert!(!suggestions.contains(&"WIRE".to_string()));
    }

    #[test]
    fn alter_expression_documents_operation_head_field_corner() {
        assert!(
            parse_alter_junction(
                "ALTER JUNCTION project_events SET FILTER WHERE concat(input.add, set);"
            )
            .is_err(),
            "an operation-head field immediately after a comma is an intentional grammar boundary"
        );
    }

    #[test]
    fn parses_qualified_udf_calls_in_route_expressions() {
        let input = r#"
            CREATE JUNCTION apply_udf
            FROM incoming
            UNBRANCHED
            TO outgoing
            SET result = udf::add_one(abs(input.value))
            WHERE udf::add_one(input.value) > 0
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG;
        "#;

        let parsed = parse_create_junction(input).expect("qualified UDF calls must parse");
        let route = &parsed.output_routes.routes[0];
        assert!(matches!(
            route.construction.assignments[0].value,
            nervix_models::Expression::UdfCall { .. }
        ));
        assert!(matches!(
            route.construction.where_clause,
            Some(nervix_models::Expression::Binary { .. })
        ));
    }

    #[test]
    fn rejects_whitespace_inside_the_udf_qualifier() {
        let input = r#"
            CREATE JUNCTION apply_udf
            FROM incoming
            UNBRANCHED
            TO outgoing
            SET result = udf : : add_one(input.value)
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG;
        "#;

        assert!(parse_create_junction(input).is_err());
    }

    #[test]
    fn parses_create_junction() {
        let input = r#"
            CREATE JUNCTION join_streams
                FROM ss1, ss2, ss3
                BRANCHED BY tenant
                TO ss10 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
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
    fn parses_optional_input_collection_after_source_list() {
        let parsed = parse_create_junction(
            "CREATE JUNCTION join_streams FROM ss1, ss2 COLLECT FOR 1s MAX BATCH SIZE 10mb \
             UNBRANCHED TO ss10 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        )
        .expect("input collection must parse");
        let policy = parsed
            .from
            .collect_policy
            .as_ref()
            .expect("input collection policy must be structured");
        assert_eq!(policy.collect_for, "1s");
        assert_eq!(policy.max_batch_size.as_deref(), Some("10mb"));
    }

    #[test]
    fn parses_input_collection_without_size_boundary() {
        let parsed = parse_create_junction(
            "CREATE JUNCTION join_streams FROM ss1 COLLECT FOR 1s UNBRANCHED TO ss10 INHERIT ALL \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        )
        .expect("timer-only input collection must parse");
        assert_eq!(
            parsed
                .from
                .collect_policy
                .as_ref()
                .expect("input collection policy must parse")
                .max_batch_size,
            None
        );
    }

    #[test]
    fn rejects_input_collection_without_duration() {
        parse_create_junction(
            "CREATE JUNCTION join_streams FROM ss1 COLLECT FOR UNBRANCHED TO ss10 INHERIT ALL \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        )
        .expect_err("input collection requires a duration");
    }

    #[test]
    fn suggests_collect_for_after_source_list() {
        let input = "CREATE JUNCTION join_streams FROM ss1 COL";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"COLLECT FOR".to_string()));
        assert!(!suggestions.contains(&"FLUSH EACH".to_string()));
    }

    #[test]
    fn suggests_max_batch_size_inside_input_collection() {
        let input = "CREATE JUNCTION join_streams FROM ss1 COLLECT FOR 1s MA";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(
            suggestions.contains(&"MAX BATCH SIZE".to_string()),
            "unexpected suggestions: {suggestions:?}"
        );
        assert!(!suggestions.contains(&"MAX TIME".to_string()));
    }

    #[test]
    fn parses_message_error_policy_on_each_output_route() {
        let input = r#"
            CREATE JUNCTION route_messages
                FROM incoming
                UNBRANCHED
                TO accepted INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR IGNORE
                TO rejected FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                    ON MESSAGE ERROR SEND TO errors
                    SET reason = error.message;
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
                UNBRANCHED
                TO accepted FLUSH IMMEDIATE ON MESSAGE ERROR IGNORE
                TO rejected FLUSH IMMEDIATE;
        "#;

        assert!(parse_create_junction(input).is_err());
    }

    #[test]
    fn completion_does_not_leak_branch_clause_before_output_message_policy() {
        let input = "CREATE JUNCTION route_messages FROM incoming UNBRANCHED TO accepted FLUSH \
                     IMMEDIATE ON ";
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
            "CREATE DETACHED JUNCTION join_streams FROM ss1, ss2 BRANCHED BY tenant TO ss10 \
             INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_junction_flush_each() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1, ss2 BRANCHED BY tenant TO ss10 INHERIT ALL \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
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
            "CREATE JUNCTION join_streams FROM ss1, ss2 BRANCHED BY tenant TO ss10 INHERIT ALL \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
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
            "CREATE JUNCTION join_streams FROM ss1, ss2 UNBRANCHED TO fast INHERIT ALL FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG TO slow INHERIT ALL FLUSH EACH 1s MAX BATCH SIZE 1MiB \
             ON MESSAGE ERROR LOG;",
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
            "CREATE JUNCTION join_streams FROM ss1, ss2 UNBRANCHED TO fast INHERIT ALL FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG TO slow INHERIT ALL ON MESSAGE ERROR LOG;",
        );
        parse_create_junction_tokens(&tokens)
            .expect_err("every output must declare its own flush policy");
    }

    #[test]
    fn suggests_flush_for_each_output_without_branch_leakage() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ss2 UNBRANCHED TO fast INHERIT ALL \
                     FLUSH IMMEDIATE ON MESSAGE ERROR LOG TO slow FL";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
        assert!(!suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(!suggestions.contains(&"UNBRANCHED".to_string()));
    }

    #[test]
    fn parses_single_source_junction() {
        let tokens = to_tokens(
            "CREATE JUNCTION join_streams FROM ss1 UNBRANCHED TO ss10 INHERIT ALL FLUSH IMMEDIATE \
             ON MESSAGE ERROR LOG;",
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
    fn suggests_branch_after_source_list_without_schema_keyword_leakage() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ss2 ";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_flush_after_target_without_schema_leakage() {
        let input = "CREATE JUNCTION join_streams FROM ss1, ss2 UNBRANCHED TO ss10 FL";
        let suggestions = suggest_create_junction(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
