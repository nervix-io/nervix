use chumsky::prelude::*;
use nervix_models::{
    AckMode, AlterReorderer, AlterReordererOperation, CreateReorderer, CreateStatement,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, alter_expression_list, alter_op_separator,
        alter_processor_operation, branch_selection, current_word_prefix, duration_lit,
        filter_where_clause, flushed_processor_outputs, from_relay_clauses, if_not_exists_clause,
        into_parse_error, kw, kw_phrase2, lex_input, materialized_state_dependencies,
        render_vm_program_tokens, reorderer_name, reorderer_ref, suggestions_from_errors, tok,
        vm_program_error_message,
    },
};

fn boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::Max,
                ..
            })
    )
}

fn by_exprs<'src>()
-> impl Parser<'src, &'src [Token], Vec<nervix_models::Expression>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::By)
        .ignore_then(
            any()
                .filter(|token: &Token| !boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .try_map(|tokens, span| {
            crate::parse_expression_list(&render_vm_program_tokens(&tokens))
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
        .labelled("reorder_by")
}

pub fn create_reorderer_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateReorderer>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Reorderer))
        .then(reorderer_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .then(by_exprs())
        .then_ignore(kw(Identifier::Max))
        .then_ignore(kw(Identifier::Time))
        .then(duration_lit())
        .then(branch_selection())
        .then(materialized_state_dependencies())
        .then(flushed_processor_outputs())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (
                            (((((if_not_exists, mode), name), from_input), filter_where), order_by),
                            max_time,
                        ),
                        branched_by,
                    ),
                    materialized_state,
                ),
                outputs,
            )| {
                CreateStatement::new(
                    CreateReorderer {
                        name,
                        from: from_input,
                        output_routes: outputs,
                        branched_by,
                        order_by,
                        max_time,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                        materialized_state,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn alter_reorderer_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterReorderer, extra::Err<ParseError<'src>>> + Clone {
    let set_order_by = kw(Identifier::Set)
        .ignore_then(kw(Identifier::By))
        .ignore_then(alter_expression_list(alter_op_separator(), "reorder_by"))
        .map(|expressions| AlterReordererOperation::SetOrderBy { expressions });
    let set_max_time = kw(Identifier::Set)
        .ignore_then(kw_phrase2(Identifier::Max, Identifier::Time))
        .ignore_then(duration_lit())
        .map(|max_time| AlterReordererOperation::SetMaxTime { max_time });
    let operation = choice((
        set_order_by,
        set_max_time,
        alter_processor_operation().map(AlterReordererOperation::Processor),
    ))
    .boxed();

    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Reorderer))
        .ignore_then(reorderer_ref())
        .then(
            operation
                .separated_by(alter_op_separator())
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(reorderer, operations)| AlterReorderer {
            reorderer,
            operations,
        })
        .boxed()
}

pub fn parse_create_reorderer_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateReorderer>, Vec<ParseError<'_>>> {
    let out = create_reorderer_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_alter_reorderer_tokens(
    tokens: &[Token],
) -> Result<AlterReorderer, Vec<ParseError<'_>>> {
    let out = alter_reorderer_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_reorderer(
    input: &str,
) -> Result<CreateStatement<CreateReorderer>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_reorderer_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_alter_reorderer(input: &str) -> Result<AlterReorderer, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_alter_reorderer_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_reorderer(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);
    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let out = create_reorderer_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }
    suggestions_from_errors(out.into_errors(), &prefix)
}

pub fn suggest_alter_reorderer(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);
    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };
    let out = alter_reorderer_parser()
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
            .map(|token| token.token)
            .collect()
    }

    #[test]
    fn parses_create_reorderer() {
        let tokens = to_tokens(
            "CREATE REORDERER order_notifications FROM s1 BY input.tenant, \
             concat(lower(input.id), '-', trim(input.kind)) MAX TIME 10s BRANCHED BY tenant TO s2 \
             SET id = trim(input.id) WHERE output.id != '' FLUSH EACH 100ms MAX BATCH SIZE 1MiB \
             ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_reorderer_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "order_notifications");
        assert_eq!(parsed.from.from[0].as_str(), "s1");
        assert_eq!(parsed.output_routes.routes[0].relay.as_str(), "s2");
        assert_eq!(parsed.order_by.len(), 2);
        assert_eq!(parsed.max_time, "10s");
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "100ms"
        );
        let construction = parsed
            .output_routes
            .routes
            .first()
            .map(|output| &output.construction)
            .expect("route construction should parse");
        assert!(construction.where_clause.is_some());
        assert!(!construction.assignments.is_empty());
    }

    #[test]
    fn parses_alter_reorderer_operations_and_expression_commas() {
        let parsed = parse_alter_reorderer(
            "ALTER REORDERER order_notifications SET BY concat(input.source, ','), input.id, SET \
             MAX TIME 20s, SET FILTER WHERE concat(input.source, ',') != '', ADD FROM s2 WHERE \
             input.active, ADD ROUTE TO s3 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        )
        .expect("ALTER REORDERER should parse");

        assert_eq!(parsed.operations.len(), 5);
        let AlterReordererOperation::SetOrderBy { expressions } = &parsed.operations[0] else {
            panic!("first operation should set the ordering key");
        };
        assert_eq!(expressions.len(), 2);
    }

    #[test]
    fn rejects_alter_reorderer_without_operations_or_complete_ordering() {
        assert!(parse_alter_reorderer("ALTER REORDERER order_notifications;").is_err());
        assert!(
            parse_alter_reorderer("ALTER REORDERER order_notifications SET BY, SET MAX TIME 1s;")
                .is_err()
        );
    }

    #[test]
    fn alter_reorderer_completion_exposes_specific_and_shared_operations() {
        let input = "ALTER REORDERER order_notifications SET ";
        let suggestions = suggest_alter_reorderer(input, input.len());
        assert!(suggestions.contains(&"BY".to_string()));
        assert!(suggestions.contains(&"MAX TIME".to_string()));
        assert!(suggestions.contains(&"FILTER".to_string()));
        assert!(suggestions.contains(&"ATTACHED".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }

    #[test]
    fn rejects_reorderer_global_error_policy() {
        let tokens = to_tokens(
            "CREATE REORDERER order_notifications FROM s1 BY input.id MAX TIME 10s BRANCHED BY \
             tenant TO s2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG \
             ON GENERAL ERROR LOG;",
        );
        assert!(parse_create_reorderer_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_flush_on_output_without_cross_branch_leakage() {
        let input = "CREATE REORDERER order_notifications FROM s1 BY input.id MAX TIME 10s \
                     UNBRANCHED TO s2 FL";
        let suggestions = suggest_create_reorderer(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
        assert!(!suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }
}
