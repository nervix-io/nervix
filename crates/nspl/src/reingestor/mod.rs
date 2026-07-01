use chumsky::prelude::*;
use nervix_models::{AckMode, CreateReingestor, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_initiator_selection,
        current_word_prefix, filter_where_clause, flush_each, from_relay_clauses,
        if_not_exists_clause, into_parse_error, kw, lex_input, message_error_policy,
        processor_outputs, reingestor_name, suggestions_from_errors, tok,
    },
};

pub fn create_reingestor_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateReingestor>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Reingestor))
        .then(reingestor_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .then(processor_outputs())
        .then(branch_initiator_selection())
        .then(flush_each())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (((((if_not_exists, mode), name), from_input), filter_where), outputs),
                        branched_by,
                    ),
                    flush_each,
                ),
                message_error_policy,
            )| {
                let (flush_each, max_batch_size) = flush_each;
                CreateStatement::new(
                    CreateReingestor {
                        name,
                        from: from_input,
                        output_routes: outputs,
                        branched_by,
                        flush_each,
                        max_batch_size,
                        mode: mode.unwrap_or(AckMode::Attached),
                        message_error_policy,
                        filter_where,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_reingestor_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateReingestor>, Vec<ParseError<'_>>> {
    let out = create_reingestor_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_reingestor(
    input: &str,
) -> Result<CreateStatement<CreateReingestor>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_reingestor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_reingestor(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_reingestor_parser()
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
    fn parses_create_reingestor() {
        let tokens = to_tokens(
            "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications BRANCHED BY \
             tenant_user VALUES { tenant = tenant_notifications.tenant } FLUSH EACH 100ms MAX \
             BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_reingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "repartition");
        assert_eq!(parsed.from.from[0].as_str(), "notifications");
        assert_eq!(
            parsed
                .output_routes
                .routes
                .first()
                .expect("output route should parse")
                .relay
                .as_str(),
            "tenant_notifications"
        );
        assert_eq!(
            parsed.branched_by.branch().map(|branch| branch.as_str()),
            Some("tenant_user")
        );
        assert_eq!(parsed.mode, AckMode::Attached);
    }

    #[test]
    fn parses_branched_by() {
        let tokens = to_tokens(
            "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications BRANCHED BY \
             tenant_branch VALUES { tenant = tenant_notifications.tenant } FLUSH EACH 100ms MAX \
             BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_reingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.branched_by.branch().map(|branch| branch.as_str()),
            Some("tenant_branch")
        );
    }

    #[test]
    fn parses_create_reingestor_unbranched() {
        let tokens = to_tokens(
            "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications UNBRANCHED \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_reingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.branched_by,
            nervix_models::BranchInitiatorSelection::unbranched()
        );
    }

    #[test]
    fn parses_branched_by_with_values_block() {
        let input = "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications \
                     BRANCHED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } \
                     FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;";

        let parsed =
            parse_create_reingestor_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(
            parsed.branched_by.branch().map(|branch| branch.as_str()),
            Some("tenant_branch")
        );
        assert_eq!(parsed.branched_by.values().len(), 1);
    }

    #[test]
    fn rejects_bare_by() {
        let input = "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications BY \
                     tenant_branch FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;";

        parse_create_reingestor_tokens(&to_tokens(input))
            .expect_err("bare BY is not a branch selection mode");
    }

    #[test]
    fn rejects_unbranched_with_ttl() {
        let input = "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications \
                     UNBRANCHED TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;";

        parse_create_reingestor_tokens(&to_tokens(input))
            .expect_err("TTL must not follow UNBRANCHED");
    }

    #[test]
    fn parses_create_reingestor_detached() {
        let tokens = to_tokens(
            "CREATE DETACHED REINGESTOR repartition FROM notifications TO tenant_notifications \
             BRANCHED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_reingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn parses_reingestor_flush_each() {
        let tokens = to_tokens(
            "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications BRANCHED BY \
             tenant_branch VALUES { tenant = tenant_notifications.tenant } FLUSH EACH 100ms MAX \
             BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_reingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "100ms");
    }

    #[test]
    fn suggests_mode_after_create() {
        let input = "CREATE ";
        let suggestions = suggest_create_reingestor(input, input.len());
        assert!(suggestions.contains(&"ATTACHED".to_string()));
        assert!(suggestions.contains(&"DETACHED".to_string()));
        assert!(!suggestions.contains(&"FROM".to_string()));
    }

    #[test]
    fn suggests_branched_by_as_compound_keyword() {
        let input = "CREATE REINGESTOR r FROM a TO b BR";
        let suggestions = suggest_create_reingestor(input, input.len());
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
    }

    #[test]
    fn suggests_unbranched_as_branch_selection_choice() {
        let input = "CREATE REINGESTOR r FROM a TO b ";
        let suggestions = suggest_create_reingestor(input, input.len());
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
    }

    #[test]
    fn suggests_values_after_branched_by() {
        let input = "CREATE REINGESTOR r FROM input TO output BRANCHED BY tenant_branch ";
        let suggestions = suggest_create_reingestor(input, input.len());
        assert!(suggestions.contains(&"VALUES".to_string()));
        assert!(!suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"TTL".to_string()));
    }

    #[test]
    fn suggests_stream_after_to() {
        let input = "CREATE REINGESTOR r FROM input TO ";
        let suggestions = suggest_create_reingestor(input, input.len());
        assert!(suggestions.contains(&"ref:relay".to_string()));
    }

    #[test]
    fn suggests_flush_after_branched_by() {
        let input = "CREATE REINGESTOR r FROM input TO output BRANCHED BY tenant_branch VALUES { \
                     tenant = output.tenant } FL";
        let suggestions = suggest_create_reingestor(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
    }
}
