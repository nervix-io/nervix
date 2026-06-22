use chumsky::prelude::*;
use nervix_models::{CreateGenerator, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, branch_parameterization, current_word_prefix, flush_each,
        generator_name, if_not_exists_clause, into_parse_error, kw, lex_input,
        message_error_policy, relay_ref, set_only_program, suggestions_from_errors, tok,
    },
};

pub fn create_generator_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateGenerator>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Generator))
        .then(generator_name())
        .then_ignore(kw(Identifier::To))
        .then(relay_ref())
        .then(branch_parameterization())
        .then_ignore(kw(Identifier::Each))
        .then(crate::parser_support::duration_lit())
        .then(flush_each())
        .then(set_only_program())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (((((if_not_exists, name), into_relay), parameterized_by), each), flush_each),
                    set,
                ),
                message_error_policy,
            )| {
                let (flush_each, max_batch_size) = flush_each;
                CreateStatement::new(
                    CreateGenerator {
                        name,
                        into_relay,
                        parameterized_by,
                        each,
                        flush_each,
                        max_batch_size,
                        set,
                        message_error_policy,
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
                TO alerts
                PARAMETERIZED BY tenant
                EACH 100ms
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                SET alerts.user_id = notifications.user_id, alerts.topic = notifications.topic ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "synth");
        assert_eq!(parsed.into_relay.as_str(), "alerts");
        assert_eq!(parsed.each, "100ms");
        assert_eq!(parsed.flush_each, "100ms");
        assert_eq!(
            parsed.set,
            "SET alerts.user_id = notifications.user_id , alerts.topic = notifications.topic"
        );
    }

    #[test]
    fn parses_create_generator_with_flush_each() {
        let input = r#"
            CREATE GENERATOR synth
                TO alerts
                PARAMETERIZED BY tenant
                EACH 100ms
                FLUSH EACH 1s MAX BATCH SIZE 1MiB
                SET alerts.user_id = notifications.user_id ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.flush_each, "1s");
    }

    #[test]
    fn parses_create_generator_with_flush_immediate() {
        let input = r#"
            CREATE GENERATOR synth
                TO alerts
                PARAMETERIZED BY tenant
                EACH 100ms
                FLUSH IMMEDIATE
                SET alerts.user_id = notifications.user_id ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.flush_each, "IMMEDIATE");
    }

    #[test]
    fn parses_create_generator_unparameterized() {
        let input = r#"
            CREATE GENERATOR synth
                TO alerts
                UNPARAMETERIZED
                EACH 100ms
                FLUSH IMMEDIATE
                SET alerts.user_id = notifications.user_id ON MESSAGE ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_generator_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.parameterized_by,
            nervix_models::BranchParameterization::unparameterized()
        );
    }

    #[test]
    fn rejects_generator_without_flush_each() {
        let tokens = to_tokens(
            "CREATE GENERATOR synth TO alerts PARAMETERIZED BY tenant EACH 100ms SET \
             alerts.user_id = notifications.user_id ON MESSAGE ERROR LOG;",
        );
        assert!(parse_create_generator_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_generator_where_clause() {
        let tokens = to_tokens(
            "CREATE GENERATOR synth TO alerts PARAMETERIZED BY tenant EACH 100ms WHERE \
             alerts.keep ON MESSAGE ERROR LOG;",
        );
        assert!(parse_create_generator_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_to_after_generator_name_without_cross_branch_leakage() {
        let input = "CREATE GENERATOR synth ";
        let suggestions = suggest_create_generator(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_flush_or_set_after_each_duration() {
        let input = "CREATE GENERATOR synth TO alerts PARAMETERIZED BY tenant EACH 100ms ";
        let suggestions = suggest_create_generator(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
    }

    #[test]
    fn suggests_parameterization_after_generator_relay_without_cross_branch_leakage() {
        let input = "CREATE GENERATOR synth TO alerts ";
        let suggestions = suggest_create_generator(input, input.len());
        assert!(suggestions.contains(&"PARAMETERIZED BY".to_string()));
        assert!(suggestions.contains(&"UNPARAMETERIZED".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }
}
