use chumsky::prelude::*;
use nervix_models::{CreateStatement, CreateUser};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, if_not_exists_clause, into_parse_error, kw, lex_input,
        string_lit, tok, user_name,
    },
};

pub fn create_user_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateUser>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::User))
        .then(user_name())
        .then_ignore(kw(Identifier::With))
        .then_ignore(kw(Identifier::Password))
        .then(string_lit())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|((if_not_exists, name), password)| {
            CreateStatement::new(CreateUser { name, password }, if_not_exists)
        })
}

pub fn parse_create_user(input: &str) -> Result<CreateStatement<CreateUser>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    let out = create_user_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if out.has_errors() {
        Err(into_parse_error(
            source,
            &spanned_tokens,
            input.len(),
            out.into_errors(),
        ))
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::Statement;

    use super::parse_create_user;
    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn parses_create_user_with_password() {
        let create = parse_create_user("CREATE USER my_username WITH PASSWORD 'secret';")
            .expect("CREATE USER should parse");

        assert_eq!(create.name.as_str(), "my_username");
        assert_eq!(create.password, "secret");
    }

    #[test]
    fn create_user_participates_in_top_level_parser() {
        let statement =
            parse_statement("CREATE USER app WITH PASSWORD 'pw';").expect("statement should parse");

        let Statement::CreateUser(create) = statement else {
            panic!("expected CREATE USER statement");
        };
        assert_eq!(create.name.as_str(), "app");
    }

    #[test]
    fn create_user_rejects_missing_password_literal() {
        let err = parse_create_user("CREATE USER app WITH PASSWORD;")
            .expect_err("password literal is required");
        let diagnostics = match err {
            crate::parser_support::ParseFromSourceError::Parse { diagnostics, .. } => diagnostics,
            crate::parser_support::ParseFromSourceError::Lex { .. } => {
                panic!("expected parse diagnostics")
            }
        };
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("string_literal")),
            "expected string_literal diagnostic, got {diagnostics:?}"
        );
    }

    #[test]
    fn create_user_completion_suggests_password_keyword() {
        assert!(
            suggest_statement("CREATE USER app WITH ", "CREATE USER app WITH ".len())
                .contains(&"PASSWORD".to_string())
        );
    }
}
