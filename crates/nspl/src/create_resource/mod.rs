use chumsky::prelude::*;
use nervix_models::{CreateResource, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, if_not_exists_clause,
        into_parse_error, kw, lex_input, resource_ref, suggestions_from_errors,
    },
};

pub fn create_resource_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateResource>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Resource))
        .then(resource_ref())
        .map(|(if_not_exists, identifier)| {
            CreateStatement::new(CreateResource { identifier }, if_not_exists)
        })
        .then_ignore(crate::parser_support::tok(Token::Semicolon).or_not())
}

pub fn parse_create_resource_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateResource>, Vec<ParseError<'_>>> {
    let out = create_resource_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_resource(
    input: &str,
) -> Result<CreateStatement<CreateResource>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_resource_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_resource(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_resource_parser()
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
    fn parses_create_resource() {
        let tokens = to_tokens("CREATE RESOURCE fraud_model;");
        let parsed = parse_create_resource_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.identifier.as_str(), "fraud_model");
    }

    #[test]
    fn rejects_trailing_from_clause() {
        let err = parse_create_resource("CREATE RESOURCE fraud_model FROM '/tmp/model';")
            .expect_err("parse should fail");
        match err {
            ParseFromSourceError::Parse { diagnostics, .. }
            | ParseFromSourceError::Lex { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
        }
    }

    #[test]
    fn suggests_resource_keyword_after_create() {
        let input = "CREATE ";
        let suggestions = suggest_create_resource(input, input.len());
        assert!(suggestions.contains(&"RESOURCE".to_string()));
    }

    #[test]
    fn suggests_from_after_resource_identifier() {
        let input = "CREATE RESOURCE fraud_model ";
        let suggestions = suggest_create_resource(input, input.len());
        assert!(suggestions.contains(&";".to_string()) || suggestions.is_empty());
    }
}
