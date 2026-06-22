use chumsky::prelude::*;
use nervix_models::UploadResource;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, into_parse_error, kw, lex_input,
        resource_ref, string_lit, suggestions_from_errors,
    },
};

pub fn upload_resource_parser<'src>()
-> impl Parser<'src, &'src [Token], UploadResource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Upload)
        .ignore_then(kw(Identifier::Resource))
        .ignore_then(resource_ref())
        .then_ignore(kw(Identifier::Version))
        .then(string_lit())
        .map(|(identifier, source_path)| UploadResource {
            identifier,
            source_path,
        })
        .then_ignore(crate::parser_support::tok(Token::Semicolon).or_not())
}

pub fn parse_upload_resource_tokens(
    tokens: &[Token],
) -> Result<UploadResource, Vec<ParseError<'_>>> {
    let out = upload_resource_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_upload_resource(input: &str) -> Result<UploadResource, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_upload_resource_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_upload_resource(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = upload_resource_parser()
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
    fn parses_upload_resource() {
        let tokens = to_tokens("UPLOAD RESOURCE fraud_model VERSION '/tmp/model';");
        let parsed = parse_upload_resource_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.identifier.as_str(), "fraud_model");
        assert_eq!(parsed.source_path, "/tmp/model");
    }

    #[test]
    fn rejects_missing_path_string_literal() {
        let err = parse_upload_resource("UPLOAD RESOURCE fraud_model VERSION /tmp/model;")
            .expect_err("parse should fail");
        match err {
            ParseFromSourceError::Parse { diagnostics, .. }
            | ParseFromSourceError::Lex { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
        }
    }

    #[test]
    fn suggests_resource_keyword_after_upload() {
        let input = "UPLOAD ";
        let suggestions = suggest_upload_resource(input, input.len());
        assert!(suggestions.contains(&"RESOURCE".to_string()));
    }

    #[test]
    fn suggests_version_after_resource_identifier() {
        let input = "UPLOAD RESOURCE fraud_model ";
        let suggestions = suggest_upload_resource(input, input.len());
        assert!(suggestions.contains(&"VERSION".to_string()));
    }
}
