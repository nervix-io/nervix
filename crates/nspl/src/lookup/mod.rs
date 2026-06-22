use chumsky::prelude::*;
use nervix_models::{CreateLookup, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, codec_ref, current_word_prefix, field_ref,
        if_not_exists_clause, into_parse_error, kw, kw_phrase2, lex_input, lookup_name,
        resource_ref, string_lit, suggestions_from_errors, tok,
    },
};

pub fn create_lookup_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateLookup>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw_phrase2(Identifier::Hash, Identifier::Map))
        .then(lookup_name())
        .then_ignore(kw(Identifier::Key))
        .then(field_ref())
        .then_ignore(kw(Identifier::From))
        .then_ignore(kw(Identifier::Resource))
        .then(resource_ref())
        .then_ignore(kw(Identifier::Path))
        .then(string_lit())
        .then_ignore(kw(Identifier::Decode))
        .then_ignore(kw(Identifier::Using))
        .then(codec_ref())
        .map(
            |(((((if_not_exists, name), key_field), resource), path), decode_using_codec)| {
                CreateStatement::new(
                    CreateLookup {
                        name,
                        key_field,
                        resource,
                        path,
                        decode_using_codec,
                    },
                    if_not_exists,
                )
            },
        )
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_create_lookup_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateLookup>, Vec<ParseError<'_>>> {
    let out = create_lookup_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_lookup(
    input: &str,
) -> Result<CreateStatement<CreateLookup>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_lookup_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_lookup(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_lookup_parser()
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
    fn parses_create_lookup_hash_map() {
        let tokens = to_tokens(
            "CREATE HASH MAP zip_codes KEY zip FROM RESOURCE zip_codes PATH 'lookup.jsonl' DECODE \
             USING zip_codec;",
        );
        let parsed = parse_create_lookup_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "zip_codes");
        assert_eq!(parsed.key_field.as_str(), "zip");
        assert_eq!(parsed.resource.as_str(), "zip_codes");
        assert_eq!(parsed.path, "lookup.jsonl");
        assert_eq!(parsed.decode_using_codec.as_str(), "zip_codec");
    }

    #[test]
    fn suggests_hash_map_after_create() {
        let input = "CREATE ";
        let suggestions = suggest_create_lookup(input, input.len());
        assert!(suggestions.contains(&"HASH MAP".to_string()));
    }

    #[test]
    fn suggests_resource_reference_after_from_resource() {
        let input = "CREATE HASH MAP zip_codes KEY zip FROM RESOURCE ";
        let suggestions = suggest_create_lookup(input, input.len());
        assert!(suggestions.contains(&"ref:resource".to_string()));
    }
}
