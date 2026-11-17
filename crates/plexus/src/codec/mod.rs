//! Codec grammar in the language layer.
//!
//! - **Owns.** The shared codec target schema, field encodings and composed family grammar.
//! - **Depends on.** Shared Plexus grammar primitives and codec vocabulary.
//! - **Must not know.** Registry validation, codec execution or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns codec declaration grammar")
)]

mod jaq;
mod protobuf;
mod syslog;
mod wire;

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    CodecEncoding, CodecEncodingRule, CreateCodec, CreateStatement, RequestedResourceVersion,
    SchemaName,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, field_ref, if_not_exists_clause,
        into_parse_error, kw, lex_input, schema_ref, suggest_from, tok,
    },
};

fn target_schema<'src>()
-> impl Parser<'src, &'src [Token], SchemaName, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(schema_ref())
}

fn encoding_rules<'src>()
-> impl Parser<'src, &'src [Token], Vec<CodecEncodingRule>, extra::Err<ParseError<'src>>> + Clone {
    let encoding_rule = field_ref()
        .then_ignore(kw(Identifier::As))
        .then(choice((
            kw(Identifier::Rfc3339).to(CodecEncoding::Rfc3339),
            kw(Identifier::Base64).to(CodecEncoding::Base64),
        )))
        .map(|(field, encoding)| CodecEncodingRule { field, encoding })
        .boxed();
    kw(Identifier::Encode)
        .ignore_then(
            encoding_rule
                .separated_by(tok(Token::Comma))
                .allow_trailing()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .or_not()
        .map(Option::unwrap_or_default)
        .boxed()
}

pub fn create_codec_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateCodec<RequestedResourceVersion>>,
    extra::Err<ParseError<'src>>,
> + Clone {
    crate::labels::finish_creation({
        kw(Identifier::Create)
            .ignore_then(if_not_exists_clause())
            .then(
                choice((
                    wire::create(),
                    protobuf::create(),
                    jaq::create(),
                    syslog::create(),
                ))
                .boxed(),
            )
            .map(|(if_not_exists, codec)| CreateStatement::new(codec, if_not_exists))
            .boxed()
    })
}

pub fn parse_create_codec_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateCodec<RequestedResourceVersion>>, Vec<ParseError<'_>>> {
    let out = create_codec_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_create_codec(
    input: &str,
) -> error_stack::Result<CreateStatement<CreateCodec<RequestedResourceVersion>>, ParseFromSourceError>
{
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_create_codec_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_codec(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_codec_parser())
}

#[cfg(test)]
mod tests;
