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
    CodecEncoding, CodecEncodingRule, CodecWireFormat, CreateCodec, CreateStatement,
    RequestedResourceVersion, SchemaName,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, codec_name, field_ref, if_not_exists_clause,
        into_parse_error, kw, lex_input, suggest_from, tok,
    },
};

/// The part of `CREATE CODEC` that follows `FROM`: the wire format the codec reads together with
/// the wire schema that format needs, the Nervix schema it decodes into, and the per-field
/// encoding rules.
struct CodecBody {
    wire_format: CodecWireFormat<RequestedResourceVersion>,
    schema: SchemaName,
    encoding_rules: Vec<CodecEncodingRule>,
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
    let encoding_rules = kw(Identifier::Encode)
        .ignore_then(
            encoding_rule
                .separated_by(tok(Token::Comma))
                .allow_trailing()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .or_not()
        .map(Option::unwrap_or_default)
        .boxed();

    encoding_rules
}

pub fn create_codec_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateCodec<RequestedResourceVersion>>,
    extra::Err<ParseError<'src>>,
> + Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Codec))
        .then(codec_name())
        .then_ignore(kw(Identifier::From))
        .boxed()
        .then(choice((wire::body(), protobuf::body(), jaq::body(), syslog::body())).boxed())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|((if_not_exists, name), body)| {
            CreateStatement::new(
                CreateCodec {
                    name,
                    wire_format: body.wire_format,
                    schema: body.schema,
                    encoding_rules: body.encoding_rules,
                },
                if_not_exists,
            )
        })
        .boxed()
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
