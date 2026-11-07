//! Codec grammar in the language layer.
//!
//! - **Owns.** Protobuf resource, message and transformation codec bodies.
//! - **Depends on.** Shared Plexus grammar primitives and codec vocabulary.
//! - **Must not know.** Registry validation, codec execution or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns codec declaration grammar")
)]

use chumsky::prelude::*;
use nervix_models::{CodecProtobufConfig, CodecWireFormat};

use super::{CodecBody, encoding_rules, jaq::transformations};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, config_entries_block, kw, kw_phrase2, resource_ref, resource_version_clause,
        schema_ref, string_lit,
    },
};

pub(super) fn body<'src>()
-> impl Parser<'src, &'src [Token], CodecBody, extra::Err<ParseError<'src>>> + Clone {
    let protobuf_codec = kw(Identifier::Protobuf)
        .ignore_then(kw(Identifier::Using))
        .ignore_then(kw(Identifier::Resource))
        .ignore_then(resource_ref())
        .then(resource_version_clause())
        .then(config_entries_block())
        .boxed()
        .then_ignore(kw(Identifier::Message))
        .then(string_lit())
        .then(
            kw_phrase2(Identifier::Batch, Identifier::Message)
                .ignore_then(string_lit())
                .or_not(),
        )
        .then_ignore(kw(Identifier::To))
        .then_ignore(kw(Identifier::Schema))
        .then(schema_ref())
        .boxed()
        .then(transformations())
        .then(encoding_rules())
        .map(
            |(
                (
                    (((((resource, resource_version), config), message), batch_message), schema),
                    transformations,
                ),
                encoding_rules,
            )| {
                CodecBody {
                    wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                        resource,
                        resource_version,
                        config,
                        message,
                        batch_message,
                        transformations,
                    }),
                    schema,
                    encoding_rules,
                }
            },
        )
        .boxed();
    protobuf_codec
}
