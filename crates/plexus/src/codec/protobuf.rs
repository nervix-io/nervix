//! Protobuf codec declaration grammar in the language layer.
//!
//! - **Owns.** The Protobuf codec head, resource binding, messages and transformations.
//! - **Depends on.** Shared Plexus grammar primitives and codec vocabulary.
//! - **Must not know.** Resource installation, Protobuf compilation or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns protobuf codec declaration grammar")
)]

use chumsky::prelude::*;
use nervix_models::{CodecProtobufConfig, CodecWireFormat, CreateCodec, RequestedResourceVersion};

use super::{encoding_rules, jaq::transformations, target_schema};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, codec_name, config_entries_block, kw, kw_phrase2, resource_ref,
        resource_version_clause, string_lit,
    },
};

pub(super) fn create<'src>()
-> impl Parser<'src, &'src [Token], CreateCodec<RequestedResourceVersion>, extra::Err<ParseError<'src>>>
+ Clone {
    kw_phrase2(Identifier::Protobuf, Identifier::Codec)
        .ignore_then(codec_name())
        .then_ignore(kw(Identifier::Using))
        .then_ignore(kw(Identifier::Resource))
        .then(resource_ref())
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
        .then(target_schema())
        .boxed()
        .then(transformations())
        .then(encoding_rules())
        .map(
            |(
                (
                    (
                        (((((name, resource), resource_version), config), message), batch_message),
                        schema,
                    ),
                    transformations,
                ),
                encoding_rules,
            )| CreateCodec {
                name,
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
            },
        )
        .boxed()
}
