//! Syslog codec declaration grammar in the language layer.
//!
//! - **Owns.** The Syslog codec head and its predefined field contract's target schema.
//! - **Depends on.** Shared Plexus grammar primitives and codec vocabulary.
//! - **Must not know.** Registry validation, codec execution or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns syslog codec declaration grammar")
)]

use chumsky::prelude::*;
use nervix_models::{CodecWireFormat, CreateCodec, RequestedResourceVersion};

use super::target_schema;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, codec_name, kw_phrase2},
};

pub(super) fn create<'src>()
-> impl Parser<'src, &'src [Token], CreateCodec<RequestedResourceVersion>, extra::Err<ParseError<'src>>>
+ Clone {
    kw_phrase2(Identifier::Syslog, Identifier::Codec)
        .ignore_then(codec_name())
        .then(target_schema())
        .map(|(name, schema)| CreateCodec {
            labels: Default::default(),
            name,
            wire_format: CodecWireFormat::Syslog,
            schema,
            encoding_rules: Vec::new(),
        })
        .boxed()
}
