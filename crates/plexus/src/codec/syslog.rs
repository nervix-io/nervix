//! Codec grammar in the language layer.
//!
//! - **Owns.** The predefined Syslog codec body.
//! - **Depends on.** Shared Plexus grammar primitives and codec vocabulary.
//! - **Must not know.** Registry validation, codec execution or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns codec declaration grammar")
)]

use chumsky::prelude::*;
use nervix_models::CodecWireFormat;

use super::CodecBody;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, kw, schema_ref},
};

pub(super) fn body<'src>()
-> impl Parser<'src, &'src [Token], CodecBody, extra::Err<ParseError<'src>>> + Clone {
    let syslog_codec = kw(Identifier::Syslog)
        .ignore_then(kw(Identifier::To))
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(schema_ref())
        .map(|schema| CodecBody {
            wire_format: CodecWireFormat::Syslog,
            schema,
            encoding_rules: Vec::new(),
        })
        .boxed();

    syslog_codec
}
