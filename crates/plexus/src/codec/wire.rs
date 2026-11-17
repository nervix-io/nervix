//! Wire-schema codec declaration grammar in the language layer.
//!
//! - **Owns.** JSON, CBOR and AVRO codec heads and their exact wire-schema references.
//! - **Depends on.** Shared Plexus grammar primitives and codec vocabulary.
//! - **Must not know.** Registry validation, codec execution or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns wire codec declaration grammar")
)]

use chumsky::prelude::*;
use nervix_models::{
    CodecName, CodecWireFormat, CreateCodec, RequestedResourceVersion, WireSchemaName,
};

use super::{encoding_rules, target_schema};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, codec_name, kw, kw_phrase3, wire_avro_schema_ref, wire_cbor_schema_ref,
        wire_json_schema_ref,
    },
};

struct NamedWireFormat {
    name: CodecName,
    wire_format: CodecWireFormat<RequestedResourceVersion>,
}

fn head_and_reference<'src>(
    format: Identifier,
    reference: impl Parser<'src, &'src [Token], WireSchemaName, extra::Err<ParseError<'src>>>
    + Clone
    + 'src,
    wire_format: impl Fn(WireSchemaName) -> CodecWireFormat<RequestedResourceVersion> + Clone + 'src,
) -> impl Parser<'src, &'src [Token], NamedWireFormat, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Identifier::Wire, format, Identifier::Codec)
        .ignore_then(codec_name())
        .then_ignore(kw(Identifier::From))
        .then_ignore(kw(Identifier::Wire))
        .then_ignore(kw(Identifier::Schema))
        .then(reference)
        .map(move |(name, wire_schema)| NamedWireFormat {
            name,
            wire_format: wire_format(wire_schema),
        })
        .boxed()
}

pub(super) fn create<'src>()
-> impl Parser<'src, &'src [Token], CreateCodec<RequestedResourceVersion>, extra::Err<ParseError<'src>>>
+ Clone {
    let json = head_and_reference(Identifier::Json, wire_json_schema_ref(), |wire_schema| {
        CodecWireFormat::Json { wire_schema }
    });
    let cbor = head_and_reference(Identifier::Cbor, wire_cbor_schema_ref(), |wire_schema| {
        CodecWireFormat::Cbor { wire_schema }
    });
    let avro = head_and_reference(Identifier::Avro, wire_avro_schema_ref(), |wire_schema| {
        CodecWireFormat::Avro { wire_schema }
    });
    choice((json, cbor, avro))
        .then(target_schema())
        .then(encoding_rules())
        .map(|((declared, schema), encoding_rules)| CreateCodec {
            labels: Default::default(),
            name: declared.name,
            wire_format: declared.wire_format,
            schema,
            encoding_rules,
        })
        .boxed()
}
