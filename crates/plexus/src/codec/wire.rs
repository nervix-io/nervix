//! Codec grammar in the language layer.
//!
//! - **Owns.** Declared JSON, CBOR and AVRO wire-schema codec bodies.
//! - **Depends on.** Shared Plexus grammar primitives and codec vocabulary.
//! - **Must not know.** Registry validation, codec execution or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns codec declaration grammar")
)]

use chumsky::prelude::*;
use nervix_models::CodecWireFormat;

use super::{CodecBody, encoding_rules};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, kw, schema_ref, wire_avro_schema_ref, wire_cbor_schema_ref,
        wire_json_schema_ref,
    },
};

pub(super) fn body<'src>()
-> impl Parser<'src, &'src [Token], CodecBody, extra::Err<ParseError<'src>>> + Clone {
    let json_wire = kw(Identifier::Json)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_json_schema_ref())
        .map(|wire_schema| CodecWireFormat::Json { wire_schema });
    let cbor_wire = kw(Identifier::Cbor)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_cbor_schema_ref())
        .map(|wire_schema| CodecWireFormat::Cbor { wire_schema });
    let avro_wire = kw(Identifier::Avro)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_avro_schema_ref())
        .map(|wire_schema| CodecWireFormat::Avro { wire_schema });
    let schemaful_codec = kw(Identifier::Wire)
        .ignore_then(choice((json_wire, cbor_wire, avro_wire)))
        .then_ignore(kw(Identifier::To))
        .then_ignore(kw(Identifier::Schema))
        .then(schema_ref())
        .boxed()
        .then(encoding_rules())
        .map(|((wire_format, schema), encoding_rules)| CodecBody {
            wire_format,
            schema,
            encoding_rules,
        })
        .boxed();
    schemaful_codec
}
