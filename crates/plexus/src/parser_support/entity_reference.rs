//! Shared typed references to domain Models.
//!
//! Layer: language.
//! - **Owns.** The composed kind-and-name grammar for an existing domain entity.
//! - **Depends on.** Shared language tokens, typed reference parsers and vocabulary identities.
//! - **Must not know.** Registry state, runtime execution or statement-specific operations.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{ModelKind, NodeRef};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, boxed_choice, branch_ref, client_ref, codec_ref, correlator_ref,
        deduplicator_ref, emitter_ref, endpoint_ref, generator_ref, inferencer_ref, ingestor_ref,
        junction_ref, kw, kw_phrase2, kw_phrase3, lookup_ref, placement_ref, reingestor_ref,
        relay_ref, reorderer_ref, schema_ref, signaling_protocol_ref, udf_ref, vhost_ref,
        wasm_processor_ref, window_processor_ref, wire_avro_schema_ref, wire_cbor_schema_ref,
        wire_json_schema_ref,
    },
};

pub(crate) fn model_ref<'src>()
-> impl Parser<'src, &'src [Token], NodeRef, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        kw(Identifier::Schema)
            .ignore_then(schema_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Schema,
                identifier: name.into(),
            }),
        kw(Identifier::Branch)
            .ignore_then(branch_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Branch,
                identifier: name.into(),
            }),
        kw_phrase3(Identifier::Wire, Identifier::Json, Identifier::Schema)
            .ignore_then(wire_json_schema_ref())
            .map(|name| NodeRef {
                kind: ModelKind::WireJsonSchema,
                identifier: name.into(),
            }),
        kw_phrase3(Identifier::Wire, Identifier::Cbor, Identifier::Schema)
            .ignore_then(wire_cbor_schema_ref())
            .map(|name| NodeRef {
                kind: ModelKind::WireCborSchema,
                identifier: name.into(),
            }),
        kw_phrase3(Identifier::Wire, Identifier::Avro, Identifier::Schema)
            .ignore_then(wire_avro_schema_ref())
            .map(|name| NodeRef {
                kind: ModelKind::WireAvroSchema,
                identifier: name.into(),
            }),
        kw(Identifier::Codec)
            .ignore_then(codec_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Codec,
                identifier: name.into(),
            }),
        kw(Identifier::Client)
            .ignore_then(client_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Client,
                identifier: name.into(),
            }),
        kw(Identifier::Vhost)
            .ignore_then(vhost_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Vhost,
                identifier: name.into(),
            }),
        kw(Identifier::Endpoint)
            .ignore_then(endpoint_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Endpoint,
                identifier: name.into(),
            }),
        kw_phrase2(Identifier::Signaling, Identifier::Protocol)
            .ignore_then(signaling_protocol_ref())
            .map(|name| NodeRef {
                kind: ModelKind::SignalingProtocol,
                identifier: name.into(),
            }),
        kw(Identifier::Generator)
            .ignore_then(generator_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Generator,
                identifier: name.into(),
            }),
        kw(Identifier::Ingestor)
            .ignore_then(ingestor_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Ingestor,
                identifier: name.into(),
            }),
        kw(Identifier::Reingestor)
            .ignore_then(reingestor_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Reingestor,
                identifier: name.into(),
            }),
        kw(Identifier::Reorderer)
            .ignore_then(reorderer_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Reorderer,
                identifier: name.into(),
            }),
        kw(Identifier::Inferencer)
            .ignore_then(inferencer_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Inferencer,
                identifier: name.into(),
            }),
        kw_phrase2(Identifier::Wasm, Identifier::Processor)
            .ignore_then(wasm_processor_ref())
            .map(|name| NodeRef {
                kind: ModelKind::WasmProcessor,
                identifier: name.into(),
            }),
        kw(Identifier::Relay)
            .ignore_then(relay_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Relay,
                identifier: name.into(),
            }),
        kw_phrase2(Identifier::Hash, Identifier::Map)
            .ignore_then(lookup_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Lookup,
                identifier: name.into(),
            }),
        kw(Identifier::Junction)
            .ignore_then(junction_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Junction,
                identifier: name.into(),
            }),
        kw(Identifier::Deduplicator)
            .ignore_then(deduplicator_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Deduplicator,
                identifier: name.into(),
            }),
        kw(Identifier::Correlator)
            .ignore_then(correlator_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Correlator,
                identifier: name.into(),
            }),
        kw_phrase2(Identifier::Window, Identifier::Processor)
            .ignore_then(window_processor_ref())
            .map(|name| NodeRef {
                kind: ModelKind::WindowProcessor,
                identifier: name.into(),
            }),
        kw(Identifier::Emitter)
            .ignore_then(emitter_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Emitter,
                identifier: name.into(),
            }),
        kw(Identifier::Placement)
            .ignore_then(placement_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Placement,
                identifier: name.into(),
            }),
        kw(Identifier::Udf)
            .ignore_then(udf_ref())
            .map(|name| NodeRef {
                kind: ModelKind::Udf,
                identifier: name.into(),
            }),
    )
}
