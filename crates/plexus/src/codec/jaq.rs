//! Codec grammar in the language layer.
//!
//! - **Owns.** JAQ format heads, their codec bodies and directed transformations.
//! - **Depends on.** Shared Plexus grammar primitives and codec vocabulary.
//! - **Must not know.** Registry validation, codec execution or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns codec declaration grammar")
)]

use chumsky::prelude::*;
use nervix_models::{
    CodecJaqFormat, CodecJaqTransformations, CodecWireFormat, CreateCodec, RequestedResourceVersion,
};

use super::{encoding_rules, target_schema};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, codec_name, kw, kw_phrase3, string_lit},
};

/// `ON EMITTING` together with the `ON EMITTING BATCH` program that may follow it.
struct EmittingTransformations {
    on_emitting: String,
    on_emitting_batch: Option<String>,
}

impl EmittingTransformations {
    fn into_transformations(
        on_ingestion: Option<String>,
        emitting: Option<Self>,
    ) -> CodecJaqTransformations {
        let Some(emitting) = emitting else {
            return CodecJaqTransformations {
                on_ingestion,
                on_emitting: None,
                on_emitting_batch: None,
            };
        };
        CodecJaqTransformations {
            on_ingestion,
            on_emitting: Some(emitting.on_emitting),
            on_emitting_batch: emitting.on_emitting_batch,
        }
    }
}

pub(super) fn transformations<'src>()
-> impl Parser<'src, &'src [Token], CodecJaqTransformations, extra::Err<ParseError<'src>>> + Clone {
    // `ON EMITTING BATCH` builds its input from the `ON EMITTING` outputs, so it is only
    // reachable after that transformation.
    let emitting_batch_transformation = kw(Identifier::On)
        .ignore_then(kw(Identifier::Emitting))
        .ignore_then(kw(Identifier::Batch))
        .ignore_then(string_lit())
        .boxed();
    let emitting_transformations = kw(Identifier::On)
        .ignore_then(kw(Identifier::Emitting))
        .ignore_then(string_lit())
        .then(emitting_batch_transformation.or_not())
        .map(|(on_emitting, on_emitting_batch)| EmittingTransformations {
            on_emitting,
            on_emitting_batch,
        })
        .boxed();
    let ingestion_transformations = kw(Identifier::On)
        .ignore_then(kw(Identifier::Ingestion))
        .ignore_then(string_lit())
        .then(emitting_transformations.clone().or_not())
        .map(|(on_ingestion, emitting)| {
            EmittingTransformations::into_transformations(Some(on_ingestion), emitting)
        });
    let emitting_only_transformations = emitting_transformations
        .map(|emitting| EmittingTransformations::into_transformations(None, Some(emitting)));
    let directed_jaq_transformations =
        choice((ingestion_transformations, emitting_only_transformations)).boxed();
    kw(Identifier::With)
        .ignore_then(kw(Identifier::Jaq))
        .ignore_then(kw(Identifier::Transformations))
        .ignore_then(directed_jaq_transformations)
        .boxed()
}

pub(super) fn create<'src>()
-> impl Parser<'src, &'src [Token], CreateCodec<RequestedResourceVersion>, extra::Err<ParseError<'src>>>
+ Clone {
    let head = choice((
        kw_phrase3(Identifier::Jaq, Identifier::Json, Identifier::Codec).to(CodecJaqFormat::Json),
        kw_phrase3(Identifier::Jaq, Identifier::Yaml, Identifier::Codec).to(CodecJaqFormat::Yaml),
        kw_phrase3(Identifier::Jaq, Identifier::Toml, Identifier::Codec).to(CodecJaqFormat::Toml),
        kw_phrase3(Identifier::Jaq, Identifier::Xml, Identifier::Codec).to(CodecJaqFormat::Xml),
        kw_phrase3(Identifier::Jaq, Identifier::Cbor, Identifier::Codec).to(CodecJaqFormat::Cbor),
    ))
    .boxed();
    head.then(codec_name())
        .then(target_schema())
        .then(transformations())
        .boxed()
        .then(encoding_rules())
        .map(
            |((((format, name), schema), transformations), encoding_rules)| CreateCodec {
                labels: Default::default(),
                name,
                wire_format: CodecWireFormat::JaqNative {
                    format,
                    transformations,
                },
                schema,
                encoding_rules,
            },
        )
        .boxed()
}
