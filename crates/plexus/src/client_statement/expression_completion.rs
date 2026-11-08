//! Semantic offers for the expression positions the composed client grammar expects.
//!
//! Layer: language.
//! - **Owns.** Resolving expression expectations through the statement's typed entity head.
//! - **Depends on.** Shared head grammar, expression labels and vocabulary declarations.
//! - **Must not know.** Registries, sessions, connector drivers or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        language,
        reason = "owns the semantic completion boundary declared above"
    )
)]

use nervix_models::IngestorMetadataType;

use super::{
    BuiltinFunctionScope, CompletionExpectation, IngestorEntityKind, IngestorReferenceKind,
    Keyword, ModelKind, SemanticReference, Token, Word, emitter_connector_kind,
    ingestor_source_kind, junction_input_relay, junction_output_relay,
};

/// One immutable statement prefix, with the ingestor head its composed grammar recognizes.
pub(super) struct ExpressionCompletion<'a> {
    tokens: &'a [Token],
    ingestor: Option<IngestorEntityKind>,
}

impl<'a> ExpressionCompletion<'a> {
    pub(super) fn new(tokens: &'a [Token]) -> Self {
        Self {
            tokens,
            ingestor: ingestor_source_kind(tokens),
        }
    }

    pub(super) fn ingestor_reference(&self) -> Option<IngestorReferenceKind> {
        self.ingestor.map(|kind| kind.capabilities().reference)
    }

    /// `None` means the label belongs to another grammar concept. A recognized expression
    /// position may have no candidate, such as metadata on an entity that declares none.
    pub(super) fn expectations(&self, label: &str) -> Option<Vec<CompletionExpectation>> {
        let expectations = match label {
            "ref:expression_function" => self.functions(),
            "ref:expression_invocation" | "invocations" => self.invocations(),
            "ref:expression_field" => self.fields(),
            "ref:expression_target" | "set_assignments" => self.targets(),
            "ref:expression_udf" => vec![CompletionExpectation::Semantic(
                SemanticReference::Model(ModelKind::Udf),
            )],
            "ref:ingestor_timestamp_metadata" => {
                self.metadata_fields(Some(IngestorMetadataType::Datetime))
            }
            _ if label.ends_with("_expression") => self.functions(),
            _ => return None,
        };
        Some(expectations)
    }

    fn functions(&self) -> Vec<CompletionExpectation> {
        let scope = match self.ingestor {
            Some(kind) => BuiltinFunctionScope::IngestorEntity(kind),
            None => BuiltinFunctionScope::Ordinary,
        };
        vec![CompletionExpectation::Semantic(
            SemanticReference::BuiltinFunction(scope),
        )]
    }

    fn invocations(&self) -> Vec<CompletionExpectation> {
        let Some(kind) = emitter_connector_kind(self.tokens) else {
            return self.functions();
        };
        vec![CompletionExpectation::Semantic(
            SemanticReference::BuiltinFunction(BuiltinFunctionScope::EmitterInvocation(kind)),
        )]
    }

    fn fields(&self) -> Vec<CompletionExpectation> {
        match self.tokens.last_chunk::<2>() {
            Some(
                [
                    Token::Word(Word::KnownWord {
                        iden: Keyword::Metadata,
                        ..
                    }),
                    Token::Dot,
                ],
            ) => self.metadata_fields(None),
            Some(
                [
                    Token::Word(Word::KnownWord {
                        iden: Keyword::Payload,
                        ..
                    }),
                    Token::Dot,
                ],
            ) => self.payload_fields(),
            Some(
                [
                    Token::Word(Word::KnownWord {
                        iden: Keyword::Input,
                        ..
                    }),
                    Token::Dot,
                ],
            ) => {
                let Some(relay) = junction_input_relay(self.tokens) else {
                    return Vec::new();
                };
                vec![CompletionExpectation::Semantic(
                    SemanticReference::RelayField(relay),
                )]
            }
            _ => Vec::new(),
        }
    }

    fn targets(&self) -> Vec<CompletionExpectation> {
        if junction_input_relay(self.tokens).is_none() {
            return Vec::new();
        }
        let Some(relay) = junction_output_relay(self.tokens) else {
            return Vec::new();
        };
        vec![CompletionExpectation::Semantic(
            SemanticReference::RelayField(relay),
        )]
    }

    fn payload_fields(&self) -> Vec<CompletionExpectation> {
        self.ingestor
            .map(|kind| {
                kind.capabilities()
                    .payload
                    .fields()
                    .iter()
                    .map(|field| CompletionExpectation::Literal(field.name().to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn metadata_fields(
        &self,
        expected_type: Option<IngestorMetadataType>,
    ) -> Vec<CompletionExpectation> {
        let Some(kind) = self.ingestor else {
            return Vec::new();
        };
        let mut expectations = Vec::new();
        for field in kind.capabilities().metadata.fields() {
            if let Some(expected_type) = expected_type
                && field.ty() != expected_type
            {
                continue;
            }
            expectations.push(CompletionExpectation::Literal(field.name().to_string()));
        }
        expectations
    }
}
