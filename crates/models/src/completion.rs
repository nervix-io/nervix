//! Semantic questions shared by text completion and typed choice controls.
//!
//! Layer: vocabulary.
//! - **Owns.** The kind of entity or value a completion candidate denotes.
//! - **Depends on.** Model identity vocabulary.
//! - **Must not know.** Parser labels, source spans, registries, sessions or presentation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use crate::{
    EmitSinkKind, IngestSourceKind, ModelKind, RelayName, SchemaName, SourceReferenceKind,
    WireSchemaName,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinFunctionScope {
    Ordinary,
    IngestSource(IngestSourceKind),
    EmitterInvocation(EmitSinkKind),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SemanticReference {
    Model(ModelKind),
    /// The client or endpoint an ingestor's body reads through, which the pair its head names
    /// narrows to one connector or one endpoint type.
    SourceReference(SourceReferenceKind),
    SchemaField(SchemaName),
    WireSchemaField(ModelKind, WireSchemaName),
    RelayField(RelayName),
    BuiltinFunction(BuiltinFunctionScope),
    Resource,
    ResourceVersion,
    CompletedResourceVersion,
    SessionSubscription,
    RuntimeNode,
    Domain,
}
