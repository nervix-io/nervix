//! Semantic questions shared by text completion and typed choice controls.
//!
//! Layer: vocabulary.
//! - **Owns.** The kind of entity or value a completion candidate denotes.
//! - **Depends on.** Model identity vocabulary.
//! - **Must not know.** Parser labels, source spans, registries, sessions or presentation.

use crate::{EmitSinkKind, IngestSourceKind, ModelKind, RelayName, SchemaName, WireSchemaName};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinFunctionScope {
    Ordinary,
    IngestSource(IngestSourceKind),
    EmitterInvocation(EmitSinkKind),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SemanticReference {
    Model(ModelKind),
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
