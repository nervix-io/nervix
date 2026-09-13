//! Why the registry refused a change.
//!
//! Layer: decisions.
//!
//! - **Owns.** The one error every registry decision returns, and the node, route and field each
//!   variant names.
//! - **Depends on.** The vocabulary the variants quote.
//! - **Must not know.** How a caller reports or recovers from a refusal.

use thiserror::Error;
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RegistryError {
    #[error("failed to open registry storage")]
    OpenStorage,
    #[cfg(test)]
    #[error("failed to open database")]
    OpenDatabase,
    #[error("failed to open keyspace")]
    OpenKeyspace,
    #[error("failed to load stored models")]
    LoadStoredModels,
    #[error("failed to encode key")]
    EncodeKey,
    #[error("failed to serialize model")]
    SerializeValue,
    #[error("failed to write model")]
    WriteValue,
    #[error("failed to read model")]
    ReadValue,
    #[error("failed to deserialize model")]
    DeserializeValue,
    #[error("failed to decode key")]
    DecodeKey,
    #[error("failed to persist model batch")]
    PersistBatch,
    #[error("model '{identifier}' already exists in domain '{domain}'")]
    AlreadyExists { domain: String, identifier: String },
    #[error("domain '{domain}' changed after the mutation batch was planned")]
    ConcurrentMutation { domain: String },
    #[error("model '{identifier}' does not exist in domain '{domain}'")]
    NotFound { domain: String, identifier: String },
    /// Stored bytes that decode to a model of a kind other than the one their key names.
    ///
    /// The registry writes every model under the key the model itself reports, so this is corrupt
    /// storage rather than configuration a domain can hold. It is reported instead of read so the
    /// stored shape is recreated rather than reinterpreted.
    #[error(
        "model '{identifier}' in domain '{domain}' is stored under kind {expected_kind} but \
         decodes as {stored_kind}"
    )]
    StoredModelKindMismatch {
        domain: String,
        identifier: String,
        expected_kind: &'static str,
        stored_kind: &'static str,
    },
    #[error(
        "model '{identifier}' in domain '{domain}' requires missing {expected_kind} '{reference}'"
    )]
    MissingReference {
        domain: String,
        identifier: String,
        expected_kind: &'static str,
        reference: String,
    },
    #[error("active configuration graph for domain '{domain}' contains a cycle")]
    ConfigurationCycle { domain: String },
    #[error(
        "placement rules '{left_rule}' and '{right_rule}' in domain '{domain}' conflict at equal \
         rank for runtime nodes {left_kind} '{left_identifier}' and {right_kind} \
         '{right_identifier}'"
    )]
    PlacementConflict {
        domain: String,
        left_rule: String,
        right_rule: String,
        left_kind: &'static str,
        left_identifier: String,
        right_kind: &'static str,
        right_identifier: String,
    },
    #[error(
        "model '{identifier}' in domain '{domain}' has incompatible schema relationship: {reason}"
    )]
    IncompatibleSchema {
        domain: String,
        identifier: String,
        reason: String,
    },
    #[error("model '{identifier}' in domain '{domain}' is invalid: {reason}")]
    InvalidModel {
        domain: String,
        identifier: String,
        reason: String,
    },
    #[error(
        "cannot delete model '{identifier}' in domain '{domain}' because it is used by {blockers}"
    )]
    DeleteInUse {
        domain: String,
        identifier: String,
        blockers: String,
    },
    #[error(
        "cannot alter model '{identifier}' in domain '{domain}' into a non-placement-eligible \
         shape because it is pinned by placements {placements}"
    )]
    PlacementMemberPinned {
        domain: String,
        identifier: String,
        placements: String,
    },
}
