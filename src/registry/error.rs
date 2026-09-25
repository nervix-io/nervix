//! Why the registry refused a change.
//!
//! Layer: decisions.
//!
//! - **Owns.** The one error every registry decision returns, and the node, route and field each
//!   variant names.
//! - **Depends on.** The vocabulary the variants quote.
//! - **Must not know.** How a caller reports or recovers from a refusal.

use std::fmt;

use nervix_models::{DomainName, FieldName, ModelName, RelayName};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(crate) enum OtelMappingSignal {
    #[strum(serialize = "LOGS")]
    Logs,
    #[strum(serialize = "TRACES")]
    Traces,
    #[strum(serialize = "METRIC GAUGE")]
    MetricGauge,
    #[strum(serialize = "METRIC SUM")]
    MetricSum,
    #[strum(serialize = "METRIC HISTOGRAM")]
    MetricHistogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(crate) enum OtelMappingSection {
    #[strum(serialize = "ATTRIBUTES")]
    Attributes,
    #[strum(serialize = "RESOURCE")]
    Resource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OtelMappingIssue {
    UnsupportedValue {
        signal: OtelMappingSignal,
        key: String,
    },
    DuplicateValue {
        signal: OtelMappingSignal,
        key: String,
    },
    MissingValue {
        signal: OtelMappingSignal,
        key: &'static str,
    },
    MissingDeltaValue {
        signal: OtelMappingSignal,
        key: &'static str,
    },
    DuplicateMetadata {
        signal: OtelMappingSignal,
        section: OtelMappingSection,
        key: String,
    },
}

impl fmt::Display for OtelMappingIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValue { signal, key } => {
                write!(
                    formatter,
                    "OTEL {signal} VALUES does not support key '{key}'"
                )
            }
            Self::DuplicateValue { signal, key } => {
                write!(
                    formatter,
                    "OTEL {signal} VALUES contains duplicate key '{key}'"
                )
            }
            Self::MissingValue { signal, key } => {
                write!(formatter, "OTEL {signal} VALUES requires key '{key}'")
            }
            Self::MissingDeltaValue { signal, key } => {
                write!(formatter, "OTEL {signal} DELTA VALUES requires key '{key}'")
            }
            Self::DuplicateMetadata { section, key, .. } => {
                write!(formatter, "OTEL {section} contains duplicate key '{key}'")
            }
        }
    }
}

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
    #[error("stored model has an invalid archive header; recreate the stored data")]
    InvalidModelArchive,
    #[error("branch '{branch}' in domain '{domain}' uses BYTES in field '{field}'")]
    BranchFieldContainsBytes {
        domain: DomainName,
        branch: ModelName,
        field: FieldName,
    },
    #[error("lookup '{lookup}' in domain '{domain}' cannot read BYTES field '{field}'")]
    LookupFieldContainsBytes {
        domain: DomainName,
        lookup: ModelName,
        field: FieldName,
    },
    #[error(
        "node '{node}' in domain '{domain}' cannot read BYTES field '{field}' from materialized \
         relay '{relay}'"
    )]
    MaterializedFieldContainsBytes {
        domain: DomainName,
        node: ModelName,
        relay: RelayName,
        field: FieldName,
    },
    #[error(
        "generator '{generator}' in domain '{domain}' cannot read BYTES field '{field}' from \
         materialized relay '{relay}'"
    )]
    GeneratorSourceFieldContainsBytes {
        domain: DomainName,
        generator: ModelName,
        relay: RelayName,
        field: FieldName,
    },
    #[error(
        "window processor '{processor}' in domain '{domain}' route '{route}' cannot retain BYTES \
         in aggregate demand {demand} argument {argument}"
    )]
    WindowArgumentContainsBytes {
        domain: DomainName,
        processor: ModelName,
        route: RelayName,
        demand: usize,
        argument: usize,
    },
    #[error(
        "window processor '{processor}' in domain '{domain}' route '{route}' uses a sketch and \
         requires MAX STATE SIZE"
    )]
    WindowSketchMissingStateLimit {
        domain: DomainName,
        processor: ModelName,
        route: RelayName,
    },
    #[error(
        "window processor '{processor}' in domain '{domain}' route '{route}' uses a sketch and \
         requires duration WIDTH and STEP"
    )]
    WindowSketchRequiresTimePanes {
        domain: DomainName,
        processor: ModelName,
        route: RelayName,
    },
    #[error(
        "window processor '{processor}' in domain '{domain}' route '{route}' uses a sketch and \
         requires its branch to declare MAX INSTANCES"
    )]
    WindowSketchRequiresBranchLimit {
        domain: DomainName,
        processor: ModelName,
        route: RelayName,
    },
    #[error(
        "window processor '{processor}' in domain '{domain}' route '{route}' requires {required} \
         bytes for {panes} sketch panes, above MAX STATE SIZE {limit} bytes"
    )]
    WindowSketchStateBudget {
        domain: DomainName,
        processor: ModelName,
        route: RelayName,
        panes: u64,
        required: u128,
        limit: u64,
    },
    #[error(
        "window processor '{processor}' in domain '{domain}' route '{route}' has a sketch pane \
         budget that cannot be represented"
    )]
    WindowSketchBudgetOverflow {
        domain: DomainName,
        processor: ModelName,
        route: RelayName,
    },
    #[error("failed to decode key")]
    DecodeKey,
    #[error("failed to persist model batch")]
    PersistBatch,
    #[error("model '{identifier}' already exists in domain '{domain}'")]
    AlreadyExists { domain: String, identifier: String },
    #[error("domain '{domain}' changed after the mutation batch was planned")]
    ConcurrentMutation { domain: String },
    #[error("domain '{domain}' has an invalid admitted transaction Model transition")]
    InvalidTransactionPlan { domain: String },
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
        "model '{identifier}' in domain '{domain}' is invalid: LOOKUP_HASH_MAP argument \
         {argument} must be a string literal"
    )]
    LookupHashMapLiteralArgument {
        domain: DomainName,
        identifier: ModelName,
        argument: usize,
    },
    #[error("model '{identifier}' in domain '{domain}' is invalid: {issue}")]
    InvalidOtelMapping {
        domain: DomainName,
        identifier: ModelName,
        issue: OtelMappingIssue,
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
