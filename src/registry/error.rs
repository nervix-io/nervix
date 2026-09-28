//! Why the registry refused a change.
//!
//! Layer: decisions.
//!
//! - **Owns.** The one error every registry decision returns, and the node, route and field each
//!   variant names.
//! - **Depends on.** The vocabulary the variants quote.
//! - **Must not know.** How a caller reports or recovers from a refusal.

use std::fmt;

use error_stack::{Context, Report};
use nervix_models::{BranchSelection, DomainName, FieldName, ModelKind, ModelName, RelayName};
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
    #[error("codec '{codec}' in domain '{domain}' has an invalid {direction} jaq transformation")]
    InvalidCodecJaq {
        domain: DomainName,
        codec: ModelName,
        direction: &'static str,
    },
    #[error(
        "{} '{node}' route '{route}' ON MESSAGE ERROR relay '{error_relay}' uses branch \
         {actual_branch}, expected {expected_branch} in domain '{domain}'",
        .node_kind.as_str()
    )]
    MessageErrorBranchMismatch {
        domain: DomainName,
        node_kind: ModelKind,
        node: ModelName,
        route: RelayName,
        error_relay: RelayName,
        actual_branch: BranchSelection,
        expected_branch: BranchSelection,
    },
    #[error(
        "emitter '{emitter}' in domain '{domain}' VALUES target '{target}' for {sink} would emit \
         sensitive data; use leak_sensitive(...) explicitly"
    )]
    SensitiveEmitterValue {
        domain: DomainName,
        emitter: ModelName,
        sink: &'static str,
        target: String,
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

impl RegistryError {
    /// Refuses model `identifier` because the vocabulary rejected an operation on it, such as an
    /// alteration the stored model cannot take.
    ///
    /// The rejection's report stays beneath the refusal, and its message is the refusal's reason,
    /// which is the text a failed command shows.
    pub(in crate::registry) fn invalid_model<C: Context>(
        domain: &DomainName,
        identifier: &str,
        rejection: Report<C>,
    ) -> Report<Self> {
        let reason = rejection.to_string();
        rejection.change_context(Self::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.to_string(),
            reason,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{
        AlterDeduplicator, AlterDeduplicatorError, AlterDeduplicatorOperation, AlterPlacement,
        AlterPlacementError, AlterPlacementOperation, AlterProcessorError, AlterProcessorOperation,
        AlterRelay, AlterRelayError, AlterRelayOperation, AlterWireSchema, AlterWireSchemaError,
        AlterWireSchemaOperation, AvroType, CreatePlacement, CreateWireSchema, JsonType, Model,
        PlacementPolicy, WireSchemaField,
    };

    use super::*;
    use crate::registry::{
        Registry, RegistryMutation,
        test_fixtures::{
            avro_wire_schema_with_type, full_graph_batch, named, placement, temp_db_path,
        },
    };

    /// Asserts that `error` refuses model `identifier` with the vocabulary's own message.
    fn assert_invalid_model(error: &Report<RegistryError>, identifier: &str, reason: &str) {
        assert_eq!(
            error.current_context(),
            &RegistryError::InvalidModel {
                domain: "default".to_string(),
                identifier: identifier.to_string(),
                reason: reason.to_string(),
            }
        );
    }

    /// Drops a field no fixture wire schema declares.
    fn drop_missing_field<T>() -> Vec<AlterWireSchemaOperation<T>> {
        vec![AlterWireSchemaOperation::DropField {
            field: named("missing"),
        }]
    }

    #[test]
    fn a_refused_model_keeps_the_vocabulary_rejection_beneath_the_refusal() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let mut models = full_graph_batch();
        models.push(Model::WireCborSchema(CreateWireSchema {
            name: named("event_cbor"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("value"),
                ty: JsonType::String,
                optional: false,
            }],
        }));
        models.push(avro_wire_schema_with_type("event_avro", AvroType::String));
        models.push(placement(
            "pin_ing",
            &["ing"],
            &["emit"],
            PlacementPolicy::PreferColocation,
            None,
        ));
        registry
            .apply_batch(&domain, models)
            .expect("the fixture graph should validate");
        let refuse = |mutation: RegistryMutation| {
            registry
                .plan_mutations(&domain, &[mutation])
                .expect_err("the stored model refuses the alteration")
        };

        let wire_alterations = [
            (
                "event_wire",
                RegistryMutation::AlterWireJsonSchema(AlterWireSchema {
                    schema: named("event_wire"),
                    operations: drop_missing_field(),
                }),
            ),
            (
                "event_cbor",
                RegistryMutation::AlterWireCborSchema(AlterWireSchema {
                    schema: named("event_cbor"),
                    operations: drop_missing_field(),
                }),
            ),
            (
                "event_avro",
                RegistryMutation::AlterWireAvroSchema(AlterWireSchema {
                    schema: named("event_avro"),
                    operations: drop_missing_field(),
                }),
            ),
        ];
        for (identifier, mutation) in wire_alterations {
            let error = refuse(mutation);
            assert_invalid_model(&error, identifier, "field `missing` does not exist");
            assert_eq!(
                error.downcast_ref::<AlterWireSchemaError>(),
                Some(&AlterWireSchemaError::FieldNotFound {
                    field: named("missing"),
                })
            );
        }

        let error = refuse(RegistryMutation::AlterRelay(AlterRelay {
            relay: named("notifications"),
            operations: vec![AlterRelayOperation::DropMaterializedState],
        }));
        assert_invalid_model(
            &error,
            "notifications",
            "relay materialized state is not configured",
        );
        assert_eq!(
            error.downcast_ref::<AlterRelayError>(),
            Some(&AlterRelayError::MaterializedStateNotConfigured)
        );

        let error = refuse(RegistryMutation::AlterDeduplicator(AlterDeduplicator {
            deduplicator: named("p99_proc"),
            operations: vec![AlterDeduplicatorOperation::Processor(Box::new(
                AlterProcessorOperation::DropRoute {
                    relay: named("missing"),
                },
            ))],
        }));
        assert_invalid_model(
            &error,
            "p99_proc",
            "route target `missing` is not configured",
        );
        assert_eq!(
            error.downcast_ref::<AlterDeduplicatorError>(),
            Some(&AlterDeduplicatorError::Processor(
                AlterProcessorError::RouteTargetNotFound {
                    relay: named("missing"),
                }
            ))
        );

        let error = refuse(RegistryMutation::AlterPlacement(AlterPlacement {
            placement: named("pin_ing"),
            operations: vec![AlterPlacementOperation::SetMembers {
                from: vec![named("ing")],
                to: Vec::new(),
            }],
        }));
        assert_invalid_model(
            &error,
            "pin_ing",
            "a placement must declare at least one TO member",
        );
        assert_eq!(
            error.downcast_ref::<AlterPlacementError>(),
            Some(&AlterPlacementError::EmptyTo)
        );

        let error = registry
            .apply_batch(
                &domain,
                vec![Model::Placement(CreatePlacement {
                    name: named("pin_nothing"),
                    from: Vec::new(),
                    to: vec![named("emit")],
                    policy: PlacementPolicy::Neutral,
                    rank: None,
                })],
            )
            .expect_err("a placement without FROM members is invalid");
        assert_invalid_model(
            &error,
            "pin_nothing",
            "a placement must declare at least one FROM member",
        );
        assert_eq!(
            error.downcast_ref::<AlterPlacementError>(),
            Some(&AlterPlacementError::EmptyFrom)
        );

        let _ = fs::remove_dir_all(path);
    }
}
