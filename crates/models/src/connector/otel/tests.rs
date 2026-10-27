//! Checks of the OTEL field and client declarations.
//!
//! Layer: test harness.
//! - **Owns.** Exact field types, requiredness, metadata and offline client-schema checks.
//! - **Depends on.** OTEL vocabulary and Arrow type descriptions.
//! - **Must not know.** Registry decisions, transport drivers or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "checks OTEL declarations")
)]

#[cfg(feature = "arrow")]
use meticulous::OptionExt as _;
use meticulous::ResultExt as _;

use super::*;
use crate::{ClientConfigEntry, ClientConfigSchemaError};

#[test]
fn every_otel_protocol_declares_a_required_export_deadline() {
    let schema = CreateClientOtel::<u64>::DECLARATION.config;
    for protocol in ["grpc", "http/protobuf"] {
        let mut config = vec![
            ClientConfigEntry {
                key: "endpoint".into(),
                value: "http://127.0.0.1:1".into(),
            },
            ClientConfigEntry {
                key: "protocol".into(),
                value: protocol.into(),
            },
        ];
        let error = schema
            .validate(&config)
            .expect_err("every export needs its declared deadline");
        assert_eq!(
            error.current_context(),
            &ClientConfigSchemaError::MissingRequired { key: "timeout_ms" }
        );
        config.push(ClientConfigEntry {
            key: "timeout_ms".into(),
            value: "5000".into(),
        });
        schema
            .validate(&config)
            .assured("a positive deadline completes either client declaration");
    }
}

#[test]
fn every_otel_field_has_one_exact_type_presence_and_constraint() {
    use std::collections::BTreeSet;

    use crate::MappedValueSection;
    for (fields, types) in [
        (OTEL_LOG_VALUE_FIELDS, OTEL_LOG_VALUE_TYPES),
        (OTEL_TRACE_VALUE_FIELDS, OTEL_TRACE_VALUE_TYPES),
        (OTEL_NUMBER_VALUE_FIELDS, OTEL_NUMBER_VALUE_TYPES),
        (OTEL_HISTOGRAM_VALUE_FIELDS, OTEL_HISTOGRAM_VALUE_TYPES),
    ] {
        let mut names = BTreeSet::new();
        for field in fields {
            assert!(
                names.insert(field.key),
                "duplicate declaration: {}",
                field.key
            );
            assert_eq!(
                types.value_type(MappedValueSection::Values, field.key),
                Some(field.rule.value_type())
            );
            assert!(!field.rule.constraint().is_empty());
        }
        assert_eq!(
            types.value_type(MappedValueSection::Attributes, "any.attribute"),
            Some(crate::MappedValueType::Attribute)
        );
        assert_eq!(
            types.value_type(MappedValueSection::Resource, "service.name"),
            Some(crate::MappedValueType::Literal)
        );
    }
}

#[test]
fn required_fields_follow_the_signal_and_delta_temporality() {
    use OtelValuePresence::{Delta, Optional, Required};
    assert!(Required.is_required(false));
    assert!(!Optional.is_required(true));
    assert!(!Delta.is_required(false));
    assert!(Delta.is_required(true));
    let required = |fields: &[OtelValueField], delta| {
        let mut keys = Vec::new();
        for field in fields {
            if field.presence.is_required(delta) {
                keys.push(field.key);
            }
        }
        keys
    };
    assert_eq!(required(OTEL_LOG_VALUE_FIELDS, false), ["time", "body"]);
    assert_eq!(
        required(OTEL_TRACE_VALUE_FIELDS, false),
        ["trace_id", "span_id", "name", "start_time", "end_time"]
    );
    assert_eq!(required(OTEL_NUMBER_VALUE_FIELDS, false), ["time", "value"]);
    assert_eq!(
        required(OTEL_NUMBER_VALUE_FIELDS, true),
        ["time", "start_time", "value"]
    );
    assert_eq!(
        required(OTEL_HISTOGRAM_VALUE_FIELDS, false),
        ["time", "count", "bucket_counts", "explicit_bounds"]
    );
    assert_eq!(
        required(OTEL_HISTOGRAM_VALUE_FIELDS, true),
        [
            "time",
            "start_time",
            "count",
            "bucket_counts",
            "explicit_bounds"
        ]
    );
    assert!(!OtelMetricKind::Gauge.is_delta());
    for temporality in [
        OtelAggregationTemporality::Delta,
        OtelAggregationTemporality::Cumulative,
    ] {
        let delta = temporality == OtelAggregationTemporality::Delta;
        assert_eq!(
            OtelMetricKind::Sum {
                monotonic: true,
                temporality
            }
            .is_delta(),
            delta
        );
        assert_eq!(OtelMetricKind::Histogram { temporality }.is_delta(), delta);
    }
}

#[cfg(feature = "arrow")]
#[test]
fn histogram_fields_admit_exact_types_and_recursive_attributes_refuse_bytes() {
    use arrow_schema::{DataType, Field};
    use nervix_primitives::sync::StdArc;

    use crate::MappedValueSection;
    let vector = |ty| DataType::List(StdArc::new(Field::new("item", ty, true)));
    struct TypeCase {
        key: &'static str,
        actual: DataType,
        admitted: bool,
    }
    for case in [
        TypeCase {
            key: "count",
            actual: DataType::UInt64,
            admitted: true,
        },
        TypeCase {
            key: "count",
            actual: DataType::Int64,
            admitted: false,
        },
        TypeCase {
            key: "sum",
            actual: DataType::Float32,
            admitted: true,
        },
        TypeCase {
            key: "sum",
            actual: DataType::Int64,
            admitted: false,
        },
        TypeCase {
            key: "min",
            actual: DataType::Float64,
            admitted: true,
        },
        TypeCase {
            key: "max",
            actual: DataType::UInt64,
            admitted: false,
        },
        TypeCase {
            key: "bucket_counts",
            actual: vector(DataType::UInt64),
            admitted: true,
        },
        TypeCase {
            key: "bucket_counts",
            actual: vector(DataType::Int64),
            admitted: false,
        },
        TypeCase {
            key: "explicit_bounds",
            actual: vector(DataType::Float64),
            admitted: true,
        },
        TypeCase {
            key: "explicit_bounds",
            actual: vector(DataType::UInt64),
            admitted: false,
        },
    ] {
        let ty = OTEL_HISTOGRAM_VALUE_TYPES
            .value_type(MappedValueSection::Values, case.key)
            .assured("the histogram declares this field");
        assert_eq!(
            ty.admits(&case.actual),
            case.admitted,
            "{}: {}",
            case.key,
            case.actual
        );
    }
    let attributes = crate::MappedValueType::Attribute;
    assert!(attributes.admits(&vector(vector(DataType::Boolean))));
    assert!(!attributes.admits(&vector(DataType::Binary)));
}
