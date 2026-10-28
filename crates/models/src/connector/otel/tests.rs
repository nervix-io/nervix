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
fn tls_files_require_https_independently_of_the_export_protocol() {
    let schema = CreateClientOtel::<u64>::DECLARATION.config;
    for protocol in ["grpc", "http/protobuf"] {
        let config = vec![
            ClientConfigEntry {
                key: "endpoint".into(),
                value: "http://127.0.0.1:1".into(),
            },
            ClientConfigEntry {
                key: "protocol".into(),
                value: protocol.into(),
            },
            ClientConfigEntry {
                key: "timeout_ms".into(),
                value: "5000".into(),
            },
            ClientConfigEntry {
                key: "tls_ca_file".into(),
                value: "/not-a-tls-file".into(),
            },
        ];
        for error in [schema.validate(&config), schema.validate_rendered(&config)] {
            let error = error.expect_err("TLS files cannot accompany a plaintext OTLP endpoint");
            assert_eq!(
                error.current_context(),
                &ClientConfigSchemaError::RequiresScheme {
                    key: "tls_ca_file",
                    address: "endpoint",
                    scheme: "https",
                }
            );
        }
        let mut secure = config;
        secure[0].value = "https://127.0.0.1:1".into();
        schema
            .validate_rendered(&secure)
            .assured("TLS configuration uses HTTPS");
        secure[0].value = "{{instance.host}}".into();
        schema
            .validate(&secure)
            .assured("the scheme is checked after the address is rendered");
        secure[0].value = "http://127.0.0.1:1".into();
        secure.pop();
        schema
            .validate_rendered(&secure)
            .assured("plaintext needs no TLS configuration");
        secure.push(ClientConfigEntry {
            key: "tls_cert_file".into(),
            value: "/certificate".into(),
        });
        secure.push(ClientConfigEntry {
            key: "tls_key_file".into(),
            value: "/private-key".into(),
        });
        assert_eq!(
            schema
                .validate(&secure)
                .expect_err("an identity also needs HTTPS")
                .current_context(),
            &ClientConfigSchemaError::RequiresScheme {
                key: "tls_cert_file",
                address: "endpoint",
                scheme: "https"
            }
        );
    }
}

#[test]
fn literal_domains_include_signed_and_cast_severity_and_exact_identifier_text() {
    use crate::{Expression, Literal, ParseAsType, UnaryOperator};
    for value in [-1, 0, 24, 25] {
        let literal = Expression::Literal(Literal::I64(value));
        let cast = Expression::Cast {
            expression: Box::new(literal),
            target: ParseAsType::I32,
        };
        assert_eq!(
            OtelValueRule::SeverityNumber.admits_literal(&cast),
            (0..=24).contains(&value)
        );
    }
    let negative = Expression::Unary {
        operator: UnaryOperator::Negate,
        expression: Box::new(Expression::Literal(Literal::I64(1))),
    };
    assert!(!OtelValueRule::SeverityNumber.admits_literal(&negative));
    struct LiteralCase {
        rule: OtelValueRule,
        text: &'static str,
        admitted: bool,
    }
    for case in [
        LiteralCase {
            rule: OtelValueRule::TraceId,
            text: "00112233445566778899AaBbCcDdEeFf",
            admitted: true,
        },
        LiteralCase {
            rule: OtelValueRule::TraceId,
            text: "0011223344556677",
            admitted: false,
        },
        LiteralCase {
            rule: OtelValueRule::SpanId,
            text: "0011223344556677",
            admitted: true,
        },
        LiteralCase {
            rule: OtelValueRule::SpanId,
            text: "0000000000000000",
            admitted: false,
        },
        LiteralCase {
            rule: OtelValueRule::SpanId,
            text: "001122334455667g",
            admitted: false,
        },
        LiteralCase {
            rule: OtelValueRule::SpanKind,
            text: "SERVER",
            admitted: true,
        },
        LiteralCase {
            rule: OtelValueRule::SpanKind,
            text: "server",
            admitted: false,
        },
        LiteralCase {
            rule: OtelValueRule::StatusCode,
            text: "OK",
            admitted: true,
        },
        LiteralCase {
            rule: OtelValueRule::StatusCode,
            text: "INVALID",
            admitted: false,
        },
    ] {
        let expression = Expression::Literal(Literal::String(case.text.into()));
        assert_eq!(case.rule.admits_literal(&expression), case.admitted);
        let cast = Expression::Cast {
            expression: Box::new(expression),
            target: ParseAsType::String,
        };
        assert_eq!(case.rule.admits_literal(&cast), case.admitted);
    }
}

#[test]
#[cfg(feature = "arrow")]
fn bolero_otel_mappings_round_trip_with_literal_domains() {
    use arrow_schema::{DataType, Field, TimeUnit};
    use nervix_primitives::sync::StdArc;

    use crate::{Expression, Literal, ParseAsType};
    bolero::check!().with_iterations(256).with_max_len(128).for_each(|input: &[u8]| {
        let mut number_bytes = [0_u8; 2];
        for (destination, source) in number_bytes.iter_mut().zip(input) { *destination = *source; }
        let severity = i64::from(i16::from_le_bytes(number_bytes));
        let text = String::from_utf8_lossy(input).into_owned();
        let candidates = [DataType::Utf8, DataType::Int32, DataType::Int64, DataType::UInt64,
            DataType::Float32, DataType::Float64, DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Boolean, DataType::Binary,
            DataType::List(StdArc::new(Field::new("item", DataType::UInt64, true))),
            DataType::List(StdArc::new(Field::new("item", DataType::Float32, true))),
            DataType::FixedSizeList(StdArc::new(Field::new("item", DataType::Float64, true)), 2),
            DataType::List(StdArc::new(Field::new("item", DataType::Int64, true)))];
        let candidate = &candidates[usize::from(number_bytes[0]) % candidates.len()];
        for (fields, types) in [(OTEL_LOG_VALUE_FIELDS, OTEL_LOG_VALUE_TYPES),
            (OTEL_TRACE_VALUE_FIELDS, OTEL_TRACE_VALUE_TYPES),
            (OTEL_NUMBER_VALUE_FIELDS, OTEL_NUMBER_VALUE_TYPES),
            (OTEL_HISTOGRAM_VALUE_FIELDS, OTEL_HISTOGRAM_VALUE_TYPES)] {
            for field in fields {
                let expected = match field.key {
                    "time" | "start_time" | "end_time" => matches!(candidate, DataType::Timestamp(TimeUnit::Nanosecond, _)),
                    "severity_number" => *candidate == DataType::Int32,
                    "value" => matches!(candidate, DataType::Int32 | DataType::Int64 | DataType::UInt64 | DataType::Float32 | DataType::Float64),
                    "count" => *candidate == DataType::UInt64,
                    "sum" | "min" | "max" => matches!(candidate, DataType::Float32 | DataType::Float64),
                    "bucket_counts" => matches!(candidate, DataType::List(element) if *element.data_type() == DataType::UInt64),
                    "explicit_bounds" => match candidate {
                        DataType::List(element) | DataType::FixedSizeList(element, _) => matches!(element.data_type(), DataType::Float32 | DataType::Float64),
                        _ => false,
                    },
                    _ => *candidate == DataType::Utf8,
                };
                let declared = types.value_type(crate::MappedValueSection::Values, field.key).assured("every field has an exact type");
                assert_eq!(declared.admits(candidate), expected, "{}: {candidate}", field.key);
            }
        }
        let mappings = OtelMappings {
            values: vec![
                OtelValueMapping { column: "severity_number".into(), expression: Expression::Cast {
                    expression: Box::new(Expression::Literal(Literal::I64(severity))), target: ParseAsType::I32 } },
                OtelValueMapping { column: "trace_id".into(), expression: Expression::Literal(Literal::String(text.clone())) },
                OtelValueMapping { column: "span_id".into(), expression: Expression::Literal(Literal::String(text.clone())) },
            ],
            attributes: vec![OtelValueMapping { column: "attribute".into(), expression: Expression::Literal(Literal::String(text.clone())) }],
            resource: vec![OtelValueMapping { column: "service.name".into(), expression: Expression::Literal(Literal::String(text.clone())) }],
            scope: Some(OtelScope { name: text.clone(), version: Some(text.clone()) }),
        };
        let json = serde_json::to_vec(&mappings).assured("current OTEL mappings serialize");
        let decoded: OtelMappings = serde_json::from_slice(&json).assured("current OTEL mappings deserialize");
        assert_eq!(decoded, mappings);
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&mappings).assured("current OTEL mappings archive");
        let decoded = rkyv::from_bytes::<OtelMappings, rkyv::rancor::Error>(&archive).assured("current OTEL mappings unarchive");
        assert_eq!(decoded, mappings);
        for mapping in &decoded.values {
            let rule = match mapping.column.as_str() {
                "severity_number" => OtelValueRule::SeverityNumber,
                "trace_id" => OtelValueRule::TraceId,
                "span_id" => OtelValueRule::SpanId,
                _ => unreachable!("the bounded mapping domain has three fields"),
            };
            let expected = match rule {
                OtelValueRule::SeverityNumber => (0..=24).contains(&severity),
                OtelValueRule::TraceId => text.len() == 32 && text.chars().all(|c| c.is_ascii_hexdigit()) && text.chars().any(|c| c != '0'),
                OtelValueRule::SpanId => text.len() == 16 && text.chars().all(|c| c.is_ascii_hexdigit()) && text.chars().any(|c| c != '0'),
                _ => unreachable!("the bounded mapping domain has three rules"),
            };
            assert_eq!(rule.admits_literal(&mapping.expression), expected);
        }
    });
}

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
