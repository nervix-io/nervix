//! The closed field contracts of OTEL logs, spans and metric points.
//!
//! Layer: vocabulary.
//! - **Owns.** Each field's exact type, presence and value constraints.
//! - **Depends on.** Shared mapped types and OTEL metric temporality.
//! - **Must not know.** Arrow values, registry decisions or OTLP encoding.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns OTEL field declarations")
)]

use crate::{
    Expression, Literal, MappedValueField, MappedValueType, MappedValueTypes, ParseAsType,
};

/// Whether a mapping must occur in the statement. Required fields also refuse NULL records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum OtelValuePresence {
    #[strum(serialize = "required")]
    Required,
    #[strum(serialize = "optional")]
    Optional,
    #[strum(serialize = "required for DELTA; optional otherwise")]
    Delta,
}

impl OtelValuePresence {
    pub const fn is_required(self, delta: bool) -> bool {
        match self {
            Self::Required => true,
            Self::Optional => false,
            Self::Delta => delta,
        }
    }
}

/// A field's type and value domain belong together, so a rule cannot contradict its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtelValueRule {
    String,
    Timestamp,
    SeverityNumber,
    TraceId,
    SpanId,
    SpanKind,
    StatusCode,
    Number,
    Count,
    Float,
    BucketCounts,
    ExplicitBounds,
}

impl OtelValueRule {
    /// Record-dependent expressions retain their row-time checks. Written literals can be
    /// refused before the graph is installed, using the same domain as the record encoder.
    pub fn admits_literal(self, expression: &Expression) -> bool {
        if let Expression::Cast {
            expression,
            target: ParseAsType::String,
        } = expression
        {
            return self.admits_literal(expression);
        }
        if self == Self::SeverityNumber
            && let Some(value) = expression.integer_literal()
        {
            return self.admits_integer(value);
        }
        if let Expression::Literal(Literal::String(value)) = expression {
            return self.admits_string(value);
        }
        true
    }

    pub fn admits_integer(self, value: i128) -> bool {
        match self {
            Self::SeverityNumber => (0..=24).contains(&value),
            _ => true,
        }
    }

    pub fn admits_string(self, value: &str) -> bool {
        match self {
            Self::TraceId => Self::admits_hex_id(value, 16),
            Self::SpanId => Self::admits_hex_id(value, 8),
            Self::SpanKind => matches!(
                value,
                "SERVER" | "CLIENT" | "INTERNAL" | "PRODUCER" | "CONSUMER"
            ),
            Self::StatusCode => matches!(value, "OK" | "ERROR" | "UNSET"),
            _ => true,
        }
    }

    fn admits_hex_id(value: &str, byte_len: usize) -> bool {
        value.len() == byte_len * 2
            && value.bytes().all(|byte| byte.is_ascii_hexdigit())
            && value.bytes().any(|byte| byte != b'0')
    }

    pub const fn value_type(self) -> MappedValueType {
        match self {
            Self::String | Self::TraceId | Self::SpanId | Self::SpanKind | Self::StatusCode => {
                MappedValueType::String
            }
            Self::Timestamp => MappedValueType::Datetime,
            Self::SeverityNumber => MappedValueType::I32,
            Self::Number => MappedValueType::Number,
            Self::Count => MappedValueType::U64,
            Self::Float => MappedValueType::Float,
            Self::BucketCounts => MappedValueType::U64Sequence,
            Self::ExplicitBounds => MappedValueType::FloatSequence,
        }
    }

    /// The current record constraint, including relationships with another field.
    pub const fn constraint(self) -> &'static str {
        match self {
            Self::String => "no additional value constraint",
            Self::Timestamp => "not before the Unix epoch",
            Self::SeverityNumber => "0..=24",
            Self::TraceId => "32 hexadecimal characters; not all zero",
            Self::SpanId => "16 hexadecimal characters; not all zero",
            Self::SpanKind => "SERVER, CLIENT, INTERNAL, PRODUCER or CONSUMER; case-sensitive",
            Self::StatusCode => "OK, ERROR or UNSET; case-sensitive",
            Self::Number => "integer values must fit I64; floating-point values use OTLP as_double",
            Self::Count => "the complete U64 range",
            Self::Float => "no additional value constraint",
            Self::BucketCounts => "no NULL elements; length equals explicit_bounds length plus one",
            Self::ExplicitBounds => "no NULL elements",
        }
    }
}

/// One public field in a signal's closed VALUES table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtelValueField {
    pub key: &'static str,
    pub presence: OtelValuePresence,
    pub rule: OtelValueRule,
}

// Both public projections come from one authored field declaration. The shared type consumer
// needs only types; OTEL requiredness and the manual also consume the complete field contract.
macro_rules! value_contract {
    ($fields:ident, $types:ident { $($key:literal => $presence:ident $rule:ident),+ $(,)? }) => {
        pub const $fields: &[OtelValueField] = &[$(OtelValueField {
            key: $key, presence: OtelValuePresence::$presence, rule: OtelValueRule::$rule,
        },)+];
        pub const $types: MappedValueTypes = MappedValueTypes::Fields {
            values: &[$(MappedValueField { key: $key, value_type: OtelValueRule::$rule.value_type() },)+],
            attributes: MappedValueType::Attribute,
            resource: MappedValueType::Literal,
        };
    };
}

value_contract!(OTEL_LOG_VALUE_FIELDS, OTEL_LOG_VALUE_TYPES {
    "time" => Required Timestamp,
    "severity_text" => Optional String,
    "severity_number" => Optional SeverityNumber,
    "body" => Required String,
    "trace_id" => Optional TraceId,
    "span_id" => Optional SpanId,
});

value_contract!(OTEL_TRACE_VALUE_FIELDS, OTEL_TRACE_VALUE_TYPES {
    "trace_id" => Required TraceId,
    "span_id" => Required SpanId,
    "parent_span_id" => Optional SpanId,
    "name" => Required String,
    "kind" => Optional SpanKind,
    "start_time" => Required Timestamp,
    "end_time" => Required Timestamp,
    "status_code" => Optional StatusCode,
    "status_message" => Optional String,
});

value_contract!(OTEL_NUMBER_VALUE_FIELDS, OTEL_NUMBER_VALUE_TYPES {
    "time" => Required Timestamp,
    "start_time" => Delta Timestamp,
    "value" => Required Number,
});

value_contract!(OTEL_HISTOGRAM_VALUE_FIELDS, OTEL_HISTOGRAM_VALUE_TYPES {
    "time" => Required Timestamp,
    "start_time" => Delta Timestamp,
    "count" => Required Count,
    "sum" => Optional Float,
    "bucket_counts" => Required BucketCounts,
    "explicit_bounds" => Required ExplicitBounds,
    "min" => Optional Float,
    "max" => Optional Float,
});

/// An OTLP feature this implementation cannot express, with its public reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtelUnsupportedFeature {
    pub feature: &'static str,
    pub reason: &'static str,
}

pub const OTEL_UNSUPPORTED_FEATURES: &[OtelUnsupportedFeature] = &[
    OtelUnsupportedFeature {
        feature: "exponential histograms and summaries",
        reason: "METRIC declares only GAUGE, SUM and explicit HISTOGRAM",
    },
    OtelUnsupportedFeature {
        feature: "per-record resource and scope",
        reason: "RESOURCE is literal-only and SCOPE is fixed for the emitter",
    },
    OtelUnsupportedFeature {
        feature: "span events and links",
        reason: "the closed span VALUES table has no event or link mapping",
    },
    OtelUnsupportedFeature {
        feature: "trace_state",
        reason: "the closed span VALUES table does not map trace state",
    },
    OtelUnsupportedFeature {
        feature: "log event_name and flags",
        reason: "the closed log VALUES table maps neither field",
    },
    OtelUnsupportedFeature {
        feature: "exemplars",
        reason: "metric points expose values and attributes without exemplar mappings",
    },
    OtelUnsupportedFeature {
        feature: "non-string log bodies",
        reason: "the log body field requires STRING",
    },
    OtelUnsupportedFeature {
        feature: "bytes_value and map attributes",
        reason: "attributes admit scalars and nested ARRAY or VEC values; BYTES and maps are \
                 unavailable",
    },
    OtelUnsupportedFeature {
        feature: "scope attributes",
        reason: "SCOPE declares only a name and optional version",
    },
    OtelUnsupportedFeature {
        feature: "OTLP/JSON",
        reason: "the client declares only grpc and http/protobuf",
    },
];

/// Exact current decoding, shared by the public declaration and the manual.
pub const OTEL_HEADERS_FORMAT: &str = "comma-separated key=value pairs; split at the first '='; \
                                       trim surrounding whitespace; lowercase keys; \
                                       percent-decode values once as UTF-8; keep literal plus \
                                       signs; empty input means no headers";

/// Pairing declared independently of the selected OTLP transport.
pub const OTEL_TLS_ENDPOINT_RULE: &str = "tls_ca_file, tls_cert_file and tls_key_file require an \
                                          https endpoint for both grpc and http/protobuf; \
                                          tls_cert_file and tls_key_file must occur together";

pub const OTEL_ATTRIBUTES_ENCODING: &str = "DATETIME becomes RFC 3339 text; ARRAY and VEC may \
                                            nest; NULL attributes are omitted; BYTES and maps are \
                                            refused";
pub const OTEL_RESOURCE_ENCODING: &str =
    "literal expressions only, including nested literal arrays; one resource for the emitter";
