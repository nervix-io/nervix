//! The OpenTelemetry connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The OTEL client Model, the logs, traces and metrics emitter Models with the value
//!   mappings they share and the metric a metrics emitter declares, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The OTLP encoding or how a signal is exported.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use super::{
    bounds::{
        BufferBound, DeadlineKey, DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline,
        WaitBound,
    },
    configured_client::configured_client,
    entity_union::connector_emitters,
};
use crate::{
    ClientName, ConnectorKind, EmitterBatchLegality, EmitterBodyForm, EmitterClientReference,
    EmitterDeclaration, EmitterEntityBody, EmitterEntityCapabilities, EmitterReference,
    EmitterReferenceKind, EmitterValueMapping, EmitterValueMappings, MappedValueTypes,
    OptionChoice, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
};

#[cfg(test)]
mod tests;

mod value_contract;
pub use value_contract::*;

configured_client!(Otel: unpooled, config CLIENT_CONFIG, CreateClientOtel => OtelConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::optional("compression", V::Choice(&["gzip"])),
            K::required("endpoint", V::Url(&["http", "https"])),
            K::optional("headers", V::PercentEncodedHeaders).secret(),
            K::required("protocol", V::Choice(&["grpc", "http/protobuf"])),
            K::required("timeout_ms", V::Milliseconds),
            K::TLS_CA.only_for_scheme("endpoint", "https"),
            K::TLS_CERT.only_for_scheme("endpoint", "https"),
            K::TLS_KEY.only_for_scheme("endpoint", "https"),
        ],
        &[],
        &[],
    )
};

pub type OtelValueMapping = EmitterValueMapping;

/// What every OTEL emitter maps a record to, whichever signal it exports: the signal's own fields,
/// its attributes, the attributes of the resource that produced it, and the instrumentation scope.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelMappings {
    pub values: Vec<OtelValueMapping>,
    pub attributes: Vec<OtelValueMapping>,
    pub resource: Vec<OtelValueMapping>,
    pub scope: Option<OtelScope>,
}

impl OtelMappings {
    fn value_mappings(&self) -> EmitterValueMappings<'_> {
        EmitterValueMappings {
            values: &self.values,
            attributes: &self.attributes,
            resource: &self.resource,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelScope {
    pub name: String,
    pub version: Option<String>,
}

/// The log records an OTEL client exports.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelLogsEmitter {
    pub mode: OtelLogsPublishingMode,
    pub client: ClientName,
    pub mappings: OtelMappings,
}

/// The spans an OTEL client exports.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelTracesEmitter {
    pub mode: OtelTracesPublishingMode,
    pub client: ClientName,
    pub mappings: OtelMappings,
}

/// The data points of one metric an OTEL client exports.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelMetricsEmitter {
    pub mode: OtelMetricsPublishingMode,
    pub client: ClientName,
    pub metric: OtelMetric,
    pub mappings: OtelMappings,
}

impl OtelLogsEmitter {
    fn declared_value_types(&self) -> MappedValueTypes {
        OTEL_LOG_VALUE_TYPES
    }
}

impl OtelTracesEmitter {
    fn declared_value_types(&self) -> MappedValueTypes {
        OTEL_TRACE_VALUE_TYPES
    }
}

impl OtelMetricsEmitter {
    fn declared_value_types(&self) -> MappedValueTypes {
        match self.metric.kind {
            OtelMetricKind::Gauge | OtelMetricKind::Sum { .. } => OTEL_NUMBER_VALUE_TYPES,
            OtelMetricKind::Histogram { .. } => OTEL_HISTOGRAM_VALUE_TYPES,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelMetric {
    pub name: String,
    pub unit: String,
    pub description: Option<String>,
    pub kind: OtelMetricKind,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum OtelMetricKind {
    Gauge,
    Sum {
        monotonic: bool,
        temporality: OtelAggregationTemporality,
    },
    Histogram {
        temporality: OtelAggregationTemporality,
    },
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum OtelAggregationTemporality {
    Delta,
    Cumulative,
}

/// `ATTRIBUTES { … }`: the attributes every exported item carries beside its values.
const ATTRIBUTES: OptionDeclaration = OptionDeclaration {
    keywords: "ATTRIBUTES",
    presence: OptionPresence::Optional { default: None },
    form: OptionForm::Value {
        value: OptionValue::Mapping,
        example: "ATTRIBUTES {'tenant' = 'eu'}",
    },
};

/// `RESOURCE { … }`: the literal attributes of the resource every export describes.
const RESOURCE: OptionDeclaration = OptionDeclaration {
    keywords: "RESOURCE",
    presence: OptionPresence::Optional { default: None },
    form: OptionForm::Value {
        value: OptionValue::Mapping,
        example: "RESOURCE {'service.name' = 'nervix'}",
    },
};

/// `SCOPE '<name>' [VERSION '<version>']`: the instrumentation scope every export names.
const SCOPE: OptionDeclaration = OptionDeclaration {
    keywords: "SCOPE",
    presence: OptionPresence::Optional { default: None },
    form: OptionForm::Value {
        value: OptionValue::Clause,
        example: "SCOPE 'nervix' VERSION '1.0'",
    },
};

/// `METRIC '<name>' UNIT '<unit>' [DESCRIPTION '<description>'] <kind>`: the one metric a metrics
/// emitter exports, chosen by its kind.
const METRIC: OptionDeclaration = OptionDeclaration {
    keywords: "METRIC",
    presence: OptionPresence::Required,
    form: OptionForm::Choice(&[
        OptionChoice {
            name: "GAUGE",
            value: Some(OptionValue::Clause),
            example: "METRIC 'temperature' UNIT 'Cel' GAUGE",
        },
        OptionChoice {
            name: "SUM",
            value: Some(OptionValue::Clause),
            example: "METRIC 'requests' UNIT '1' SUM MONOTONIC DELTA",
        },
        OptionChoice {
            name: "HISTOGRAM",
            value: Some(OptionValue::Clause),
            example: "METRIC 'latency' UNIT 'ms' HISTOGRAM CUMULATIVE",
        },
    ]),
};

/// What the connector of a OTEL holds and how long it waits.
const OTEL_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "the export request of a publish without BATCH",
        bound: BufferBound::Unbounded,
    }],
    waits: &[DeclaredWait {
        wait: "an export: name resolution, connection, TLS and the response",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Required,
        },
    }],
    driver_limits: &["gRPC responses above 4 MiB are refused."],
};

/// What every OTEL emitter declares: an export is one request the collector answers, and its fields
/// are mapped directly rather than encoded by a codec.
const fn otel_capabilities(
    publishing_modes: &'static [crate::EmitterPublishingModeKind],
    options: &'static [OptionDeclaration],
) -> EmitterEntityCapabilities {
    EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::Otel),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes,
        locator: None,
        options,
        envelope: &[],
        properties: &[],
        bounds: OTEL_BOUNDS,
    }
}

macro_rules! otel_emitter_declarations {
    ($($Emitter:ident($Mode:ident) => $options:expr),+ $(,)?) => {
        $(
            impl EmitterDeclaration for $Emitter {
                const CAPABILITIES: EmitterEntityCapabilities = otel_capabilities($Mode::KINDS, $options);
            }

            impl EmitterEntityBody for $Emitter {
                fn reference(&self) -> EmitterReference<'_> {
                    EmitterReference::Client(EmitterClientReference {
                        client: &self.client,
                        connector: ConnectorKind::Otel,
                    })
                }

                fn client_mut(&mut self) -> Option<&mut ClientName> {
                    Some(&mut self.client)
                }

                fn value_mappings(&self) -> EmitterValueMappings<'_> {
                    self.mappings.value_mappings()
                }

                fn mapped_value_types(&self) -> Option<MappedValueTypes> {
                    Some(self.declared_value_types())
                }
            }
        )+
    };
}

otel_emitter_declarations!(
    OtelLogsEmitter(OtelLogsPublishingMode) => &[ATTRIBUTES, RESOURCE, SCOPE],
    OtelTracesEmitter(OtelTracesPublishingMode) => &[ATTRIBUTES, RESOURCE, SCOPE],
    OtelMetricsEmitter(OtelMetricsPublishingMode) => &[METRIC, ATTRIBUTES, RESOURCE, SCOPE],
);

connector_emitters! {
    connector Otel = "OTEL";
    /// What an OTEL emitter writes.
    pub enum OtelEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum OtelEmitterMode;
    /// The entities an OTEL emitter writes: one per signal.
    pub enum OtelEmitterEntity {
        Logs(OtelLogsEmitter, OtelLogsPublishingMode) = "LOGS";
        Traces(OtelTracesEmitter, OtelTracesPublishingMode) = "TRACES";
        Metrics(OtelMetricsEmitter, OtelMetricsPublishingMode) = "METRICS";
    }
}

impl OtelEmitter {
    /// The complete current VALUES table of the selected signal and metric shape.
    pub fn value_fields(&self) -> &'static [OtelValueField] {
        match self {
            Self::Logs(_) => OTEL_LOG_VALUE_FIELDS,
            Self::Traces(_) => OTEL_TRACE_VALUE_FIELDS,
            Self::Metrics(connector) => match connector.metric.kind {
                OtelMetricKind::Gauge | OtelMetricKind::Sum { .. } => OTEL_NUMBER_VALUE_FIELDS,
                OtelMetricKind::Histogram { .. } => OTEL_HISTOGRAM_VALUE_FIELDS,
            },
        }
    }

    pub fn is_delta(&self) -> bool {
        if let Self::Metrics(connector) = self {
            connector.metric.kind.is_delta()
        } else {
            false
        }
    }

    pub fn client(&self) -> &ClientName {
        match self {
            Self::Logs(connector) => &connector.client,
            Self::Traces(connector) => &connector.client,
            Self::Metrics(connector) => &connector.client,
        }
    }

    /// The mappings every signal shares.
    pub fn mappings(&self) -> &OtelMappings {
        match self {
            Self::Logs(connector) => &connector.mappings,
            Self::Traces(connector) => &connector.mappings,
            Self::Metrics(connector) => &connector.mappings,
        }
    }
}

impl OtelMetricKind {
    pub const fn is_delta(&self) -> bool {
        matches!(
            self,
            Self::Sum {
                temporality: OtelAggregationTemporality::Delta,
                ..
            } | Self::Histogram {
                temporality: OtelAggregationTemporality::Delta
            }
        )
    }
}

super::publishing::emitter_modes! {
    pub enum OtelLogsPublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::publishing::emitter_modes! {
    pub enum OtelTracesPublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::publishing::emitter_modes! {
    pub enum OtelMetricsPublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}
