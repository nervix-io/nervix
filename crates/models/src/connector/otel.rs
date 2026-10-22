//! The OpenTelemetry connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The OTEL client Model, the logs, traces and metrics sink Models with the value
//!   mappings they share and the metric a metrics sink declares, and what each entity declares.
//! - **Depends on.** The shared sink declarations and the names a body references.
//! - **Must not know.** The OTLP encoding, the client's configuration keys, or how a signal is
//!   exported.

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
    entity_union::connector_sinks,
};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm,
    OptionChoice, OptionDeclaration, OptionForm, OptionPresence, OptionValue, SinkBody,
    SinkClientReference, SinkEntity, SinkReference, SinkReferenceKind, SinkValueMapping,
    SinkValueMappings,
};

configured_client!(Otel: unpooled, config Nervix, CreateClientOtel => OtelConfigEntry);

pub type OtelValueMapping = SinkValueMapping;

/// What every OTEL sink maps a record to, whichever signal it exports: the signal's own fields,
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
    fn value_mappings(&self) -> SinkValueMappings<'_> {
        SinkValueMappings {
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
pub struct OtelLogsSink {
    pub mode: OtelLogsPublishingMode,
    pub client: ClientName,
    pub mappings: OtelMappings,
}

/// The spans an OTEL client exports.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelTracesSink {
    pub mode: OtelTracesPublishingMode,
    pub client: ClientName,
    pub mappings: OtelMappings,
}

/// The data points of one metric an OTEL client exports.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelMetricsSink {
    pub mode: OtelMetricsPublishingMode,
    pub client: ClientName,
    pub metric: OtelMetric,
    pub mappings: OtelMappings,
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
/// sink exports, chosen by its kind.
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
            omitted: OmittedDeadline::Unbounded,
        },
    }],
    driver_limits: &["gRPC responses above 4 MiB are refused."],
};

/// What every OTEL sink declares: an export is one request the collector answers, and its fields
/// are mapped directly rather than encoded by a codec.
const fn otel_capabilities(
    publishing_modes: &'static [crate::EmitterPublishingModeKind],
    options: &'static [OptionDeclaration],
) -> EmitSinkCapabilities {
    EmitSinkCapabilities {
        headers: None,
        reference: SinkReferenceKind::Client(ConnectorKind::Otel),
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

macro_rules! otel_sink_declarations {
    ($($Sink:ident($Mode:ident) => $options:expr),+ $(,)?) => {
        $(
            impl SinkEntity for $Sink {
                const CAPABILITIES: EmitSinkCapabilities = otel_capabilities($Mode::KINDS, $options);
            }

            impl SinkBody for $Sink {
                fn reference(&self) -> SinkReference<'_> {
                    SinkReference::Client(SinkClientReference {
                        client: &self.client,
                        connector: ConnectorKind::Otel,
                    })
                }

                fn client_mut(&mut self) -> Option<&mut ClientName> {
                    Some(&mut self.client)
                }

                fn value_mappings(&self) -> SinkValueMappings<'_> {
                    self.mappings.value_mappings()
                }
            }
        )+
    };
}

otel_sink_declarations!(
    OtelLogsSink(OtelLogsPublishingMode) => &[ATTRIBUTES, RESOURCE, SCOPE],
    OtelTracesSink(OtelTracesPublishingMode) => &[ATTRIBUTES, RESOURCE, SCOPE],
    OtelMetricsSink(OtelMetricsPublishingMode) => &[METRIC, ATTRIBUTES, RESOURCE, SCOPE],
);

connector_sinks! {
    connector Otel = "OTEL";
    /// What an OTEL sink writes.
    pub enum OtelSink;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum OtelEmitterMode;
    /// The entities an OTEL sink writes: one per signal.
    pub enum OtelSinkEntity {
        Logs(OtelLogsSink, OtelLogsPublishingMode) = "LOGS";
        Traces(OtelTracesSink, OtelTracesPublishingMode) = "TRACES";
        Metrics(OtelMetricsSink, OtelMetricsPublishingMode) = "METRICS";
    }
}

impl OtelSink {
    pub fn client(&self) -> &ClientName {
        match self {
            Self::Logs(sink) => &sink.client,
            Self::Traces(sink) => &sink.client,
            Self::Metrics(sink) => &sink.client,
        }
    }

    /// The mappings every signal shares.
    pub fn mappings(&self) -> &OtelMappings {
        match self {
            Self::Logs(sink) => &sink.mappings,
            Self::Traces(sink) => &sink.mappings,
            Self::Metrics(sink) => &sink.mappings,
        }
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
