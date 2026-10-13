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

use super::{configured_client::configured_client, entity_union::connector_sinks};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, SinkBody, SinkEntity, SinkReference, SinkValueMapping,
    SinkValueMappings,
};

configured_client!(Otel: unpooled CreateClientOtel => OtelConfigEntry);

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
    pub client: ClientName,
    pub mappings: OtelMappings,
}

/// The spans an OTEL client exports.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelTracesSink {
    pub client: ClientName,
    pub mappings: OtelMappings,
}

/// The data points of one metric an OTEL client exports.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelMetricsSink {
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

/// What every OTEL sink declares: an export is one request the collector answers, and its fields
/// are mapped directly rather than encoded by a codec.
const OTEL_SINK_CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
    writes_headers: false,
    requires_codec: false,
    batch_requirement: EmitterBatchRequirement::Optional,
    requires_batch_transformation: false,
    batch_size_maximum: None,
};

macro_rules! otel_sink_declarations {
    ($($Sink:ident),+ $(,)?) => {
        $(
            impl SinkEntity for $Sink {
                const CAPABILITIES: EmitSinkCapabilities = OTEL_SINK_CAPABILITIES;

                fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
                    matches!(mode, EmitterPublishingMode::RequestAck { .. })
                }
            }

            impl SinkBody for $Sink {
                fn reference(&self) -> SinkReference<'_> {
                    SinkReference::Client {
                        client: &self.client,
                        connector: ConnectorKind::Otel,
                    }
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

otel_sink_declarations!(OtelLogsSink, OtelTracesSink, OtelMetricsSink);

connector_sinks! {
    connector Otel = "OTEL";
    /// What an OTEL sink writes.
    pub enum OtelSink;
    /// The entities an OTEL sink writes: one per signal.
    pub enum OtelSinkEntity {
        Logs(OtelLogsSink) = "LOGS";
        Traces(OtelTracesSink) = "TRACES";
        Metrics(OtelMetricsSink) = "METRICS";
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
