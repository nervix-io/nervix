//! OpenTelemetry LOGS, TRACES and METRICS emitter entities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One typed emitter and configuration per OTLP entity, over a private encoder.
//! - **Depends on.** The connector contract, vocabulary values and the private OTLP engine.
//! - **Must not know.** Models, graphs, branches, schedules or host retry cadence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns typed OTLP entities")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies typed entity plans")
)]

pub mod logs;
pub mod metrics;
mod otlp;
pub mod traces;

pub use logs::{OtelLogsEmitter, OtelLogsEmitterConfig};
pub use metrics::{OtelMetricsEmitter, OtelMetricsEmitterConfig};
pub use otlp::{
    OtelAggregationTemporality, OtelLiteral, OtelMetric, OtelMetricKind, OtelResourceAttribute,
    OtelScope,
};
pub use traces::{OtelTracesEmitter, OtelTracesEmitterConfig};

/// Parameters common to every OTLP export, without an entity selector.
pub struct OtelExportConfig {
    pub config: Vec<nervix_models::ClientConfigEntry>,
    pub dns: nervix_dns::DnsResolver,
    pub batch: Option<nervix_models::EmitterBatchPolicy>,
    pub values: Vec<String>,
    pub attributes: Vec<String>,
    pub resource: Vec<OtelResourceAttribute>,
    pub scope: Option<OtelScope>,
    pub mapped_schema: nervix_primitives::sync::StdArc<arrow_schema::Schema>,
}

impl OtelExportConfig {
    fn into_encoder(self, signal: otlp::OtlpSignal) -> otlp::OtlpEncoderConfig {
        let Self {
            config,
            dns,
            batch,
            values,
            attributes,
            resource,
            scope,
            mapped_schema,
        } = self;
        otlp::OtlpEncoderConfig {
            config,
            dns,
            signal,
            batch,
            values,
            attributes,
            resource,
            scope,
            mapped_schema,
        }
    }
}
