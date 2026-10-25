//! The OpenTelemetry METRICS emitter entity.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The entity's typed configuration and connector declaration.
//! - **Depends on.** The connector contract and private OTLP encoder.
//! - **Must not know.** Graphs, Models, branches or host scheduling.

#![cfg_attr(nervix_lint, nervix::layer(engine, reason = "owns one OTLP entity"))]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "receives a typed entity configuration")
)]

use async_trait::async_trait;
use nervix_connector::{
    EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterRecordId, EmitterRowRequest,
    EmitterStartResult, EmitterWriteResult, MappedEmitterRows, PerRecordOutcome, RowRequestEmitter,
    RowRequestPreparation,
};
use nervix_primitives::time::Instant;

use super::{
    OtelExportConfig, OtelMetric,
    otlp::{OtlpEncoder, OtlpSignal},
};

/// The resolved parameters of one metrics emitter.
pub struct OtelMetricsEmitterConfig {
    pub export: OtelExportConfig,
    pub metric: OtelMetric,
}

/// Exports mapped rows as OTLP metrics.
pub struct OtelMetricsEmitter(OtlpEncoder);

impl OtelMetricsEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Metrics);

    pub fn new(config: OtelMetricsEmitterConfig, _host: EmitterHost) -> EmitterStartResult<Self> {
        OtlpEncoder::new(
            config
                .export
                .into_encoder(OtlpSignal::Metric(config.metric)),
            _host,
        )
        .map(Self)
    }
}

#[async_trait]
impl EmitterLifecycle for OtelMetricsEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        self.0.finish(deadline).await
    }
}

#[async_trait]
impl RowRequestEmitter for OtelMetricsEmitter {
    async fn prepare(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> EmitterWriteResult<RowRequestPreparation> {
        self.0.prepare(rows).await
    }
    async fn publish(
        &mut self,
        requests: Vec<EmitterRowRequest>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        self.0.publish(requests).await
    }
}
