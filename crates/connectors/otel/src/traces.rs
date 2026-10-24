//! The OpenTelemetry TRACES emitter entity.
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
    OtelExportConfig,
    otlp::{OtlpEncoder, OtlpSignal},
};

/// The resolved parameters of one traces emitter.
pub struct OtelTracesEmitterConfig {
    pub export: OtelExportConfig,
}

/// Exports mapped rows as OTLP traces.
pub struct OtelTracesEmitter(OtlpEncoder);

impl OtelTracesEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Traces);

    pub fn new(config: OtelTracesEmitterConfig, _host: EmitterHost) -> EmitterStartResult<Self> {
        OtlpEncoder::new(config.export.into_encoder(OtlpSignal::Traces), _host).map(Self)
    }
}

#[async_trait]
impl EmitterLifecycle for OtelTracesEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        self.0.finish(deadline).await
    }
}

#[async_trait]
impl RowRequestEmitter for OtelTracesEmitter {
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
