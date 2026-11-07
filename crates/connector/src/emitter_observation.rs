//! Payload preparation quantities reported through an emitter's opaque host.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Count-only preparation measurements and the service a native encoder reports through.
//! - **Depends on.** Integer quantities and the host-service contract.
//! - **Must not know.** Payload values, metric implementations, schedules or delivery state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the connector observation boundary declared above"
    )
)]

/// One newly prepared logical payload at its documented size boundary.
///
/// This is a preparation observation, independent of the destination's answer. A retained retry
/// does not prepare another payload. Driver framing and compression are outside these quantities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitterPayloadMeasurement {
    pub source_messages: u64,
    /// Exact bytes at the boundary to which the emitter's MAX SIZE applies.
    pub encoded_bytes: u64,
    /// Content after connector preparation, which may add an envelope around codec bytes.
    pub prepared_bytes: u64,
}

impl EmitterPayloadMeasurement {
    /// A native request or data file whose completed size is also its encoded size bound.
    pub const fn sized(source_messages: u64, bytes: u64) -> Self {
        Self {
            source_messages,
            encoded_bytes: bytes,
            prepared_bytes: bytes,
        }
    }
}

/// Native encoders report quantities they already measured; the host owns their metric children.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "reports prepared quantities without graph or label access"
    )
)]
pub trait EmitterPayloadObserver: Send + Sync + 'static {
    fn record_prepared_payload(&self, measurement: EmitterPayloadMeasurement);
    fn record_payload_subdivisions(&self, subdivisions: u64);
}
