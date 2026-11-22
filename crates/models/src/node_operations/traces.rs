//! The capabilities a node's own trace export runs with.
//!
//! Layer: vocabulary.
//! - **Owns.** The sample ratio, the export timeout and interval, the span queue and the export
//!   batch of a node's own trace export, the relationship between the batch and the queue, and
//!   their validated serialization.
//! - **Depends on.** Setting classes, the operational capability validation and validated
//!   serialization.
//! - **Must not know.** The exporter library, the collector or how spans are recorded.

use std::{fmt, num::NonZeroU64, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    rancor::{Fallible, Source},
};
use serde::{Deserialize, Serialize};

use super::{NodeOperationalError, validate_count, validate_duration};
use crate::{NodeCapabilities, Setting, SettingDefinition, settings::archive};

/// A share of root traces, from none at `0` to every one at `1`.
///
/// It keeps its value's bits, so two ratios compare and archive exactly. A ratio becomes a
/// setting only through [`TraceSampleRatio`]'s validation, which refuses NaN and every value
/// outside `0..=1`.
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
)]
#[serde(transparent)]
pub struct SampleRatio(u64);

impl SampleRatio {
    pub fn new(ratio: f64) -> Self {
        Self(ratio.to_bits())
    }

    pub fn value(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl fmt::Display for SampleRatio {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value(), formatter)
    }
}

/// The share of root traces the node exports, parent-based, between nothing and everything.
pub struct TraceSampleRatio;

impl TraceSampleRatio {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this setting definition owns its single compile-time default"
        )
    )]
    pub const DEFAULT: f64 = 1.0;
}

impl SettingDefinition for TraceSampleRatio {
    type Class = NodeCapabilities;
    type Value = SampleRatio;
    type Context = ();
    type Error = NodeOperationalError;
    const NAME: &'static str = "otel-trace-sample-ratio";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> SampleRatio {
        SampleRatio::new(Self::DEFAULT)
    }

    fn validate(ratio: &SampleRatio, _: &()) -> Result<(), Report<Self::Error>> {
        if (0.0..=1.0).contains(&ratio.value()) {
            return Ok(());
        }
        Err(Report::new(NodeOperationalError::SampleRatio {
            setting: Self::NAME,
            ratio: *ratio,
        }))
    }
}

capability_definition!(
    TraceExportTimeout,
    "otel-trace-export-timeout",
    Duration,
    Duration::from_secs(10)
);
capability_definition!(
    TraceExportInterval,
    "otel-trace-export-interval",
    Duration,
    Duration::from_secs(5)
);
capability_definition!(TraceQueueCapacity, "otel-trace-queue-capacity", Count, 2048);

/// The most spans one export request carries. It cannot exceed the span queue, because the queue
/// holds every span a batch takes.
pub struct TraceExportBatchSpans;

impl TraceExportBatchSpans {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this setting definition owns its single compile-time default"
        )
    )]
    pub const DEFAULT: u64 = 512;
}

impl SettingDefinition for TraceExportBatchSpans {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = Setting<TraceQueueCapacity>;
    type Error = NodeOperationalError;
    const NAME: &'static str = "otel-trace-export-batch-spans";
    const DERIVATION: Option<&'static str> = Some("at most the resolved otel-trace-queue-capacity");

    fn default_value(queue: &Self::Context) -> NonZeroU64 {
        NonZeroU64::new(Self::DEFAULT)
            .assured("the definition's default count is positive")
            .min(*queue.value())
    }

    fn validate(batch: &NonZeroU64, queue: &Self::Context) -> Result<(), Report<Self::Error>> {
        if batch > queue.value() {
            return Err(Report::new(NodeOperationalError::TraceExportBatch {
                batch_setting: Self::NAME,
                batch: batch.get(),
                queue_setting: TraceQueueCapacity::NAME,
                queue: queue.value().get(),
            }));
        }
        Ok(())
    }
}

/// How a node exports its own traces once it has a collector: which share of root traces it
/// samples, how long one export may take, how long a span waits for its batch, how many spans it
/// holds for export and how many one export request carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
#[serde(deny_unknown_fields)]
pub struct TraceExportCapabilities {
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    sample_ratio: Setting<TraceSampleRatio>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    export_timeout: Setting<TraceExportTimeout>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    export_interval: Setting<TraceExportInterval>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    queue_capacity: Setting<TraceQueueCapacity>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    export_batch_spans: Setting<TraceExportBatchSpans>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TraceExportCapabilityValues {
    pub sample_ratio: Option<SampleRatio>,
    pub export_timeout: Option<Duration>,
    pub export_interval: Option<Duration>,
    pub queue_capacity: Option<NonZeroU64>,
    pub export_batch_spans: Option<NonZeroU64>,
}

impl TraceExportCapabilities {
    pub fn resolve(
        values: TraceExportCapabilityValues,
    ) -> Result<Self, Report<NodeOperationalError>> {
        let queue_capacity = Setting::resolve(values.queue_capacity, &())?;
        let export_batch_spans = Setting::resolve(values.export_batch_spans, &queue_capacity)?;
        Ok(Self {
            sample_ratio: Setting::resolve(values.sample_ratio, &())?,
            export_timeout: Setting::resolve(values.export_timeout, &())?,
            export_interval: Setting::resolve(values.export_interval, &())?,
            queue_capacity,
            export_batch_spans,
        })
    }

    pub fn sample_ratio(&self) -> SampleRatio {
        *self.sample_ratio.value()
    }
    pub fn export_timeout(&self) -> Duration {
        *self.export_timeout.value()
    }
    pub fn export_interval(&self) -> Duration {
        *self.export_interval.value()
    }
    pub fn queue_capacity(&self) -> NonZeroU64 {
        *self.queue_capacity.value()
    }
    pub fn export_batch_spans(&self) -> NonZeroU64 {
        *self.export_batch_spans.value()
    }

    /// The span queue's capacity as the exporter's channel takes it.
    pub fn queued_spans(&self) -> usize {
        usize::try_from(self.queue_capacity().get())
            .assured("the count validation bounds the queue by the portable channel capacity")
    }

    /// The export batch as the exporter takes it.
    pub fn batched_spans(&self) -> usize {
        usize::try_from(self.export_batch_spans().get())
            .assured("the batch is at most the queue, which fits the portable channel capacity")
    }

    pub fn values(&self) -> TraceExportCapabilityValues {
        TraceExportCapabilityValues {
            sample_ratio: Some(self.sample_ratio()),
            export_timeout: Some(self.export_timeout()),
            export_interval: Some(self.export_interval()),
            queue_capacity: Some(self.queue_capacity()),
            export_batch_spans: Some(self.export_batch_spans()),
        }
    }

    pub fn described(&self) -> Vec<(&'static str, String)> {
        vec![
            (TraceSampleRatio::NAME, self.sample_ratio().to_string()),
            (
                TraceExportTimeout::NAME,
                crate::timing_text(self.export_timeout()),
            ),
            (
                TraceExportInterval::NAME,
                crate::timing_text(self.export_interval()),
            ),
            (TraceQueueCapacity::NAME, self.queue_capacity().to_string()),
            (
                TraceExportBatchSpans::NAME,
                self.export_batch_spans().to_string(),
            ),
        ]
    }
}

impl Default for TraceExportCapabilities {
    fn default() -> Self {
        Self::resolve(TraceExportCapabilityValues::default())
            .assured("the trace export defaults satisfy their relationship")
    }
}

impl<'de> Deserialize<'de> for TraceExportCapabilities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            sample_ratio: SampleRatio,
            export_timeout: Duration,
            export_interval: Duration,
            queue_capacity: NonZeroU64,
            export_batch_spans: NonZeroU64,
        }
        let fields = Fields::deserialize(deserializer)?;
        Self::resolve(TraceExportCapabilityValues {
            sample_ratio: Some(fields.sample_ratio),
            export_timeout: Some(fields.export_timeout),
            export_interval: Some(fields.export_interval),
            queue_capacity: Some(fields.queue_capacity),
            export_batch_spans: Some(fields.export_batch_spans),
        })
        .map_err(|error| D::Error::custom(format_args!("{error:#}")))
    }
}

impl<D: Fallible + ?Sized> RkyvDeserialize<TraceExportCapabilities, D>
    for ArchivedTraceExportCapabilities
where
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<TraceExportCapabilities, D::Error> {
        TraceExportCapabilities::resolve(TraceExportCapabilityValues {
            sample_ratio: Some(self.sample_ratio.deserialize(deserializer)?),
            export_timeout: Some(self.export_timeout.deserialize(deserializer)?),
            export_interval: Some(self.export_interval.deserialize(deserializer)?),
            queue_capacity: Some(self.queue_capacity.deserialize(deserializer)?),
            export_batch_spans: Some(self.export_batch_spans.deserialize(deserializer)?),
        })
        .map_err(|error| D::Error::new(error.into_error()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_exporter_values_a_node_ran_with_before_they_were_declared() {
        let capabilities = TraceExportCapabilities::default();
        assert_eq!(capabilities.sample_ratio(), SampleRatio::new(1.0));
        assert_eq!(capabilities.export_timeout(), Duration::from_secs(10));
        assert_eq!(capabilities.export_interval(), Duration::from_secs(5));
        assert_eq!(capabilities.queued_spans(), 2048);
        assert_eq!(capabilities.batched_spans(), 512);
        assert_eq!(
            capabilities.described(),
            vec![
                ("otel-trace-sample-ratio", "1".to_owned()),
                ("otel-trace-export-timeout", "10s".to_owned()),
                ("otel-trace-export-interval", "5s".to_owned()),
                ("otel-trace-queue-capacity", "2048".to_owned()),
                ("otel-trace-export-batch-spans", "512".to_owned()),
            ]
        );
    }

    #[test]
    fn an_omitted_batch_follows_a_queue_smaller_than_its_default() {
        let capabilities = TraceExportCapabilities::resolve(TraceExportCapabilityValues {
            queue_capacity: NonZeroU64::new(100),
            ..Default::default()
        })
        .assured("an omitted batch takes at most the queue");
        assert_eq!(capabilities.batched_spans(), 100);
    }

    #[test]
    fn a_batch_larger_than_the_queue_names_both_settings() {
        let refused = TraceExportCapabilities::resolve(TraceExportCapabilityValues {
            export_batch_spans: NonZeroU64::new(4096),
            ..Default::default()
        })
        .expect_err("a batch cannot take more spans than the queue holds");
        assert_eq!(
            refused.current_context(),
            &NodeOperationalError::TraceExportBatch {
                batch_setting: "otel-trace-export-batch-spans",
                batch: 4096,
                queue_setting: "otel-trace-queue-capacity",
                queue: 2048,
            }
        );
        assert_eq!(
            refused.current_context().to_string(),
            "otel-trace-export-batch-spans (4096) must not exceed otel-trace-queue-capacity (2048)"
        );
    }

    #[test]
    fn a_ratio_outside_nothing_and_everything_is_refused() {
        for ratio in [-0.5, 1.5, f64::NAN, f64::INFINITY] {
            let refused = TraceExportCapabilities::resolve(TraceExportCapabilityValues {
                sample_ratio: Some(SampleRatio::new(ratio)),
                ..Default::default()
            })
            .expect_err("only 0 through 1 is a share of traces");
            assert!(matches!(
                refused.current_context(),
                NodeOperationalError::SampleRatio {
                    setting: "otel-trace-sample-ratio",
                    ..
                }
            ));
        }
        for ratio in [0.0, 0.25, 1.0] {
            let accepted = TraceExportCapabilities::resolve(TraceExportCapabilityValues {
                sample_ratio: Some(SampleRatio::new(ratio)),
                ..Default::default()
            })
            .assured("0 through 1 is a share of traces");
            assert_eq!(accepted.sample_ratio().value().to_bits(), ratio.to_bits());
        }
    }

    #[test]
    fn decoding_checks_the_batch_against_the_queue_again() {
        let json = serde_json::json!({
            "sample_ratio": SampleRatio::new(0.5),
            "export_timeout": Duration::from_secs(7),
            "export_interval": Duration::from_secs(2),
            "queue_capacity": 16,
            "export_batch_spans": 32,
        });
        assert!(serde_json::from_value::<TraceExportCapabilities>(json).is_err());

        let capabilities = TraceExportCapabilities::resolve(TraceExportCapabilityValues {
            sample_ratio: Some(SampleRatio::new(0.5)),
            export_timeout: Some(Duration::from_secs(7)),
            export_interval: Some(Duration::from_secs(2)),
            queue_capacity: NonZeroU64::new(16),
            export_batch_spans: NonZeroU64::new(8),
        })
        .assured("the values satisfy every bound");
        let encoded = serde_json::to_vec(&capabilities).assured("capabilities encode");
        let decoded = serde_json::from_slice::<TraceExportCapabilities>(&encoded)
            .assured("valid capabilities decode");
        assert_eq!(decoded, capabilities);
        let archived =
            rkyv::to_bytes::<rkyv::rancor::Error>(&capabilities).assured("capabilities archive");
        let restored = rkyv::from_bytes::<TraceExportCapabilities, rkyv::rancor::Error>(&archived)
            .assured("a valid archive restores");
        assert_eq!(restored, capabilities);
    }
}
