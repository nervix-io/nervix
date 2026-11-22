//! The trace export capability options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the node's trace export capabilities: the sample ratio,
//!   the export timeout and interval, the span queue and the export batch; their conversion into
//!   the capability group, and the rendering of that group back into options.
//! - **Depends on.** The vocabulary's trace export definitions and the edge's value forms.
//! - **Must not know.** How traces are exported, or the collector a node is provisioned with.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_models::{
    NodeOperationalError, SampleRatio, SettingDefinition as _, TraceExportBatchSpans,
    TraceExportCapabilities, TraceExportCapabilityValues, TraceExportInterval, TraceExportTimeout,
    TraceQueueCapacity, TraceSampleRatio,
};

use super::settings::{CommandLine, capability_help};
use crate::application::cli_values::{HumanDuration, parse_trace_sample_ratio};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Trace export")]
pub struct TraceExportArgs {
    #[arg(
        long = TraceSampleRatio::NAME,
        env = "NERVIX_OTEL_TRACE_SAMPLE_RATIO",
        value_parser = parse_trace_sample_ratio,
        help = capability_help::<TraceSampleRatio>(
            "Share of root traces the node exports, parent-based, from 0 through 1",
            TraceSampleRatio::DEFAULT,
        )
    )]
    pub otel_trace_sample_ratio: Option<SampleRatio>,
    #[arg(
        long = TraceExportTimeout::NAME,
        env = "NERVIX_OTEL_TRACE_EXPORT_TIMEOUT",
        help = capability_help::<TraceExportTimeout>(
            "Limit on reaching the collector and on each export request",
            HumanDuration(TraceExportTimeout::DEFAULT),
        )
    )]
    pub otel_trace_export_timeout: Option<HumanDuration>,
    #[arg(
        long = TraceExportInterval::NAME,
        env = "NERVIX_OTEL_TRACE_EXPORT_INTERVAL",
        help = capability_help::<TraceExportInterval>(
            "Longest a span waits for its export batch; a full batch exports at once",
            HumanDuration(TraceExportInterval::DEFAULT),
        )
    )]
    pub otel_trace_export_interval: Option<HumanDuration>,
    #[arg(
        long = TraceQueueCapacity::NAME,
        env = "NERVIX_OTEL_TRACE_QUEUE_CAPACITY",
        help = capability_help::<TraceQueueCapacity>(
            "Spans the node holds for export; a span that finds the queue full is dropped",
            TraceQueueCapacity::DEFAULT,
        )
    )]
    pub otel_trace_queue_capacity: Option<NonZeroU64>,
    #[arg(
        long = TraceExportBatchSpans::NAME,
        env = "NERVIX_OTEL_TRACE_EXPORT_BATCH_SPANS",
        help = capability_help::<TraceExportBatchSpans>(
            "Most spans one export request carries",
            TraceExportBatchSpans::DEFAULT,
        )
    )]
    pub otel_trace_export_batch_spans: Option<NonZeroU64>,
}

impl TraceExportArgs {
    pub(super) fn settings(&self) -> Result<TraceExportCapabilities, Report<NodeOperationalError>> {
        TraceExportCapabilities::resolve(TraceExportCapabilityValues {
            sample_ratio: self.otel_trace_sample_ratio,
            export_timeout: self.otel_trace_export_timeout.map(|value| value.0),
            export_interval: self.otel_trace_export_interval.map(|value| value.0),
            queue_capacity: self.otel_trace_queue_capacity,
            export_batch_spans: self.otel_trace_export_batch_spans,
        })
    }

    pub(super) fn from_settings(capabilities: &TraceExportCapabilities) -> Self {
        Self {
            otel_trace_sample_ratio: Some(capabilities.sample_ratio()),
            otel_trace_export_timeout: Some(HumanDuration(capabilities.export_timeout())),
            otel_trace_export_interval: Some(HumanDuration(capabilities.export_interval())),
            otel_trace_queue_capacity: Some(capabilities.queue_capacity()),
            otel_trace_export_batch_spans: Some(capabilities.export_batch_spans()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(TraceSampleRatio::NAME, self.otel_trace_sample_ratio);
        command_line.optional(TraceExportTimeout::NAME, self.otel_trace_export_timeout);
        command_line.optional(TraceExportInterval::NAME, self.otel_trace_export_interval);
        command_line.optional(TraceQueueCapacity::NAME, self.otel_trace_queue_capacity);
        command_line.optional(
            TraceExportBatchSpans::NAME,
            self.otel_trace_export_batch_spans,
        );
    }
}
