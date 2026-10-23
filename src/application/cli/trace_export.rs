//! The trace export options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the OpenTelemetry trace export switch, its endpoint and
//!   service identity, and the trace sample ratio setting.
//! - **Depends on.** The trace sample ratio definition and the edge's ratio parser.
//! - **Must not know.** How traces are exported.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_models::SettingDefinition;

use crate::application::{cli_values::parse_trace_sample_ratio, tracing_setup::TraceSampleRatio};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Tracing")]
pub struct TracingArgs {
    #[arg(
        long,
        env = "NERVIX_OTEL_ENABLED",
        default_value_t = false,
        help = "Enable optional OpenTelemetry OTLP trace export"
    )]
    pub otel_enabled: bool,
    #[arg(
        long,
        env = "NERVIX_OTEL_OTLP_ENDPOINT",
        default_value = "http://127.0.0.1:4317",
        help = "OpenTelemetry OTLP gRPC endpoint used when trace export is enabled"
    )]
    pub otel_otlp_endpoint: String,
    #[arg(
        long,
        env = "NERVIX_OTEL_SERVICE_NAME",
        default_value = "nervix",
        help = "OpenTelemetry service name used when trace export is enabled"
    )]
    pub otel_service_name: String,
    #[arg(
        long = TraceSampleRatio::NAME,
        env = "NERVIX_OTEL_TRACE_SAMPLE_RATIO",
        default_value_t = TraceSampleRatio::default_value(&()),
        value_parser = parse_trace_sample_ratio,
        help = "OpenTelemetry parent-based trace sample ratio used when trace export is enabled"
    )]
    pub otel_trace_sample_ratio: f64,
}
