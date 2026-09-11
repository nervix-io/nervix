//! The Nervix benchmark harness.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The benchmark catalog, the load definitions, running one implementation under a
//!   shared load, the metrics report each run produces, and the same-hardware A/B comparison
//!   between two arms.
//! - **Depends on.** Whatever it measures, plus `nervix-test-environment`.
//! - **Must not know.** How the systems it measures are built. Every implementation is driven
//!   through its public interface, which is what keeps a comparison fair.
//!
mod ab;
mod catalog;
mod comparison;
mod definition;
mod kafka;
mod metrics_report;
mod settings;

pub use ab::{AbArm, AbError, AbSummary};
pub use catalog::{BenchmarkCatalog, BenchmarkError, KafkaRenderInputs, LoadedBenchmark};
pub use comparison::{
    BenchmarkComparison, BenchmarkRunFailure, BenchmarkSuiteReport, ComparisonError,
};
pub use definition::{
    BenchmarkDefinition, BenchmarkDependency, ContainerImplementation, Implementation,
    LoadConfiguration, LoadDuration, LoadShape, NervixImplementation,
};
pub use kafka::provision_topics;
pub use metrics_report::{
    BatchTargetMetrics, MetricsReportError, NERVIX_METRICS_PROMETHEUS_FILE,
    NERVIX_METRICS_REPORT_FILE, NervixMetricsReport, RelayBufferMetrics,
};
pub use settings::{RunSettings, SettingsError};
