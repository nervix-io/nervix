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
pub use catalog::{
    BenchmarkCatalog, BenchmarkError, BenchmarkFileError, KafkaRenderInputs, LoadedBenchmark,
    TemplateDiagnostic,
};
pub use comparison::{
    BenchmarkComparison, BenchmarkRunFailure, BenchmarkSuiteReport, ComparisonError,
    ImageIdentityError, LoadReportError,
};
pub use definition::{
    BenchmarkDefinition, BenchmarkDependency, ContainerImplementation, DefinitionError,
    Implementation, LoadConfiguration, LoadDuration, LoadSetting, LoadShape, NervixImplementation,
};
pub use kafka::{TopicProvisionError, provision_topics};
pub use metrics_report::{
    BatchTargetMetrics, HistogramError, MetricSeries, MetricsReportError,
    NERVIX_METRICS_PROMETHEUS_FILE, NERVIX_METRICS_REPORT_FILE, NervixMetricsReport,
    PercentileError, PrometheusSampleError, Quantile, RelayBufferMetrics, ReportEntry,
};
pub use settings::{ByteSizeError, ParameterValueError, RunSettings, SettingsError};
