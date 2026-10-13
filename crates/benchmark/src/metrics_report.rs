use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Path, PathBuf},
};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_models::ClusterNodeName;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const NERVIX_METRICS_PROMETHEUS_FILE: &str = "nervix-metrics.prom";
pub const NERVIX_METRICS_REPORT_FILE: &str = "nervix-metrics.toml";

const MESSAGES_TOTAL: &str = "nervix_messages_total";
const BATCHES_TOTAL: &str = "nervix_batches_total";
const MESSAGES_PER_BATCH_BUCKET: &str = "nervix_messages_per_batch_bucket";
const MESSAGES_PER_BATCH_COUNT: &str = "nervix_messages_per_batch_count";
const RELAY_BUFFER_LEN_BUCKET: &str = "nervix_relay_buffer_len_bucket";
const RELAY_BUFFER_LEN_COUNT: &str = "nervix_relay_buffer_len_count";
/// How an absent optional label is spelled in the Prometheus exposition Nervix writes.
const ABSENT_LABEL: &str = "-";
const REQUIRED_LABELS: &[&str] = &[
    "domain",
    "target_kind",
    "target",
    "physical_node_id",
    "direction",
    "relay",
    "peer_kind",
    "peer",
];

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NervixMetricsReport {
    pub batch_targets: Vec<BatchTargetMetrics>,
    pub relay_buffers: Vec<RelayBufferMetrics>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BatchTargetMetrics {
    pub domain: String,
    pub target_kind: String,
    pub target: String,
    pub physical_node_id: Option<ClusterNodeName>,
    pub direction: String,
    pub relay: String,
    pub messages_total: u64,
    pub batches_total: u64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

impl BatchTargetMetrics {
    #[must_use]
    pub fn mean_messages_per_batch(&self) -> f64 {
        self.messages_total.approx_into::<f64>() / self.batches_total.approx_into::<f64>()
    }

    fn validate(&self) -> error_stack::Result<(), MetricsReportError> {
        if self.batches_total == 0 {
            return Err(Report::new(MetricsReportError::TargetWithoutBatches {
                target_kind: self.target_kind.clone(),
                target: self.target.clone(),
            }));
        }
        validate_percentiles(self.p50, self.p90, self.p99).change_context_lazy(|| {
            MetricsReportError::InvalidPercentiles {
                entry: ReportEntry::BatchTarget {
                    target_kind: self.target_kind.clone(),
                    target: self.target.clone(),
                },
            }
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RelayBufferMetrics {
    pub domain: String,
    pub relay: String,
    pub physical_node_id: Option<ClusterNodeName>,
    pub direction: String,
    pub observations: u64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

impl RelayBufferMetrics {
    fn validate(&self) -> error_stack::Result<(), MetricsReportError> {
        if self.observations == 0 {
            return Err(Report::new(MetricsReportError::RelayWithoutObservations {
                relay: self.relay.clone(),
            }));
        }
        validate_percentiles(self.p50, self.p90, self.p99).change_context_lazy(|| {
            MetricsReportError::InvalidPercentiles {
                entry: ReportEntry::RelayBuffer {
                    relay: self.relay.clone(),
                },
            }
        })
    }
}

#[derive(Debug, Error)]
pub enum MetricsReportError {
    /// The [`PrometheusSampleError`] beneath says what is wrong with the line.
    #[error("invalid Prometheus sample on line {line}")]
    InvalidPrometheusSample { line: usize },

    #[error("Prometheus metric '{metric}' has a duplicate series for {series}")]
    DuplicateSeries {
        metric: &'static str,
        series: MetricSeries,
    },

    #[error("target {series} is missing Prometheus metric '{metric}'")]
    MissingTargetMetric {
        metric: &'static str,
        series: MetricSeries,
    },

    /// The [`HistogramError`] beneath says which property of the histogram does not hold.
    #[error("invalid Prometheus histogram '{metric}' for {series}")]
    InvalidHistogram {
        metric: &'static str,
        series: MetricSeries,
    },

    #[error(
        "{quantile} for Prometheus histogram '{metric}' on {series} exceeds its largest finite \
         bucket {largest_finite}"
    )]
    HistogramQuantileOverflow {
        metric: &'static str,
        series: MetricSeries,
        quantile: Quantile,
        largest_finite: f64,
    },

    #[error("scraped metrics contain no batch observations for Nervix runtime targets")]
    NoBatchTargets,

    #[error("scraped metrics contain no relay buffer observations")]
    NoRelayBuffers,

    #[error("{target_kind} '{target}' has no observed batches")]
    TargetWithoutBatches { target_kind: String, target: String },

    #[error("relay '{relay}' has no buffer observations")]
    RelayWithoutObservations { relay: String },

    /// The [`PercentileError`] beneath says which property of the percentiles does not hold.
    #[error("{entry} has invalid percentiles")]
    InvalidPercentiles { entry: ReportEntry },

    #[error("failed to read Nervix metrics report {path}")]
    Read { path: PathBuf },

    #[error("failed to parse Nervix metrics report {path}")]
    Parse { path: PathBuf },

    #[error("Nervix metrics report {path} is invalid")]
    Invalid { path: PathBuf },

    #[error("failed to serialize Nervix metrics report")]
    Serialize,

    #[error("failed to write Nervix metrics report {path}")]
    Write { path: PathBuf },
}

/// What is wrong with one Prometheus sample line, beneath the
/// [`MetricsReportError::InvalidPrometheusSample`] that names the line.
#[derive(Debug, Error)]
pub enum PrometheusSampleError {
    #[error("sample has no value")]
    MissingValue,

    #[error("metric label set is not closed")]
    UnclosedLabelSet,

    #[error("invalid label name or missing '='")]
    InvalidLabelName,

    #[error("label '{label}' value is not quoted")]
    UnquotedLabelValue { label: String },

    #[error("label '{label}' value is not closed")]
    UnclosedLabelValue { label: String },

    /// The JSON string decoder's failure is beneath.
    #[error("label '{label}' has invalid escaping")]
    InvalidLabelEscaping { label: String },

    #[error("label '{label}' is duplicated")]
    DuplicateLabel { label: String },

    #[error("label '{label}' is not followed by ','")]
    MissingLabelSeparator { label: String },

    #[error("missing required label '{label}'")]
    MissingLabel { label: &'static str },

    #[error("unexpected labels: {}", .labels.join(", "))]
    UnexpectedLabels { labels: Vec<String> },

    /// The cluster node name's validation failure is beneath.
    #[error("label 'physical_node_id' value '{value}' is not a cluster node name")]
    InvalidPhysicalNode { value: String },

    #[error("metric '{metric}' is missing label 'le'")]
    MissingBucketBound { metric: &'static str },

    /// The number parser's failure is beneath.
    #[error("invalid sample value '{value}'")]
    InvalidValue { value: String },

    #[error("invalid histogram bucket bound '{value}'")]
    InvalidBucketBound { value: String },

    #[error("metric '{metric}' value '{value}' is not a non-negative integer count")]
    NotACount { metric: String, value: f64 },
}

/// Which property of a scraped histogram does not hold, beneath the
/// [`MetricsReportError::InvalidHistogram`] that names the histogram.
#[derive(Debug, Error)]
pub enum HistogramError {
    #[error("histogram count is zero")]
    ZeroCount,

    #[error("missing +Inf bucket")]
    MissingInfiniteBucket,

    #[error("+Inf bucket count {infinite_count} does not equal _count {count}")]
    InfiniteBucketMismatch { infinite_count: u64, count: u64 },

    #[error(
        "bucket {upper_bound} count {cumulative} is below the preceding cumulative count \
         {previous}"
    )]
    DecreasingBucket {
        upper_bound: f64,
        cumulative: u64,
        previous: u64,
    },

    #[error("histogram has no finite buckets")]
    NoFiniteBuckets,

    #[error("no bucket contains {quantile} rank {rank}")]
    MissingRank { quantile: Quantile, rank: u64 },

    #[error("histogram count {histogram_count} does not equal batches_total {batches_total}")]
    BatchCountMismatch {
        histogram_count: u64,
        batches_total: u64,
    },
}

/// Which property of an entry's percentiles does not hold, beneath the
/// [`MetricsReportError::InvalidPercentiles`] that names the entry.
#[derive(Debug, Error)]
pub enum PercentileError {
    #[error("a percentile is non-finite or negative")]
    NonFinite,

    #[error("p50, p90 and p99 are not monotonic")]
    NotMonotonic,
}

/// A percentile the metrics report derives from a histogram.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::Display)]
pub enum Quantile {
    #[strum(serialize = "p50")]
    P50,
    #[strum(serialize = "p90")]
    P90,
    #[strum(serialize = "p99")]
    P99,
}

impl Quantile {
    fn fraction(self) -> f64 {
        match self {
            Self::P50 => 0.50,
            Self::P90 => 0.90,
            Self::P99 => 0.99,
        }
    }
}

/// The Nervix runtime target one scraped series describes, by the labels that tell it apart
/// within its domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricSeries {
    pub target_kind: String,
    pub target: String,
    pub direction: String,
    pub relay: String,
    pub physical_node_id: Option<ClusterNodeName>,
}

impl fmt::Display for MetricSeries {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let physical_node = match &self.physical_node_id {
            Some(physical_node) => physical_node.as_str(),
            None => ABSENT_LABEL,
        };
        write!(
            formatter,
            "{} '{}' direction '{}' relay '{}' on '{physical_node}'",
            self.target_kind, self.target, self.direction, self.relay,
        )
    }
}

/// One entry of a metrics report, as its validation names it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReportEntry {
    BatchTarget { target_kind: String, target: String },
    RelayBuffer { relay: String },
}

impl fmt::Display for ReportEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BatchTarget {
                target_kind,
                target,
            } => write!(formatter, "{target_kind} '{target}'"),
            Self::RelayBuffer { relay } => write!(formatter, "relay '{relay}'"),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SeriesKey {
    domain: String,
    target_kind: String,
    target: String,
    physical_node_id: Option<ClusterNodeName>,
    direction: String,
    relay: String,
    peer_kind: String,
    peer: String,
}

impl SeriesKey {
    fn from_labels(
        mut labels: BTreeMap<String, String>,
    ) -> error_stack::Result<Self, PrometheusSampleError> {
        let mut take = |label: &'static str| {
            labels
                .remove(label)
                .ok_or_else(|| Report::new(PrometheusSampleError::MissingLabel { label }))
        };
        let domain = take(REQUIRED_LABELS[0])?;
        let target_kind = take(REQUIRED_LABELS[1])?;
        let target = take(REQUIRED_LABELS[2])?;
        // Nervix writes `-` for a series it observed without a placed owner; any other value names
        // a cluster node.
        let physical_node_id = match take(REQUIRED_LABELS[3])? {
            node if node == ABSENT_LABEL => None,
            node => match ClusterNodeName::parse(&node) {
                Ok(name) => Some(name),
                Err(report) => {
                    return Err(report.change_context(
                        PrometheusSampleError::InvalidPhysicalNode { value: node },
                    ));
                }
            },
        };
        let direction = take(REQUIRED_LABELS[4])?;
        let relay = take(REQUIRED_LABELS[5])?;
        let peer_kind = take(REQUIRED_LABELS[6])?;
        let peer = take(REQUIRED_LABELS[7])?;
        if !labels.is_empty() {
            let unexpected = labels.into_keys().collect();
            return Err(Report::new(PrometheusSampleError::UnexpectedLabels {
                labels: unexpected,
            }));
        }
        Ok(Self {
            domain,
            target_kind,
            target,
            physical_node_id,
            direction,
            relay,
            peer_kind,
            peer,
        })
    }

    fn series(&self) -> MetricSeries {
        MetricSeries {
            target_kind: self.target_kind.clone(),
            target: self.target.clone(),
            direction: self.direction.clone(),
            relay: self.relay.clone(),
            physical_node_id: self.physical_node_id.clone(),
        }
    }

    fn into_batch_target(
        self,
        messages_total: u64,
        batches_total: u64,
        percentiles: HistogramPercentiles,
    ) -> BatchTargetMetrics {
        BatchTargetMetrics {
            domain: self.domain,
            target_kind: self.target_kind,
            target: self.target,
            physical_node_id: self.physical_node_id,
            direction: self.direction,
            relay: self.relay,
            messages_total,
            batches_total,
            p50: percentiles.p50,
            p90: percentiles.p90,
            p99: percentiles.p99,
        }
    }

    fn into_relay_buffer(
        self,
        observations: u64,
        percentiles: HistogramPercentiles,
    ) -> RelayBufferMetrics {
        RelayBufferMetrics {
            domain: self.domain,
            relay: self.relay,
            physical_node_id: self.physical_node_id,
            direction: self.direction,
            observations,
            p50: percentiles.p50,
            p90: percentiles.p90,
            p99: percentiles.p99,
        }
    }
}

#[derive(Default)]
struct Histogram {
    buckets: BTreeMap<OrderedFloat<f64>, u64>,
    count: Option<u64>,
}

impl Histogram {
    fn merge_max(&mut self, other: Self) {
        for (upper_bound, count) in other.buckets {
            self.buckets
                .entry(upper_bound)
                .and_modify(|current| *current = (*current).max(count))
                .or_insert(count);
        }
        if let Some(count) = other.count {
            self.count = Some(self.count.unwrap_or_default().max(count));
        }
    }

    fn insert_bucket(
        &mut self,
        metric: &'static str,
        key: &SeriesKey,
        upper_bound: f64,
        count: u64,
    ) -> error_stack::Result<(), MetricsReportError> {
        if self
            .buckets
            .insert(OrderedFloat(upper_bound), count)
            .is_some()
        {
            return Err(Report::new(MetricsReportError::DuplicateSeries {
                metric,
                series: key.series(),
            }));
        }
        Ok(())
    }

    fn set_count(
        &mut self,
        metric: &'static str,
        key: &SeriesKey,
        count: u64,
    ) -> error_stack::Result<(), MetricsReportError> {
        if self.count.replace(count).is_some() {
            return Err(Report::new(MetricsReportError::DuplicateSeries {
                metric,
                series: key.series(),
            }));
        }
        Ok(())
    }

    fn summarize(
        &self,
        bucket_metric: &'static str,
        count_metric: &'static str,
        key: &SeriesKey,
    ) -> error_stack::Result<(u64, HistogramPercentiles), MetricsReportError> {
        let Some(count) = self.count else {
            return Err(Report::new(MetricsReportError::MissingTargetMetric {
                metric: count_metric,
                series: key.series(),
            }));
        };
        let invalid = || MetricsReportError::InvalidHistogram {
            metric: bucket_metric,
            series: key.series(),
        };
        if count == 0 {
            return Err(Report::new(HistogramError::ZeroCount).change_context(invalid()));
        }
        let Some(infinite_count) = self.buckets.get(&OrderedFloat(f64::INFINITY)).copied() else {
            return Err(
                Report::new(HistogramError::MissingInfiniteBucket).change_context(invalid())
            );
        };
        if infinite_count != count {
            return Err(Report::new(HistogramError::InfiniteBucketMismatch {
                infinite_count,
                count,
            })
            .change_context(invalid()));
        }
        let mut previous = 0;
        let mut largest_finite = None;
        for (upper_bound, cumulative) in &self.buckets {
            if *cumulative < previous {
                return Err(Report::new(HistogramError::DecreasingBucket {
                    upper_bound: upper_bound.0,
                    cumulative: *cumulative,
                    previous,
                })
                .change_context(invalid()));
            }
            previous = *cumulative;
            if upper_bound.is_finite() {
                largest_finite = Some(upper_bound.0);
            }
        }
        let Some(largest_finite) = largest_finite else {
            return Err(Report::new(HistogramError::NoFiniteBuckets).change_context(invalid()));
        };
        let p50 =
            self.quantile_upper_bound(bucket_metric, key, count, Quantile::P50, largest_finite)?;
        let p90 =
            self.quantile_upper_bound(bucket_metric, key, count, Quantile::P90, largest_finite)?;
        let p99 =
            self.quantile_upper_bound(bucket_metric, key, count, Quantile::P99, largest_finite)?;
        Ok((count, HistogramPercentiles { p50, p90, p99 }))
    }

    fn quantile_upper_bound(
        &self,
        metric: &'static str,
        key: &SeriesKey,
        count: u64,
        quantile: Quantile,
        largest_finite: f64,
    ) -> error_stack::Result<f64, MetricsReportError> {
        let rank: u64 = (count.approx_into::<f64>() * quantile.fraction())
            .ceil()
            .checked_approx_into()
            .unwrap_or(u64::MAX);
        for (upper_bound, cumulative) in &self.buckets {
            if *cumulative >= rank {
                if upper_bound.is_finite() {
                    return Ok(upper_bound.0);
                }
                return Err(Report::new(MetricsReportError::HistogramQuantileOverflow {
                    metric,
                    series: key.series(),
                    quantile,
                    largest_finite,
                }));
            }
        }
        Err(
            Report::new(HistogramError::MissingRank { quantile, rank }).change_context(
                MetricsReportError::InvalidHistogram {
                    metric,
                    series: key.series(),
                },
            ),
        )
    }
}

struct HistogramPercentiles {
    p50: f64,
    p90: f64,
    p99: f64,
}

#[derive(Default)]
struct ScrapedMetrics {
    messages_total: BTreeMap<SeriesKey, u64>,
    batches_total: BTreeMap<SeriesKey, u64>,
    messages_per_batch: BTreeMap<SeriesKey, Histogram>,
    relay_buffer_len: BTreeMap<SeriesKey, Histogram>,
}

impl ScrapedMetrics {
    fn parse(input: &str, domain: &str) -> error_stack::Result<Self, MetricsReportError> {
        let mut metrics = Self::default();
        for (index, raw_line) in input.lines().enumerate() {
            let line_number = index + 1;
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let name = metric_name(line);
            if !is_report_metric(name) {
                continue;
            }
            let sample = PrometheusSample::parse(line).change_context(
                MetricsReportError::InvalidPrometheusSample { line: line_number },
            )?;
            if sample.labels.get("domain").map(String::as_str) != Some(domain) {
                continue;
            }
            metrics.insert(sample, line_number)?;
        }
        Ok(metrics)
    }

    fn insert(
        &mut self,
        mut sample: PrometheusSample,
        line: usize,
    ) -> error_stack::Result<(), MetricsReportError> {
        let invalid = || MetricsReportError::InvalidPrometheusSample { line };
        let metric = match sample.name.as_str() {
            MESSAGES_TOTAL => MESSAGES_TOTAL,
            BATCHES_TOTAL => BATCHES_TOTAL,
            MESSAGES_PER_BATCH_BUCKET => MESSAGES_PER_BATCH_BUCKET,
            MESSAGES_PER_BATCH_COUNT => MESSAGES_PER_BATCH_COUNT,
            RELAY_BUFFER_LEN_BUCKET => RELAY_BUFFER_LEN_BUCKET,
            RELAY_BUFFER_LEN_COUNT => RELAY_BUFFER_LEN_COUNT,
            _ => unreachable!("only report metrics reach insertion"),
        };
        let upper_bound =
            if metric == MESSAGES_PER_BATCH_BUCKET || metric == RELAY_BUFFER_LEN_BUCKET {
                let Some(value) = sample.labels.remove("le") else {
                    return Err(
                        Report::new(PrometheusSampleError::MissingBucketBound { metric })
                            .change_context(invalid()),
                    );
                };
                Some(parse_bucket_bound(&value).change_context_lazy(invalid)?)
            } else {
                None
            };
        let value = sample.count().change_context_lazy(invalid)?;
        let key = SeriesKey::from_labels(sample.labels).change_context_lazy(invalid)?;
        match metric {
            MESSAGES_TOTAL => insert_counter(&mut self.messages_total, metric, key, value),
            BATCHES_TOTAL => insert_counter(&mut self.batches_total, metric, key, value),
            MESSAGES_PER_BATCH_BUCKET => self
                .messages_per_batch
                .entry(key.clone())
                .or_default()
                .insert_bucket(
                    metric,
                    &key,
                    upper_bound.verified(
                        "this branch only runs for a bucket sample, which always parses an upper \
                         bound",
                    ),
                    value,
                ),
            MESSAGES_PER_BATCH_COUNT => self
                .messages_per_batch
                .entry(key.clone())
                .or_default()
                .set_count(metric, &key, value),
            RELAY_BUFFER_LEN_BUCKET => self
                .relay_buffer_len
                .entry(key.clone())
                .or_default()
                .insert_bucket(
                    metric,
                    &key,
                    upper_bound.verified(
                        "this branch only runs for a bucket sample, which always parses an upper \
                         bound",
                    ),
                    value,
                ),
            RELAY_BUFFER_LEN_COUNT => self
                .relay_buffer_len
                .entry(key.clone())
                .or_default()
                .set_count(metric, &key, value),
            _ => unreachable!("all report metrics are handled"),
        }
    }

    fn merge_max(&mut self, other: Self) {
        merge_counter_max(&mut self.messages_total, other.messages_total);
        merge_counter_max(&mut self.batches_total, other.batches_total);
        merge_histogram_max(&mut self.messages_per_batch, other.messages_per_batch);
        merge_histogram_max(&mut self.relay_buffer_len, other.relay_buffer_len);
    }

    fn into_report(self) -> error_stack::Result<NervixMetricsReport, MetricsReportError> {
        for (key, batches) in &self.batches_total {
            if *batches > 0
                && key.target_kind != "RELAY"
                && !self.messages_per_batch.contains_key(key)
            {
                return Err(Report::new(MetricsReportError::MissingTargetMetric {
                    metric: MESSAGES_PER_BATCH_BUCKET,
                    series: key.series(),
                }));
            }
        }

        let mut batch_targets = Vec::new();
        for (key, histogram) in self.messages_per_batch {
            if key.target_kind == "RELAY" {
                continue;
            }
            let Some(messages_total) = self.messages_total.get(&key).copied() else {
                return Err(Report::new(MetricsReportError::MissingTargetMetric {
                    metric: MESSAGES_TOTAL,
                    series: key.series(),
                }));
            };
            let Some(batches_total) = self.batches_total.get(&key).copied() else {
                return Err(Report::new(MetricsReportError::MissingTargetMetric {
                    metric: BATCHES_TOTAL,
                    series: key.series(),
                }));
            };
            let (histogram_count, percentiles) =
                histogram.summarize(MESSAGES_PER_BATCH_BUCKET, MESSAGES_PER_BATCH_COUNT, &key)?;
            if histogram_count != batches_total {
                return Err(Report::new(HistogramError::BatchCountMismatch {
                    histogram_count,
                    batches_total,
                })
                .change_context(MetricsReportError::InvalidHistogram {
                    metric: MESSAGES_PER_BATCH_BUCKET,
                    series: key.series(),
                }));
            }
            batch_targets.push(key.into_batch_target(messages_total, batches_total, percentiles));
        }

        let mut relay_buffers = Vec::new();
        for (key, histogram) in self.relay_buffer_len {
            let (observations, percentiles) =
                histogram.summarize(RELAY_BUFFER_LEN_BUCKET, RELAY_BUFFER_LEN_COUNT, &key)?;
            relay_buffers.push(key.into_relay_buffer(observations, percentiles));
        }
        if batch_targets.is_empty() {
            return Err(Report::new(MetricsReportError::NoBatchTargets));
        }
        if relay_buffers.is_empty() {
            return Err(Report::new(MetricsReportError::NoRelayBuffers));
        }
        let report = NervixMetricsReport {
            batch_targets,
            relay_buffers,
        };
        report.validate()?;
        Ok(report)
    }
}

struct PrometheusSample {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

impl PrometheusSample {
    fn parse(line: &str) -> error_stack::Result<Self, PrometheusSampleError> {
        let (metric, value) = split_metric_and_value(line)?;
        let (name, labels) = parse_metric(metric)?;
        let value = parse_prometheus_number(value)?;
        Ok(Self {
            name: name.to_string(),
            labels,
            value,
        })
    }

    fn count(&self) -> error_stack::Result<u64, PrometheusSampleError> {
        if self.value.fract() != 0.0 {
            return Err(self.not_a_count());
        }
        self.value
            .checked_approx_into()
            .ok_or_else(|| self.not_a_count())
    }

    fn not_a_count(&self) -> Report<PrometheusSampleError> {
        Report::new(PrometheusSampleError::NotACount {
            metric: self.name.clone(),
            value: self.value,
        })
    }
}

impl NervixMetricsReport {
    pub fn from_prometheus(
        input: &str,
        domain: &str,
    ) -> error_stack::Result<Self, MetricsReportError> {
        ScrapedMetrics::parse(input, domain)?.into_report()
    }

    pub fn from_prometheus_scrapes<'a>(
        inputs: impl IntoIterator<Item = &'a str>,
        domain: &str,
    ) -> error_stack::Result<Self, MetricsReportError> {
        let mut combined = ScrapedMetrics::default();
        for input in inputs {
            let scrape = ScrapedMetrics::parse(input, domain)?;
            // Every node exposes zero-filled series for the complete graph as well as the local
            // node's live counters. Select the greatest observation for each node-labelled series
            // so a cluster scrape neither loses the owner nor counts its replicas more than once.
            combined.merge_max(scrape);
        }
        combined.into_report()
    }

    pub fn read(path: impl AsRef<Path>) -> error_stack::Result<Self, MetricsReportError> {
        let path = path.as_ref();
        let contents =
            fs::read_to_string(path).change_context_lazy(|| MetricsReportError::Read {
                path: path.to_path_buf(),
            })?;
        let report = toml::from_str::<Self>(&contents).change_context_lazy(|| {
            MetricsReportError::Parse {
                path: path.to_path_buf(),
            }
        })?;
        report
            .validate()
            .change_context_lazy(|| MetricsReportError::Invalid {
                path: path.to_path_buf(),
            })?;
        Ok(report)
    }

    pub fn write(&self, path: impl AsRef<Path>) -> error_stack::Result<(), MetricsReportError> {
        let path = path.as_ref();
        self.validate()
            .change_context_lazy(|| MetricsReportError::Invalid {
                path: path.to_path_buf(),
            })?;
        let contents =
            toml::to_string_pretty(self).change_context(MetricsReportError::Serialize)?;
        fs::write(path, contents).change_context_lazy(|| MetricsReportError::Write {
            path: path.to_path_buf(),
        })
    }

    fn validate(&self) -> error_stack::Result<(), MetricsReportError> {
        if self.batch_targets.is_empty() {
            return Err(Report::new(MetricsReportError::NoBatchTargets));
        }
        if self.relay_buffers.is_empty() {
            return Err(Report::new(MetricsReportError::NoRelayBuffers));
        }
        for target in &self.batch_targets {
            target.validate()?;
        }
        for relay in &self.relay_buffers {
            relay.validate()?;
        }
        Ok(())
    }
}

fn validate_percentiles(p50: f64, p90: f64, p99: f64) -> error_stack::Result<(), PercentileError> {
    if [p50, p90, p99]
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
    {
        return Err(Report::new(PercentileError::NonFinite));
    }
    if p50 > p90 || p90 > p99 {
        return Err(Report::new(PercentileError::NotMonotonic));
    }
    Ok(())
}

fn insert_counter(
    counters: &mut BTreeMap<SeriesKey, u64>,
    metric: &'static str,
    key: SeriesKey,
    value: u64,
) -> error_stack::Result<(), MetricsReportError> {
    let series = key.series();
    if counters.insert(key, value).is_some() {
        return Err(Report::new(MetricsReportError::DuplicateSeries {
            metric,
            series,
        }));
    }
    Ok(())
}

fn merge_counter_max(counters: &mut BTreeMap<SeriesKey, u64>, other: BTreeMap<SeriesKey, u64>) {
    for (key, value) in other {
        counters
            .entry(key)
            .and_modify(|current| *current = (*current).max(value))
            .or_insert(value);
    }
}

fn merge_histogram_max(
    histograms: &mut BTreeMap<SeriesKey, Histogram>,
    other: BTreeMap<SeriesKey, Histogram>,
) {
    for (key, histogram) in other {
        histograms.entry(key).or_default().merge_max(histogram);
    }
}

fn metric_name(line: &str) -> &str {
    line.split(|character: char| character == '{' || character.is_whitespace())
        .next()
        .unwrap_or_default()
}

fn is_report_metric(name: &str) -> bool {
    matches!(
        name,
        MESSAGES_TOTAL
            | BATCHES_TOTAL
            | MESSAGES_PER_BATCH_BUCKET
            | MESSAGES_PER_BATCH_COUNT
            | RELAY_BUFFER_LEN_BUCKET
            | RELAY_BUFFER_LEN_COUNT
    )
}

fn split_metric_and_value(line: &str) -> error_stack::Result<(&str, &str), PrometheusSampleError> {
    let mut braces = 0_u8;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in line.char_indices() {
        if quoted {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                quoted = false;
            }
            continue;
        }
        match character {
            '"' => quoted = true,
            '{' => {
                braces = braces
                    .checked_add(1)
                    .assured("the braces counted here belong to one line held in memory");
            }
            // An unbalanced closing brace belongs to no label set, so the depth stays at zero.
            '}' => braces = braces.saturating_sub(1),
            character if character.is_whitespace() && braces == 0 => {
                let value = line[index..].trim();
                let Some(value) = value.split_whitespace().next() else {
                    break;
                };
                return Ok((&line[..index], value));
            }
            _ => {}
        }
    }
    Err(Report::new(PrometheusSampleError::MissingValue))
}

fn parse_metric(
    metric: &str,
) -> error_stack::Result<(&str, BTreeMap<String, String>), PrometheusSampleError> {
    let Some(open) = metric.find('{') else {
        return Ok((metric, BTreeMap::new()));
    };
    if !metric.ends_with('}') {
        return Err(Report::new(PrometheusSampleError::UnclosedLabelSet));
    }
    let name = &metric[..open];
    let labels = parse_labels(&metric[open + 1..metric.len() - 1])?;
    Ok((name, labels))
}

fn parse_labels(
    input: &str,
) -> error_stack::Result<BTreeMap<String, String>, PrometheusSampleError> {
    let bytes = input.as_bytes();
    let mut labels = BTreeMap::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let name_start = cursor;
        while cursor < bytes.len()
            && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
        {
            cursor += 1;
        }
        if cursor == name_start || bytes.get(cursor) != Some(&b'=') {
            return Err(Report::new(PrometheusSampleError::InvalidLabelName));
        }
        let name = &input[name_start..cursor];
        cursor += 1;
        if bytes.get(cursor) != Some(&b'"') {
            return Err(Report::new(PrometheusSampleError::UnquotedLabelValue {
                label: name.to_string(),
            }));
        }
        let value_start = cursor;
        cursor += 1;
        let mut escaped = false;
        let mut value_end = None;
        while cursor < bytes.len() {
            if escaped {
                escaped = false;
            } else if bytes[cursor] == b'\\' {
                escaped = true;
            } else if bytes[cursor] == b'"' {
                value_end = Some(cursor + 1);
                break;
            }
            cursor += 1;
        }
        let Some(value_end) = value_end else {
            return Err(Report::new(PrometheusSampleError::UnclosedLabelValue {
                label: name.to_string(),
            }));
        };
        let value = serde_json::from_str::<String>(&input[value_start..value_end])
            .change_context_lazy(|| PrometheusSampleError::InvalidLabelEscaping {
                label: name.to_string(),
            })?;
        if labels.insert(name.to_string(), value).is_some() {
            return Err(Report::new(PrometheusSampleError::DuplicateLabel {
                label: name.to_string(),
            }));
        }
        cursor = value_end;
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor == bytes.len() {
            break;
        }
        if bytes[cursor] != b',' {
            return Err(Report::new(PrometheusSampleError::MissingLabelSeparator {
                label: name.to_string(),
            }));
        }
        cursor += 1;
    }
    Ok(labels)
}

fn parse_prometheus_number(value: &str) -> error_stack::Result<f64, PrometheusSampleError> {
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        "NaN" => Ok(f64::NAN),
        value => value
            .parse::<f64>()
            .change_context_lazy(|| PrometheusSampleError::InvalidValue {
                value: value.to_string(),
            }),
    }
}

fn parse_bucket_bound(value: &str) -> error_stack::Result<f64, PrometheusSampleError> {
    let bound = parse_prometheus_number(value)?;
    if bound.is_nan() || bound < 0.0 {
        return Err(Report::new(PrometheusSampleError::InvalidBucketBound {
            value: value.to_string(),
        }));
    }
    Ok(bound)
}
