use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

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
    pub physical_node_id: String,
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
        self.messages_total as f64 / self.batches_total as f64
    }

    fn validate(&self) -> Result<(), MetricsReportError> {
        let target = format!("{} '{}'", self.target_kind, self.target);
        if self.batches_total == 0 {
            return Err(MetricsReportError::InvalidReport {
                reason: format!("{target} has no observed batches"),
            });
        }
        validate_percentiles(&target, self.p50, self.p90, self.p99)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RelayBufferMetrics {
    pub domain: String,
    pub relay: String,
    pub physical_node_id: String,
    pub direction: String,
    pub observations: u64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

impl RelayBufferMetrics {
    fn validate(&self) -> Result<(), MetricsReportError> {
        let target = format!("relay '{}'", self.relay);
        if self.observations == 0 {
            return Err(MetricsReportError::InvalidReport {
                reason: format!("{target} has no buffer observations"),
            });
        }
        validate_percentiles(&target, self.p50, self.p90, self.p99)
    }
}

#[derive(Debug, Error)]
pub enum MetricsReportError {
    #[error("invalid Prometheus sample on line {line}: {reason}")]
    InvalidPrometheusSample { line: usize, reason: String },

    #[error("Prometheus metric '{metric}' has a duplicate series for {target}")]
    DuplicateSeries {
        metric: &'static str,
        target: String,
    },

    #[error("target {target} is missing Prometheus metric '{metric}'")]
    MissingTargetMetric {
        metric: &'static str,
        target: String,
    },

    #[error("invalid Prometheus histogram '{metric}' for {target}: {reason}")]
    InvalidHistogram {
        metric: &'static str,
        target: String,
        reason: String,
    },

    #[error(
        "{quantile} for Prometheus histogram '{metric}' on {target} exceeds its largest finite \
         bucket {largest_finite}"
    )]
    HistogramQuantileOverflow {
        metric: &'static str,
        target: String,
        quantile: &'static str,
        largest_finite: f64,
    },

    #[error("scraped metrics contain no batch observations for Nervix runtime targets")]
    NoBatchTargets,

    #[error("scraped metrics contain no relay buffer observations")]
    NoRelayBuffers,

    #[error("invalid Nervix metrics report: {reason}")]
    InvalidReport { reason: String },

    #[error("failed to read Nervix metrics report {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to parse Nervix metrics report {path}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("failed to serialize Nervix metrics report")]
    Serialize(#[source] toml::ser::Error),

    #[error("failed to write Nervix metrics report {path}")]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SeriesKey {
    domain: String,
    target_kind: String,
    target: String,
    physical_node_id: String,
    direction: String,
    relay: String,
    peer_kind: String,
    peer: String,
}

impl SeriesKey {
    fn from_labels(
        mut labels: BTreeMap<String, String>,
        line: usize,
    ) -> Result<Self, MetricsReportError> {
        let mut take = |name: &'static str| {
            labels
                .remove(name)
                .ok_or_else(|| MetricsReportError::InvalidPrometheusSample {
                    line,
                    reason: format!("missing required label '{name}'"),
                })
        };
        let key = Self {
            domain: take(REQUIRED_LABELS[0])?,
            target_kind: take(REQUIRED_LABELS[1])?,
            target: take(REQUIRED_LABELS[2])?,
            physical_node_id: take(REQUIRED_LABELS[3])?,
            direction: take(REQUIRED_LABELS[4])?,
            relay: take(REQUIRED_LABELS[5])?,
            peer_kind: take(REQUIRED_LABELS[6])?,
            peer: take(REQUIRED_LABELS[7])?,
        };
        if !labels.is_empty() {
            return Err(MetricsReportError::InvalidPrometheusSample {
                line,
                reason: format!(
                    "unexpected labels: {}",
                    labels.keys().cloned().collect::<Vec<_>>().join(", ")
                ),
            });
        }
        Ok(key)
    }

    fn description(&self) -> String {
        format!(
            "{} '{}' direction '{}' relay '{}' on '{}'",
            self.target_kind, self.target, self.direction, self.relay, self.physical_node_id
        )
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
    fn insert_bucket(
        &mut self,
        metric: &'static str,
        key: &SeriesKey,
        upper_bound: f64,
        count: u64,
    ) -> Result<(), MetricsReportError> {
        if self
            .buckets
            .insert(OrderedFloat(upper_bound), count)
            .is_some()
        {
            return Err(MetricsReportError::DuplicateSeries {
                metric,
                target: key.description(),
            });
        }
        Ok(())
    }

    fn set_count(
        &mut self,
        metric: &'static str,
        key: &SeriesKey,
        count: u64,
    ) -> Result<(), MetricsReportError> {
        if self.count.replace(count).is_some() {
            return Err(MetricsReportError::DuplicateSeries {
                metric,
                target: key.description(),
            });
        }
        Ok(())
    }

    fn summarize(
        &self,
        bucket_metric: &'static str,
        count_metric: &'static str,
        key: &SeriesKey,
    ) -> Result<(u64, HistogramPercentiles), MetricsReportError> {
        let target = key.description();
        let count = self
            .count
            .ok_or_else(|| MetricsReportError::MissingTargetMetric {
                metric: count_metric,
                target: target.clone(),
            })?;
        if count == 0 {
            return Err(MetricsReportError::InvalidHistogram {
                metric: bucket_metric,
                target,
                reason: "histogram count is zero".to_string(),
            });
        }
        let Some(infinite_count) = self.buckets.get(&OrderedFloat(f64::INFINITY)).copied() else {
            return Err(MetricsReportError::InvalidHistogram {
                metric: bucket_metric,
                target,
                reason: "missing +Inf bucket".to_string(),
            });
        };
        if infinite_count != count {
            return Err(MetricsReportError::InvalidHistogram {
                metric: bucket_metric,
                target,
                reason: format!("+Inf bucket count {infinite_count} does not equal _count {count}"),
            });
        }
        let mut previous = 0;
        let mut largest_finite = None;
        for (upper_bound, cumulative) in &self.buckets {
            if *cumulative < previous {
                return Err(MetricsReportError::InvalidHistogram {
                    metric: bucket_metric,
                    target,
                    reason: format!(
                        "bucket {} count {} is below the preceding cumulative count {previous}",
                        upper_bound.0, cumulative
                    ),
                });
            }
            previous = *cumulative;
            if upper_bound.is_finite() {
                largest_finite = Some(upper_bound.0);
            }
        }
        let Some(largest_finite) = largest_finite else {
            return Err(MetricsReportError::InvalidHistogram {
                metric: bucket_metric,
                target,
                reason: "histogram has no finite buckets".to_string(),
            });
        };
        let percentile = |quantile, name| {
            self.quantile_upper_bound(bucket_metric, key, count, quantile, name, largest_finite)
        };
        Ok((
            count,
            HistogramPercentiles {
                p50: percentile(0.50, "p50")?,
                p90: percentile(0.90, "p90")?,
                p99: percentile(0.99, "p99")?,
            },
        ))
    }

    fn quantile_upper_bound(
        &self,
        metric: &'static str,
        key: &SeriesKey,
        count: u64,
        quantile: f64,
        quantile_name: &'static str,
        largest_finite: f64,
    ) -> Result<f64, MetricsReportError> {
        let rank = (count as f64 * quantile).ceil() as u64;
        for (upper_bound, cumulative) in &self.buckets {
            if *cumulative >= rank {
                if upper_bound.is_finite() {
                    return Ok(upper_bound.0);
                }
                return Err(MetricsReportError::HistogramQuantileOverflow {
                    metric,
                    target: key.description(),
                    quantile: quantile_name,
                    largest_finite,
                });
            }
        }
        Err(MetricsReportError::InvalidHistogram {
            metric,
            target: key.description(),
            reason: format!("no bucket contains {quantile_name} rank {rank}"),
        })
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
    fn parse(input: &str, domain: &str) -> Result<Self, MetricsReportError> {
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
            let sample = PrometheusSample::parse(line, line_number)?;
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
    ) -> Result<(), MetricsReportError> {
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
                let value = sample.labels.remove("le").ok_or_else(|| {
                    MetricsReportError::InvalidPrometheusSample {
                        line,
                        reason: format!("metric '{metric}' is missing label 'le'"),
                    }
                })?;
                Some(parse_bucket_bound(&value, line)?)
            } else {
                None
            };
        let value = sample.count(line)?;
        let key = SeriesKey::from_labels(sample.labels, line)?;
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
                    upper_bound.expect("histogram bucket has an upper bound"),
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
                    upper_bound.expect("histogram bucket has an upper bound"),
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

    fn into_report(self) -> Result<NervixMetricsReport, MetricsReportError> {
        for (key, batches) in &self.batches_total {
            if *batches > 0
                && key.target_kind != "RELAY"
                && !self.messages_per_batch.contains_key(key)
            {
                return Err(MetricsReportError::MissingTargetMetric {
                    metric: MESSAGES_PER_BATCH_BUCKET,
                    target: key.description(),
                });
            }
        }

        let mut batch_targets = Vec::new();
        for (key, histogram) in self.messages_per_batch {
            if key.target_kind == "RELAY" {
                continue;
            }
            let target = key.description();
            let messages_total = self.messages_total.get(&key).copied().ok_or_else(|| {
                MetricsReportError::MissingTargetMetric {
                    metric: MESSAGES_TOTAL,
                    target: target.clone(),
                }
            })?;
            let batches_total = self.batches_total.get(&key).copied().ok_or_else(|| {
                MetricsReportError::MissingTargetMetric {
                    metric: BATCHES_TOTAL,
                    target: target.clone(),
                }
            })?;
            let (histogram_count, percentiles) =
                histogram.summarize(MESSAGES_PER_BATCH_BUCKET, MESSAGES_PER_BATCH_COUNT, &key)?;
            if histogram_count != batches_total {
                return Err(MetricsReportError::InvalidHistogram {
                    metric: MESSAGES_PER_BATCH_BUCKET,
                    target,
                    reason: format!(
                        "histogram count {histogram_count} does not equal batches_total \
                         {batches_total}"
                    ),
                });
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
            return Err(MetricsReportError::NoBatchTargets);
        }
        if relay_buffers.is_empty() {
            return Err(MetricsReportError::NoRelayBuffers);
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
    fn parse(line: &str, line_number: usize) -> Result<Self, MetricsReportError> {
        let (metric, value) = split_metric_and_value(line, line_number)?;
        let (name, labels) = parse_metric(metric, line_number)?;
        let value = parse_prometheus_number(value, line_number)?;
        Ok(Self {
            name: name.to_string(),
            labels,
            value,
        })
    }

    fn count(&self, line: usize) -> Result<u64, MetricsReportError> {
        if !self.value.is_finite()
            || self.value < 0.0
            || self.value.fract() != 0.0
            || self.value > u64::MAX as f64
        {
            return Err(MetricsReportError::InvalidPrometheusSample {
                line,
                reason: format!(
                    "metric '{}' value '{}' is not a non-negative integer count",
                    self.name, self.value
                ),
            });
        }
        Ok(self.value as u64)
    }
}

impl NervixMetricsReport {
    pub fn from_prometheus(input: &str, domain: &str) -> Result<Self, MetricsReportError> {
        ScrapedMetrics::parse(input, domain)?.into_report()
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self, MetricsReportError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| MetricsReportError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let report =
            toml::from_str::<Self>(&contents).map_err(|source| MetricsReportError::Parse {
                path: path.to_path_buf(),
                source,
            })?;
        report.validate()?;
        Ok(report)
    }

    pub fn write(&self, path: impl AsRef<Path>) -> Result<(), MetricsReportError> {
        self.validate()?;
        let contents = toml::to_string_pretty(self).map_err(MetricsReportError::Serialize)?;
        let path = path.as_ref();
        fs::write(path, contents).map_err(|source| MetricsReportError::Write {
            path: path.to_path_buf(),
            source,
        })
    }

    fn validate(&self) -> Result<(), MetricsReportError> {
        if self.batch_targets.is_empty() {
            return Err(MetricsReportError::NoBatchTargets);
        }
        if self.relay_buffers.is_empty() {
            return Err(MetricsReportError::NoRelayBuffers);
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

fn validate_percentiles(
    target: &str,
    p50: f64,
    p90: f64,
    p99: f64,
) -> Result<(), MetricsReportError> {
    if [p50, p90, p99]
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
    {
        return Err(MetricsReportError::InvalidReport {
            reason: format!("{target} has non-finite or negative percentiles"),
        });
    }
    if p50 > p90 || p90 > p99 {
        return Err(MetricsReportError::InvalidReport {
            reason: format!("{target} percentiles are not monotonic"),
        });
    }
    Ok(())
}

fn insert_counter(
    counters: &mut BTreeMap<SeriesKey, u64>,
    metric: &'static str,
    key: SeriesKey,
    value: u64,
) -> Result<(), MetricsReportError> {
    let target = key.description();
    if counters.insert(key, value).is_some() {
        return Err(MetricsReportError::DuplicateSeries { metric, target });
    }
    Ok(())
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

fn split_metric_and_value(
    line: &str,
    line_number: usize,
) -> Result<(&str, &str), MetricsReportError> {
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
            '{' => braces = braces.saturating_add(1),
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
    Err(MetricsReportError::InvalidPrometheusSample {
        line: line_number,
        reason: "sample has no value".to_string(),
    })
}

fn parse_metric(
    metric: &str,
    line: usize,
) -> Result<(&str, BTreeMap<String, String>), MetricsReportError> {
    let Some(open) = metric.find('{') else {
        return Ok((metric, BTreeMap::new()));
    };
    if !metric.ends_with('}') {
        return Err(MetricsReportError::InvalidPrometheusSample {
            line,
            reason: "metric label set is not closed".to_string(),
        });
    }
    let name = &metric[..open];
    let labels = parse_labels(&metric[open + 1..metric.len() - 1], line)?;
    Ok((name, labels))
}

fn parse_labels(input: &str, line: usize) -> Result<BTreeMap<String, String>, MetricsReportError> {
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
            return Err(MetricsReportError::InvalidPrometheusSample {
                line,
                reason: "invalid label name or missing '='".to_string(),
            });
        }
        let name = &input[name_start..cursor];
        cursor += 1;
        if bytes.get(cursor) != Some(&b'"') {
            return Err(MetricsReportError::InvalidPrometheusSample {
                line,
                reason: format!("label '{name}' value is not quoted"),
            });
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
            return Err(MetricsReportError::InvalidPrometheusSample {
                line,
                reason: format!("label '{name}' value is not closed"),
            });
        };
        let value =
            serde_json::from_str::<String>(&input[value_start..value_end]).map_err(|error| {
                MetricsReportError::InvalidPrometheusSample {
                    line,
                    reason: format!("label '{name}' has invalid escaping: {error}"),
                }
            })?;
        if labels.insert(name.to_string(), value).is_some() {
            return Err(MetricsReportError::InvalidPrometheusSample {
                line,
                reason: format!("label '{name}' is duplicated"),
            });
        }
        cursor = value_end;
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor == bytes.len() {
            break;
        }
        if bytes[cursor] != b',' {
            return Err(MetricsReportError::InvalidPrometheusSample {
                line,
                reason: format!("label '{name}' is not followed by ','"),
            });
        }
        cursor += 1;
    }
    Ok(labels)
}

fn parse_prometheus_number(value: &str, line: usize) -> Result<f64, MetricsReportError> {
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        "NaN" => Ok(f64::NAN),
        value => value
            .parse()
            .map_err(|error| MetricsReportError::InvalidPrometheusSample {
                line,
                reason: format!("invalid sample value '{value}': {error}"),
            }),
    }
}

fn parse_bucket_bound(value: &str, line: usize) -> Result<f64, MetricsReportError> {
    let bound = parse_prometheus_number(value, line)?;
    if bound.is_nan() || bound < 0.0 {
        return Err(MetricsReportError::InvalidPrometheusSample {
            line,
            reason: format!("invalid histogram bucket bound '{value}'"),
        });
    }
    Ok(bound)
}
