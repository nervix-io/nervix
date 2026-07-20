use std::{
    cmp::Ordering,
    collections::VecDeque,
    sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use hdrhistogram::Histogram as HdrHistogram;
use nervix_dataflow_graph::{DataflowBranchStatistics, DataflowMetricRef, DataflowStatistics};
use nervix_models::{Domain, Identifier, ModelKind, Timestamp};
use parking_lot::Mutex;
use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
    core::{Collector, Desc},
    proto::MetricFamily,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats};
use triomphe::Arc;

const MESSAGES_TOTAL: &str = "messages_total";
const BATCHES_TOTAL: &str = "batches_total";
const BYTES_TOTAL: &str = "bytes_total";
const MESSAGES_PER_BATCH: &str = "messages_per_batch";
const DELIVERY_LATENCY_SECONDS: &str = "delivery_latency_seconds";
const RELAY_BUFFER_LEN: &str = "relay_buffer_len";
const JEMALLOC_SUBSYSTEM: &str = "jemalloc";
const DOMAIN_TARGET_KIND: &str = "DOMAIN";
const DOMAIN_INPUT_OUTPUT_TARGET: &str = "input_output";
const DOMAIN_PROCESSED_TARGET: &str = "processed";
const MESSAGE_BATCH_BUCKETS: &[f64] = &[1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0];
const LATENCY_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0];
const RELAY_BUFFER_LEN_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0,
];
const INTERNAL_MESSAGE_BATCH_BUCKETS: &[f64] = &[
    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 12.0, 15.0, 20.0, 30.0, 40.0, 50.0, 75.0,
    100.0, 150.0, 200.0, 300.0, 500.0, 750.0, 1000.0,
];
const INTERNAL_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0,
];
const PROMETHEUS_LABELS: &[&str] = &[
    "domain",
    "target_kind",
    "target",
    "physical_node_id",
    "direction",
    "relay",
    "peer_kind",
    "peer",
];
const NO_DOMAIN_TIMESTAMP: i64 = i64::MIN;
const NO_HISTOGRAM_CAPACITY: u64 = u64::MAX;
const ONE_MINUTE_SECONDS: f64 = 60.0;
const FIFTEEN_MINUTES_SECONDS: f64 = 15.0 * 60.0;
const RATE_DECAY_TAU_FRACTION: f64 = 20.0;
const WALL_HISTOGRAM_1M_STEP: Duration = Duration::from_secs(10);
const WALL_HISTOGRAM_15M_STEP: Duration = Duration::from_secs(60);
const DOMAIN_HISTOGRAM_1M_STEP: Duration = Duration::from_secs(10);
const DOMAIN_HISTOGRAM_15M_STEP: Duration = Duration::from_secs(60);
const HISTOGRAM_VALUE_SCALE: f64 = 1_000.0;
const HISTOGRAM_DISPLAY_DECIMAL_SCALE: f64 = 10.0;
const HDR_HISTOGRAM_SIGFIG: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MetricKey {
    domain: String,
    target_kind: String,
    target: String,
    physical_node_id: String,
    relay: String,
    peer_kind: String,
    peer: String,
    direction: String,
    metric: &'static str,
}

impl MetricKey {
    fn relay(
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        direction: &'static str,
        metric: &'static str,
    ) -> Self {
        Self {
            domain: domain.as_str().to_string(),
            target_kind: "RELAY".to_string(),
            target: relay.as_str().to_string(),
            physical_node_id: physical_node_id.unwrap_or("-").to_string(),
            relay: relay.as_str().to_string(),
            peer_kind: String::new(),
            peer: String::new(),
            direction: direction.to_string(),
            metric,
        }
    }

    fn node(
        domain: &Domain,
        kind: ModelKind,
        node: &Identifier,
        physical_node_id: Option<&str>,
        relay: &Identifier,
        direction: &'static str,
        metric: &'static str,
    ) -> Self {
        Self {
            domain: domain.as_str().to_string(),
            target_kind: kind.as_str().to_ascii_uppercase(),
            target: node.as_str().to_string(),
            physical_node_id: physical_node_id.unwrap_or("-").to_string(),
            relay: relay.as_str().to_string(),
            peer_kind: "RELAY".to_string(),
            peer: relay.as_str().to_string(),
            direction: direction.to_string(),
            metric,
        }
    }

    fn node_without_stream(
        domain: &Domain,
        kind: ModelKind,
        node: &Identifier,
        physical_node_id: Option<&str>,
        direction: &'static str,
        metric: &'static str,
    ) -> Self {
        Self {
            domain: domain.as_str().to_string(),
            target_kind: kind.as_str().to_ascii_uppercase(),
            target: node.as_str().to_string(),
            physical_node_id: physical_node_id.unwrap_or("-").to_string(),
            relay: "-".to_string(),
            peer_kind: String::new(),
            peer: String::new(),
            direction: direction.to_string(),
            metric,
        }
    }

    fn matches_dataflow_metric_ref(&self, domain: &Domain, metric: &DataflowMetricRef) -> bool {
        self.domain == domain.as_str()
            && self.target_kind.eq_ignore_ascii_case(&metric.target_kind)
            && self.target == metric.target
            && self.direction == metric.direction
            && self.relay == metric.relay.as_deref().unwrap_or("-")
    }

    fn is_relay_buffer_len(&self) -> bool {
        self.metric == RELAY_BUFFER_LEN
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct BranchMetricKey {
    branch_key: String,
    key: MetricKey,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct WallEmaSnapshot {
    value: Option<f64>,
    last_elapsed_seconds: Option<f64>,
    last_at_wall_nanos: Option<i64>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct DomainEmaSnapshot {
    value: Option<f64>,
    last_at_nanos: Option<i64>,
}

#[derive(Debug)]
struct WallEma {
    tau_seconds: f64,
    value: Option<f64>,
    last_at: Option<Instant>,
}

impl WallEma {
    fn new(tau_seconds: f64) -> Self {
        Self {
            tau_seconds,
            value: None,
            last_at: None,
        }
    }

    fn from_snapshot(
        snapshot: &WallEmaSnapshot,
        series_started_at: Instant,
        tau_seconds: f64,
    ) -> Self {
        Self {
            tau_seconds,
            value: snapshot.value,
            last_at: snapshot
                .last_at_wall_nanos
                .and_then(instant_from_wall_unix_nanos)
                .or_else(|| {
                    snapshot
                        .last_elapsed_seconds
                        .map(|elapsed| instant_from_series_elapsed(series_started_at, elapsed))
                }),
        }
    }

    fn observe_rate_delta(&mut self, delta: u64, now: Instant) {
        let Some(last_at) = self.last_at.replace(now) else {
            return;
        };
        let elapsed_seconds = now.duration_since(last_at).as_secs_f64();
        if elapsed_seconds <= 0.0 {
            return;
        }
        self.observe_sample(delta as f64 / elapsed_seconds, elapsed_seconds);
    }

    fn observe_sample(&mut self, sample: f64, elapsed_seconds: f64) {
        let Some(current) = self.value else {
            self.value = Some(sample);
            return;
        };
        let alpha = time_decay_alpha(elapsed_seconds, self.tau_seconds);
        self.value = Some(current + alpha * (sample - current));
    }

    fn value_at(&self, now: Instant) -> Option<f64> {
        let value = self.value?;
        let last_at = self.last_at?;
        let elapsed_seconds = now.duration_since(last_at).as_secs_f64();
        Some(value * decay_factor(elapsed_seconds, self.tau_seconds))
    }

    fn to_snapshot(&self, series_started_at: Instant) -> WallEmaSnapshot {
        WallEmaSnapshot {
            value: self.value,
            last_elapsed_seconds: self
                .last_at
                .map(|last_at| last_at.duration_since(series_started_at).as_secs_f64()),
            last_at_wall_nanos: self.last_at.and_then(wall_unix_nanos_from_instant),
        }
    }
}

#[derive(Debug)]
struct DomainEma {
    tau_seconds: f64,
    value: Option<f64>,
    last_at_nanos: Option<i64>,
}

impl DomainEma {
    fn new(tau_seconds: f64) -> Self {
        Self {
            tau_seconds,
            value: None,
            last_at_nanos: None,
        }
    }

    fn from_snapshot(snapshot: &DomainEmaSnapshot, tau_seconds: f64) -> Self {
        Self {
            tau_seconds,
            value: snapshot.value,
            last_at_nanos: snapshot.last_at_nanos,
        }
    }

    fn observe_rate_delta(&mut self, delta: u64, now: Timestamp) {
        let now = now.unix_nanos();
        let Some(last_at) = self.last_at_nanos.replace(now) else {
            return;
        };
        let Some(elapsed_nanos) = now.checked_sub(last_at) else {
            return;
        };
        if elapsed_nanos <= 0 {
            return;
        }
        let elapsed_seconds = elapsed_nanos as f64 / 1_000_000_000.0;
        self.observe_sample(delta as f64 / elapsed_seconds, elapsed_seconds);
    }

    fn observe_sample(&mut self, sample: f64, elapsed_seconds: f64) {
        let Some(current) = self.value else {
            self.value = Some(sample);
            return;
        };
        let alpha = time_decay_alpha(elapsed_seconds, self.tau_seconds);
        self.value = Some(current + alpha * (sample - current));
    }

    fn value_at(&self, now: Option<Timestamp>) -> Option<f64> {
        let value = self.value?;
        let last_at = self.last_at_nanos?;
        let now = now?.unix_nanos();
        let elapsed_nanos = now.checked_sub(last_at)?;
        if elapsed_nanos < 0 {
            return Some(value);
        }
        let elapsed_seconds = elapsed_nanos as f64 / 1_000_000_000.0;
        Some(value * decay_factor(elapsed_seconds, self.tau_seconds))
    }

    fn to_snapshot(&self) -> DomainEmaSnapshot {
        DomainEmaSnapshot {
            value: self.value,
            last_at_nanos: self.last_at_nanos,
        }
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct RollingRatesSnapshot {
    wall_1m: WallEmaSnapshot,
    wall_15m: WallEmaSnapshot,
    domain_1m: DomainEmaSnapshot,
    domain_15m: DomainEmaSnapshot,
}

#[derive(Debug)]
struct RollingRates {
    wall_1m: WallEma,
    wall_15m: WallEma,
    domain_1m: DomainEma,
    domain_15m: DomainEma,
}

impl RollingRates {
    fn new() -> Self {
        Self {
            wall_1m: WallEma::new(rate_decay_tau_seconds(ONE_MINUTE_SECONDS)),
            wall_15m: WallEma::new(rate_decay_tau_seconds(FIFTEEN_MINUTES_SECONDS)),
            domain_1m: DomainEma::new(rate_decay_tau_seconds(ONE_MINUTE_SECONDS)),
            domain_15m: DomainEma::new(rate_decay_tau_seconds(FIFTEEN_MINUTES_SECONDS)),
        }
    }

    fn from_snapshot(snapshot: Option<&RollingRatesSnapshot>, series_started_at: Instant) -> Self {
        let Some(snapshot) = snapshot else {
            return Self::new();
        };
        Self {
            wall_1m: WallEma::from_snapshot(
                &snapshot.wall_1m,
                series_started_at,
                rate_decay_tau_seconds(ONE_MINUTE_SECONDS),
            ),
            wall_15m: WallEma::from_snapshot(
                &snapshot.wall_15m,
                series_started_at,
                rate_decay_tau_seconds(FIFTEEN_MINUTES_SECONDS),
            ),
            domain_1m: DomainEma::from_snapshot(
                &snapshot.domain_1m,
                rate_decay_tau_seconds(ONE_MINUTE_SECONDS),
            ),
            domain_15m: DomainEma::from_snapshot(
                &snapshot.domain_15m,
                rate_decay_tau_seconds(FIFTEEN_MINUTES_SECONDS),
            ),
        }
    }

    fn observe(&mut self, delta: u64, domain_timestamp: Option<Timestamp>) {
        let now = Instant::now();
        self.wall_1m.observe_rate_delta(delta, now);
        self.wall_15m.observe_rate_delta(delta, now);
        if let Some(domain_timestamp) = domain_timestamp {
            self.domain_1m.observe_rate_delta(delta, domain_timestamp);
            self.domain_15m.observe_rate_delta(delta, domain_timestamp);
        }
    }

    fn summary(&self, domain_timestamp: Option<Timestamp>) -> RollingRateSummary {
        let now = Instant::now();
        RollingRateSummary {
            wall_1m_per_sec: self.wall_1m.value_at(now),
            wall_15m_per_sec: self.wall_15m.value_at(now),
            domain_1m_per_sec: self.domain_1m.value_at(domain_timestamp),
            domain_15m_per_sec: self.domain_15m.value_at(domain_timestamp),
        }
    }

    fn to_snapshot(&self, series_started_at: Instant) -> RollingRatesSnapshot {
        RollingRatesSnapshot {
            wall_1m: self.wall_1m.to_snapshot(series_started_at),
            wall_15m: self.wall_15m.to_snapshot(series_started_at),
            domain_1m: self.domain_1m.to_snapshot(),
            domain_15m: self.domain_15m.to_snapshot(),
        }
    }
}

#[derive(Debug, Clone)]
struct RollingRateSummary {
    wall_1m_per_sec: Option<f64>,
    wall_15m_per_sec: Option<f64>,
    domain_1m_per_sec: Option<f64>,
    domain_15m_per_sec: Option<f64>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct HdrRecordedValueSnapshot {
    value: u64,
    count: u64,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct RollingHistogramBucketSnapshot {
    start_at_nanos: i64,
    values: Vec<HdrRecordedValueSnapshot>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct WallRollingHistogramSnapshot {
    buckets: Vec<RollingHistogramBucketSnapshot>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct DomainRollingHistogramSnapshot {
    buckets: Vec<RollingHistogramBucketSnapshot>,
}

#[derive(Debug, Clone)]
struct HistogramBucket {
    start_at_nanos: i64,
    histogram: HdrHistogram<u64>,
}

#[derive(Debug, Clone, Copy)]
struct HistogramConfig {
    highest_trackable_value: u64,
    significant_figures: u8,
}

impl HistogramConfig {
    fn for_buckets(buckets: &'static [f64]) -> Self {
        let highest_bucket = buckets
            .iter()
            .copied()
            .filter(|bucket| bucket.is_finite() && *bucket > 0.0)
            .fold(1.0, f64::max);
        Self {
            highest_trackable_value: scaled_histogram_value(highest_bucket).max(2),
            significant_figures: HDR_HISTOGRAM_SIGFIG,
        }
    }

    fn new_histogram(self) -> HdrHistogram<u64> {
        HdrHistogram::<u64>::new_with_max(self.highest_trackable_value, self.significant_figures)
            .expect("valid internal histogram configuration")
    }
}

#[derive(Debug)]
struct TimeRollingHistogram {
    window: Duration,
    step: Duration,
    config: HistogramConfig,
    buckets: VecDeque<HistogramBucket>,
}

impl TimeRollingHistogram {
    fn new(window: Duration, step: Duration, buckets: &'static [f64]) -> Self {
        Self {
            window,
            step,
            config: HistogramConfig::for_buckets(buckets),
            buckets: VecDeque::new(),
        }
    }

    fn from_snapshot(
        snapshot: &[RollingHistogramBucketSnapshot],
        window: Duration,
        step: Duration,
        buckets: &'static [f64],
    ) -> Self {
        let config = HistogramConfig::for_buckets(buckets);
        let mut buckets = snapshot
            .iter()
            .map(|bucket| HistogramBucket {
                start_at_nanos: bucket.start_at_nanos,
                histogram: hdr_histogram_from_snapshot(&bucket.values, config),
            })
            .filter(|bucket| !bucket.histogram.is_empty())
            .collect::<Vec<_>>();
        buckets.sort_by_key(|bucket| bucket.start_at_nanos);
        Self {
            window,
            step,
            config,
            buckets: buckets.into(),
        }
    }

    fn observe_at(&mut self, value: f64, now_nanos: i64) {
        if !value.is_finite() || value < 0.0 {
            return;
        }
        let current_start = bucket_start(now_nanos, self.step);
        self.ensure_current_bucket(current_start);
        if let Some(bucket) = self
            .buckets
            .iter_mut()
            .find(|bucket| bucket.start_at_nanos == current_start)
        {
            let _ = bucket.histogram.record(scaled_histogram_value(value));
        }
    }

    fn summary_at(&self, now_nanos: i64) -> HistogramPercentileSummary {
        let current_start = bucket_start(now_nanos, self.step);
        let Some(oldest_start) = oldest_bucket_start(current_start, self.window, self.step) else {
            return HistogramPercentileSummary::empty();
        };
        let mut merged = self.config.new_histogram();
        for bucket in self.buckets.iter().filter(|bucket| {
            bucket.start_at_nanos >= oldest_start && bucket.start_at_nanos <= current_start
        }) {
            let _ = merged.add(&bucket.histogram);
        }
        HistogramPercentileSummary::from_histogram(&merged)
    }

    fn to_snapshot(&self) -> Vec<RollingHistogramBucketSnapshot> {
        self.buckets
            .iter()
            .map(|bucket| RollingHistogramBucketSnapshot {
                start_at_nanos: bucket.start_at_nanos,
                values: hdr_histogram_to_snapshot(&bucket.histogram),
            })
            .collect()
    }

    fn merge_from(&mut self, other: &Self) {
        for bucket in &other.buckets {
            if let Some(existing) = self
                .buckets
                .iter_mut()
                .find(|existing| existing.start_at_nanos == bucket.start_at_nanos)
            {
                let _ = existing.histogram.add(&bucket.histogram);
            } else {
                self.buckets.push_back(bucket.clone());
            }
        }
        self.buckets
            .make_contiguous()
            .sort_by_key(|bucket| bucket.start_at_nanos);
    }

    fn ensure_current_bucket(&mut self, current_start: i64) {
        let Some(last_start) = self.buckets.back().map(|bucket| bucket.start_at_nanos) else {
            self.buckets.push_back(HistogramBucket {
                start_at_nanos: current_start,
                histogram: self.config.new_histogram(),
            });
            return;
        };
        if current_start < last_start {
            return;
        }
        if current_start > last_start {
            self.buckets.push_back(HistogramBucket {
                start_at_nanos: current_start,
                histogram: self.config.new_histogram(),
            });
        }
        self.drop_expired(current_start);
    }

    fn drop_expired(&mut self, current_start: i64) {
        let Some(oldest_start) = oldest_bucket_start(current_start, self.window, self.step) else {
            self.buckets.clear();
            return;
        };
        while self
            .buckets
            .front()
            .is_some_and(|bucket| bucket.start_at_nanos < oldest_start)
        {
            self.buckets.pop_front();
        }
    }
}

#[derive(Debug)]
struct WallRollingHistogram {
    inner: TimeRollingHistogram,
}

impl WallRollingHistogram {
    fn new(window: Duration, step: Duration, buckets: &'static [f64]) -> Self {
        Self {
            inner: TimeRollingHistogram::new(window, step, buckets),
        }
    }

    fn from_snapshot(
        snapshot: &WallRollingHistogramSnapshot,
        window: Duration,
        step: Duration,
        buckets: &'static [f64],
    ) -> Self {
        Self {
            inner: TimeRollingHistogram::from_snapshot(&snapshot.buckets, window, step, buckets),
        }
    }

    fn observe(&mut self, value: f64) {
        if let Some(now_nanos) = current_wall_unix_nanos() {
            self.inner.observe_at(value, now_nanos);
        }
    }

    fn summary(&self) -> HistogramPercentileSummary {
        current_wall_unix_nanos()
            .map(|now| self.inner.summary_at(now))
            .unwrap_or_else(HistogramPercentileSummary::empty)
    }

    fn to_snapshot(&self) -> WallRollingHistogramSnapshot {
        WallRollingHistogramSnapshot {
            buckets: self.inner.to_snapshot(),
        }
    }
}

#[derive(Debug)]
struct DomainRollingHistogram {
    inner: TimeRollingHistogram,
}

impl DomainRollingHistogram {
    fn new(window: Duration, step: Duration, buckets: &'static [f64]) -> Self {
        Self {
            inner: TimeRollingHistogram::new(window, step, buckets),
        }
    }

    fn from_snapshot(
        snapshot: &DomainRollingHistogramSnapshot,
        window: Duration,
        step: Duration,
        buckets: &'static [f64],
    ) -> Self {
        Self {
            inner: TimeRollingHistogram::from_snapshot(&snapshot.buckets, window, step, buckets),
        }
    }

    fn observe(&mut self, value: f64, now: Timestamp) {
        self.inner.observe_at(value, now.unix_nanos());
    }

    fn summary(&self, now: Option<Timestamp>) -> Option<HistogramPercentileSummary> {
        Some(self.inner.summary_at(now?.unix_nanos()))
    }

    fn to_snapshot(&self) -> DomainRollingHistogramSnapshot {
        DomainRollingHistogramSnapshot {
            buckets: self.inner.to_snapshot(),
        }
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct RollingHistogramsSnapshot {
    wall_1m: WallRollingHistogramSnapshot,
    wall_15m: WallRollingHistogramSnapshot,
    domain_1m: DomainRollingHistogramSnapshot,
    domain_15m: DomainRollingHistogramSnapshot,
}

#[derive(Debug)]
struct RollingHistograms {
    wall_1m: WallRollingHistogram,
    wall_15m: WallRollingHistogram,
    domain_1m: DomainRollingHistogram,
    domain_15m: DomainRollingHistogram,
}

impl RollingHistograms {
    fn new(buckets: &'static [f64]) -> Self {
        Self {
            wall_1m: WallRollingHistogram::new(
                Duration::from_secs(ONE_MINUTE_SECONDS as u64),
                WALL_HISTOGRAM_1M_STEP,
                buckets,
            ),
            wall_15m: WallRollingHistogram::new(
                Duration::from_secs(FIFTEEN_MINUTES_SECONDS as u64),
                WALL_HISTOGRAM_15M_STEP,
                buckets,
            ),
            domain_1m: DomainRollingHistogram::new(
                Duration::from_secs(ONE_MINUTE_SECONDS as u64),
                DOMAIN_HISTOGRAM_1M_STEP,
                buckets,
            ),
            domain_15m: DomainRollingHistogram::new(
                Duration::from_secs(FIFTEEN_MINUTES_SECONDS as u64),
                DOMAIN_HISTOGRAM_15M_STEP,
                buckets,
            ),
        }
    }

    fn from_snapshot(
        snapshot: Option<&RollingHistogramsSnapshot>,
        _series_started_at: Instant,
        buckets: &'static [f64],
    ) -> Self {
        let Some(snapshot) = snapshot else {
            return Self::new(buckets);
        };
        Self {
            wall_1m: WallRollingHistogram::from_snapshot(
                &snapshot.wall_1m,
                Duration::from_secs(ONE_MINUTE_SECONDS as u64),
                WALL_HISTOGRAM_1M_STEP,
                buckets,
            ),
            wall_15m: WallRollingHistogram::from_snapshot(
                &snapshot.wall_15m,
                Duration::from_secs(FIFTEEN_MINUTES_SECONDS as u64),
                WALL_HISTOGRAM_15M_STEP,
                buckets,
            ),
            domain_1m: DomainRollingHistogram::from_snapshot(
                &snapshot.domain_1m,
                Duration::from_secs(ONE_MINUTE_SECONDS as u64),
                DOMAIN_HISTOGRAM_1M_STEP,
                buckets,
            ),
            domain_15m: DomainRollingHistogram::from_snapshot(
                &snapshot.domain_15m,
                Duration::from_secs(FIFTEEN_MINUTES_SECONDS as u64),
                DOMAIN_HISTOGRAM_15M_STEP,
                buckets,
            ),
        }
    }

    fn observe(&mut self, value: f64, domain_timestamp: Option<Timestamp>) {
        self.wall_1m.observe(value);
        self.wall_15m.observe(value);
        if let Some(domain_timestamp) = domain_timestamp {
            self.domain_1m.observe(value, domain_timestamp);
            self.domain_15m.observe(value, domain_timestamp);
        }
    }

    fn summary(&self, domain_timestamp: Option<Timestamp>) -> RollingHistogramSummary {
        RollingHistogramSummary {
            wall_1m: self.wall_1m.summary(),
            wall_15m: self.wall_15m.summary(),
            domain_1m: self.domain_1m.summary(domain_timestamp),
            domain_15m: self.domain_15m.summary(domain_timestamp),
        }
    }

    fn to_snapshot(&self, _series_started_at: Instant) -> RollingHistogramsSnapshot {
        RollingHistogramsSnapshot {
            wall_1m: self.wall_1m.to_snapshot(),
            wall_15m: self.wall_15m.to_snapshot(),
            domain_1m: self.domain_1m.to_snapshot(),
            domain_15m: self.domain_15m.to_snapshot(),
        }
    }
}

#[derive(Debug)]
struct AggregatedRollingHistograms {
    wall_1m: TimeRollingHistogram,
    wall_15m: TimeRollingHistogram,
    domain_1m: TimeRollingHistogram,
    domain_15m: TimeRollingHistogram,
    domain_last_at_nanos: Option<i64>,
}

impl AggregatedRollingHistograms {
    fn new(buckets: &'static [f64]) -> Self {
        Self {
            wall_1m: TimeRollingHistogram::new(
                Duration::from_secs(ONE_MINUTE_SECONDS as u64),
                WALL_HISTOGRAM_1M_STEP,
                buckets,
            ),
            wall_15m: TimeRollingHistogram::new(
                Duration::from_secs(FIFTEEN_MINUTES_SECONDS as u64),
                WALL_HISTOGRAM_15M_STEP,
                buckets,
            ),
            domain_1m: TimeRollingHistogram::new(
                Duration::from_secs(ONE_MINUTE_SECONDS as u64),
                DOMAIN_HISTOGRAM_1M_STEP,
                buckets,
            ),
            domain_15m: TimeRollingHistogram::new(
                Duration::from_secs(FIFTEEN_MINUTES_SECONDS as u64),
                DOMAIN_HISTOGRAM_15M_STEP,
                buckets,
            ),
            domain_last_at_nanos: None,
        }
    }

    fn add_series(&mut self, series: &HistogramSeries) {
        let rolling = series.rolling_histograms.lock();
        self.wall_1m.merge_from(&rolling.wall_1m.inner);
        self.wall_15m.merge_from(&rolling.wall_15m.inner);
        self.domain_1m.merge_from(&rolling.domain_1m.inner);
        self.domain_15m.merge_from(&rolling.domain_15m.inner);
        if let Some(last_at_nanos) = optional_domain_timestamp(&series.domain_last_at_nanos) {
            self.domain_last_at_nanos = Some(
                self.domain_last_at_nanos
                    .map(|current| current.max(last_at_nanos))
                    .unwrap_or(last_at_nanos),
            );
        }
    }

    fn summary(&self) -> HistogramSummary {
        let domain_timestamp = self.domain_last_at_nanos.map(Timestamp::from_unix_nanos);
        HistogramSummary {
            capacity: None,
            rolling_histograms: RollingHistogramSummary {
                wall_1m: current_wall_unix_nanos()
                    .map(|now| self.wall_1m.summary_at(now))
                    .unwrap_or_else(HistogramPercentileSummary::empty),
                wall_15m: current_wall_unix_nanos()
                    .map(|now| self.wall_15m.summary_at(now))
                    .unwrap_or_else(HistogramPercentileSummary::empty),
                domain_1m: domain_timestamp.map(|now| self.domain_1m.summary_at(now.unix_nanos())),
                domain_15m: domain_timestamp
                    .map(|now| self.domain_15m.summary_at(now.unix_nanos())),
            },
        }
    }
}

#[derive(Debug, Clone)]
struct HistogramPercentileSummary {
    p50: Option<f64>,
    p90: Option<f64>,
    p99: Option<f64>,
}

impl HistogramPercentileSummary {
    fn empty() -> Self {
        Self {
            p50: None,
            p90: None,
            p99: None,
        }
    }

    fn from_histogram(histogram: &HdrHistogram<u64>) -> Self {
        if histogram.is_empty() {
            return Self::empty();
        }
        Self {
            p50: Some(unscale_histogram_value(histogram.value_at_quantile(0.50))),
            p90: Some(unscale_histogram_value(histogram.value_at_quantile(0.90))),
            p99: Some(unscale_histogram_value(histogram.value_at_quantile(0.99))),
        }
    }
}

#[derive(Debug, Clone)]
struct RollingHistogramSummary {
    wall_1m: HistogramPercentileSummary,
    wall_15m: HistogramPercentileSummary,
    domain_1m: Option<HistogramPercentileSummary>,
    domain_15m: Option<HistogramPercentileSummary>,
}

#[derive(Debug)]
struct CounterSeries {
    started_at: Instant,
    domain_started_at_nanos: AtomicI64,
    domain_last_at_nanos: AtomicI64,
    value: AtomicU64,
    rolling: Mutex<RollingRates>,
}

impl Default for CounterSeries {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            domain_started_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
            domain_last_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
            value: AtomicU64::new(0),
            rolling: Mutex::new(RollingRates::new()),
        }
    }
}

impl CounterSeries {
    fn from_snapshot(snapshot: &MetricCounterSnapshot) -> Self {
        let started_at = snapshot
            .started_at_wall_nanos
            .and_then(instant_from_wall_unix_nanos)
            .unwrap_or_else(|| started_at_from_elapsed(snapshot.elapsed_seconds));
        Self {
            started_at,
            domain_started_at_nanos: AtomicI64::new(
                snapshot
                    .domain_started_at_nanos
                    .unwrap_or(NO_DOMAIN_TIMESTAMP),
            ),
            domain_last_at_nanos: AtomicI64::new(
                snapshot.domain_last_at_nanos.unwrap_or(NO_DOMAIN_TIMESTAMP),
            ),
            value: AtomicU64::new(snapshot.value),
            rolling: Mutex::new(RollingRates::from_snapshot(
                snapshot.rolling.as_ref(),
                started_at,
            )),
        }
    }

    fn increment(&self, value: u64, domain_timestamp: Option<Timestamp>) {
        self.value.fetch_add(value, AtomicOrdering::Relaxed);
        self.observe_domain_timestamp(domain_timestamp);
        self.rolling.lock().observe(value, domain_timestamp);
    }

    fn observe_domain_timestamp(&self, domain_timestamp: Option<Timestamp>) {
        if let Some(domain_timestamp) = domain_timestamp {
            observe_domain_timestamp(
                &self.domain_started_at_nanos,
                &self.domain_last_at_nanos,
                domain_timestamp,
            );
        }
    }

    fn to_snapshot(&self, key: MetricKey) -> MetricCounterSnapshot {
        MetricCounterSnapshot {
            key: key.into(),
            elapsed_seconds: self.started_at.elapsed().as_secs_f64(),
            started_at_wall_nanos: wall_unix_nanos_from_instant(self.started_at),
            domain_started_at_nanos: optional_domain_timestamp(&self.domain_started_at_nanos),
            domain_last_at_nanos: optional_domain_timestamp(&self.domain_last_at_nanos),
            rolling: Some(self.rolling.lock().to_snapshot(self.started_at)),
            value: self.value.load(AtomicOrdering::Relaxed),
        }
    }

    fn summary(&self) -> CounterSummary {
        let value = self.value.load(AtomicOrdering::Relaxed);
        CounterSummary {
            value,
            wall_rate_per_sec: wall_rate(value, self.started_at),
            domain_rate_per_sec: domain_rate(
                value,
                &self.domain_started_at_nanos,
                &self.domain_last_at_nanos,
            ),
            rolling: self.rolling.lock().summary(
                optional_domain_timestamp(&self.domain_last_at_nanos)
                    .map(Timestamp::from_unix_nanos),
            ),
        }
    }
}

#[derive(Debug)]
struct HistogramSeries {
    started_at: Instant,
    domain_started_at_nanos: AtomicI64,
    domain_last_at_nanos: AtomicI64,
    capacity: AtomicU64,
    rolling_histograms: Mutex<RollingHistograms>,
}

impl HistogramSeries {
    fn new(buckets: &'static [f64]) -> Self {
        Self {
            started_at: Instant::now(),
            domain_started_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
            domain_last_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
            capacity: AtomicU64::new(NO_HISTOGRAM_CAPACITY),
            rolling_histograms: Mutex::new(RollingHistograms::new(buckets)),
        }
    }

    fn observe_with_capacity(
        &self,
        value: f64,
        capacity: Option<u64>,
        domain_timestamp: Option<Timestamp>,
    ) {
        if !value.is_finite() {
            return;
        }
        if let Some(capacity) = capacity {
            self.capacity.store(capacity, AtomicOrdering::Relaxed);
        }
        if let Some(domain_timestamp) = domain_timestamp {
            observe_domain_timestamp(
                &self.domain_started_at_nanos,
                &self.domain_last_at_nanos,
                domain_timestamp,
            );
        }
        self.rolling_histograms
            .lock()
            .observe(value, domain_timestamp);
    }

    fn from_snapshot(snapshot: &MetricHistogramSnapshot) -> Self {
        let started_at = snapshot
            .started_at_wall_nanos
            .and_then(instant_from_wall_unix_nanos)
            .unwrap_or_else(|| started_at_from_elapsed(snapshot.elapsed_seconds));
        let buckets = internal_buckets_for_metric(&snapshot.key.metric);
        Self {
            started_at,
            domain_started_at_nanos: AtomicI64::new(
                snapshot
                    .domain_started_at_nanos
                    .unwrap_or(NO_DOMAIN_TIMESTAMP),
            ),
            domain_last_at_nanos: AtomicI64::new(
                snapshot.domain_last_at_nanos.unwrap_or(NO_DOMAIN_TIMESTAMP),
            ),
            capacity: AtomicU64::new(NO_HISTOGRAM_CAPACITY),
            rolling_histograms: Mutex::new(RollingHistograms::from_snapshot(
                snapshot.rolling_histograms.as_ref(),
                started_at,
                buckets,
            )),
        }
    }

    fn to_snapshot(&self, key: MetricKey) -> MetricHistogramSnapshot {
        MetricHistogramSnapshot {
            key: key.into(),
            elapsed_seconds: self.started_at.elapsed().as_secs_f64(),
            started_at_wall_nanos: wall_unix_nanos_from_instant(self.started_at),
            domain_started_at_nanos: optional_domain_timestamp(&self.domain_started_at_nanos),
            domain_last_at_nanos: optional_domain_timestamp(&self.domain_last_at_nanos),
            rolling_rates: None,
            rolling_histograms: Some(self.rolling_histograms.lock().to_snapshot(self.started_at)),
            bucket_counts: Vec::new(),
            count: 0,
            sum: 0.0,
        }
    }

    fn summary(&self) -> HistogramSummary {
        let domain_timestamp =
            optional_domain_timestamp(&self.domain_last_at_nanos).map(Timestamp::from_unix_nanos);
        HistogramSummary {
            capacity: optional_histogram_capacity(&self.capacity),
            rolling_histograms: self.rolling_histograms.lock().summary(domain_timestamp),
        }
    }
}

#[derive(Debug, Clone)]
struct HistogramSummary {
    capacity: Option<u64>,
    rolling_histograms: RollingHistogramSummary,
}

#[derive(Debug, Clone)]
struct CounterSummary {
    value: u64,
    wall_rate_per_sec: f64,
    domain_rate_per_sec: Option<f64>,
    rolling: RollingRateSummary,
}

#[derive(Debug, Clone, Default)]
struct AggregatedRollingRateSummary {
    wall_1m_per_sec: Option<f64>,
    wall_15m_per_sec: Option<f64>,
    domain_1m_per_sec: Option<f64>,
    domain_15m_per_sec: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct AggregatedCounterSummary {
    value: u64,
    wall_rate_per_sec: f64,
    domain_rate_per_sec: Option<f64>,
    rolling: AggregatedRollingRateSummary,
}

impl AggregatedCounterSummary {
    fn add(&mut self, summary: CounterSummary) {
        self.value = self.value.saturating_add(summary.value);
        self.wall_rate_per_sec += summary.wall_rate_per_sec;
        self.domain_rate_per_sec =
            add_optional_metric(self.domain_rate_per_sec, summary.domain_rate_per_sec);
        self.rolling.wall_1m_per_sec = add_optional_metric(
            self.rolling.wall_1m_per_sec,
            summary.rolling.wall_1m_per_sec,
        );
        self.rolling.wall_15m_per_sec = add_optional_metric(
            self.rolling.wall_15m_per_sec,
            summary.rolling.wall_15m_per_sec,
        );
        self.rolling.domain_1m_per_sec = add_optional_metric(
            self.rolling.domain_1m_per_sec,
            summary.rolling.domain_1m_per_sec,
        );
        self.rolling.domain_15m_per_sec = add_optional_metric(
            self.rolling.domain_15m_per_sec,
            summary.rolling.domain_15m_per_sec,
        );
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeMetrics {
    counters: Arc<DashMap<MetricKey, Arc<CounterSeries>>>,
    histograms: Arc<DashMap<MetricKey, Arc<HistogramSeries>>>,
    branch_counters: Arc<DashMap<BranchMetricKey, Arc<CounterSeries>>>,
    branch_histograms: Arc<DashMap<BranchMetricKey, Arc<HistogramSeries>>>,
    prometheus: Arc<PrometheusMetrics>,
}

#[derive(Debug, Clone)]
struct PrometheusMetrics {
    registry: Registry,
    messages_total: IntCounterVec,
    batches_total: IntCounterVec,
    bytes_total: IntCounterVec,
    messages_per_batch: HistogramVec,
    delivery_latency_seconds: HistogramVec,
    relay_buffer_len: HistogramVec,
}

struct JemallocMetricsCollector {
    epoch: epoch_mib,
    active: stats::active_mib,
    allocated: stats::allocated_mib,
    mapped: stats::mapped_mib,
    metadata: stats::metadata_mib,
    resident: stats::resident_mib,
    retained: stats::retained_mib,
    active_gauge: Gauge,
    allocated_gauge: Gauge,
    mapped_gauge: Gauge,
    metadata_gauge: Gauge,
    resident_gauge: Gauge,
    retained_gauge: Gauge,
    descs: Vec<Desc>,
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            histograms: Arc::new(DashMap::new()),
            branch_counters: Arc::new(DashMap::new()),
            branch_histograms: Arc::new(DashMap::new()),
            prometheus: Arc::new(PrometheusMetrics::new()),
        }
    }
}

impl PrometheusMetrics {
    fn new() -> Self {
        let registry = Registry::new();
        let messages_total = IntCounterVec::new(
            Opts::new(
                MESSAGES_TOTAL,
                "Total graph messages observed by Nervix runtime targets.",
            )
            .namespace("nervix"),
            PROMETHEUS_LABELS,
        )
        .expect("valid messages_total prometheus counter");
        let batches_total = IntCounterVec::new(
            Opts::new(
                BATCHES_TOTAL,
                "Total graph batches observed by Nervix runtime targets.",
            )
            .namespace("nervix"),
            PROMETHEUS_LABELS,
        )
        .expect("valid batches_total prometheus counter");
        let bytes_total = IntCounterVec::new(
            Opts::new(
                BYTES_TOTAL,
                "Total graph bytes observed by Nervix runtime targets.",
            )
            .namespace("nervix"),
            PROMETHEUS_LABELS,
        )
        .expect("valid bytes_total prometheus counter");
        let messages_per_batch = HistogramVec::new(
            HistogramOpts::new(
                MESSAGES_PER_BATCH,
                "Raw graph messages per batch observed by Nervix runtime targets.",
            )
            .namespace("nervix")
            .buckets(MESSAGE_BATCH_BUCKETS.to_vec()),
            PROMETHEUS_LABELS,
        )
        .expect("valid messages_per_batch prometheus histogram");
        let delivery_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                DELIVERY_LATENCY_SECONDS,
                "Raw graph delivery latency in seconds observed by Nervix runtime targets.",
            )
            .namespace("nervix")
            .buckets(LATENCY_BUCKETS.to_vec()),
            PROMETHEUS_LABELS,
        )
        .expect("valid delivery_latency_seconds prometheus histogram");
        let relay_buffer_len = HistogramVec::new(
            HistogramOpts::new(
                RELAY_BUFFER_LEN,
                "Runtime relay buffer occupancy observed by Nervix runtime targets.",
            )
            .namespace("nervix")
            .buckets(RELAY_BUFFER_LEN_BUCKETS.to_vec()),
            PROMETHEUS_LABELS,
        )
        .expect("valid relay_buffer_len prometheus histogram");

        registry
            .register(Box::new(messages_total.clone()))
            .expect("messages_total registered once");
        registry
            .register(Box::new(batches_total.clone()))
            .expect("batches_total registered once");
        registry
            .register(Box::new(bytes_total.clone()))
            .expect("bytes_total registered once");
        registry
            .register(Box::new(messages_per_batch.clone()))
            .expect("messages_per_batch registered once");
        registry
            .register(Box::new(delivery_latency_seconds.clone()))
            .expect("delivery_latency_seconds registered once");
        registry
            .register(Box::new(relay_buffer_len.clone()))
            .expect("relay_buffer_len registered once");
        registry
            .register(Box::new(JemallocMetricsCollector::new()))
            .expect("jemalloc metrics registered once");

        Self {
            registry,
            messages_total,
            batches_total,
            bytes_total,
            messages_per_batch,
            delivery_latency_seconds,
            relay_buffer_len,
        }
    }

    fn register_counter(&self, key: &MetricKey) {
        match key.metric {
            MESSAGES_TOTAL => {
                self.messages_total
                    .with_label_values(&prometheus_label_values(key));
            }
            BATCHES_TOTAL => {
                self.batches_total
                    .with_label_values(&prometheus_label_values(key));
            }
            BYTES_TOTAL => {
                self.bytes_total
                    .with_label_values(&prometheus_label_values(key));
            }
            _ => {}
        }
    }

    fn increment_counter(&self, key: &MetricKey, value: u64) {
        match key.metric {
            MESSAGES_TOTAL => self
                .messages_total
                .with_label_values(&prometheus_label_values(key))
                .inc_by(value),
            BATCHES_TOTAL => self
                .batches_total
                .with_label_values(&prometheus_label_values(key))
                .inc_by(value),
            BYTES_TOTAL => self
                .bytes_total
                .with_label_values(&prometheus_label_values(key))
                .inc_by(value),
            _ => {}
        }
    }

    fn observe_histogram(&self, key: &MetricKey, value: f64) {
        if !value.is_finite() {
            return;
        }
        match key.metric {
            MESSAGES_PER_BATCH => self
                .messages_per_batch
                .with_label_values(&prometheus_label_values(key))
                .observe(value),
            DELIVERY_LATENCY_SECONDS => self
                .delivery_latency_seconds
                .with_label_values(&prometheus_label_values(key))
                .observe(value),
            RELAY_BUFFER_LEN => self
                .relay_buffer_len
                .with_label_values(&prometheus_label_values(key))
                .observe(value),
            _ => {}
        }
    }

    fn text(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl JemallocMetricsCollector {
    fn new() -> Self {
        let mut descs = Vec::new();
        let active_gauge = jemalloc_gauge(
            "active_bytes",
            "Total number of bytes in active pages allocated by the process.",
            &mut descs,
        );
        let allocated_gauge = jemalloc_gauge(
            "allocated_bytes",
            "Total number of bytes allocated by the process.",
            &mut descs,
        );
        let mapped_gauge = jemalloc_gauge(
            "mapped_bytes",
            "Total number of bytes in active extents mapped by the allocator.",
            &mut descs,
        );
        let metadata_gauge = jemalloc_gauge(
            "metadata_bytes",
            "Total number of bytes dedicated to jemalloc metadata.",
            &mut descs,
        );
        let resident_gauge = jemalloc_gauge(
            "resident_bytes",
            "Total number of bytes in physically resident data pages mapped by the allocator.",
            &mut descs,
        );
        let retained_gauge = jemalloc_gauge(
            "retained_bytes",
            "Total number of bytes in virtual memory mappings retained by jemalloc.",
            &mut descs,
        );

        Self {
            epoch: epoch::mib().expect("jemalloc epoch mib available"),
            active: stats::active::mib().expect("jemalloc active stats mib available"),
            allocated: stats::allocated::mib().expect("jemalloc allocated stats mib available"),
            mapped: stats::mapped::mib().expect("jemalloc mapped stats mib available"),
            metadata: stats::metadata::mib().expect("jemalloc metadata stats mib available"),
            resident: stats::resident::mib().expect("jemalloc resident stats mib available"),
            retained: stats::retained::mib().expect("jemalloc retained stats mib available"),
            active_gauge,
            allocated_gauge,
            mapped_gauge,
            metadata_gauge,
            resident_gauge,
            retained_gauge,
            descs,
        }
    }
}

impl Collector for JemallocMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.epoch.advance().expect("jemalloc epoch can advance");
        self.active_gauge
            .set(self.active.read().expect("jemalloc active stats readable") as f64);
        self.allocated_gauge.set(
            self.allocated
                .read()
                .expect("jemalloc allocated stats readable") as f64,
        );
        self.mapped_gauge
            .set(self.mapped.read().expect("jemalloc mapped stats readable") as f64);
        self.metadata_gauge.set(
            self.metadata
                .read()
                .expect("jemalloc metadata stats readable") as f64,
        );
        self.resident_gauge.set(
            self.resident
                .read()
                .expect("jemalloc resident stats readable") as f64,
        );
        self.retained_gauge.set(
            self.retained
                .read()
                .expect("jemalloc retained stats readable") as f64,
        );

        let mut metric_families = Vec::with_capacity(self.descs.len());
        metric_families.extend(self.active_gauge.collect());
        metric_families.extend(self.allocated_gauge.collect());
        metric_families.extend(self.mapped_gauge.collect());
        metric_families.extend(self.metadata_gauge.collect());
        metric_families.extend(self.resident_gauge.collect());
        metric_families.extend(self.retained_gauge.collect());
        metric_families
    }
}

fn jemalloc_gauge(name: &str, help: &str, descs: &mut Vec<Desc>) -> Gauge {
    let gauge = Gauge::with_opts(
        Opts::new(name, help)
            .namespace("nervix")
            .subsystem(JEMALLOC_SUBSYSTEM),
    )
    .expect("valid jemalloc prometheus gauge");
    descs.extend(gauge.desc().into_iter().cloned());
    gauge
}

#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct RuntimeMetricsSnapshot {
    counters: Vec<MetricCounterSnapshot>,
    histograms: Vec<MetricHistogramSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
struct MetricSnapshotKey {
    domain: String,
    target_kind: String,
    target: String,
    physical_node_id: String,
    relay: String,
    peer_kind: String,
    peer: String,
    direction: String,
    metric: String,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct MetricCounterSnapshot {
    key: MetricSnapshotKey,
    elapsed_seconds: f64,
    started_at_wall_nanos: Option<i64>,
    domain_started_at_nanos: Option<i64>,
    domain_last_at_nanos: Option<i64>,
    rolling: Option<RollingRatesSnapshot>,
    value: u64,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct MetricHistogramSnapshot {
    key: MetricSnapshotKey,
    elapsed_seconds: f64,
    started_at_wall_nanos: Option<i64>,
    domain_started_at_nanos: Option<i64>,
    domain_last_at_nanos: Option<i64>,
    rolling_rates: Option<RollingRatesSnapshot>,
    rolling_histograms: Option<RollingHistogramsSnapshot>,
    bucket_counts: Vec<u64>,
    count: u64,
    sum: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct NodeBatchObservation<'a> {
    pub domain: &'a Domain,
    pub kind: ModelKind,
    pub node: &'a Identifier,
    pub relay: &'a Identifier,
    pub physical_node_id: Option<&'a str>,
    pub messages: u64,
    pub bytes: u64,
    pub domain_timestamp: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy)]
pub struct NodeWithoutRelayObservation<'a> {
    pub domain: &'a Domain,
    pub kind: ModelKind,
    pub node: &'a Identifier,
    pub physical_node_id: Option<&'a str>,
    pub messages: u64,
    pub bytes: u64,
    pub domain_timestamp: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy)]
pub struct RelayBatchObservation<'a> {
    pub domain: &'a Domain,
    pub relay: &'a Identifier,
    pub physical_node_id: Option<&'a str>,
    pub messages: u64,
    pub bytes: u64,
    pub domain_timestamp: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy)]
pub struct RelayBufferObservation<'a> {
    pub domain: &'a Domain,
    pub relay: &'a Identifier,
    pub physical_node_id: Option<&'a str>,
    pub direction: &'static str,
    pub len: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct NodeLatencyObservation<'a> {
    pub domain: &'a Domain,
    pub kind: ModelKind,
    pub node: &'a Identifier,
    pub relay: &'a Identifier,
    pub physical_node_id: Option<&'a str>,
    pub seconds: f64,
    pub domain_timestamp: Option<Timestamp>,
}

impl RuntimeMetrics {
    pub fn register_global_node(
        &self,
        domain: &Domain,
        kind: ModelKind,
        node: &Identifier,
        physical_node_id: Option<&str>,
    ) {
        self.register_counter(MetricKey::node_without_stream(
            domain,
            kind,
            node,
            physical_node_id,
            "sent",
            MESSAGES_TOTAL,
        ));
        self.register_counter(MetricKey::node_without_stream(
            domain,
            kind,
            node,
            physical_node_id,
            "received",
            MESSAGES_TOTAL,
        ));
    }

    pub fn register_global_stream(
        &self,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
    ) {
        self.register_counter(MetricKey::relay(
            domain,
            relay,
            physical_node_id,
            "received",
            MESSAGES_TOTAL,
        ));
    }

    pub fn observe_global_stream_received(
        &self,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        messages: u64,
        bytes: u64,
        domain_timestamp: Option<Timestamp>,
    ) {
        self.observe_batch(
            MetricKey::relay(domain, relay, physical_node_id, "received", MESSAGES_TOTAL),
            messages,
            bytes,
            domain_timestamp,
        );
    }

    pub fn observe_global_node_sent(&self, observation: NodeBatchObservation<'_>) {
        self.observe_batch(
            MetricKey::node(
                observation.domain,
                observation.kind,
                observation.node,
                observation.physical_node_id,
                observation.relay,
                "sent",
                MESSAGES_TOTAL,
            ),
            observation.messages,
            observation.bytes,
            observation.domain_timestamp,
        );
    }

    pub fn observe_global_node_received(&self, observation: NodeBatchObservation<'_>) {
        self.observe_batch(
            MetricKey::node(
                observation.domain,
                observation.kind,
                observation.node,
                observation.physical_node_id,
                observation.relay,
                "received",
                MESSAGES_TOTAL,
            ),
            observation.messages,
            observation.bytes,
            observation.domain_timestamp,
        );
    }

    pub fn observe_global_node_without_stream_received(
        &self,
        observation: NodeWithoutRelayObservation<'_>,
    ) {
        let messages_key = MetricKey::node_without_stream(
            observation.domain,
            observation.kind,
            observation.node,
            observation.physical_node_id,
            "received",
            MESSAGES_TOTAL,
        );
        self.increment(
            messages_key.clone(),
            observation.messages,
            observation.domain_timestamp,
        );
        self.increment(
            with_metric(&messages_key, BYTES_TOTAL),
            observation.bytes,
            observation.domain_timestamp,
        );
    }

    pub fn observe_branch_node_without_stream_received(
        &self,
        branch_key: &str,
        observation: NodeWithoutRelayObservation<'_>,
    ) {
        let messages_key = MetricKey::node_without_stream(
            observation.domain,
            observation.kind,
            observation.node,
            observation.physical_node_id,
            "received",
            MESSAGES_TOTAL,
        );
        self.increment_branch(
            branch_key,
            messages_key.clone(),
            observation.messages,
            observation.domain_timestamp,
        );
        self.increment_branch(
            branch_key,
            with_metric(&messages_key, BYTES_TOTAL),
            observation.bytes,
            observation.domain_timestamp,
        );
    }

    pub fn observe_global_delivery_latency(
        &self,
        domain: &Domain,
        kind: ModelKind,
        node: &Identifier,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        seconds: f64,
    ) {
        let key = MetricKey::node(
            domain,
            kind,
            node,
            physical_node_id,
            relay,
            "received",
            DELIVERY_LATENCY_SECONDS,
        );
        self.observe_histogram(key.clone(), seconds, None);
        self.prometheus.observe_histogram(&key, seconds);
    }

    pub fn observe_global_delivery_latency_at_domain_time(
        &self,
        observation: NodeLatencyObservation<'_>,
    ) {
        let key = MetricKey::node(
            observation.domain,
            observation.kind,
            observation.node,
            observation.physical_node_id,
            observation.relay,
            "received",
            DELIVERY_LATENCY_SECONDS,
        );
        self.observe_histogram(
            key.clone(),
            observation.seconds,
            observation.domain_timestamp,
        );
        self.prometheus.observe_histogram(&key, observation.seconds);
    }

    pub fn observe_branch_stream_received(
        &self,
        branch_key: &str,
        observation: RelayBatchObservation<'_>,
    ) {
        self.observe_branch_batch(
            branch_key,
            MetricKey::relay(
                observation.domain,
                observation.relay,
                observation.physical_node_id,
                "received",
                MESSAGES_TOTAL,
            ),
            observation.messages,
            observation.bytes,
            observation.domain_timestamp,
        );
    }

    pub fn observe_branch_node_sent(
        &self,
        branch_key: &str,
        observation: NodeBatchObservation<'_>,
    ) {
        self.observe_branch_batch(
            branch_key,
            MetricKey::node(
                observation.domain,
                observation.kind,
                observation.node,
                observation.physical_node_id,
                observation.relay,
                "sent",
                MESSAGES_TOTAL,
            ),
            observation.messages,
            observation.bytes,
            observation.domain_timestamp,
        );
    }

    pub fn observe_branch_node_received(
        &self,
        branch_key: &str,
        observation: NodeBatchObservation<'_>,
    ) {
        self.observe_branch_batch(
            branch_key,
            MetricKey::node(
                observation.domain,
                observation.kind,
                observation.node,
                observation.physical_node_id,
                observation.relay,
                "received",
                MESSAGES_TOTAL,
            ),
            observation.messages,
            observation.bytes,
            observation.domain_timestamp,
        );
    }

    pub fn observe_branch_delivery_latency(
        &self,
        branch_key: &str,
        observation: NodeLatencyObservation<'_>,
    ) {
        self.observe_branch_histogram(
            branch_key,
            MetricKey::node(
                observation.domain,
                observation.kind,
                observation.node,
                observation.physical_node_id,
                observation.relay,
                "received",
                DELIVERY_LATENCY_SECONDS,
            ),
            observation.seconds,
            observation.domain_timestamp,
        );
    }

    pub fn observe_global_relay_buffer_len(&self, observation: RelayBufferObservation<'_>) {
        let key = MetricKey::relay(
            observation.domain,
            observation.relay,
            observation.physical_node_id,
            observation.direction,
            RELAY_BUFFER_LEN,
        );
        self.observe_histogram_with_capacity(
            key.clone(),
            observation.len as f64,
            Some(observation.capacity),
            None,
        );
        self.prometheus
            .observe_histogram(&key, observation.len as f64);
    }

    pub fn observe_branch_relay_buffer_len(
        &self,
        branch_key: &str,
        observation: RelayBufferObservation<'_>,
    ) {
        self.observe_branch_histogram_with_capacity(
            branch_key,
            MetricKey::relay(
                observation.domain,
                observation.relay,
                observation.physical_node_id,
                observation.direction,
                RELAY_BUFFER_LEN,
            ),
            observation.len as f64,
            Some(observation.capacity),
            None,
        );
    }

    pub fn prometheus_text(&self) -> String {
        self.prometheus.text()
    }

    pub fn snapshot_global_target(
        &self,
        domain: &Domain,
        kind: ModelKind,
        target: &Identifier,
        physical_node_id: &str,
    ) -> RuntimeMetricsSnapshot {
        let target_kind = kind.as_str().to_ascii_uppercase();
        let mut counters = self
            .counters
            .iter()
            .filter(|entry| {
                key_matches_target(entry.key(), domain, &target_kind, target, physical_node_id)
            })
            .map(|entry| entry.value().to_snapshot(entry.key().clone()))
            .collect::<Vec<_>>();
        counters.sort_by(|left, right| left.key.cmp(&right.key));
        let mut histograms = self
            .histograms
            .iter()
            .filter(|entry| {
                key_matches_target(entry.key(), domain, &target_kind, target, physical_node_id)
            })
            .map(|entry| entry.value().to_snapshot(entry.key().clone()))
            .collect::<Vec<_>>();
        histograms.sort_by(|left, right| left.key.cmp(&right.key));
        RuntimeMetricsSnapshot {
            counters,
            histograms,
        }
    }

    pub fn snapshot_branch_target(
        &self,
        branch_key: &str,
        domain: &Domain,
        kind: ModelKind,
        target: &Identifier,
        physical_node_id: &str,
    ) -> RuntimeMetricsSnapshot {
        let target_kind = kind.as_str().to_ascii_uppercase();
        let mut counters = self
            .branch_counters
            .iter()
            .filter(|entry| {
                entry.key().branch_key == branch_key
                    && key_matches_target(
                        &entry.key().key,
                        domain,
                        &target_kind,
                        target,
                        physical_node_id,
                    )
            })
            .map(|entry| entry.value().to_snapshot(entry.key().key.clone()))
            .collect::<Vec<_>>();
        counters.sort_by(|left, right| left.key.cmp(&right.key));
        let mut histograms = self
            .branch_histograms
            .iter()
            .filter(|entry| {
                entry.key().branch_key == branch_key
                    && key_matches_target(
                        &entry.key().key,
                        domain,
                        &target_kind,
                        target,
                        physical_node_id,
                    )
            })
            .map(|entry| entry.value().to_snapshot(entry.key().key.clone()))
            .collect::<Vec<_>>();
        histograms.sort_by(|left, right| left.key.cmp(&right.key));
        RuntimeMetricsSnapshot {
            counters,
            histograms,
        }
    }

    pub fn apply_global_target_snapshot(
        &self,
        domain: &Domain,
        kind: ModelKind,
        target: &Identifier,
        physical_node_id: &str,
        snapshot: RuntimeMetricsSnapshot,
    ) {
        let target_kind = kind.as_str().to_ascii_uppercase();
        let counter_keys = self
            .counters
            .iter()
            .filter(|entry| {
                key_matches_target(entry.key(), domain, &target_kind, target, physical_node_id)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in counter_keys {
            self.counters.remove(&key);
        }
        let histogram_keys = self
            .histograms
            .iter()
            .filter(|entry| {
                key_matches_target(entry.key(), domain, &target_kind, target, physical_node_id)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in histogram_keys {
            self.histograms.remove(&key);
        }

        for counter in snapshot.counters {
            let Ok(key) = MetricKey::try_from(counter.key.clone()) else {
                continue;
            };
            self.counters
                .insert(key, Arc::new(CounterSeries::from_snapshot(&counter)));
        }
        for histogram in snapshot.histograms {
            let Ok(key) = MetricKey::try_from(histogram.key.clone()) else {
                continue;
            };
            self.histograms
                .insert(key, Arc::new(HistogramSeries::from_snapshot(&histogram)));
        }
    }

    pub fn has_global_target_measurements(
        &self,
        domain: &Domain,
        kind: ModelKind,
        target: &Identifier,
    ) -> bool {
        let target_kind = kind.as_str().to_ascii_uppercase();
        self.counters.iter().any(|entry| {
            let key = entry.key();
            key.domain == domain.as_str()
                && key.target_kind == target_kind
                && key.target == target.as_str()
                && key.relay != "-"
                && entry.value().value.load(AtomicOrdering::Relaxed) > 0
        }) || self.histograms.iter().any(|entry| {
            let key = entry.key();
            key.domain == domain.as_str()
                && key.target_kind == target_kind
                && key.target == target.as_str()
                && key.relay != "-"
        })
    }

    pub fn apply_global_snapshot(&self, snapshot: RuntimeMetricsSnapshot) {
        for counter in snapshot.counters {
            let Ok(key) = MetricKey::try_from(counter.key.clone()) else {
                continue;
            };
            self.counters.remove(&key);
            self.counters
                .insert(key, Arc::new(CounterSeries::from_snapshot(&counter)));
        }
        for histogram in snapshot.histograms {
            let Ok(key) = MetricKey::try_from(histogram.key.clone()) else {
                continue;
            };
            self.histograms.remove(&key);
            self.histograms
                .insert(key, Arc::new(HistogramSeries::from_snapshot(&histogram)));
        }
    }

    pub fn apply_branch_target_snapshot(
        &self,
        branch_key: &str,
        domain: &Domain,
        kind: ModelKind,
        target: &Identifier,
        physical_node_id: &str,
        snapshot: RuntimeMetricsSnapshot,
    ) {
        let target_kind = kind.as_str().to_ascii_uppercase();
        let counter_keys = self
            .branch_counters
            .iter()
            .filter(|entry| {
                entry.key().branch_key == branch_key
                    && key_matches_target(
                        &entry.key().key,
                        domain,
                        &target_kind,
                        target,
                        physical_node_id,
                    )
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in counter_keys {
            self.branch_counters.remove(&key);
        }
        let histogram_keys = self
            .branch_histograms
            .iter()
            .filter(|entry| {
                entry.key().branch_key == branch_key
                    && key_matches_target(
                        &entry.key().key,
                        domain,
                        &target_kind,
                        target,
                        physical_node_id,
                    )
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in histogram_keys {
            self.branch_histograms.remove(&key);
        }

        for counter in snapshot.counters {
            let Ok(key) = MetricKey::try_from(counter.key.clone()) else {
                continue;
            };
            self.branch_counters.insert(
                BranchMetricKey {
                    branch_key: branch_key.to_string(),
                    key,
                },
                Arc::new(CounterSeries::from_snapshot(&counter)),
            );
        }
        for histogram in snapshot.histograms {
            let Ok(key) = MetricKey::try_from(histogram.key.clone()) else {
                continue;
            };
            self.branch_histograms.insert(
                BranchMetricKey {
                    branch_key: branch_key.to_string(),
                    key,
                },
                Arc::new(HistogramSeries::from_snapshot(&histogram)),
            );
        }
    }

    pub fn describe_global_target(
        &self,
        domain: &Domain,
        kind: &str,
        target: &Identifier,
    ) -> Vec<String> {
        let mut lines = Vec::new();
        let mut counters = self
            .counters
            .iter()
            .filter(|entry| {
                entry.key().domain == domain.as_str()
                    && entry.key().target_kind == kind
                    && entry.key().target == target.as_str()
            })
            .map(|entry| (entry.key().clone(), entry.value().summary()))
            .collect::<Vec<_>>();
        counters.sort_by(|left, right| left.0.cmp(&right.0));
        let mut histograms = self
            .histograms
            .iter()
            .filter(|entry| {
                entry.key().domain == domain.as_str()
                    && entry.key().target_kind == kind
                    && entry.key().target == target.as_str()
            })
            .map(|entry| (entry.key().clone(), entry.value().summary()))
            .collect::<Vec<_>>();
        histograms.sort_by(|left, right| left.0.cmp(&right.0));
        if kind.eq_ignore_ascii_case("RELAY") {
            counters.clear();
            histograms.retain(|(key, _)| key.is_relay_buffer_len());
        }
        if counters.is_empty() && histograms.is_empty() {
            return lines;
        }
        lines.push("metrics:".to_string());
        let mut incoming_counters = Vec::new();
        let mut outgoing_counters = Vec::new();
        let mut other_counters = Vec::new();
        for item in counters {
            let direction = item.0.direction.as_str();
            if direction == "received" {
                incoming_counters.push(item);
            } else if direction == "sent" {
                outgoing_counters.push(item);
            } else {
                other_counters.push(item);
            }
        }
        let mut incoming_histograms = Vec::new();
        let mut outgoing_histograms = Vec::new();
        let mut buffer_histograms = Vec::new();
        let mut other_histograms = Vec::new();
        for item in histograms {
            if item.0.is_relay_buffer_len() {
                buffer_histograms.push(item);
                continue;
            }
            let direction = item.0.direction.as_str();
            if direction == "received" {
                incoming_histograms.push(item);
            } else if direction == "sent" {
                outgoing_histograms.push(item);
            } else {
                other_histograms.push(item);
            }
        }
        if !incoming_counters.is_empty() || !incoming_histograms.is_empty() {
            lines.push("  incoming_edges:".to_string());
            for (key, summary) in incoming_counters {
                lines.push(format_counter_metric_line("    ", &key, &summary));
            }
            for (key, summary) in incoming_histograms {
                lines.push(format_histogram_metric_line("    ", &key, &summary));
            }
        }
        if !outgoing_counters.is_empty() || !outgoing_histograms.is_empty() {
            lines.push("  outgoing_edges:".to_string());
            for (key, summary) in outgoing_counters {
                lines.push(format_counter_metric_line("    ", &key, &summary));
            }
            for (key, summary) in outgoing_histograms {
                lines.push(format_histogram_metric_line("    ", &key, &summary));
            }
        }
        if !buffer_histograms.is_empty() {
            lines.push("  relay_buffers:".to_string());
            for (key, summary) in buffer_histograms {
                lines.push(format_histogram_metric_line("    ", &key, &summary));
            }
        }
        if !other_counters.is_empty() || !other_histograms.is_empty() {
            lines.push("  other:".to_string());
            for (key, summary) in other_counters {
                lines.push(format_counter_metric_line("    ", &key, &summary));
            }
            for (key, summary) in other_histograms {
                lines.push(format_histogram_metric_line("    ", &key, &summary));
            }
        }
        lines
    }

    pub fn describe_domain_statistics(&self, domain: &Domain) -> Vec<String> {
        let mut input_output =
            self.aggregate_domain_counters(domain, DomainMetricScope::InputOutput);
        let mut processed = self.aggregate_domain_counters(domain, DomainMetricScope::Processed);
        let mut input_output_histograms =
            self.aggregate_domain_histograms(domain, DomainMetricScope::InputOutput);
        let mut processed_histograms =
            self.aggregate_domain_histograms(domain, DomainMetricScope::Processed);
        if input_output.is_empty()
            && processed.is_empty()
            && input_output_histograms.is_empty()
            && processed_histograms.is_empty()
        {
            return Vec::new();
        }

        input_output.sort_by(|left, right| left.0.cmp(&right.0));
        processed.sort_by(|left, right| left.0.cmp(&right.0));
        input_output_histograms.sort_by(|left, right| left.0.cmp(&right.0));
        processed_histograms.sort_by(|left, right| left.0.cmp(&right.0));

        let mut lines = vec!["metrics:".to_string()];
        if !input_output.is_empty() || !input_output_histograms.is_empty() {
            lines.push("  input_output:".to_string());
            for (key, summary) in input_output {
                lines.push(format_aggregated_counter_metric_line(
                    "    ", &key, &summary,
                ));
            }
            for (key, summary) in input_output_histograms {
                lines.push(format_histogram_metric_line("    ", &key, &summary));
            }
        }
        if !processed.is_empty() || !processed_histograms.is_empty() {
            lines.push("  processed:".to_string());
            for (key, summary) in processed {
                lines.push(format_aggregated_counter_metric_line(
                    "    ", &key, &summary,
                ));
            }
            for (key, summary) in processed_histograms {
                lines.push(format_histogram_metric_line("    ", &key, &summary));
            }
        }
        lines
    }

    pub fn dataflow_domain_statistics(&self, domain: &Domain) -> DataflowStatistics {
        self.dataflow_statistics_for_global_keys(|key| key.domain == domain.as_str())
    }

    pub fn dataflow_node_statistics(
        &self,
        domain: &Domain,
        kind: &str,
        target: &Identifier,
    ) -> DataflowStatistics {
        self.dataflow_statistics_for_global_keys(|key| {
            key.domain == domain.as_str()
                && key.target_kind == kind
                && key.target == target.as_str()
        })
    }

    pub fn dataflow_edge_statistics(
        &self,
        domain: &Domain,
        metric: &DataflowMetricRef,
    ) -> DataflowStatistics {
        self.dataflow_statistics_for_global_keys(|key| {
            key.matches_dataflow_metric_ref(domain, metric)
        })
    }

    pub fn dataflow_relay_buffer_statistics(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> DataflowStatistics {
        self.dataflow_statistics_for_global_keys(|key| {
            key.domain == domain.as_str()
                && key.target_kind == "RELAY"
                && key.target == relay.as_str()
                && key.is_relay_buffer_len()
        })
    }

    pub fn dataflow_branch_statistics(
        &self,
        domain: &Domain,
        kind: &str,
        target: &Identifier,
    ) -> Vec<DataflowBranchStatistics> {
        let mut branches = Vec::<(String, DataflowStatistics)>::new();
        for entry in self.branch_counters.iter() {
            let branch_key = entry.key();
            if branch_key.key.domain != domain.as_str()
                || branch_key.key.target_kind != kind
                || branch_key.key.target != target.as_str()
            {
                continue;
            }
            let Some(statistics) =
                counter_dataflow_statistics(&branch_key.key, &entry.value().summary())
            else {
                continue;
            };
            if let Some((_, existing)) = branches
                .iter_mut()
                .find(|(branch, _)| branch == &branch_key.branch_key)
            {
                add_dataflow_statistics(existing, statistics);
            } else {
                branches.push((branch_key.branch_key.clone(), statistics));
            }
        }
        for entry in self.branch_histograms.iter() {
            let branch_key = entry.key();
            if branch_key.key.domain != domain.as_str()
                || branch_key.key.target_kind != kind
                || branch_key.key.target != target.as_str()
            {
                continue;
            }
            let Some(statistics) =
                histogram_dataflow_statistics(&branch_key.key, &entry.value().summary())
            else {
                continue;
            };
            if let Some((_, existing)) = branches
                .iter_mut()
                .find(|(branch, _)| branch == &branch_key.branch_key)
            {
                add_dataflow_statistics(existing, statistics);
            } else {
                branches.push((branch_key.branch_key.clone(), statistics));
            }
        }
        branches.sort_by(|left, right| left.0.cmp(&right.0));
        branches
            .into_iter()
            .map(|(branch, statistics)| DataflowBranchStatistics { branch, statistics })
            .collect()
    }

    pub fn dataflow_edge_branch_statistics(
        &self,
        domain: &Domain,
        metric: &DataflowMetricRef,
    ) -> Vec<DataflowBranchStatistics> {
        let mut branches = Vec::<(String, DataflowStatistics)>::new();
        for entry in self.branch_counters.iter() {
            let branch_key = entry.key();
            if !branch_key.key.matches_dataflow_metric_ref(domain, metric) {
                continue;
            }
            let Some(statistics) =
                counter_dataflow_statistics(&branch_key.key, &entry.value().summary())
            else {
                continue;
            };
            if let Some((_, existing)) = branches
                .iter_mut()
                .find(|(branch, _)| branch == &branch_key.branch_key)
            {
                add_dataflow_statistics(existing, statistics);
            } else {
                branches.push((branch_key.branch_key.clone(), statistics));
            }
        }
        for entry in self.branch_histograms.iter() {
            let branch_key = entry.key();
            if !branch_key.key.matches_dataflow_metric_ref(domain, metric) {
                continue;
            }
            let Some(statistics) =
                histogram_dataflow_statistics(&branch_key.key, &entry.value().summary())
            else {
                continue;
            };
            if let Some((_, existing)) = branches
                .iter_mut()
                .find(|(branch, _)| branch == &branch_key.branch_key)
            {
                add_dataflow_statistics(existing, statistics);
            } else {
                branches.push((branch_key.branch_key.clone(), statistics));
            }
        }
        branches.sort_by(|left, right| left.0.cmp(&right.0));
        branches
            .into_iter()
            .map(|(branch, statistics)| DataflowBranchStatistics { branch, statistics })
            .collect()
    }

    fn dataflow_statistics_for_global_keys(
        &self,
        include: impl Fn(&MetricKey) -> bool,
    ) -> DataflowStatistics {
        let mut statistics = DataflowStatistics::default();
        for entry in self.counters.iter() {
            if !include(entry.key()) {
                continue;
            }
            if let Some(counter_statistics) =
                counter_dataflow_statistics(entry.key(), &entry.value().summary())
            {
                add_dataflow_statistics(&mut statistics, counter_statistics);
            }
        }
        for entry in self.histograms.iter() {
            if !include(entry.key()) {
                continue;
            }
            if let Some(histogram_statistics) =
                histogram_dataflow_statistics(entry.key(), &entry.value().summary())
            {
                add_dataflow_statistics(&mut statistics, histogram_statistics);
            }
        }
        statistics
    }

    fn aggregate_domain_counters(
        &self,
        domain: &Domain,
        scope: DomainMetricScope,
    ) -> Vec<(MetricKey, AggregatedCounterSummary)> {
        let mut counters = Vec::<(MetricKey, AggregatedCounterSummary)>::new();
        for entry in self.counters.iter() {
            let key = entry.key();
            if key.domain != domain.as_str() || !scope.includes_target_kind(&key.target_kind) {
                continue;
            }
            let aggregate_key = MetricKey {
                domain: key.domain.clone(),
                target_kind: DOMAIN_TARGET_KIND.to_string(),
                target: scope.target().to_string(),
                physical_node_id: key.physical_node_id.clone(),
                relay: key.relay.clone(),
                peer_kind: String::new(),
                peer: String::new(),
                direction: key.direction.clone(),
                metric: key.metric,
            };
            if let Some((_, summary)) = counters
                .iter_mut()
                .find(|(existing_key, _)| existing_key == &aggregate_key)
            {
                summary.add(entry.value().summary());
            } else {
                let mut summary = AggregatedCounterSummary::default();
                summary.add(entry.value().summary());
                counters.push((aggregate_key, summary));
            }
        }
        counters
    }

    fn aggregate_domain_histograms(
        &self,
        domain: &Domain,
        scope: DomainMetricScope,
    ) -> Vec<(MetricKey, HistogramSummary)> {
        let mut histograms = Vec::<(MetricKey, AggregatedRollingHistograms)>::new();
        for entry in self.histograms.iter() {
            let key = entry.key();
            if key.domain != domain.as_str() || !scope.includes_target_kind(&key.target_kind) {
                continue;
            }
            let aggregate_key = MetricKey {
                domain: key.domain.clone(),
                target_kind: DOMAIN_TARGET_KIND.to_string(),
                target: scope.target().to_string(),
                physical_node_id: key.physical_node_id.clone(),
                relay: key.relay.clone(),
                peer_kind: String::new(),
                peer: String::new(),
                direction: key.direction.clone(),
                metric: key.metric,
            };
            if let Some((_, summary)) = histograms
                .iter_mut()
                .find(|(existing_key, _)| existing_key == &aggregate_key)
            {
                summary.add_series(entry.value());
            } else {
                let mut summary =
                    AggregatedRollingHistograms::new(internal_buckets_for_metric(key.metric));
                summary.add_series(entry.value());
                histograms.push((aggregate_key, summary));
            }
        }
        histograms
            .into_iter()
            .map(|(key, summary)| (key, summary.summary()))
            .collect()
    }

    fn observe_batch(
        &self,
        messages_key: MetricKey,
        messages: u64,
        bytes: u64,
        domain_timestamp: Option<Timestamp>,
    ) {
        self.increment(messages_key.clone(), messages, domain_timestamp);
        self.increment(
            with_metric(&messages_key, BATCHES_TOTAL),
            1,
            domain_timestamp,
        );
        self.increment(
            with_metric(&messages_key, BYTES_TOTAL),
            bytes,
            domain_timestamp,
        );
        let batch_key = with_metric(&messages_key, MESSAGES_PER_BATCH);
        self.observe_histogram(batch_key.clone(), messages as f64, domain_timestamp);
        self.prometheus
            .observe_histogram(&batch_key, messages as f64);
    }

    fn observe_branch_batch(
        &self,
        branch_key: &str,
        messages_key: MetricKey,
        messages: u64,
        bytes: u64,
        domain_timestamp: Option<Timestamp>,
    ) {
        self.increment_branch(branch_key, messages_key.clone(), messages, domain_timestamp);
        self.increment_branch(
            branch_key,
            with_metric(&messages_key, BATCHES_TOTAL),
            1,
            domain_timestamp,
        );
        self.increment_branch(
            branch_key,
            with_metric(&messages_key, BYTES_TOTAL),
            bytes,
            domain_timestamp,
        );
        self.observe_branch_histogram(
            branch_key,
            with_metric(&messages_key, MESSAGES_PER_BATCH),
            messages as f64,
            domain_timestamp,
        );
    }

    fn increment(&self, key: MetricKey, value: u64, domain_timestamp: Option<Timestamp>) {
        self.register_counter(key.clone());
        if let Some(series) = self.counters.get(&key) {
            series.increment(value, domain_timestamp);
        }
        self.prometheus.increment_counter(&key, value);
    }

    fn register_counter(&self, key: MetricKey) {
        self.counters
            .entry(key.clone())
            .or_insert_with(|| Arc::new(CounterSeries::default()));
        self.prometheus.register_counter(&key);
    }

    fn observe_histogram(&self, key: MetricKey, value: f64, domain_timestamp: Option<Timestamp>) {
        self.observe_histogram_with_capacity(key, value, None, domain_timestamp);
    }

    fn observe_histogram_with_capacity(
        &self,
        key: MetricKey,
        value: f64,
        capacity: Option<usize>,
        domain_timestamp: Option<Timestamp>,
    ) {
        let buckets = internal_buckets_for_metric(key.metric);
        self.histograms
            .entry(key)
            .or_insert_with(|| Arc::new(HistogramSeries::new(buckets)))
            .observe_with_capacity(
                value,
                capacity.map(|capacity| u64::try_from(capacity).unwrap_or(u64::MAX)),
                domain_timestamp,
            );
    }

    fn increment_branch(
        &self,
        branch_key: &str,
        key: MetricKey,
        value: u64,
        domain_timestamp: Option<Timestamp>,
    ) {
        let key = BranchMetricKey {
            branch_key: branch_key.to_string(),
            key,
        };
        self.branch_counters
            .entry(key.clone())
            .or_insert_with(|| Arc::new(CounterSeries::default()));
        if let Some(series) = self.branch_counters.get(&key) {
            series.increment(value, domain_timestamp);
        }
    }

    fn observe_branch_histogram(
        &self,
        branch_key: &str,
        key: MetricKey,
        value: f64,
        domain_timestamp: Option<Timestamp>,
    ) {
        self.observe_branch_histogram_with_capacity(branch_key, key, value, None, domain_timestamp);
    }

    fn observe_branch_histogram_with_capacity(
        &self,
        branch_key: &str,
        key: MetricKey,
        value: f64,
        capacity: Option<usize>,
        domain_timestamp: Option<Timestamp>,
    ) {
        let buckets = internal_buckets_for_metric(key.metric);
        self.branch_histograms
            .entry(BranchMetricKey {
                branch_key: branch_key.to_string(),
                key,
            })
            .or_insert_with(|| Arc::new(HistogramSeries::new(buckets)))
            .observe_with_capacity(
                value,
                capacity.map(|capacity| u64::try_from(capacity).unwrap_or(u64::MAX)),
                domain_timestamp,
            );
    }
}

impl From<MetricKey> for MetricSnapshotKey {
    fn from(key: MetricKey) -> Self {
        Self {
            domain: key.domain,
            target_kind: key.target_kind,
            target: key.target,
            physical_node_id: key.physical_node_id,
            relay: key.relay,
            peer_kind: key.peer_kind,
            peer: key.peer,
            direction: key.direction,
            metric: key.metric.to_string(),
        }
    }
}

impl TryFrom<MetricSnapshotKey> for MetricKey {
    type Error = ();

    fn try_from(key: MetricSnapshotKey) -> Result<Self, Self::Error> {
        Ok(Self {
            domain: key.domain,
            target_kind: key.target_kind,
            target: key.target,
            physical_node_id: key.physical_node_id,
            relay: key.relay,
            peer_kind: key.peer_kind,
            peer: key.peer,
            direction: key.direction,
            metric: metric_name_to_static(&key.metric).ok_or(())?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum DomainMetricScope {
    InputOutput,
    Processed,
}

impl DomainMetricScope {
    fn target(self) -> &'static str {
        match self {
            Self::InputOutput => DOMAIN_INPUT_OUTPUT_TARGET,
            Self::Processed => DOMAIN_PROCESSED_TARGET,
        }
    }

    fn includes_target_kind(self, target_kind: &str) -> bool {
        match self {
            Self::InputOutput => target_kind == "INGESTOR" || target_kind == "EMITTER",
            Self::Processed => target_kind != "RELAY" && target_kind != DOMAIN_TARGET_KIND,
        }
    }
}

impl Eq for MetricSnapshotKey {}

impl PartialOrd for MetricSnapshotKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MetricSnapshotKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            &self.domain,
            &self.target_kind,
            &self.target,
            &self.physical_node_id,
            &self.relay,
            &self.peer_kind,
            &self.peer,
            &self.direction,
            &self.metric,
        )
            .cmp(&(
                &other.domain,
                &other.target_kind,
                &other.target,
                &other.physical_node_id,
                &other.relay,
                &other.peer_kind,
                &other.peer,
                &other.direction,
                &other.metric,
            ))
    }
}

fn with_metric(key: &MetricKey, metric: &'static str) -> MetricKey {
    let mut key = key.clone();
    key.metric = metric;
    key
}

fn key_matches_target(
    key: &MetricKey,
    domain: &Domain,
    target_kind: &str,
    target: &Identifier,
    physical_node_id: &str,
) -> bool {
    key.domain == domain.as_str()
        && key.target_kind == target_kind
        && key.target == target.as_str()
        && key.physical_node_id == physical_node_id
}

fn metric_name_to_static(metric: &str) -> Option<&'static str> {
    match metric {
        MESSAGES_TOTAL => Some(MESSAGES_TOTAL),
        BATCHES_TOTAL => Some(BATCHES_TOTAL),
        BYTES_TOTAL => Some(BYTES_TOTAL),
        MESSAGES_PER_BATCH => Some(MESSAGES_PER_BATCH),
        DELIVERY_LATENCY_SECONDS => Some(DELIVERY_LATENCY_SECONDS),
        RELAY_BUFFER_LEN => Some(RELAY_BUFFER_LEN),
        _ => None,
    }
}

fn internal_buckets_for_metric(metric: &str) -> &'static [f64] {
    match metric {
        DELIVERY_LATENCY_SECONDS => INTERNAL_LATENCY_BUCKETS,
        RELAY_BUFFER_LEN => RELAY_BUFFER_LEN_BUCKETS,
        _ => INTERNAL_MESSAGE_BATCH_BUCKETS,
    }
}

fn started_at_from_elapsed(elapsed_seconds: f64) -> Instant {
    let elapsed_seconds = if elapsed_seconds.is_finite() && elapsed_seconds >= 0.0 {
        elapsed_seconds
    } else {
        0.0
    };
    Instant::now()
        .checked_sub(std::time::Duration::from_secs_f64(elapsed_seconds))
        .unwrap_or_else(Instant::now)
}

fn instant_from_series_elapsed(series_started_at: Instant, elapsed_seconds: f64) -> Instant {
    let elapsed_seconds = if elapsed_seconds.is_finite() && elapsed_seconds >= 0.0 {
        elapsed_seconds
    } else {
        0.0
    };
    series_started_at
        .checked_add(std::time::Duration::from_secs_f64(elapsed_seconds))
        .unwrap_or(series_started_at)
}

fn current_wall_unix_nanos() -> Option<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(duration.as_nanos()).ok()
}

fn wall_unix_nanos_from_instant(instant: Instant) -> Option<i64> {
    let now = current_wall_unix_nanos()?;
    let elapsed = i64::try_from(instant.elapsed().as_nanos()).ok()?;
    now.checked_sub(elapsed)
}

fn instant_from_wall_unix_nanos(wall_unix_nanos: i64) -> Option<Instant> {
    let now = current_wall_unix_nanos()?;
    let elapsed = now.checked_sub(wall_unix_nanos)?;
    if elapsed <= 0 {
        return Some(Instant::now());
    }
    let elapsed = u64::try_from(elapsed).ok()?;
    Instant::now().checked_sub(Duration::from_nanos(elapsed))
}

fn wall_rate(value: u64, started_at: Instant) -> f64 {
    let elapsed = started_at.elapsed().as_secs_f64();
    if elapsed <= 0.0 {
        0.0
    } else {
        value as f64 / elapsed
    }
}

fn observe_domain_timestamp(
    started_at_nanos: &AtomicI64,
    last_at_nanos: &AtomicI64,
    timestamp: Timestamp,
) {
    let timestamp = timestamp.unix_nanos();
    if timestamp == NO_DOMAIN_TIMESTAMP {
        return;
    }
    fetch_min_or_empty(started_at_nanos, timestamp);
    fetch_max_or_empty(last_at_nanos, timestamp);
}

fn fetch_min_or_empty(target: &AtomicI64, value: i64) {
    let mut current = target.load(AtomicOrdering::Relaxed);
    loop {
        let next = if current == NO_DOMAIN_TIMESTAMP {
            value
        } else {
            current.min(value)
        };
        if next == current {
            return;
        }
        match target.compare_exchange_weak(
            current,
            next,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn fetch_max_or_empty(target: &AtomicI64, value: i64) {
    let mut current = target.load(AtomicOrdering::Relaxed);
    loop {
        let next = if current == NO_DOMAIN_TIMESTAMP {
            value
        } else {
            current.max(value)
        };
        if next == current {
            return;
        }
        match target.compare_exchange_weak(
            current,
            next,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn optional_domain_timestamp(value: &AtomicI64) -> Option<i64> {
    let value = value.load(AtomicOrdering::Relaxed);
    (value != NO_DOMAIN_TIMESTAMP).then_some(value)
}

fn optional_histogram_capacity(value: &AtomicU64) -> Option<u64> {
    let value = value.load(AtomicOrdering::Relaxed);
    (value != NO_HISTOGRAM_CAPACITY).then_some(value)
}

fn domain_rate(value: u64, started_at_nanos: &AtomicI64, last_at_nanos: &AtomicI64) -> Option<f64> {
    let started_at_nanos = started_at_nanos.load(AtomicOrdering::Relaxed);
    let last_at_nanos = last_at_nanos.load(AtomicOrdering::Relaxed);
    if started_at_nanos == NO_DOMAIN_TIMESTAMP || last_at_nanos == NO_DOMAIN_TIMESTAMP {
        return None;
    }
    let elapsed_nanos = last_at_nanos.checked_sub(started_at_nanos)?;
    if elapsed_nanos <= 0 {
        return None;
    }
    Some(value as f64 / ((elapsed_nanos as f64) / 1_000_000_000.0))
}

fn decay_factor(elapsed_seconds: f64, tau_seconds: f64) -> f64 {
    if elapsed_seconds <= 0.0 || tau_seconds <= 0.0 {
        return 1.0;
    }
    (-elapsed_seconds / tau_seconds).exp()
}

fn time_decay_alpha(elapsed_seconds: f64, tau_seconds: f64) -> f64 {
    1.0 - decay_factor(elapsed_seconds, tau_seconds)
}

fn rate_decay_tau_seconds(window_seconds: f64) -> f64 {
    window_seconds / RATE_DECAY_TAU_FRACTION
}

fn scaled_histogram_value(value: f64) -> u64 {
    (value * HISTOGRAM_VALUE_SCALE).round().max(0.0) as u64
}

fn unscale_histogram_value(value: u64) -> f64 {
    value as f64 / HISTOGRAM_VALUE_SCALE
}

fn hdr_histogram_to_snapshot(histogram: &HdrHistogram<u64>) -> Vec<HdrRecordedValueSnapshot> {
    histogram
        .iter_recorded()
        .map(|value| HdrRecordedValueSnapshot {
            value: value.value_iterated_to(),
            count: value.count_at_value(),
        })
        .collect()
}

fn hdr_histogram_from_snapshot(
    snapshot: &[HdrRecordedValueSnapshot],
    config: HistogramConfig,
) -> HdrHistogram<u64> {
    let mut histogram = config.new_histogram();
    for value in snapshot {
        let _ = histogram.record_n(value.value, value.count);
    }
    histogram
}

fn bucket_start(timestamp_nanos: i64, step: Duration) -> i64 {
    let step_nanos = duration_nanos_i64(step);
    timestamp_nanos - timestamp_nanos.rem_euclid(step_nanos)
}

fn oldest_bucket_start(current_start: i64, window: Duration, step: Duration) -> Option<i64> {
    let window_nanos = duration_nanos_i64(window);
    let step_nanos = duration_nanos_i64(step);
    current_start.checked_sub(window_nanos.checked_sub(step_nanos)?)
}

fn duration_nanos_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).expect("metric duration fits into i64 nanoseconds")
}

fn format_counter_metric_line(prefix: &str, key: &MetricKey, summary: &CounterSummary) -> String {
    format!(
        "{prefix}{} {} relay={} physical_node={} total={} wall_rate_per_sec={} \
         domain_rate_per_sec={} wall_rate_ema_1m_per_sec={} wall_rate_ema_15m_per_sec={} \
         domain_rate_ema_1m_per_sec={} domain_rate_ema_15m_per_sec={}",
        key.metric,
        key.direction,
        empty_as_dash(&key.relay),
        empty_as_dash(&key.physical_node_id),
        summary.value,
        format_number(summary.wall_rate_per_sec),
        format_optional(summary.domain_rate_per_sec),
        format_optional(summary.rolling.wall_1m_per_sec),
        format_optional(summary.rolling.wall_15m_per_sec),
        format_optional(summary.rolling.domain_1m_per_sec),
        format_optional(summary.rolling.domain_15m_per_sec)
    )
}

fn counter_dataflow_statistics(
    key: &MetricKey,
    summary: &CounterSummary,
) -> Option<DataflowStatistics> {
    let rate = summary
        .rolling
        .wall_1m_per_sec
        .unwrap_or(summary.wall_rate_per_sec);
    match key.metric {
        MESSAGES_TOTAL => Some(DataflowStatistics {
            messages_per_second: rate,
            messages_total: summary.value,
            ..DataflowStatistics::default()
        }),
        BYTES_TOTAL => Some(DataflowStatistics {
            bytes_per_second: rate,
            bytes_total: summary.value,
            ..DataflowStatistics::default()
        }),
        BATCHES_TOTAL => Some(DataflowStatistics {
            batches_per_second: rate,
            batches_total: summary.value,
            ..DataflowStatistics::default()
        }),
        _ => None,
    }
}

fn histogram_dataflow_statistics(
    key: &MetricKey,
    summary: &HistogramSummary,
) -> Option<DataflowStatistics> {
    match key.metric {
        RELAY_BUFFER_LEN => {
            let wall_1m = &summary.rolling_histograms.wall_1m;
            let wall_15m = &summary.rolling_histograms.wall_15m;
            Some(DataflowStatistics {
                relay_buffer_capacity: summary.capacity,
                relay_buffer_len_p50: wall_1m.p50.or(wall_15m.p50),
                relay_buffer_len_p90: wall_1m.p90.or(wall_15m.p90),
                relay_buffer_len_p99: wall_1m.p99.or(wall_15m.p99),
                ..DataflowStatistics::default()
            })
        }
        _ => None,
    }
}

fn add_dataflow_statistics(target: &mut DataflowStatistics, source: DataflowStatistics) {
    target.messages_per_second += source.messages_per_second;
    target.bytes_per_second += source.bytes_per_second;
    target.batches_per_second += source.batches_per_second;
    target.messages_total = target.messages_total.saturating_add(source.messages_total);
    target.bytes_total = target.bytes_total.saturating_add(source.bytes_total);
    target.batches_total = target.batches_total.saturating_add(source.batches_total);
    target.relay_buffer_capacity =
        max_optional_u64(target.relay_buffer_capacity, source.relay_buffer_capacity);
    target.relay_buffer_len_p50 =
        max_optional_f64(target.relay_buffer_len_p50, source.relay_buffer_len_p50);
    target.relay_buffer_len_p90 =
        max_optional_f64(target.relay_buffer_len_p90, source.relay_buffer_len_p90);
    target.relay_buffer_len_p99 =
        max_optional_f64(target.relay_buffer_len_p99, source.relay_buffer_len_p99);
}

fn format_aggregated_counter_metric_line(
    prefix: &str,
    key: &MetricKey,
    summary: &AggregatedCounterSummary,
) -> String {
    format!(
        "{prefix}{} {} relay={} physical_node={} total={} wall_rate_per_sec={} \
         domain_rate_per_sec={} wall_rate_ema_1m_per_sec={} wall_rate_ema_15m_per_sec={} \
         domain_rate_ema_1m_per_sec={} domain_rate_ema_15m_per_sec={}",
        key.metric,
        key.direction,
        empty_as_dash(&key.relay),
        empty_as_dash(&key.physical_node_id),
        summary.value,
        format_number(summary.wall_rate_per_sec),
        format_optional(summary.domain_rate_per_sec),
        format_optional(summary.rolling.wall_1m_per_sec),
        format_optional(summary.rolling.wall_15m_per_sec),
        format_optional(summary.rolling.domain_1m_per_sec),
        format_optional(summary.rolling.domain_15m_per_sec)
    )
}

fn format_histogram_metric_line(
    prefix: &str,
    key: &MetricKey,
    summary: &HistogramSummary,
) -> String {
    let capacity = summary
        .capacity
        .map(|capacity| format!(" capacity={capacity}"))
        .unwrap_or_default();
    format!(
        "{prefix}{} {} relay={} physical_node={}{} p50_1m={} p90_1m={} p99_1m={} p50_15m={} \
         p90_15m={} p99_15m={} domain_p50_1m={} domain_p90_1m={} domain_p99_1m={} \
         domain_p50_15m={} domain_p90_15m={} domain_p99_15m={}",
        key.metric,
        key.direction,
        empty_as_dash(&key.relay),
        empty_as_dash(&key.physical_node_id),
        capacity,
        format_histogram_optional(summary.rolling_histograms.wall_1m.p50),
        format_histogram_optional(summary.rolling_histograms.wall_1m.p90),
        format_histogram_optional(summary.rolling_histograms.wall_1m.p99),
        format_histogram_optional(summary.rolling_histograms.wall_15m.p50),
        format_histogram_optional(summary.rolling_histograms.wall_15m.p90),
        format_histogram_optional(summary.rolling_histograms.wall_15m.p99),
        format_histogram_optional(
            summary
                .rolling_histograms
                .domain_1m
                .as_ref()
                .and_then(|summary| summary.p50)
        ),
        format_histogram_optional(
            summary
                .rolling_histograms
                .domain_1m
                .as_ref()
                .and_then(|summary| summary.p90)
        ),
        format_histogram_optional(
            summary
                .rolling_histograms
                .domain_1m
                .as_ref()
                .and_then(|summary| summary.p99)
        ),
        format_histogram_optional(
            summary
                .rolling_histograms
                .domain_15m
                .as_ref()
                .and_then(|summary| summary.p50)
        ),
        format_histogram_optional(
            summary
                .rolling_histograms
                .domain_15m
                .as_ref()
                .and_then(|summary| summary.p90)
        ),
        format_histogram_optional(
            summary
                .rolling_histograms
                .domain_15m
                .as_ref()
                .and_then(|summary| summary.p99)
        )
    )
}

fn prometheus_label_values(key: &MetricKey) -> [&str; 8] {
    [
        key.domain.as_str(),
        key.target_kind.as_str(),
        key.target.as_str(),
        key.physical_node_id.as_str(),
        key.direction.as_str(),
        empty_as_dash(&key.relay),
        empty_as_dash(&key.peer_kind),
        empty_as_dash(&key.peer),
    ]
}

fn empty_as_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn format_optional(value: Option<f64>) -> String {
    value.map(format_number).unwrap_or_else(|| "-".to_string())
}

fn add_optional_metric(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_optional_f64(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn format_histogram_optional(value: Option<f64>) -> String {
    value
        .map(format_histogram_number)
        .unwrap_or_else(|| "-".to_string())
}

fn format_histogram_number(value: f64) -> String {
    let rounded = if value.abs() >= 1.0 {
        (value * HISTOGRAM_DISPLAY_DECIMAL_SCALE).round() / HISTOGRAM_DISPLAY_DECIMAL_SCALE
    } else {
        value
    };
    let rendered = format_number(rounded);
    if rendered.contains('.') || rendered == "-" {
        rendered
    } else {
        format!("{rendered}.0")
    }
}

fn format_number(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let rendered = format!("{value:.6}");
    rendered
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_histogram_percentile_near(actual: Option<f64>, expected: f64) {
        let Some(actual) = actual else {
            panic!("expected histogram percentile near {expected}, got None");
        };
        let tolerance = (expected.abs() * 0.01).max(1.0 / HISTOGRAM_VALUE_SCALE);
        assert!(
            (actual - expected).abs() <= tolerance,
            "expected histogram percentile near {expected} within {tolerance}, got {actual}"
        );
    }

    fn has_graph_prometheus_samples(rendered: &str) -> bool {
        rendered.lines().any(|line| {
            !line.starts_with('#')
                && (line.starts_with("nervix_messages_total{")
                    || line.starts_with("nervix_batches_total{")
                    || line.starts_with("nervix_bytes_total{")
                    || line.starts_with("nervix_messages_per_batch_")
                    || line.starts_with("nervix_delivery_latency_seconds_")
                    || line.starts_with("nervix_relay_buffer_len_"))
        })
    }

    #[test]
    fn local_summary_reports_rates_and_percentiles() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let node = Identifier::parse("dedupe").expect("valid identifier");
        let relay = Identifier::parse("input").expect("valid identifier");

        metrics.observe_global_node_received(NodeBatchObservation {
            domain: &domain,
            kind: ModelKind::Deduplicator,
            node: &node,
            relay: &relay,
            physical_node_id: Some("node-1"),
            messages: 3,
            bytes: 128,
            domain_timestamp: Some(Timestamp::from_unix_nanos(1_000_000_000)),
        });
        metrics.observe_global_delivery_latency(
            &domain,
            ModelKind::Deduplicator,
            &node,
            &relay,
            Some("node-1"),
            0.25,
        );

        let rendered = metrics.describe_global_target(&domain, "DEDUPLICATOR", &node);
        assert!(rendered.iter().any(|line| line.contains("metrics:")));
        assert!(
            rendered
                .iter()
                .any(|line| line.contains("wall_rate_per_sec="))
        );
        let histogram_line = rendered
            .iter()
            .find(|line| line.contains("delivery_latency_seconds"))
            .expect("delivery latency histogram should be rendered");
        assert!(histogram_line.contains("p90_1m=0.25"));
        assert!(histogram_line.contains("p90_15m=0.25"));
        assert!(!histogram_line.contains("count="));
        assert!(!histogram_line.contains("sum="));
        assert!(!histogram_line.contains("wall_rate_per_sec="));
        assert!(!histogram_line.contains("domain_rate_per_sec="));
    }

    #[test]
    fn dataflow_statistics_include_domain_node_and_branch_counters() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let node = Identifier::parse("dedupe").expect("valid identifier");
        let relay = Identifier::parse("input").expect("valid identifier");

        metrics.observe_global_node_received(NodeBatchObservation {
            domain: &domain,
            kind: ModelKind::Deduplicator,
            node: &node,
            relay: &relay,
            physical_node_id: Some("node-1"),
            messages: 3,
            bytes: 128,
            domain_timestamp: None,
        });
        metrics.observe_branch_node_received(
            r#"{"tenant":"alpha"}"#,
            NodeBatchObservation {
                domain: &domain,
                kind: ModelKind::Deduplicator,
                node: &node,
                relay: &relay,
                physical_node_id: Some("node-1"),
                messages: 2,
                bytes: 64,
                domain_timestamp: None,
            },
        );

        let domain_statistics = metrics.dataflow_domain_statistics(&domain);
        assert_eq!(domain_statistics.messages_total, 3);
        assert_eq!(domain_statistics.bytes_total, 128);
        assert_eq!(domain_statistics.batches_total, 1);

        let node_statistics = metrics.dataflow_node_statistics(&domain, "DEDUPLICATOR", &node);
        assert_eq!(node_statistics.messages_total, 3);
        assert_eq!(node_statistics.bytes_total, 128);
        assert_eq!(node_statistics.batches_total, 1);

        let branch_statistics = metrics.dataflow_branch_statistics(&domain, "DEDUPLICATOR", &node);
        assert_eq!(branch_statistics.len(), 1);
        assert_eq!(branch_statistics[0].branch, r#"{"tenant":"alpha"}"#);
        assert_eq!(branch_statistics[0].statistics.messages_total, 2);
        assert_eq!(branch_statistics[0].statistics.bytes_total, 64);
        assert_eq!(branch_statistics[0].statistics.batches_total, 1);

        let edge_metric =
            DataflowMetricRef::new("DEDUPLICATOR", "dedupe", "received", Some("input"));
        let edge_statistics = metrics.dataflow_edge_statistics(&domain, &edge_metric);
        assert_eq!(edge_statistics.messages_total, 3);
        assert_eq!(edge_statistics.bytes_total, 128);
        assert_eq!(edge_statistics.batches_total, 1);
        let edge_branch_statistics = metrics.dataflow_edge_branch_statistics(&domain, &edge_metric);
        assert_eq!(edge_branch_statistics.len(), 1);
        assert_eq!(edge_branch_statistics[0].branch, r#"{"tenant":"alpha"}"#);
    }

    #[test]
    fn client_to_ingestor_edge_statistics_do_not_create_batches() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let ingestor = Identifier::parse("ing").expect("valid identifier");

        metrics.observe_global_node_without_stream_received(NodeWithoutRelayObservation {
            domain: &domain,
            kind: ModelKind::Ingestor,
            node: &ingestor,
            physical_node_id: Some("node-1"),
            messages: 2,
            bytes: 34,
            domain_timestamp: None,
        });
        metrics.observe_branch_node_without_stream_received(
            r#"{"tenant":"alpha"}"#,
            NodeWithoutRelayObservation {
                domain: &domain,
                kind: ModelKind::Ingestor,
                node: &ingestor,
                physical_node_id: Some("node-1"),
                messages: 2,
                bytes: 34,
                domain_timestamp: None,
            },
        );

        let edge_metric = DataflowMetricRef::new("INGESTOR", "ing", "received", None::<String>);
        let statistics = metrics.dataflow_edge_statistics(&domain, &edge_metric);
        assert_eq!(statistics.messages_total, 2);
        assert_eq!(statistics.bytes_total, 34);
        assert_eq!(statistics.batches_total, 0);
        let branch_statistics = metrics.dataflow_edge_branch_statistics(&domain, &edge_metric);
        assert_eq!(branch_statistics.len(), 1);
        assert_eq!(branch_statistics[0].statistics.batches_total, 0);
    }

    #[test]
    fn histogram_percentiles_render_as_decimal_estimates() {
        assert_eq!(format_histogram_number(1.0), "1.0");
        assert_eq!(format_histogram_number(1.003), "1.0");
        assert_eq!(format_histogram_number(10.047), "10.0");
        assert_eq!(format_histogram_number(0.25), "0.25");
        assert_eq!(format_histogram_number(12.5), "12.5");
    }

    #[test]
    fn internal_histogram_storage_is_bounded_for_branch_cardinality() {
        const MAX_INTERNAL_BUCKET_BYTES: usize = 64 * 1024;

        let messages = HistogramConfig::for_buckets(MESSAGE_BATCH_BUCKETS).new_histogram();
        assert!(
            messages
                .distinct_values()
                .saturating_mul(std::mem::size_of::<u64>())
                <= MAX_INTERNAL_BUCKET_BYTES,
            "messages_per_batch histogram count storage is too large: {} values",
            messages.distinct_values()
        );

        let relay_buffer = HistogramConfig::for_buckets(RELAY_BUFFER_LEN_BUCKETS).new_histogram();
        assert!(
            relay_buffer
                .distinct_values()
                .saturating_mul(std::mem::size_of::<u64>())
                <= MAX_INTERNAL_BUCKET_BYTES,
            "relay_buffer_len histogram count storage is too large: {} values",
            relay_buffer.distinct_values()
        );
    }

    #[test]
    fn messages_per_batch_percentiles_follow_observed_values_not_bucket_boundaries() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let node = Identifier::parse("dedupe").expect("valid identifier");
        let relay = Identifier::parse("events").expect("valid identifier");

        for _ in 0..100 {
            metrics.observe_global_node_received(NodeBatchObservation {
                domain: &domain,
                kind: ModelKind::Deduplicator,
                node: &node,
                relay: &relay,
                physical_node_id: Some("node-1"),
                messages: 2,
                bytes: 64,
                domain_timestamp: None,
            });
        }
        metrics.observe_global_node_received(NodeBatchObservation {
            domain: &domain,
            kind: ModelKind::Deduplicator,
            node: &node,
            relay: &relay,
            physical_node_id: Some("node-1"),
            messages: 500,
            bytes: 64,
            domain_timestamp: None,
        });

        let rendered = metrics.describe_global_target(&domain, "DEDUPLICATOR", &node);
        let line = rendered
            .iter()
            .find(|line| line.contains("messages_per_batch received relay=events"))
            .expect("messages_per_batch should be rendered");
        assert!(line.contains("p50_1m=2.0"), "{line}");
        assert!(line.contains("p90_1m=2.0"), "{line}");
        assert!(line.contains("p99_1m=2.0"), "{line}");
    }

    #[test]
    fn relay_buffer_len_reports_capacity_and_dataflow_statistics() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let relay = Identifier::parse("events").expect("valid identifier");

        metrics.observe_global_relay_buffer_len(RelayBufferObservation {
            domain: &domain,
            relay: &relay,
            physical_node_id: Some("node-1"),
            direction: "concrete",
            len: 2,
            capacity: 3,
        });
        metrics.observe_global_stream_received(&domain, &relay, Some("node-1"), 2, 64, None);

        let rendered = metrics.describe_global_target(&domain, "RELAY", &relay);
        let line = rendered
            .iter()
            .find(|line| line.contains("relay_buffer_len concrete relay=events"))
            .expect("relay buffer length should be rendered");
        assert!(
            !rendered
                .iter()
                .any(|line| line.contains("messages_total received relay=events")),
            "{rendered:?}"
        );
        assert!(
            !rendered
                .iter()
                .any(|line| line.contains("messages_per_batch received relay=events")),
            "{rendered:?}"
        );
        assert!(line.contains("capacity=3"), "{line}");
        assert!(line.contains("p50_1m=2.0"), "{line}");
        assert!(line.contains("p90_1m=2.0"), "{line}");
        assert!(line.contains("domain_p90_1m=-"), "{line}");

        let statistics = metrics.dataflow_relay_buffer_statistics(&domain, &relay);
        assert_eq!(statistics.relay_buffer_capacity, Some(3));
        assert_histogram_percentile_near(statistics.relay_buffer_len_p50, 2.0);
        assert_histogram_percentile_near(statistics.relay_buffer_len_p90, 2.0);
        assert_histogram_percentile_near(statistics.relay_buffer_len_p99, 2.0);
    }

    #[test]
    fn rolling_histogram_percentiles_expire_by_window_bucket_age() {
        let mut histogram = TimeRollingHistogram::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            MESSAGE_BATCH_BUCKETS,
        );
        histogram.observe_at(10.0, 0);
        histogram.observe_at(20.0, 0);

        let present = histogram.summary_at(59 * 1_000_000_000);
        assert_histogram_percentile_near(present.p50, 10.0);
        assert_histogram_percentile_near(present.p90, 20.0);

        let expired = histogram.summary_at(70 * 1_000_000_000);
        assert_eq!(expired.p50, None);
        assert_eq!(expired.p90, None);
        assert_eq!(expired.p99, None);
    }

    #[test]
    fn rolling_histogram_uses_observed_values_not_prometheus_bucket_boundaries() {
        let mut histogram = TimeRollingHistogram::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            MESSAGE_BATCH_BUCKETS,
        );
        for _ in 0..100 {
            histogram.observe_at(2.0, 0);
        }
        histogram.observe_at(500.0, 0);

        let summary = histogram.summary_at(1_000_000_000);
        assert_histogram_percentile_near(summary.p50, 2.0);
        assert_histogram_percentile_near(summary.p90, 2.0);
        assert_histogram_percentile_near(summary.p99, 2.0);
    }

    #[test]
    fn describe_renders_expired_one_minute_histogram_percentiles_as_absent() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let node = Identifier::parse("dedupe").expect("valid identifier");
        let relay = Identifier::parse("events").expect("valid identifier");
        let key = MetricKey::node(
            &domain,
            ModelKind::Deduplicator,
            &node,
            Some("node-1"),
            &relay,
            "received",
            MESSAGES_PER_BATCH,
        );
        let histogram = HistogramSeries::new(MESSAGE_BATCH_BUCKETS);
        let now_wall = current_wall_unix_nanos().expect("wall clock should be available");
        let old = now_wall - 2 * 60 * 1_000_000_000;
        {
            let mut rolling = histogram.rolling_histograms.lock();
            rolling.wall_1m.inner.observe_at(10.0, old);
            rolling.wall_15m.inner.observe_at(10.0, old);
        }
        metrics.histograms.insert(key, Arc::new(histogram));

        let rendered = metrics.describe_global_target(&domain, "DEDUPLICATOR", &node);
        let line = rendered
            .iter()
            .find(|line| line.contains("messages_per_batch received relay=events"))
            .expect("messages_per_batch should be rendered");
        assert!(line.contains("p50_1m=-"), "{line}");
        assert!(line.contains("p90_1m=-"), "{line}");
        assert!(line.contains("p99_1m=-"), "{line}");
        assert!(line.contains("p50_15m=10.0"), "{line}");
    }

    #[test]
    fn wall_ema_rate_decays_when_no_new_samples_arrive() {
        let mut ema = WallEma::new(rate_decay_tau_seconds(ONE_MINUTE_SECONDS));
        let now = Instant::now();
        let last = now
            .checked_sub(std::time::Duration::from_secs(5 * 60))
            .expect("old instant should be representable");
        ema.value = Some(100.0);
        ema.last_at = Some(last);

        let decayed = ema
            .value_at(now)
            .expect("ema with value and timestamp should summarize");
        assert!(decayed < 1.0, "expected old EMA to decay, got {decayed}");
    }

    #[test]
    fn one_minute_rate_ema_is_nearly_zero_after_one_minute_without_activity() {
        let mut ema = WallEma::new(rate_decay_tau_seconds(ONE_MINUTE_SECONDS));
        let now = Instant::now();
        let last = now
            .checked_sub(std::time::Duration::from_secs(60))
            .expect("old instant should be representable");
        ema.value = Some(100.0);
        ema.last_at = Some(last);

        let decayed = ema
            .value_at(now)
            .expect("ema with value and timestamp should summarize");
        assert!(
            decayed < 1.0,
            "expected 1m EMA to be nearly zero after 1m of inactivity, got {decayed}"
        );
    }

    #[test]
    fn one_minute_rate_ema_reacts_to_short_rate_changes() {
        let mut ema = WallEma::new(rate_decay_tau_seconds(ONE_MINUTE_SECONDS));

        ema.value = Some(10.0);
        ema.observe_sample(100.0, 5.0);

        let value = ema.value.expect("sampled EMA should have a value");
        assert!(
            value >= 80.0,
            "expected 1m EMA to move most of the way toward a 5s rate change, got {value}"
        );
    }

    #[test]
    fn wall_ema_snapshot_restore_preserves_downtime_age() {
        let now_wall = current_wall_unix_nanos().expect("wall clock should be available");
        let five_minutes = 5_i64 * 60 * 1_000_000_000;
        let snapshot = WallEmaSnapshot {
            value: Some(100.0),
            last_elapsed_seconds: Some(0.0),
            last_at_wall_nanos: Some(now_wall - five_minutes),
        };

        let restored = WallEma::from_snapshot(
            &snapshot,
            Instant::now(),
            rate_decay_tau_seconds(ONE_MINUTE_SECONDS),
        );
        let decayed = restored
            .value_at(Instant::now())
            .expect("restored EMA should summarize");
        assert!(
            decayed < 1.0,
            "expected restored old EMA to include downtime decay, got {decayed}"
        );
    }

    #[test]
    fn wall_histogram_snapshot_restore_preserves_downtime_age() {
        let now_wall = current_wall_unix_nanos().expect("wall clock should be available");
        let five_minutes = 5_i64 * 60 * 1_000_000_000;
        let old = now_wall - five_minutes;
        let mut histogram = HistogramConfig::for_buckets(MESSAGE_BATCH_BUCKETS).new_histogram();
        let _ = histogram.record(scaled_histogram_value(10.0));
        let snapshot = WallRollingHistogramSnapshot {
            buckets: vec![RollingHistogramBucketSnapshot {
                start_at_nanos: bucket_start(old, WALL_HISTOGRAM_1M_STEP),
                values: hdr_histogram_to_snapshot(&histogram),
            }],
        };

        let restored = WallRollingHistogram::from_snapshot(
            &snapshot,
            Duration::from_secs(ONE_MINUTE_SECONDS as u64),
            WALL_HISTOGRAM_1M_STEP,
            MESSAGE_BATCH_BUCKETS,
        );
        let summary = restored.summary();
        assert_eq!(summary.p50, None);
        assert_eq!(summary.p90, None);
        assert_eq!(summary.p99, None);
    }

    #[test]
    fn prometheus_export_uses_shared_labels_and_raw_counts() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let relay = Identifier::parse("events").expect("valid identifier");

        metrics.observe_global_stream_received(&domain, &relay, Some("node-1"), 2, 64, None);

        let rendered = metrics.prometheus_text();
        assert!(rendered.contains("nervix_messages_total"));
        assert!(rendered.contains("domain=\"main\""));
        assert!(rendered.contains("target_kind=\"RELAY\""));
        assert!(rendered.contains("relay=\"events\""));
        assert!(rendered.contains("physical_node_id=\"node-1\""));
        assert!(rendered.contains(" 2"));
    }

    #[test]
    fn prometheus_export_includes_jemalloc_metrics() {
        let metrics = RuntimeMetrics::default();

        let rendered = metrics.prometheus_text();

        assert!(rendered.contains("nervix_jemalloc_active_bytes"));
        assert!(rendered.contains("nervix_jemalloc_allocated_bytes"));
        assert!(rendered.contains("nervix_jemalloc_resident_bytes"));
    }

    #[test]
    fn prometheus_histograms_are_not_internal_snapshot_storage() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let node = Identifier::parse("dedupe").expect("valid identifier");
        let relay = Identifier::parse("events").expect("valid identifier");

        metrics.observe_global_node_received(NodeBatchObservation {
            domain: &domain,
            kind: ModelKind::Deduplicator,
            node: &node,
            relay: &relay,
            physical_node_id: Some("node-1"),
            messages: 3,
            bytes: 96,
            domain_timestamp: None,
        });

        let prometheus = metrics.prometheus_text();
        assert!(prometheus.contains("nervix_messages_per_batch_bucket"));
        assert!(prometheus.contains("nervix_messages_per_batch_count"));

        let snapshot =
            metrics.snapshot_global_target(&domain, ModelKind::Deduplicator, &node, "node-1");
        let histogram = snapshot
            .histograms
            .iter()
            .find(|histogram| histogram.key.metric == MESSAGES_PER_BATCH)
            .expect("messages_per_batch internal histogram should be snapshotted");
        assert!(histogram.rolling_histograms.is_some());
        assert!(histogram.bucket_counts.is_empty());
        assert_eq!(histogram.count, 0);
        assert_eq!(histogram.sum, 0.0);

        let restored = RuntimeMetrics::default();
        restored.apply_global_target_snapshot(
            &domain,
            ModelKind::Deduplicator,
            &node,
            "node-1",
            snapshot,
        );
        assert!(
            !restored
                .prometheus_text()
                .contains("nervix_messages_per_batch_count")
        );
        let restored_lines = restored.describe_global_target(&domain, "DEDUPLICATOR", &node);
        assert!(
            restored_lines
                .iter()
                .any(|line| line.contains("messages_per_batch received relay=events"))
        );
    }

    #[test]
    fn prometheus_export_uses_global_metrics_only() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let relay = Identifier::parse("events").expect("valid identifier");

        metrics.observe_branch_stream_received(
            r#"{"tenant":"acme"}"#,
            RelayBatchObservation {
                domain: &domain,
                relay: &relay,
                physical_node_id: Some("node-1"),
                messages: 9,
                bytes: 128,
                domain_timestamp: None,
            },
        );

        let rendered = metrics.prometheus_text();
        assert!(!rendered.contains(r#"{"tenant":"acme"}"#));
        assert!(!has_graph_prometheus_samples(&rendered));
    }

    #[test]
    fn global_snapshot_uses_global_metrics_only() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let relay = Identifier::parse("events").expect("valid identifier");
        metrics.observe_branch_stream_received(
            r#"{"tenant":"acme"}"#,
            RelayBatchObservation {
                domain: &domain,
                relay: &relay,
                physical_node_id: Some("node-1"),
                messages: 9,
                bytes: 128,
                domain_timestamp: None,
            },
        );
        metrics.observe_global_stream_received(&domain, &relay, Some("node-1"), 2, 64, None);

        let snapshot = metrics.snapshot_global_target(&domain, ModelKind::Relay, &relay, "node-1");
        assert_eq!(snapshot.counters.len(), 3);
        assert!(
            snapshot
                .counters
                .iter()
                .any(|counter| { counter.key.metric == MESSAGES_TOTAL && counter.value == 2 })
        );
        assert!(snapshot.counters.iter().all(|counter| counter.value != 9));
    }

    #[test]
    fn branch_snapshot_roundtrips_separately_from_global_metrics() {
        let metrics = RuntimeMetrics::default();
        let domain = Domain::parse("main").expect("valid domain");
        let relay = Identifier::parse("events").expect("valid identifier");
        metrics.observe_branch_stream_received(
            r#"{"tenant":"acme"}"#,
            RelayBatchObservation {
                domain: &domain,
                relay: &relay,
                physical_node_id: Some("node-1"),
                messages: 9,
                bytes: 128,
                domain_timestamp: None,
            },
        );
        metrics.observe_global_stream_received(&domain, &relay, Some("node-1"), 2, 64, None);

        let snapshot = metrics.snapshot_branch_target(
            r#"{"tenant":"acme"}"#,
            &domain,
            ModelKind::Relay,
            &relay,
            "node-1",
        );
        assert_eq!(snapshot.counters.len(), 3);
        assert!(
            snapshot
                .counters
                .iter()
                .any(|counter| { counter.key.metric == MESSAGES_TOTAL && counter.value == 9 })
        );

        let restored = RuntimeMetrics::default();
        restored.apply_branch_target_snapshot(
            r#"{"tenant":"acme"}"#,
            &domain,
            ModelKind::Relay,
            &relay,
            "node-1",
            snapshot,
        );
        assert!(!has_graph_prometheus_samples(&restored.prometheus_text()));
        let restored_branch = restored.snapshot_branch_target(
            r#"{"tenant":"acme"}"#,
            &domain,
            ModelKind::Relay,
            &relay,
            "node-1",
        );
        assert!(
            restored_branch
                .counters
                .iter()
                .any(|counter| { counter.key.metric == MESSAGES_TOTAL && counter.value == 9 })
        );
    }
}
