//! Every series a node records into, and the registry they are exported through.
//!
//! Layer: engines and infrastructure, with an edge inside it.
//!
//! - **Owns.** The counters, gauges and histograms a node records, their per-domain and
//!   per-branch aggregation, the snapshot branch-aggregated state replicates, and the Prometheus
//!   registry and encoder that expose them.
//! - **Depends on.** The vocabulary for the names it labels with, and the dataflow-graph
//!   description for the statistics it fills in.
//! - **Must not know.** How the values it records were produced. It is handed observations and
//!   never reaches back into the runtime for more.
//!
//! This module breaks its own contract: recording is infrastructure the data plane calls inward,
//! but the Prometheus exposition beside it is an edge. The two separate when the crate does.

use std::{
    cmp::Ordering,
    collections::VecDeque,
    num::NonZeroU64,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arch_into::ArchInto as _;
use hdrhistogram::Histogram as HdrHistogram;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_dataflow_graph::{DataflowBranchStatistics, DataflowMetricRef, DataflowStatistics};
use nervix_models::{
    BranchName, ClusterNodeName, DomainName, EmitterName, IngestorName, ModelKind, ModelName,
    RelayName, Timestamp,
};
use nervix_primitives::{
    collections::{DashMap, dash_map::Entry},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering as AtomicOrdering},
        blocking::Mutex,
    },
    time::Instant,
};
use nervix_recovery::Discarded as _;
use nervix_simd_kernels::{ElapsedHistogram, ElapsedLayout, elapsed_nanos};
use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
    core::{Collector, Desc},
    proto::{MetricFamily, MetricType},
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use strum::{AsRefStr, EnumIter, IntoEnumIterator};
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats};

mod interconnection;

pub use interconnection::NodeObservations;
use interconnection::{InterconnectionCollector, InterconnectionCollectorHandle};

/// Why withdrawing a label set discards its outcome. See [`PrometheusMetrics::remove`].
const WITHDRAWN_SERIES_MAY_NOT_EXIST: &str =
    "an entity torn down before its first observation has no series to withdraw";

const MESSAGES_TOTAL: &str = "messages_total";
const BATCHES_TOTAL: &str = "batches_total";
const BYTES_TOTAL: &str = "bytes_total";
const MESSAGES_PER_BATCH: &str = "messages_per_batch";
const DELIVERY_LATENCY_SECONDS: &str = "delivery_latency_seconds";
const RELAY_BUFFER_LEN: &str = "relay_buffer_len";
const BRANCH_INSTANCES: &str = "branch_instances";
const BRANCH_EVICTIONS_TOTAL: &str = "branch_evictions_total";
const INGESTOR_QUIESCE_BUFFERED_RECORDS: &str = "ingestor_quiesce_buffered_records";
const INGESTOR_QUIESCE_BUFFERED_BYTES: &str = "ingestor_quiesce_buffered_bytes";
const INGESTOR_QUIESCE_DROPPED_TOTAL: &str = "ingestor_quiesce_dropped_total";
const INGESTOR_QUIESCE_REJECTED_TOTAL: &str = "ingestor_quiesce_rejected_total";
const SESSION_SUBSCRIPTIONS: &str = "session_subscriptions";
const CLIENT_INGESTOR_PRODUCERS: &str = "client_ingestor_producers";
const CLIENT_INGESTOR_FORWARDED_PRODUCERS: &str = "client_ingestor_forwarded_producers";
const CLIENT_INGESTOR_OUTSTANDING_BATCHES: &str = "client_ingestor_outstanding_batches";
const CLIENT_INGESTOR_OUTSTANDING_BYTES: &str = "client_ingestor_outstanding_bytes";
const CLIENT_INGESTOR_ADMITTED_BATCHES: &str = "client_ingestor_admitted_batches";
const CLIENT_INGESTOR_SUBMISSIONS_TOTAL: &str = "client_ingestor_submissions_total";
const CLIENT_EMITTER_CONSUMERS: &str = "client_emitter_consumers";
const CLIENT_EMITTER_FORWARDED_CONSUMERS: &str = "client_emitter_forwarded_consumers";
const CLIENT_EMITTER_FORWARDED_CREDIT_BYTES: &str = "client_emitter_forwarded_credit_bytes";
const CLIENT_EMITTER_FORWARDED_RETAINED_BATCHES: &str = "client_emitter_forwarded_retained_batches";
const CLIENT_EMITTER_FORWARDED_RETAINED_BYTES: &str = "client_emitter_forwarded_retained_bytes";
const CLIENT_EMITTER_RETAINED_BATCHES: &str = "client_emitter_retained_batches";
const CLIENT_EMITTER_RETAINED_BYTES: &str = "client_emitter_retained_bytes";
const CLIENT_EMITTER_INCOMPLETE_BATCHES: &str = "client_emitter_incomplete_batches";
const CLIENT_EMITTER_RETRIES_TOTAL: &str = "client_emitter_retries_total";
const CLIENT_EMITTER_ACKS_TOTAL: &str = "client_emitter_acks_total";
const CLIENT_EMITTER_REJECTIONS_TOTAL: &str = "client_emitter_rejections_total";
const SESSION_SUBSCRIPTION_DROPPED_ROWS_TOTAL: &str = "session_subscription_dropped_rows_total";
const JEMALLOC_SUBSYSTEM: &str = "jemalloc";
const DOMAIN_TARGET_KIND: &str = "DOMAIN";
const DOMAIN_INPUT_OUTPUT_TARGET: &str = "input_output";
const DOMAIN_PROCESSED_TARGET: &str = "processed";
const MESSAGE_BATCH_BUCKETS: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 1024.0, 2048.0, 4096.0, 8192.0, 16_384.0,
    32_768.0, 65_536.0,
];
const LATENCY_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0];
const RELAY_BUFFER_LEN_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0,
];
const INTERNAL_MESSAGE_BATCH_BUCKETS: &[f64] = &[
    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 12.0, 15.0, 20.0, 30.0, 40.0, 50.0, 75.0,
    100.0, 150.0, 200.0, 300.0, 500.0, 750.0, 1000.0, 1024.0, 2048.0, 4096.0, 8192.0, 16_384.0,
    32_768.0, 65_536.0,
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
const BRANCH_PROMETHEUS_LABELS: &[&str] = &["domain", "branch", "physical_node_id"];
const BRANCH_EVICTION_PROMETHEUS_LABELS: &[&str] =
    &["domain", "branch", "physical_node_id", "reason"];
const INGESTOR_QUIESCE_PROMETHEUS_LABELS: &[&str] = &["domain", "ingestor", "physical_node_id"];
const SESSION_SUBSCRIPTION_PROMETHEUS_LABELS: &[&str] = &["domain", "relay"];
const CLIENT_INGESTOR_PROMETHEUS_LABELS: &[&str] = &["domain", "ingestor"];
const CLIENT_EMITTER_PROMETHEUS_LABELS: &[&str] = &["domain", "emitter"];
const CLIENT_INGESTOR_SUBMISSION_PROMETHEUS_LABELS: &[&str] =
    &["domain", "ingestor", "outcome", "cause"];
const NO_DOMAIN_TIMESTAMP: i64 = i64::MIN;
const NO_HISTOGRAM_CAPACITY: u64 = u64::MAX;
const NO_WALL_ELAPSED_NANOS: u64 = u64::MAX;
const NO_EMA_VALUE_BITS: u64 = f64::NAN.to_bits();
const ONE_MINUTE: Duration = Duration::from_secs(60);
const FIFTEEN_MINUTES: Duration = Duration::from_secs(15 * 60);
const RATE_DECAY_TAU_FRACTION: f64 = 20.0;
const WALL_HISTOGRAM_1M_STEP: Duration = Duration::from_secs(10);
const WALL_HISTOGRAM_15M_STEP: Duration = Duration::from_secs(60);
const DOMAIN_HISTOGRAM_1M_STEP: Duration = Duration::from_secs(10);
const DOMAIN_HISTOGRAM_15M_STEP: Duration = Duration::from_secs(60);
const HISTOGRAM_VALUE_SCALE: f64 = 1_000.0;
const NANOS_PER_SECOND: f64 = 1_000_000_000.0;
const HISTOGRAM_DISPLAY_DECIMAL_SCALE: f64 = 10.0;
const HDR_HISTOGRAM_SIGFIG: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MetricKey {
    domain: String,
    target_kind: String,
    target: String,
    physical_node_id: Option<ClusterNodeName>,
    relay: String,
    peer_kind: String,
    peer: String,
    direction: String,
    metric: &'static str,
}

impl MetricKey {
    fn relay(
        domain: &DomainName,
        relay: &RelayName,
        physical_node_id: Option<&ClusterNodeName>,
        direction: &'static str,
        metric: &'static str,
    ) -> Self {
        Self {
            domain: domain.as_str().to_string(),
            target_kind: "RELAY".to_string(),
            target: relay.as_str().to_string(),
            physical_node_id: physical_node_id.cloned(),
            relay: relay.as_str().to_string(),
            peer_kind: String::new(),
            peer: String::new(),
            direction: direction.to_string(),
            metric,
        }
    }

    fn node(
        domain: &DomainName,
        kind: ModelKind,
        node: &ModelName,
        physical_node_id: Option<&ClusterNodeName>,
        relay: &RelayName,
        direction: &'static str,
        metric: &'static str,
    ) -> Self {
        Self {
            domain: domain.as_str().to_string(),
            target_kind: kind.as_str().to_ascii_uppercase(),
            target: node.as_str().to_string(),
            physical_node_id: physical_node_id.cloned(),
            relay: relay.as_str().to_string(),
            peer_kind: "RELAY".to_string(),
            peer: relay.as_str().to_string(),
            direction: direction.to_string(),
            metric,
        }
    }

    fn node_without_stream(
        domain: &DomainName,
        kind: ModelKind,
        node: &ModelName,
        physical_node_id: Option<&ClusterNodeName>,
        direction: &'static str,
        metric: &'static str,
    ) -> Self {
        Self {
            domain: domain.as_str().to_string(),
            target_kind: kind.as_str().to_ascii_uppercase(),
            target: node.as_str().to_string(),
            physical_node_id: physical_node_id.cloned(),
            relay: "-".to_string(),
            peer_kind: String::new(),
            peer: String::new(),
            direction: direction.to_string(),
            metric,
        }
    }

    fn matches_dataflow_metric_ref(&self, domain: &DomainName, metric: &DataflowMetricRef) -> bool {
        self.domain == domain.as_str()
            && self.target_kind.eq_ignore_ascii_case(&metric.target_kind)
            && self.target == metric.target
            && self.direction == metric.direction
            && self.relay == metric.relay.as_deref().unwrap_or("-")
    }

    fn is_relay_buffer_len(&self) -> bool {
        self.metric == RELAY_BUFFER_LEN
    }

    fn belongs_to_relay(&self, domain: &DomainName, relay: &RelayName) -> bool {
        self.domain == domain.as_str()
            && self.target_kind == "RELAY"
            && self.target == relay.as_str()
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
    series_started_at: Instant,
    value_bits: AtomicU64,
    last_elapsed_nanos: AtomicU64,
}

impl WallEma {
    fn new(series_started_at: Instant, tau_seconds: f64) -> Self {
        Self {
            tau_seconds,
            series_started_at,
            value_bits: AtomicU64::new(NO_EMA_VALUE_BITS),
            last_elapsed_nanos: AtomicU64::new(NO_WALL_ELAPSED_NANOS),
        }
    }

    fn from_snapshot(
        snapshot: &WallEmaSnapshot,
        series_started_at: Instant,
        tau_seconds: f64,
    ) -> Self {
        let last_at = match snapshot
            .last_at_wall_nanos
            .and_then(instant_from_wall_unix_nanos)
        {
            Some(last_at) => Some(last_at),
            None => snapshot
                .last_elapsed_seconds
                .map(|elapsed| instant_from_series_elapsed(series_started_at, elapsed)),
        };
        let series_started_at = match last_at {
            Some(last_at) if last_at < series_started_at => last_at,
            Some(_) | None => series_started_at,
        };
        let mut last_elapsed_nanos = NO_WALL_ELAPSED_NANOS;
        if let Some(last_at) = last_at
            && let Some(elapsed) = last_at.checked_duration_since(series_started_at)
            && let Ok(elapsed_nanos) = u64::try_from(elapsed.as_nanos())
        {
            last_elapsed_nanos = elapsed_nanos;
        }
        Self {
            tau_seconds,
            series_started_at,
            value_bits: AtomicU64::new(ema_value_bits(snapshot.value)),
            last_elapsed_nanos: AtomicU64::new(last_elapsed_nanos),
        }
    }

    fn observe_rate_delta(&self, delta: u64, now: Instant) {
        let Some(now_elapsed_nanos) = now
            .checked_duration_since(self.series_started_at)
            .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
        else {
            return;
        };
        let Some(last_elapsed_nanos) =
            advance_wall_elapsed(&self.last_elapsed_nanos, now_elapsed_nanos)
        else {
            return;
        };
        let elapsed_nanos = now_elapsed_nanos
            .checked_sub(last_elapsed_nanos)
            .verified("advance_wall_elapsed returns only an earlier elapsed timestamp");
        let elapsed_seconds = Duration::from_nanos(elapsed_nanos).as_secs_f64();
        if elapsed_seconds <= 0.0 {
            return;
        }
        observe_ema_sample(
            &self.value_bits,
            delta.approx_into::<f64>() / elapsed_seconds,
            elapsed_seconds,
            self.tau_seconds,
        );
    }

    fn value_at(&self, now: Instant) -> Option<f64> {
        let value = load_ema_value(&self.value_bits)?;
        let last_elapsed_nanos = self.last_elapsed_nanos.load(AtomicOrdering::Relaxed);
        if last_elapsed_nanos == NO_WALL_ELAPSED_NANOS {
            return None;
        }
        let now_elapsed_nanos = now
            .checked_duration_since(self.series_started_at)
            .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())?;
        let elapsed_nanos = now_elapsed_nanos.checked_sub(last_elapsed_nanos)?;
        let elapsed_seconds = Duration::from_nanos(elapsed_nanos).as_secs_f64();
        Some(value * decay_factor(elapsed_seconds, self.tau_seconds))
    }

    fn to_snapshot(&self, _series_started_at: Instant) -> WallEmaSnapshot {
        let last_elapsed_nanos = self.last_elapsed_nanos.load(AtomicOrdering::Relaxed);
        let last_elapsed = if last_elapsed_nanos == NO_WALL_ELAPSED_NANOS {
            None
        } else {
            Some(Duration::from_nanos(last_elapsed_nanos))
        };
        let last_at = last_elapsed.and_then(|elapsed| self.series_started_at.checked_add(elapsed));
        WallEmaSnapshot {
            value: load_ema_value(&self.value_bits),
            last_elapsed_seconds: last_elapsed.map(|elapsed| elapsed.as_secs_f64()),
            last_at_wall_nanos: last_at.and_then(wall_unix_nanos_from_instant),
        }
    }
}

#[derive(Debug)]
struct DomainEma {
    tau_seconds: f64,
    value_bits: AtomicU64,
    last_at_nanos: AtomicI64,
}

impl DomainEma {
    fn new(tau_seconds: f64) -> Self {
        Self {
            tau_seconds,
            value_bits: AtomicU64::new(NO_EMA_VALUE_BITS),
            last_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
        }
    }

    fn from_snapshot(snapshot: &DomainEmaSnapshot, tau_seconds: f64) -> Self {
        Self {
            tau_seconds,
            value_bits: AtomicU64::new(ema_value_bits(snapshot.value)),
            last_at_nanos: AtomicI64::new(snapshot.last_at_nanos.unwrap_or(NO_DOMAIN_TIMESTAMP)),
        }
    }

    fn observe_rate_delta(&self, delta: u64, now: Timestamp) {
        let now = now.unix_nanos();
        if now == NO_DOMAIN_TIMESTAMP {
            return;
        }
        let Some(last_at) = advance_domain_timestamp(&self.last_at_nanos, now) else {
            return;
        };
        let elapsed_nanos = now
            .checked_sub(last_at)
            .verified("advance_domain_timestamp returns only an earlier domain timestamp");
        let Ok(elapsed_nanos) = u64::try_from(elapsed_nanos) else {
            return;
        };
        let elapsed_seconds = Duration::from_nanos(elapsed_nanos).as_secs_f64();
        if elapsed_seconds <= 0.0 {
            return;
        }
        observe_ema_sample(
            &self.value_bits,
            delta.approx_into::<f64>() / elapsed_seconds,
            elapsed_seconds,
            self.tau_seconds,
        );
    }

    fn value_at(&self, now: Option<Timestamp>) -> Option<f64> {
        let value = load_ema_value(&self.value_bits)?;
        let last_at = self.last_at_nanos.load(AtomicOrdering::Relaxed);
        if last_at == NO_DOMAIN_TIMESTAMP {
            return None;
        }
        let now = now?.unix_nanos();
        let elapsed_nanos = now.checked_sub(last_at)?;
        let Ok(elapsed_nanos) = u64::try_from(elapsed_nanos) else {
            return Some(value);
        };
        let elapsed_seconds = Duration::from_nanos(elapsed_nanos).as_secs_f64();
        Some(value * decay_factor(elapsed_seconds, self.tau_seconds))
    }

    fn to_snapshot(&self) -> DomainEmaSnapshot {
        let last_at_nanos = self.last_at_nanos.load(AtomicOrdering::Relaxed);
        DomainEmaSnapshot {
            value: load_ema_value(&self.value_bits),
            last_at_nanos: (last_at_nanos != NO_DOMAIN_TIMESTAMP).then_some(last_at_nanos),
        }
    }
}

fn advance_wall_elapsed(target: &AtomicU64, elapsed_nanos: u64) -> Option<u64> {
    if elapsed_nanos == NO_WALL_ELAPSED_NANOS {
        return None;
    }
    let mut current = target.load(AtomicOrdering::Relaxed);
    loop {
        if current != NO_WALL_ELAPSED_NANOS && elapsed_nanos <= current {
            return None;
        }
        match target.compare_exchange_weak(
            current,
            elapsed_nanos,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(previous) => {
                if previous == NO_WALL_ELAPSED_NANOS {
                    return None;
                }
                return Some(previous);
            }
            Err(observed) => current = observed,
        }
    }
}

fn advance_domain_timestamp(target: &AtomicI64, timestamp: i64) -> Option<i64> {
    if timestamp == NO_DOMAIN_TIMESTAMP {
        return None;
    }
    let mut current = target.load(AtomicOrdering::Relaxed);
    loop {
        if current != NO_DOMAIN_TIMESTAMP && timestamp <= current {
            return None;
        }
        match target.compare_exchange_weak(
            current,
            timestamp,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(previous) => {
                if previous == NO_DOMAIN_TIMESTAMP {
                    return None;
                }
                return Some(previous);
            }
            Err(observed) => current = observed,
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
    fn new(series_started_at: Instant) -> Self {
        Self {
            wall_1m: WallEma::new(series_started_at, rate_decay_tau_seconds(ONE_MINUTE)),
            wall_15m: WallEma::new(series_started_at, rate_decay_tau_seconds(FIFTEEN_MINUTES)),
            domain_1m: DomainEma::new(rate_decay_tau_seconds(ONE_MINUTE)),
            domain_15m: DomainEma::new(rate_decay_tau_seconds(FIFTEEN_MINUTES)),
        }
    }

    fn from_snapshot(snapshot: Option<&RollingRatesSnapshot>, series_started_at: Instant) -> Self {
        let Some(snapshot) = snapshot else {
            return Self::new(series_started_at);
        };
        Self {
            wall_1m: WallEma::from_snapshot(
                &snapshot.wall_1m,
                series_started_at,
                rate_decay_tau_seconds(ONE_MINUTE),
            ),
            wall_15m: WallEma::from_snapshot(
                &snapshot.wall_15m,
                series_started_at,
                rate_decay_tau_seconds(FIFTEEN_MINUTES),
            ),
            domain_1m: DomainEma::from_snapshot(
                &snapshot.domain_1m,
                rate_decay_tau_seconds(ONE_MINUTE),
            ),
            domain_15m: DomainEma::from_snapshot(
                &snapshot.domain_15m,
                rate_decay_tau_seconds(FIFTEEN_MINUTES),
            ),
        }
    }

    fn observe(&self, delta: u64, domain_timestamp: Option<Timestamp>) {
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
            highest_trackable_value: scaled_histogram_value(highest_bucket)
                .assured(
                    "the fold above starts at 1.0 and keeps only finite positive buckets, and \
                     every bucket ladder in this module ends far below the u64 range",
                )
                .max(2),
            significant_figures: HDR_HISTOGRAM_SIGFIG,
        }
    }

    fn new_histogram(self) -> HdrHistogram<u64> {
        HdrHistogram::<u64>::new_with_max(self.highest_trackable_value, self.significant_figures)
            .assured(
                "for_buckets raises the maximum to at least 2 and HDR_HISTOGRAM_SIGFIG is within \
                 the 0..=5 hdrhistogram accepts",
            )
    }

    /// How a batch of delivery latencies folds into these histograms' buckets: elapsed nanoseconds
    /// rounded to the recorded unit, clamped at the same maximum, at the same precision.
    fn delivery_latency_layout(self) -> ElapsedLayout {
        let unit_nanos: u64 = (NANOS_PER_SECOND / HISTOGRAM_VALUE_SCALE)
            .checked_approx_into()
            .assured("a second divides into the recorded units as a whole number of nanoseconds");
        let unit_nanos = NonZeroU64::new(unit_nanos)
            .assured("a recorded unit is a thousandth of a second, which is not zero nanoseconds");
        ElapsedLayout::new(
            unit_nanos,
            self.highest_trackable_value,
            self.significant_figures,
        )
        .assured(
            "the latency ladder tops out at 30 s, far inside the range the kernel rounds exactly, \
             for_buckets raises the maximum to at least 2, and HDR_HISTOGRAM_SIGFIG is within \
             0..=5",
        )
    }
}

/// What one recording adds to a histogram series.
#[derive(Debug, Clone, Copy)]
enum HistogramSamples<'a> {
    /// One observation, in recorded units.
    One(u64),
    /// Every delivery latency of one batch, already folded into recorded-unit buckets.
    Elapsed(&'a ElapsedHistogram),
}

impl HistogramSamples<'_> {
    /// One observation in recorded units, or `None` for one that has none: a negative sample, or
    /// one whose recorded magnitude leaves the `u64` range. Neither belongs in a histogram.
    fn one(value: f64) -> Option<Self> {
        if value < 0.0 {
            return None;
        }
        let units = scaled_histogram_value(value)?;
        Some(Self::One(units))
    }

    fn record_into(self, histogram: &mut HdrHistogram<u64>) {
        // The configured maximum is the top of this metric's bucket ladder, so a sample above it
        // belongs in the top bucket exactly as one past the last explicit boundary does. Clamping
        // here is what puts it there; letting `record` reject it instead would drop the sample and
        // pull every percentile below the truth, which is the one outcome a latency histogram must
        // not produce. The kernel already clamps a batch at the same maximum.
        let highest = histogram.high();
        match self {
            Self::One(units) => {
                histogram
                    .record(units.min(highest))
                    .assured("the value was just clamped to the histogram's own maximum");
            }
            Self::Elapsed(latencies) => {
                for bucket in latencies.buckets() {
                    histogram
                        .record_n(bucket.lowest_units.min(highest), bucket.count)
                        .assured("the value was just clamped to the histogram's own maximum");
                }
            }
        }
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

    /// Records `samples` in the bucket of the window step that holds `now_nanos`.
    ///
    /// A step the window has already moved past keeps samples only while its bucket still exists;
    /// an older step is never reopened, so its samples are not recorded.
    fn record_at(&mut self, samples: HistogramSamples<'_>, now_nanos: i64) {
        let current_start = bucket_start(now_nanos, self.step);
        self.ensure_current_bucket(current_start);
        // Buckets are ordered by start, and the current step is normally the newest.
        let current = self
            .buckets
            .iter_mut()
            .rev()
            .find(|bucket| bucket.start_at_nanos == current_start);
        if let Some(bucket) = current {
            samples.record_into(&mut bucket.histogram);
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
            merged
                .add(&bucket.histogram)
                .assured("merged was built from the same self.config as every bucket it holds");
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
                existing.histogram.add(&bucket.histogram).assured(
                    "both ladders come from internal_buckets_for_metric, and aggregation only \
                     merges series whose aggregate key carries the same metric",
                );
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

    fn record(&mut self, samples: HistogramSamples<'_>, now_nanos: i64) {
        self.inner.record_at(samples, now_nanos);
    }

    fn summary(&self) -> HistogramPercentileSummary {
        match current_wall_unix_nanos() {
            Some(now) => self.inner.summary_at(now),
            None => HistogramPercentileSummary::empty(),
        }
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

    fn record(&mut self, samples: HistogramSamples<'_>, domain_now_nanos: i64) {
        self.inner.record_at(samples, domain_now_nanos);
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
            wall_1m: WallRollingHistogram::new(ONE_MINUTE, WALL_HISTOGRAM_1M_STEP, buckets),
            wall_15m: WallRollingHistogram::new(FIFTEEN_MINUTES, WALL_HISTOGRAM_15M_STEP, buckets),
            domain_1m: DomainRollingHistogram::new(ONE_MINUTE, DOMAIN_HISTOGRAM_1M_STEP, buckets),
            domain_15m: DomainRollingHistogram::new(
                FIFTEEN_MINUTES,
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
                ONE_MINUTE,
                WALL_HISTOGRAM_1M_STEP,
                buckets,
            ),
            wall_15m: WallRollingHistogram::from_snapshot(
                &snapshot.wall_15m,
                FIFTEEN_MINUTES,
                WALL_HISTOGRAM_15M_STEP,
                buckets,
            ),
            domain_1m: DomainRollingHistogram::from_snapshot(
                &snapshot.domain_1m,
                ONE_MINUTE,
                DOMAIN_HISTOGRAM_1M_STEP,
                buckets,
            ),
            domain_15m: DomainRollingHistogram::from_snapshot(
                &snapshot.domain_15m,
                FIFTEEN_MINUTES,
                DOMAIN_HISTOGRAM_15M_STEP,
                buckets,
            ),
        }
    }

    /// Records `samples` in every window: the wall windows at `wall_now_nanos` when the wall clock
    /// has a nanosecond reading, the domain windows at `domain_now_nanos` when the samples carry
    /// domain time.
    fn record(
        &mut self,
        samples: HistogramSamples<'_>,
        wall_now_nanos: Option<i64>,
        domain_now_nanos: Option<i64>,
    ) {
        if let Some(wall_now_nanos) = wall_now_nanos {
            self.wall_1m.record(samples, wall_now_nanos);
            self.wall_15m.record(samples, wall_now_nanos);
        }
        if let Some(domain_now_nanos) = domain_now_nanos {
            self.domain_1m.record(samples, domain_now_nanos);
            self.domain_15m.record(samples, domain_now_nanos);
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
            wall_1m: TimeRollingHistogram::new(ONE_MINUTE, WALL_HISTOGRAM_1M_STEP, buckets),
            wall_15m: TimeRollingHistogram::new(FIFTEEN_MINUTES, WALL_HISTOGRAM_15M_STEP, buckets),
            domain_1m: TimeRollingHistogram::new(ONE_MINUTE, DOMAIN_HISTOGRAM_1M_STEP, buckets),
            domain_15m: TimeRollingHistogram::new(
                FIFTEEN_MINUTES,
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
            self.domain_last_at_nanos = Some(match self.domain_last_at_nanos {
                Some(current) => current.max(last_at_nanos),
                None => last_at_nanos,
            });
        }
    }

    fn summary(&self) -> HistogramSummary {
        let domain_timestamp = self.domain_last_at_nanos.map(Timestamp::from_unix_nanos);
        HistogramSummary {
            capacity: None,
            rolling_histograms: RollingHistogramSummary {
                wall_1m: match current_wall_unix_nanos() {
                    Some(now) => self.wall_1m.summary_at(now),
                    None => HistogramPercentileSummary::empty(),
                },
                wall_15m: match current_wall_unix_nanos() {
                    Some(now) => self.wall_15m.summary_at(now),
                    None => HistogramPercentileSummary::empty(),
                },
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
    rolling: RollingRates,
}

impl Default for CounterSeries {
    fn default() -> Self {
        let started_at = Instant::now();
        Self {
            started_at,
            domain_started_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
            domain_last_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
            value: AtomicU64::new(0),
            rolling: RollingRates::new(started_at),
        }
    }
}

impl CounterSeries {
    fn from_snapshot(snapshot: &MetricCounterSnapshot) -> Self {
        let started_at = match snapshot
            .started_at_wall_nanos
            .and_then(instant_from_wall_unix_nanos)
        {
            Some(started_at) => started_at,
            None => started_at_from_elapsed(snapshot.elapsed_seconds),
        };
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
            rolling: RollingRates::from_snapshot(snapshot.rolling.as_ref(), started_at),
        }
    }

    fn increment(&self, value: u64, domain_timestamp: Option<Timestamp>) {
        self.value.fetch_add(value, AtomicOrdering::Relaxed);
        self.observe_domain_timestamp(domain_timestamp);
        self.rolling.observe(value, domain_timestamp);
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
            rolling: Some(self.rolling.to_snapshot(self.started_at)),
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
            rolling: self.rolling.summary(
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
    /// Whether anything was ever recorded. It is set while the rolling histograms are locked for
    /// the recording, so a reader that sees it and then locks them also sees that recording.
    observed: AtomicBool,
    rolling_histograms: Mutex<RollingHistograms>,
}

impl HistogramSeries {
    fn new(buckets: &'static [f64]) -> Self {
        Self {
            started_at: Instant::now(),
            domain_started_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
            domain_last_at_nanos: AtomicI64::new(NO_DOMAIN_TIMESTAMP),
            capacity: AtomicU64::new(NO_HISTOGRAM_CAPACITY),
            observed: AtomicBool::new(false),
            rolling_histograms: Mutex::new(RollingHistograms::new(buckets)),
        }
    }

    /// Records one observation or one batch: it reads the wall clock once and locks the rolling
    /// histograms once, however many samples a batch holds.
    fn record(
        &self,
        samples: HistogramSamples<'_>,
        capacity: Option<u64>,
        domain_timestamp: Option<Timestamp>,
    ) {
        if let Some(capacity) = capacity {
            self.capacity.store(capacity, AtomicOrdering::Relaxed);
        }
        let domain_now_nanos = match domain_timestamp {
            Some(domain_timestamp) => {
                observe_domain_timestamp(
                    &self.domain_started_at_nanos,
                    &self.domain_last_at_nanos,
                    domain_timestamp,
                );
                Some(domain_timestamp.unix_nanos())
            }
            None => None,
        };
        let wall_now_nanos = current_wall_unix_nanos();
        let mut rolling_histograms = self.rolling_histograms.lock();
        rolling_histograms.record(samples, wall_now_nanos, domain_now_nanos);
        self.observed.store(true, AtomicOrdering::Relaxed);
    }

    fn from_snapshot(snapshot: &MetricHistogramSnapshot) -> Self {
        let started_at = match snapshot
            .started_at_wall_nanos
            .and_then(instant_from_wall_unix_nanos)
        {
            Some(started_at) => started_at,
            None => started_at_from_elapsed(snapshot.elapsed_seconds),
        };
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
            observed: AtomicBool::new(snapshot.rolling_histograms.is_some()),
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

    fn was_observed(&self) -> bool {
        self.observed.load(AtomicOrdering::Relaxed)
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
        self.value = self
            .value
            .checked_add(summary.value)
            .assured("both totals count events this cluster already observed");
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

/// The handle a runtime, an ingestor quiesce control, or a branch task keeps to record
/// measurements. Every series lives together for as long as the node does, so the handle is one
/// `Arc` over all of them and cloning it costs a single refcount.
#[derive(Debug, Clone)]
pub struct RuntimeMetrics {
    series: Arc<MetricSeries>,
}

/// Every series one node records into, plus the Prometheus registry they are exported through.
#[derive(Debug)]
struct MetricSeries {
    counters: DashMap<MetricKey, Arc<CounterSeries>>,
    histograms: DashMap<MetricKey, Arc<HistogramSeries>>,
    branch_counters: DashMap<BranchMetricKey, Arc<CounterSeries>>,
    branch_histograms: DashMap<BranchMetricKey, Arc<HistogramSeries>>,
    branch_instance_references: DashMap<BranchInstanceMetricKey, BranchInstanceReferences>,
    prometheus: PrometheusMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumIter)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum BranchEvictionReason {
    Lru,
    Ttl,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BranchInstanceMetricKey {
    domain: String,
    branch: String,
    physical_node_id: Option<ClusterNodeName>,
    concrete_key: String,
}

#[derive(Debug)]
struct BranchInstanceReferences {
    count: usize,
    eviction_reason: Option<BranchEvictionReason>,
}

#[derive(Debug, Clone)]
struct PrometheusMetrics {
    registry: Registry,
    /// Registered empty and filled in once the node's transport, executor and consensus observer
    /// exist. See [`RuntimeMetrics::install_node_observations`].
    interconnection: Arc<InterconnectionCollector>,
    messages_total: IntCounterVec,
    batches_total: IntCounterVec,
    bytes_total: IntCounterVec,
    messages_per_batch: HistogramVec,
    delivery_latency_seconds: HistogramVec,
    relay_buffer_len: HistogramVec,
    branch_instances: IntGaugeVec,
    branch_evictions_total: IntCounterVec,
    ingestor_quiesce_buffered_records: IntGaugeVec,
    ingestor_quiesce_buffered_bytes: IntGaugeVec,
    ingestor_quiesce_dropped_total: IntCounterVec,
    ingestor_quiesce_rejected_total: IntCounterVec,
    session_subscriptions: IntGaugeVec,
    session_subscription_dropped_rows_total: IntCounterVec,
    client_ingestor_producers: IntGaugeVec,
    client_ingestor_forwarded_producers: IntGaugeVec,
    client_ingestor_outstanding_batches: IntGaugeVec,
    client_ingestor_outstanding_bytes: IntGaugeVec,
    client_ingestor_admitted_batches: IntGaugeVec,
    client_ingestor_submissions_total: IntCounterVec,
    client_emitter_consumers: IntGaugeVec,
    client_emitter_forwarded_consumers: IntGaugeVec,
    client_emitter_forwarded_credit_bytes: IntGaugeVec,
    client_emitter_forwarded_retained_batches: IntGaugeVec,
    client_emitter_forwarded_retained_bytes: IntGaugeVec,
    client_emitter_retained_batches: IntGaugeVec,
    client_emitter_retained_bytes: IntGaugeVec,
    client_emitter_incomplete_batches: IntGaugeVec,
    client_emitter_retries_total: IntCounterVec,
    client_emitter_acks_total: IntCounterVec,
    client_emitter_rejections_total: IntCounterVec,
}

#[derive(Debug, Clone)]
pub(crate) struct IngestorQuiesceMetricLabels {
    domain: String,
    ingestor: String,
    physical_node_id: Option<ClusterNodeName>,
}

impl IngestorQuiesceMetricLabels {
    fn values(&self) -> [&str; 3] {
        [
            self.domain.as_str(),
            self.ingestor.as_str(),
            physical_node_label(self.physical_node_id.as_ref()),
        ]
    }
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
            series: Arc::new(MetricSeries {
                counters: DashMap::new(),
                histograms: DashMap::new(),
                branch_counters: DashMap::new(),
                branch_histograms: DashMap::new(),
                branch_instance_references: DashMap::new(),
                prometheus: PrometheusMetrics::new(),
            }),
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
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let batches_total = IntCounterVec::new(
            Opts::new(
                BATCHES_TOTAL,
                "Total graph batches observed by Nervix runtime targets.",
            )
            .namespace("nervix"),
            PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let bytes_total = IntCounterVec::new(
            Opts::new(
                BYTES_TOTAL,
                "Total graph bytes observed by Nervix runtime targets.",
            )
            .namespace("nervix"),
            PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let messages_per_batch = HistogramVec::new(
            HistogramOpts::new(
                MESSAGES_PER_BATCH,
                "Raw graph messages per batch observed by Nervix runtime targets.",
            )
            .namespace("nervix")
            .buckets(MESSAGE_BATCH_BUCKETS.to_vec()),
            PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let delivery_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                DELIVERY_LATENCY_SECONDS,
                "Raw graph delivery latency in seconds observed by Nervix runtime targets.",
            )
            .namespace("nervix")
            .buckets(LATENCY_BUCKETS.to_vec()),
            PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let relay_buffer_len = HistogramVec::new(
            HistogramOpts::new(
                RELAY_BUFFER_LEN,
                "Runtime relay buffer occupancy observed by Nervix runtime targets.",
            )
            .namespace("nervix")
            .buckets(RELAY_BUFFER_LEN_BUCKETS.to_vec()),
            PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let branch_instances = IntGaugeVec::new(
            Opts::new(
                BRANCH_INSTANCES,
                "Current concrete branch keys with runtime instances on this Nervix node.",
            )
            .namespace("nervix"),
            BRANCH_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let branch_evictions_total = IntCounterVec::new(
            Opts::new(
                BRANCH_EVICTIONS_TOTAL,
                "Total concrete branch keys evicted by Nervix.",
            )
            .namespace("nervix"),
            BRANCH_EVICTION_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let ingestor_quiesce_buffered_records = IntGaugeVec::new(
            Opts::new(
                INGESTOR_QUIESCE_BUFFERED_RECORDS,
                "Current raw source payloads buffered while an ingestor is quiesced.",
            )
            .namespace("nervix"),
            INGESTOR_QUIESCE_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let ingestor_quiesce_buffered_bytes = IntGaugeVec::new(
            Opts::new(
                INGESTOR_QUIESCE_BUFFERED_BYTES,
                "Current raw source payload bytes buffered while an ingestor is quiesced.",
            )
            .namespace("nervix"),
            INGESTOR_QUIESCE_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let ingestor_quiesce_dropped_total = IntCounterVec::new(
            Opts::new(
                INGESTOR_QUIESCE_DROPPED_TOTAL,
                "Total source payloads discarded under an ingestor quiesce contract.",
            )
            .namespace("nervix"),
            INGESTOR_QUIESCE_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let ingestor_quiesce_rejected_total = IntCounterVec::new(
            Opts::new(
                INGESTOR_QUIESCE_REJECTED_TOTAL,
                "Total endpoint requests rejected while an ingestor is quiesced.",
            )
            .namespace("nervix"),
            INGESTOR_QUIESCE_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );

        registry.register(Box::new(messages_total.clone())).assured(
            "this registry is built here and each metric is registered once under a distinct name",
        );
        registry.register(Box::new(batches_total.clone())).assured(
            "this registry is built here and each metric is registered once under a distinct name",
        );
        registry.register(Box::new(bytes_total.clone())).assured(
            "this registry is built here and each metric is registered once under a distinct name",
        );
        registry
            .register(Box::new(messages_per_batch.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        registry
            .register(Box::new(delivery_latency_seconds.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        registry
            .register(Box::new(relay_buffer_len.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        registry
            .register(Box::new(branch_instances.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        registry
            .register(Box::new(branch_evictions_total.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        registry
            .register(Box::new(ingestor_quiesce_buffered_records.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        registry
            .register(Box::new(ingestor_quiesce_buffered_bytes.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        registry
            .register(Box::new(ingestor_quiesce_dropped_total.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        registry
            .register(Box::new(ingestor_quiesce_rejected_total.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        let session_subscriptions = IntGaugeVec::new(
            Opts::new(
                SESSION_SUBSCRIPTIONS,
                "Open session subscriptions this node delivers from the relay. The node \
                 advertises interest in the relay to the cluster exactly while this is above zero.",
            )
            .namespace("nervix"),
            SESSION_SUBSCRIPTION_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        registry
            .register(Box::new(session_subscriptions.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        let session_subscription_dropped_rows_total = IntCounterVec::new(
            Opts::new(
                SESSION_SUBSCRIPTION_DROPPED_ROWS_TOTAL,
                "Rows dropping session subscriptions on this node discarded because their session \
                 could not take them in time.",
            )
            .namespace("nervix"),
            SESSION_SUBSCRIPTION_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        registry
            .register(Box::new(session_subscription_dropped_rows_total.clone()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        let client_ingestor_producers = IntGaugeVec::new(
            Opts::new(
                CLIENT_INGESTOR_PRODUCERS,
                "Producers attached to a client ingestor this node executes.",
            )
            .namespace("nervix"),
            CLIENT_INGESTOR_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let client_ingestor_forwarded_producers = IntGaugeVec::new(
            Opts::new(
                CLIENT_INGESTOR_FORWARDED_PRODUCERS,
                "Producers of a client ingestor this node executes whose sessions another node \
                 serves.",
            )
            .namespace("nervix"),
            CLIENT_INGESTOR_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let client_ingestor_outstanding_batches = IntGaugeVec::new(
            Opts::new(
                CLIENT_INGESTOR_OUTSTANDING_BATCHES,
                "Batches the producers of a client ingestor submitted and have no outcome for yet.",
            )
            .namespace("nervix"),
            CLIENT_INGESTOR_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let client_ingestor_outstanding_bytes = IntGaugeVec::new(
            Opts::new(
                CLIENT_INGESTOR_OUTSTANDING_BYTES,
                "Arrow IPC bytes of the batches a client ingestor's producers have outstanding.",
            )
            .namespace("nervix"),
            CLIENT_INGESTOR_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let client_ingestor_admitted_batches = IntGaugeVec::new(
            Opts::new(
                CLIENT_INGESTOR_ADMITTED_BATCHES,
                "Batches of a client ingestor holding a slot of its acknowledgement window: being \
                 admitted, or admitted and awaiting their acknowledgement.",
            )
            .namespace("nervix"),
            CLIENT_INGESTOR_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let client_ingestor_submissions_total = IntCounterVec::new(
            Opts::new(
                CLIENT_INGESTOR_SUBMISSIONS_TOTAL,
                "Batches a client ingestor answered, by outcome and cause.",
            )
            .namespace("nervix"),
            CLIENT_INGESTOR_SUBMISSION_PROMETHEUS_LABELS,
        )
        .assured(
            "the metric name, help text and label names are constants that satisfy Prometheus \
             naming rules",
        );
        let client_ingestor_collectors: [Box<dyn prometheus::core::Collector>; 6] = [
            Box::new(client_ingestor_producers.clone()),
            Box::new(client_ingestor_forwarded_producers.clone()),
            Box::new(client_ingestor_outstanding_batches.clone()),
            Box::new(client_ingestor_outstanding_bytes.clone()),
            Box::new(client_ingestor_admitted_batches.clone()),
            Box::new(client_ingestor_submissions_total.clone()),
        ];
        for collector in client_ingestor_collectors {
            registry.register(collector).assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        }
        let emitter_gauge = |name, help| {
            IntGaugeVec::new(
                Opts::new(name, help).namespace("nervix"),
                CLIENT_EMITTER_PROMETHEUS_LABELS,
            )
            .assured("client emitter metric names and labels are fixed valid Prometheus names")
        };
        let emitter_counter = |name, help| {
            IntCounterVec::new(
                Opts::new(name, help).namespace("nervix"),
                CLIENT_EMITTER_PROMETHEUS_LABELS,
            )
            .assured("client emitter metric names and labels are fixed valid Prometheus names")
        };
        let client_emitter_consumers = emitter_gauge(
            CLIENT_EMITTER_CONSUMERS,
            "Application consumers attached to this client emitter.",
        );
        let client_emitter_forwarded_consumers = emitter_gauge(
            CLIENT_EMITTER_FORWARDED_CONSUMERS,
            "Client emitter consumers served on another node.",
        );
        let client_emitter_forwarded_credit_bytes = emitter_gauge(
            CLIENT_EMITTER_FORWARDED_CREDIT_BYTES,
            "Reserved byte credit of client emitter consumers served on another node.",
        );
        let client_emitter_forwarded_retained_batches = emitter_gauge(
            CLIENT_EMITTER_FORWARDED_RETAINED_BATCHES,
            "Retained output batches assigned to consumers served on another node.",
        );
        let client_emitter_forwarded_retained_bytes = emitter_gauge(
            CLIENT_EMITTER_FORWARDED_RETAINED_BYTES,
            "Arrow IPC bytes retained for consumers served on another node.",
        );
        let client_emitter_retained_batches = emitter_gauge(
            CLIENT_EMITTER_RETAINED_BATCHES,
            "Client emitter output batches still awaiting application settlement.",
        );
        let client_emitter_retained_bytes = emitter_gauge(
            CLIENT_EMITTER_RETAINED_BYTES,
            "Arrow IPC bytes retained while client emitter output awaits application settlement.",
        );
        let client_emitter_incomplete_batches = emitter_gauge(
            CLIENT_EMITTER_INCOMPLETE_BATCHES,
            "Assigned output batches awaiting application processing.",
        );
        let client_emitter_retries_total = emitter_counter(
            CLIENT_EMITTER_RETRIES_TOTAL,
            "Client emitter output attempts revoked for retry, timeout or consumer loss.",
        );
        let client_emitter_acks_total = emitter_counter(
            CLIENT_EMITTER_ACKS_TOTAL,
            "Client emitter output batches acknowledged by applications.",
        );
        let client_emitter_rejections_total = emitter_counter(
            CLIENT_EMITTER_REJECTIONS_TOTAL,
            "Client emitter output batches rejected by applications.",
        );
        let client_emitter_collectors: [Box<dyn prometheus::core::Collector>; 11] = [
            Box::new(client_emitter_consumers.clone()),
            Box::new(client_emitter_forwarded_consumers.clone()),
            Box::new(client_emitter_forwarded_credit_bytes.clone()),
            Box::new(client_emitter_forwarded_retained_batches.clone()),
            Box::new(client_emitter_forwarded_retained_bytes.clone()),
            Box::new(client_emitter_retained_batches.clone()),
            Box::new(client_emitter_retained_bytes.clone()),
            Box::new(client_emitter_incomplete_batches.clone()),
            Box::new(client_emitter_retries_total.clone()),
            Box::new(client_emitter_acks_total.clone()),
            Box::new(client_emitter_rejections_total.clone()),
        ];
        for collector in client_emitter_collectors {
            registry.register(collector).assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        }
        registry
            .register(Box::new(JemallocMetricsCollector::new()))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );
        let interconnection = Arc::new(InterconnectionCollector::new());
        registry
            .register(Box::new(InterconnectionCollectorHandle {
                collector: Arc::clone(&interconnection),
            }))
            .assured(
                "this registry is built here and each metric is registered once under a distinct \
                 name",
            );

        Self {
            registry,
            interconnection,
            messages_total,
            batches_total,
            bytes_total,
            messages_per_batch,
            delivery_latency_seconds,
            relay_buffer_len,
            branch_instances,
            branch_evictions_total,
            ingestor_quiesce_buffered_records,
            ingestor_quiesce_buffered_bytes,
            ingestor_quiesce_dropped_total,
            ingestor_quiesce_rejected_total,
            session_subscriptions,
            session_subscription_dropped_rows_total,
            client_ingestor_producers,
            client_ingestor_forwarded_producers,
            client_ingestor_outstanding_batches,
            client_ingestor_outstanding_bytes,
            client_ingestor_admitted_batches,
            client_ingestor_submissions_total,
            client_emitter_consumers,
            client_emitter_forwarded_consumers,
            client_emitter_forwarded_credit_bytes,
            client_emitter_forwarded_retained_batches,
            client_emitter_forwarded_retained_bytes,
            client_emitter_retained_batches,
            client_emitter_retained_bytes,
            client_emitter_incomplete_batches,
            client_emitter_retries_total,
            client_emitter_acks_total,
            client_emitter_rejections_total,
        }
    }

    fn counter(&self, key: &MetricKey) -> Option<IntCounter> {
        match key.metric {
            MESSAGES_TOTAL => Some(
                self.messages_total
                    .with_label_values(&prometheus_label_values(key)),
            ),
            BATCHES_TOTAL => Some(
                self.batches_total
                    .with_label_values(&prometheus_label_values(key)),
            ),
            BYTES_TOTAL => Some(
                self.bytes_total
                    .with_label_values(&prometheus_label_values(key)),
            ),
            _ => None,
        }
    }

    fn histogram(&self, key: &MetricKey) -> Option<Histogram> {
        match key.metric {
            MESSAGES_PER_BATCH => Some(
                self.messages_per_batch
                    .with_label_values(&prometheus_label_values(key)),
            ),
            DELIVERY_LATENCY_SECONDS => Some(
                self.delivery_latency_seconds
                    .with_label_values(&prometheus_label_values(key)),
            ),
            RELAY_BUFFER_LEN => Some(
                self.relay_buffer_len
                    .with_label_values(&prometheus_label_values(key)),
            ),
            _ => None,
        }
    }

    /// Withdraw one label set from the Prometheus registry.
    ///
    /// Removal reports an error when the label set was never registered, which happens whenever an
    /// entity is torn down before it produced its first observation of that metric. There is
    /// nothing to withdraw and nothing to report, which is what
    /// [`WITHDRAWN_SERIES_MAY_NOT_EXIST`] states at each removal below.
    fn remove(&self, key: &MetricKey) {
        let labels = prometheus_label_values(key);
        match key.metric {
            MESSAGES_TOTAL => {
                self.messages_total
                    .remove_label_values(&labels)
                    .discarded(WITHDRAWN_SERIES_MAY_NOT_EXIST);
            }
            BATCHES_TOTAL => {
                self.batches_total
                    .remove_label_values(&labels)
                    .discarded(WITHDRAWN_SERIES_MAY_NOT_EXIST);
            }
            BYTES_TOTAL => {
                self.bytes_total
                    .remove_label_values(&labels)
                    .discarded(WITHDRAWN_SERIES_MAY_NOT_EXIST);
            }
            MESSAGES_PER_BATCH => {
                self.messages_per_batch
                    .remove_label_values(&labels)
                    .discarded(WITHDRAWN_SERIES_MAY_NOT_EXIST);
            }
            DELIVERY_LATENCY_SECONDS => {
                self.delivery_latency_seconds
                    .remove_label_values(&labels)
                    .discarded(WITHDRAWN_SERIES_MAY_NOT_EXIST);
            }
            RELAY_BUFFER_LEN => {
                self.relay_buffer_len
                    .remove_label_values(&labels)
                    .discarded(WITHDRAWN_SERIES_MAY_NOT_EXIST);
            }
            _ => {}
        }
    }

    fn text(&self) -> String {
        let encoder = TextEncoder::new();
        let mut metric_families = self.registry.gather();
        for family in &mut metric_families {
            if family.get_field_type() != MetricType::HISTOGRAM {
                continue;
            }
            family
                .mut_metric()
                .retain(|metric| metric.get_histogram().sample_count() > 0);
        }
        metric_families.retain(|family| !family.get_metric().is_empty());
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
            epoch: epoch::mib()
                .assured("the statically linked tikv-jemalloc build exposes this control key"),
            active: stats::active::mib()
                .assured("the statically linked tikv-jemalloc build exposes this control key"),
            allocated: stats::allocated::mib()
                .assured("the statically linked tikv-jemalloc build exposes this control key"),
            mapped: stats::mapped::mib()
                .assured("the statically linked tikv-jemalloc build exposes this control key"),
            metadata: stats::metadata::mib()
                .assured("the statically linked tikv-jemalloc build exposes this control key"),
            resident: stats::resident::mib()
                .assured("the statically linked tikv-jemalloc build exposes this control key"),
            retained: stats::retained::mib()
                .assured("the statically linked tikv-jemalloc build exposes this control key"),
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
        self.epoch
            .advance()
            .verified("this MIB was resolved when the collector was built");
        self.active_gauge.set(
            self.active
                .read()
                .verified("this MIB was resolved when the collector was built")
                .approx_into(),
        );
        self.allocated_gauge.set(
            self.allocated
                .read()
                .verified("this MIB was resolved when the collector was built")
                .approx_into(),
        );
        self.mapped_gauge.set(
            self.mapped
                .read()
                .verified("this MIB was resolved when the collector was built")
                .approx_into(),
        );
        self.metadata_gauge.set(
            self.metadata
                .read()
                .verified("this MIB was resolved when the collector was built")
                .approx_into(),
        );
        self.resident_gauge.set(
            self.resident
                .read()
                .verified("this MIB was resolved when the collector was built")
                .approx_into(),
        );
        self.retained_gauge.set(
            self.retained
                .read()
                .verified("this MIB was resolved when the collector was built")
                .approx_into(),
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
    .assured(
        "the metric name, help text and label names are constants that satisfy Prometheus naming \
         rules",
    );
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
    physical_node_id: Option<ClusterNodeName>,
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

#[derive(Debug)]
struct CounterRecorder {
    /// The registry owns this series independently so snapshots and descriptions can outlive the
    /// task handle that records it.
    series: Arc<CounterSeries>,
    prometheus: Option<IntCounter>,
}

impl CounterRecorder {
    fn increment(&self, value: u64, domain_timestamp: Option<Timestamp>) {
        self.series.increment(value, domain_timestamp);
        if let Some(prometheus) = &self.prometheus {
            prometheus.inc_by(value);
        }
    }
}

#[derive(Debug)]
struct CounterRecorders {
    primary: CounterRecorder,
    secondary: Option<CounterRecorder>,
}

impl CounterRecorders {
    fn increment(&self, value: u64, domain_timestamp: Option<Timestamp>) {
        self.primary.increment(value, domain_timestamp);
        if let Some(secondary) = &self.secondary {
            secondary.increment(value, domain_timestamp);
        }
    }
}

#[derive(Debug)]
struct HistogramRecorder {
    /// The registry owns this series independently so snapshots and descriptions can outlive the
    /// task handle that records it.
    series: Arc<HistogramSeries>,
    prometheus: Option<Histogram>,
}

impl HistogramRecorder {
    fn observe(&self, value: f64, capacity: Option<u64>, domain_timestamp: Option<Timestamp>) {
        if !value.is_finite() {
            return;
        }
        if let Some(samples) = HistogramSamples::one(value) {
            self.series.record(samples, capacity, domain_timestamp);
        }
        if let Some(prometheus) = &self.prometheus {
            prometheus.observe(value);
        }
    }

    fn observe_delivery_latencies(&self, latencies: &DeliveryLatencies<'_>) {
        self.series.record(
            HistogramSamples::Elapsed(&latencies.buckets),
            None,
            latencies.domain_timestamp,
        );
        let Some(prometheus) = &self.prometheus else {
            return;
        };
        // The Prometheus client takes one sample per call. Its local histogram folds the batch
        // without touching the shared child, which then takes the whole batch in one flush.
        let local = prometheus.local();
        for elapsed in elapsed_nanos(latencies.delivered_at_nanos, latencies.ingested_at) {
            local.observe(Duration::from_nanos(elapsed).as_secs_f64());
        }
        local.flush();
    }
}

#[derive(Debug)]
struct HistogramRecorders {
    primary: HistogramRecorder,
    secondary: Option<HistogramRecorder>,
}

impl HistogramRecorders {
    fn observe(&self, value: f64, capacity: Option<u64>, domain_timestamp: Option<Timestamp>) {
        self.primary.observe(value, capacity, domain_timestamp);
        if let Some(secondary) = &self.secondary {
            secondary.observe(value, capacity, domain_timestamp);
        }
    }

    fn observe_delivery_latencies(&self, latencies: &DeliveryLatencies<'_>) {
        self.primary.observe_delivery_latencies(latencies);
        if let Some(secondary) = &self.secondary {
            secondary.observe_delivery_latencies(latencies);
        }
    }
}

/// One batch as a node input accepts it.
pub(crate) struct DeliveryObservation<'a> {
    pub(crate) messages: u64,
    pub(crate) bytes: u64,
    /// The instant the input accepted the batch, which every row's delivery latency is measured
    /// to.
    pub(crate) delivered_at: Timestamp,
    /// Every row's ingestion high watermark in Unix nanoseconds, the instant its delivery latency
    /// is measured from.
    pub(crate) ingested_at: &'a [i64],
}

/// The delivery latencies of one batch, as every latency series of a node input records them.
struct DeliveryLatencies<'a> {
    /// The kernel's fold of every latency, which the rolling histograms merge bucket by bucket.
    buckets: ElapsedHistogram,
    delivered_at_nanos: i64,
    ingested_at: &'a [i64],
    /// The batch's latest ingestion watermark, which places it in the domain-time windows.
    domain_timestamp: Option<Timestamp>,
}

#[derive(Debug)]
struct BatchMetricRecorders {
    messages: CounterRecorders,
    batches: CounterRecorders,
    bytes: CounterRecorders,
    messages_per_batch: HistogramRecorders,
}

/// A task-local handle for one node/relay/direction label set.
///
/// Every map lookup, label construction and Prometheus child lookup happens when this handle is
/// resolved. Recording through it touches only the resolved series.
#[derive(Debug, Clone)]
pub(crate) struct BatchMetricsHandle {
    inner: Arc<BatchMetricRecorders>,
}

impl BatchMetricsHandle {
    pub(crate) fn observe(&self, messages: u64, bytes: u64, domain_timestamp: Option<Timestamp>) {
        self.inner.messages.increment(messages, domain_timestamp);
        self.inner.batches.increment(1, domain_timestamp);
        self.inner.bytes.increment(bytes, domain_timestamp);
        self.inner
            .messages_per_batch
            .observe(messages.approx_into(), None, domain_timestamp);
    }
}

#[derive(Debug)]
struct MessageMetricRecorders {
    messages: CounterRecorders,
    bytes: CounterRecorders,
}

/// A task-local handle for node observations that have no relay batch boundary.
#[derive(Debug, Clone)]
pub(crate) struct MessageMetricsHandle {
    inner: Arc<MessageMetricRecorders>,
}

impl MessageMetricsHandle {
    pub(crate) fn observe(&self, messages: u64, bytes: u64, domain_timestamp: Option<Timestamp>) {
        self.inner.messages.increment(messages, domain_timestamp);
        self.inner.bytes.increment(bytes, domain_timestamp);
    }
}

#[derive(Debug)]
struct NodeInputMetricRecorders {
    batch: BatchMetricRecorders,
    delivery_latency: HistogramRecorders,
    /// How one batch's delivery latencies fold into the latency histograms' buckets.
    delivery_latency_layout: ElapsedLayout,
}

/// A task-local handle for one node input edge, including delivery latency.
#[derive(Debug, Clone)]
pub(crate) struct NodeInputMetricsHandle {
    inner: Arc<NodeInputMetricRecorders>,
}

impl NodeInputMetricsHandle {
    /// Records one delivered batch.
    ///
    /// The batch's traffic is stamped with its latest ingestion watermark. Every row ingested at or
    /// before the delivery instant adds its delivery latency: one kernel pass folds them all into
    /// buckets, and each latency series merges those buckets in one recording.
    pub(crate) fn observe_delivery(&self, delivery: &DeliveryObservation<'_>) {
        let delivered_at_nanos = delivery.delivered_at.unix_nanos();
        let buckets = ElapsedHistogram::new(
            &self.inner.delivery_latency_layout,
            delivered_at_nanos,
            delivery.ingested_at,
        );
        let domain_timestamp = buckets.latest().map(Timestamp::from_unix_nanos);
        self.observe_batch(delivery.messages, delivery.bytes, domain_timestamp);
        if buckets.total() == 0 {
            return;
        }
        self.inner
            .delivery_latency
            .observe_delivery_latencies(&DeliveryLatencies {
                buckets,
                delivered_at_nanos,
                ingested_at: delivery.ingested_at,
                domain_timestamp,
            });
    }

    fn observe_batch(&self, messages: u64, bytes: u64, domain_timestamp: Option<Timestamp>) {
        self.inner
            .batch
            .messages
            .increment(messages, domain_timestamp);
        self.inner.batch.batches.increment(1, domain_timestamp);
        self.inner.batch.bytes.increment(bytes, domain_timestamp);
        self.inner
            .batch
            .messages_per_batch
            .observe(messages.approx_into(), None, domain_timestamp);
    }
}

#[derive(Debug)]
pub(crate) struct RelayMetricRecorders {
    batch: BatchMetricRecorders,
    buffer: HistogramRecorders,
}

/// A task-local handle for one relay and concrete branch.
#[derive(Debug, Clone)]
pub(crate) struct RelayMetricsHandle {
    inner: Arc<RelayMetricRecorders>,
}

impl RelayMetricsHandle {
    pub(crate) fn from_recorders(inner: Arc<RelayMetricRecorders>) -> Self {
        Self { inner }
    }

    pub(crate) fn observe_batch(
        &self,
        messages: u64,
        bytes: u64,
        domain_timestamp: Option<Timestamp>,
    ) {
        self.inner
            .batch
            .messages
            .increment(messages, domain_timestamp);
        self.inner.batch.batches.increment(1, domain_timestamp);
        self.inner.batch.bytes.increment(bytes, domain_timestamp);
        self.inner
            .batch
            .messages_per_batch
            .observe(messages.approx_into(), None, domain_timestamp);
    }

    pub(crate) fn observe_buffer(&self, len: usize, capacity: usize) {
        self.inner
            .buffer
            .observe(len.approx_into(), Some(capacity.arch_into()), None);
    }
}

#[derive(Debug, Clone, Copy)]
enum RecordingScope<'a> {
    Global,
    Branch(&'a str),
    GlobalAndBranch(&'a str),
}

pub(crate) struct NodeBatchMetricsSpec<'a> {
    pub(crate) domain: &'a DomainName,
    pub(crate) kind: ModelKind,
    pub(crate) node: &'a ModelName,
    pub(crate) relay: &'a RelayName,
    pub(crate) physical_node_id: Option<&'a ClusterNodeName>,
    pub(crate) direction: &'static str,
    pub(crate) branch_key: Option<&'a str>,
}

impl RuntimeMetrics {
    pub(crate) fn resolve_node_batch_metrics(
        &self,
        spec: NodeBatchMetricsSpec<'_>,
    ) -> BatchMetricsHandle {
        let NodeBatchMetricsSpec {
            domain,
            kind,
            node,
            relay,
            physical_node_id,
            direction,
            branch_key,
        } = spec;
        let scope = match branch_key {
            Some(branch_key) => RecordingScope::GlobalAndBranch(branch_key),
            None => RecordingScope::Global,
        };
        self.resolve_batch_metrics(
            MetricKey::node(
                domain,
                kind,
                node,
                physical_node_id,
                relay,
                direction,
                MESSAGES_TOTAL,
            ),
            scope,
        )
    }

    pub(crate) fn resolve_global_node_message_metrics(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        node: &ModelName,
        physical_node_id: Option<&ClusterNodeName>,
        direction: &'static str,
    ) -> MessageMetricsHandle {
        self.resolve_message_metrics(
            MetricKey::node_without_stream(
                domain,
                kind,
                node,
                physical_node_id,
                direction,
                MESSAGES_TOTAL,
            ),
            RecordingScope::Global,
        )
    }

    pub(crate) fn resolve_branch_node_message_metrics(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        node: &ModelName,
        physical_node_id: Option<&ClusterNodeName>,
        direction: &'static str,
        branch_key: &str,
    ) -> MessageMetricsHandle {
        self.resolve_message_metrics(
            MetricKey::node_without_stream(
                domain,
                kind,
                node,
                physical_node_id,
                direction,
                MESSAGES_TOTAL,
            ),
            RecordingScope::Branch(branch_key),
        )
    }

    pub(crate) fn resolve_node_input_metrics(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        node: &ModelName,
        relay: &RelayName,
        physical_node_id: Option<&ClusterNodeName>,
        branch_key: Option<&str>,
    ) -> NodeInputMetricsHandle {
        let scope = match branch_key {
            Some(branch_key) => RecordingScope::GlobalAndBranch(branch_key),
            None => RecordingScope::Global,
        };
        let messages_key = MetricKey::node(
            domain,
            kind,
            node,
            physical_node_id,
            relay,
            "received",
            MESSAGES_TOTAL,
        );
        let delivery_latency_layout =
            HistogramConfig::for_buckets(internal_buckets_for_metric(DELIVERY_LATENCY_SECONDS))
                .delivery_latency_layout();
        NodeInputMetricsHandle {
            inner: Arc::new(NodeInputMetricRecorders {
                batch: self.resolve_batch_metric_recorders(messages_key.clone(), scope),
                delivery_latency: self.resolve_histogram_recorders(
                    with_metric(&messages_key, DELIVERY_LATENCY_SECONDS),
                    scope,
                ),
                delivery_latency_layout,
            }),
        }
    }

    pub(crate) fn resolve_relay_metrics(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        physical_node_id: Option<&ClusterNodeName>,
        direction: &'static str,
        branch_key: Option<&str>,
    ) -> RelayMetricsHandle {
        RelayMetricsHandle {
            inner: Arc::new(self.resolve_relay_metric_recorders(
                domain,
                relay,
                physical_node_id,
                direction,
                branch_key,
            )),
        }
    }

    pub(crate) fn resolve_relay_metric_recorders(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        physical_node_id: Option<&ClusterNodeName>,
        direction: &'static str,
        branch_key: Option<&str>,
    ) -> RelayMetricRecorders {
        let scope = match branch_key {
            Some(branch_key) => RecordingScope::GlobalAndBranch(branch_key),
            None => RecordingScope::Global,
        };
        let messages_key =
            MetricKey::relay(domain, relay, physical_node_id, "received", MESSAGES_TOTAL);
        RelayMetricRecorders {
            batch: self.resolve_batch_metric_recorders(messages_key, scope),
            buffer: self.resolve_histogram_recorders(
                MetricKey::relay(domain, relay, physical_node_id, direction, RELAY_BUFFER_LEN),
                scope,
            ),
        }
    }

    fn resolve_batch_metrics(
        &self,
        messages_key: MetricKey,
        scope: RecordingScope<'_>,
    ) -> BatchMetricsHandle {
        let recorders = self.resolve_batch_metric_recorders(messages_key, scope);
        BatchMetricsHandle {
            inner: Arc::new(recorders),
        }
    }

    fn resolve_batch_metric_recorders(
        &self,
        messages_key: MetricKey,
        scope: RecordingScope<'_>,
    ) -> BatchMetricRecorders {
        BatchMetricRecorders {
            messages: self.resolve_counter_recorders(messages_key.clone(), scope),
            batches: self
                .resolve_counter_recorders(with_metric(&messages_key, BATCHES_TOTAL), scope),
            bytes: self.resolve_counter_recorders(with_metric(&messages_key, BYTES_TOTAL), scope),
            messages_per_batch: self
                .resolve_histogram_recorders(with_metric(&messages_key, MESSAGES_PER_BATCH), scope),
        }
    }

    fn resolve_message_metrics(
        &self,
        messages_key: MetricKey,
        scope: RecordingScope<'_>,
    ) -> MessageMetricsHandle {
        let recorders = MessageMetricRecorders {
            messages: self.resolve_counter_recorders(messages_key.clone(), scope),
            bytes: self.resolve_counter_recorders(with_metric(&messages_key, BYTES_TOTAL), scope),
        };
        MessageMetricsHandle {
            inner: Arc::new(recorders),
        }
    }

    fn resolve_counter_recorders(
        &self,
        key: MetricKey,
        scope: RecordingScope<'_>,
    ) -> CounterRecorders {
        match scope {
            RecordingScope::Global => CounterRecorders {
                primary: self.resolve_global_counter(key),
                secondary: None,
            },
            RecordingScope::Branch(branch_key) => CounterRecorders {
                primary: self.resolve_branch_counter(branch_key, key),
                secondary: None,
            },
            RecordingScope::GlobalAndBranch(branch_key) => CounterRecorders {
                primary: self.resolve_global_counter(key.clone()),
                secondary: Some(self.resolve_branch_counter(branch_key, key)),
            },
        }
    }

    fn resolve_histogram_recorders(
        &self,
        key: MetricKey,
        scope: RecordingScope<'_>,
    ) -> HistogramRecorders {
        match scope {
            RecordingScope::Global => HistogramRecorders {
                primary: self.resolve_global_histogram(key),
                secondary: None,
            },
            RecordingScope::Branch(branch_key) => HistogramRecorders {
                primary: self.resolve_branch_histogram(branch_key, key),
                secondary: None,
            },
            RecordingScope::GlobalAndBranch(branch_key) => HistogramRecorders {
                primary: self.resolve_global_histogram(key.clone()),
                secondary: Some(self.resolve_branch_histogram(branch_key, key)),
            },
        }
    }

    fn resolve_global_counter(&self, key: MetricKey) -> CounterRecorder {
        let prometheus = self
            .series
            .prometheus
            .counter(&key)
            .assured("counter metric keys are built with one of the three counter constants");
        let series = match self.series.counters.entry(key) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let series = Arc::new(CounterSeries::default());
                entry.insert(Arc::clone(&series));
                series
            }
        };
        CounterRecorder {
            series,
            prometheus: Some(prometheus),
        }
    }

    fn resolve_branch_counter(&self, branch_key: &str, key: MetricKey) -> CounterRecorder {
        let key = BranchMetricKey {
            branch_key: branch_key.to_string(),
            key,
        };
        let series = match self.series.branch_counters.entry(key) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let series = Arc::new(CounterSeries::default());
                entry.insert(Arc::clone(&series));
                series
            }
        };
        CounterRecorder {
            series,
            prometheus: None,
        }
    }

    fn resolve_global_histogram(&self, key: MetricKey) -> HistogramRecorder {
        let prometheus =
            self.series.prometheus.histogram(&key).assured(
                "histogram metric keys are built with one of the three histogram constants",
            );
        let buckets = internal_buckets_for_metric(key.metric);
        let series = match self.series.histograms.entry(key) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let series = Arc::new(HistogramSeries::new(buckets));
                entry.insert(Arc::clone(&series));
                series
            }
        };
        HistogramRecorder {
            series,
            prometheus: Some(prometheus),
        }
    }

    fn resolve_branch_histogram(&self, branch_key: &str, key: MetricKey) -> HistogramRecorder {
        let buckets = internal_buckets_for_metric(key.metric);
        let key = BranchMetricKey {
            branch_key: branch_key.to_string(),
            key,
        };
        let series = match self.series.branch_histograms.entry(key) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let series = Arc::new(HistogramSeries::new(buckets));
                entry.insert(Arc::clone(&series));
                series
            }
        };
        HistogramRecorder {
            series,
            prometheus: None,
        }
    }

    pub(crate) fn register_ingestor_quiesce(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        physical_node_id: Option<&ClusterNodeName>,
    ) -> IngestorQuiesceMetricLabels {
        let labels = IngestorQuiesceMetricLabels {
            domain: domain.as_str().to_string(),
            ingestor: ingestor.as_str().to_string(),
            physical_node_id: physical_node_id.cloned(),
        };
        let values = labels.values();
        self.series
            .prometheus
            .ingestor_quiesce_buffered_records
            .with_label_values(&values)
            .set(0);
        self.series
            .prometheus
            .ingestor_quiesce_buffered_bytes
            .with_label_values(&values)
            .set(0);
        self.series
            .prometheus
            .ingestor_quiesce_dropped_total
            .with_label_values(&values);
        self.series
            .prometheus
            .ingestor_quiesce_rejected_total
            .with_label_values(&values);
        labels
    }

    /// Records how many open session subscriptions this node delivers from `relay`. A relay the
    /// node once delivered from keeps its series at zero after its last subscription closes, so
    /// the withdrawal of the node's interest is itself observable.
    pub(crate) fn set_session_subscriptions(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        subscriptions: usize,
    ) {
        self.series
            .prometheus
            .session_subscriptions
            .with_label_values(&[domain.as_str(), relay.as_str()])
            .set(
                i64::try_from(subscriptions)
                    .assured("every open subscription occupies memory, so the count fits in i64"),
            );
    }

    /// The series of one client ingestor this node executes. Its gauges are resolved once, so its
    /// endpoint sets them without looking their labels up again.
    pub(crate) fn client_ingestor_series(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> ClientIngestorSeries {
        let labels = [domain.as_str(), ingestor.as_str()];
        let prometheus = &self.series.prometheus;
        ClientIngestorSeries {
            producers: prometheus
                .client_ingestor_producers
                .with_label_values(&labels),
            forwarded_producers: prometheus
                .client_ingestor_forwarded_producers
                .with_label_values(&labels),
            outstanding_batches: prometheus
                .client_ingestor_outstanding_batches
                .with_label_values(&labels),
            outstanding_bytes: prometheus
                .client_ingestor_outstanding_bytes
                .with_label_values(&labels),
            admitted_batches: prometheus
                .client_ingestor_admitted_batches
                .with_label_values(&labels),
            submissions: prometheus.client_ingestor_submissions_total.clone(),
            domain: domain.clone(),
            ingestor: ingestor.clone(),
        }
    }

    /// The per-emitter metrics that the volatile client delivery owner updates at each boundary.
    pub(crate) fn client_emitter_series(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) -> ClientEmitterSeries {
        let labels = [domain.as_str(), emitter.as_str()];
        let prometheus = &self.series.prometheus;
        ClientEmitterSeries {
            consumers: prometheus
                .client_emitter_consumers
                .with_label_values(&labels),
            forwarded_consumers: prometheus
                .client_emitter_forwarded_consumers
                .with_label_values(&labels),
            forwarded_credit_bytes: prometheus
                .client_emitter_forwarded_credit_bytes
                .with_label_values(&labels),
            forwarded_retained_batches: prometheus
                .client_emitter_forwarded_retained_batches
                .with_label_values(&labels),
            forwarded_retained_bytes: prometheus
                .client_emitter_forwarded_retained_bytes
                .with_label_values(&labels),
            retained_batches: prometheus
                .client_emitter_retained_batches
                .with_label_values(&labels),
            retained_bytes: prometheus
                .client_emitter_retained_bytes
                .with_label_values(&labels),
            incomplete_batches: prometheus
                .client_emitter_incomplete_batches
                .with_label_values(&labels),
            retries: prometheus
                .client_emitter_retries_total
                .with_label_values(&labels),
            acks: prometheus
                .client_emitter_acks_total
                .with_label_values(&labels),
            rejections: prometheus
                .client_emitter_rejections_total
                .with_label_values(&labels),
        }
    }

    /// Records rows a dropping session subscription to `relay` discarded.
    pub(crate) fn increment_session_subscription_dropped_rows(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        rows: u64,
    ) {
        self.series
            .prometheus
            .session_subscription_dropped_rows_total
            .with_label_values(&[domain.as_str(), relay.as_str()])
            .inc_by(rows);
    }

    pub(crate) fn set_ingestor_quiesce_buffered(
        &self,
        labels: &IngestorQuiesceMetricLabels,
        records: usize,
        bytes: usize,
    ) {
        let values = labels.values();
        self.series
            .prometheus
            .ingestor_quiesce_buffered_records
            .with_label_values(&values)
            .set(i64::try_from(records).assured(
                "buffered records occupy memory and cannot exceed the allocator's isize limit",
            ));
        self.series
            .prometheus
            .ingestor_quiesce_buffered_bytes
            .with_label_values(&values)
            .set(i64::try_from(bytes).assured(
                "buffered bytes occupy memory and cannot exceed the allocator's isize limit",
            ));
    }

    pub(crate) fn increment_ingestor_quiesce_dropped(
        &self,
        labels: &IngestorQuiesceMetricLabels,
        count: u64,
    ) {
        self.series
            .prometheus
            .ingestor_quiesce_dropped_total
            .with_label_values(&labels.values())
            .inc_by(count);
    }

    pub(crate) fn increment_ingestor_quiesce_rejected(
        &self,
        labels: &IngestorQuiesceMetricLabels,
        count: u64,
    ) {
        self.series
            .prometheus
            .ingestor_quiesce_rejected_total
            .with_label_values(&labels.values())
            .inc_by(count);
    }

    pub(crate) fn register_branch(
        &self,
        domain: &DomainName,
        branch: &BranchName,
        physical_node_id: Option<&ClusterNodeName>,
    ) {
        let physical_node = physical_node_label(physical_node_id);
        self.series.prometheus.branch_instances.with_label_values(&[
            domain.as_str(),
            branch.as_str(),
            physical_node,
        ]);
        for reason in BranchEvictionReason::iter() {
            self.series
                .prometheus
                .branch_evictions_total
                .with_label_values(&[
                    domain.as_str(),
                    branch.as_str(),
                    physical_node,
                    reason.as_ref(),
                ]);
        }
    }

    pub(crate) fn observe_branch_instance_created(
        &self,
        domain: &DomainName,
        branch: &BranchName,
        physical_node_id: Option<&ClusterNodeName>,
        concrete_key: &str,
    ) {
        let physical_node = physical_node_label(physical_node_id);
        let metric_key = BranchInstanceMetricKey {
            domain: domain.as_str().to_string(),
            branch: branch.as_str().to_string(),
            physical_node_id: physical_node_id.cloned(),
            concrete_key: concrete_key.to_string(),
        };
        match self.series.branch_instance_references.entry(metric_key) {
            Entry::Occupied(mut entry) => {
                let references = entry.get_mut();
                references.count = references
                    .count
                    .checked_add(1)
                    .assured("the references counted here are branch instances held in memory");
                references.eviction_reason = None;
            }
            Entry::Vacant(entry) => {
                entry.insert(BranchInstanceReferences {
                    count: 1,
                    eviction_reason: None,
                });
                self.series
                    .prometheus
                    .branch_instances
                    .with_label_values(&[domain.as_str(), branch.as_str(), physical_node])
                    .inc();
            }
        }
    }

    pub(crate) fn observe_branch_instance_removed(
        &self,
        domain: &DomainName,
        branch: &BranchName,
        physical_node_id: Option<&ClusterNodeName>,
        concrete_key: &str,
        reason: BranchEvictionReason,
    ) {
        let physical_node = physical_node_label(physical_node_id);
        let metric_key = BranchInstanceMetricKey {
            domain: domain.as_str().to_string(),
            branch: branch.as_str().to_string(),
            physical_node_id: physical_node_id.cloned(),
            concrete_key: concrete_key.to_string(),
        };
        let (removed_key, record_eviction) =
            match self.series.branch_instance_references.entry(metric_key) {
                Entry::Occupied(mut entry) if entry.get().count > 1 => {
                    let references = entry.get_mut();
                    references.count -= 1;
                    let record_eviction = references.eviction_reason.is_none();
                    if record_eviction {
                        references.eviction_reason = Some(reason);
                    }
                    (false, record_eviction)
                }
                Entry::Occupied(entry) => {
                    let references = entry.remove();
                    (true, references.eviction_reason.is_none())
                }
                Entry::Vacant(_) => return,
            };
        if removed_key {
            self.series
                .prometheus
                .branch_instances
                .with_label_values(&[domain.as_str(), branch.as_str(), physical_node])
                .dec();
        }
        if record_eviction {
            self.series
                .prometheus
                .branch_evictions_total
                .with_label_values(&[
                    domain.as_str(),
                    branch.as_str(),
                    physical_node,
                    reason.as_ref(),
                ])
                .inc();
        }
    }

    pub(crate) fn observe_branch_instance_detached(
        &self,
        domain: &DomainName,
        branch: &BranchName,
        physical_node_id: Option<&ClusterNodeName>,
        concrete_key: &str,
    ) {
        let physical_node = physical_node_label(physical_node_id);
        let metric_key = BranchInstanceMetricKey {
            domain: domain.as_str().to_string(),
            branch: branch.as_str().to_string(),
            physical_node_id: physical_node_id.cloned(),
            concrete_key: concrete_key.to_string(),
        };
        let removed_key = match self.series.branch_instance_references.entry(metric_key) {
            Entry::Occupied(mut entry) if entry.get().count > 1 => {
                entry.get_mut().count -= 1;
                false
            }
            Entry::Occupied(entry) => {
                entry.remove();
                true
            }
            Entry::Vacant(_) => return,
        };
        if removed_key {
            self.series
                .prometheus
                .branch_instances
                .with_label_values(&[domain.as_str(), branch.as_str(), physical_node])
                .dec();
        }
    }

    pub fn register_global_node(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        node: &ModelName,
        physical_node_id: Option<&ClusterNodeName>,
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
        domain: &DomainName,
        relay: &RelayName,
        physical_node_id: Option<&ClusterNodeName>,
    ) {
        self.register_counter(MetricKey::relay(
            domain,
            relay,
            physical_node_id,
            "received",
            MESSAGES_TOTAL,
        ));
    }

    pub(crate) fn remove_relay(&self, domain: &DomainName, relay: &RelayName) {
        let counter_keys = self
            .series
            .counters
            .iter()
            .filter(|entry| entry.key().belongs_to_relay(domain, relay))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in counter_keys {
            self.series.counters.remove(&key);
            self.series.prometheus.remove(&key);
        }
        let histogram_keys = self
            .series
            .histograms
            .iter()
            .filter(|entry| entry.key().belongs_to_relay(domain, relay))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in histogram_keys {
            self.series.histograms.remove(&key);
            self.series.prometheus.remove(&key);
        }
        self.series
            .branch_counters
            .retain(|key, _| !key.key.belongs_to_relay(domain, relay));
        self.series
            .branch_histograms
            .retain(|key, _| !key.key.belongs_to_relay(domain, relay));
    }

    pub fn prometheus_text(&self) -> String {
        self.series.prometheus.text()
    }

    /// Give this node's exposition the transport, execution and consensus state its
    /// interconnection series are read from. A node installs its own once, during startup.
    pub fn install_node_observations(&self, observations: NodeObservations) {
        self.series.prometheus.interconnection.install(observations);
    }

    pub fn snapshot_global_target(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        target: &ModelName,
        physical_node_id: &ClusterNodeName,
    ) -> RuntimeMetricsSnapshot {
        let target_kind = kind.as_str().to_ascii_uppercase();
        let mut counters = self
            .series
            .counters
            .iter()
            .filter(|entry| {
                key_matches_target(entry.key(), domain, &target_kind, target, physical_node_id)
            })
            .map(|entry| entry.value().to_snapshot(entry.key().clone()))
            .collect::<Vec<_>>();
        counters.sort_by(|left, right| left.key.cmp(&right.key));
        let mut histograms = self
            .series
            .histograms
            .iter()
            .filter(|entry| {
                key_matches_target(entry.key(), domain, &target_kind, target, physical_node_id)
                    && entry.value().was_observed()
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
        domain: &DomainName,
        kind: ModelKind,
        target: &ModelName,
        physical_node_id: &ClusterNodeName,
    ) -> RuntimeMetricsSnapshot {
        let target_kind = kind.as_str().to_ascii_uppercase();
        let mut counters = self
            .series
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
            .series
            .branch_histograms
            .iter()
            .filter(|entry| {
                entry.key().branch_key == branch_key
                    && entry.value().was_observed()
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
        domain: &DomainName,
        kind: ModelKind,
        target: &ModelName,
        physical_node_id: &ClusterNodeName,
        snapshot: RuntimeMetricsSnapshot,
    ) {
        let target_kind = kind.as_str().to_ascii_uppercase();
        let counter_keys = self
            .series
            .counters
            .iter()
            .filter(|entry| {
                key_matches_target(entry.key(), domain, &target_kind, target, physical_node_id)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in counter_keys {
            self.series.counters.remove(&key);
        }
        let histogram_keys = self
            .series
            .histograms
            .iter()
            .filter(|entry| {
                key_matches_target(entry.key(), domain, &target_kind, target, physical_node_id)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in histogram_keys {
            self.series.histograms.remove(&key);
        }

        for counter in snapshot.counters {
            let Ok(key) = MetricKey::try_from(counter.key.clone()) else {
                continue;
            };
            self.series
                .counters
                .insert(key, Arc::new(CounterSeries::from_snapshot(&counter)));
        }
        for histogram in snapshot.histograms {
            let Ok(key) = MetricKey::try_from(histogram.key.clone()) else {
                continue;
            };
            self.series
                .histograms
                .insert(key, Arc::new(HistogramSeries::from_snapshot(&histogram)));
        }
    }

    pub fn has_global_target_measurements(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        target: impl Into<ModelName>,
    ) -> bool {
        let target = target.into();
        let target_kind = kind.as_str().to_ascii_uppercase();
        self.series.counters.iter().any(|entry| {
            let key = entry.key();
            key.domain == domain.as_str()
                && key.target_kind == target_kind
                && key.target == target.as_str()
                && key.relay != "-"
                && entry.value().value.load(AtomicOrdering::Relaxed) > 0
        }) || self.series.histograms.iter().any(|entry| {
            let key = entry.key();
            key.domain == domain.as_str()
                && key.target_kind == target_kind
                && key.target == target.as_str()
                && key.relay != "-"
                && entry.value().was_observed()
        })
    }

    pub fn apply_global_snapshot(&self, snapshot: RuntimeMetricsSnapshot) {
        for counter in snapshot.counters {
            let Ok(key) = MetricKey::try_from(counter.key.clone()) else {
                continue;
            };
            self.series.counters.remove(&key);
            self.series
                .counters
                .insert(key, Arc::new(CounterSeries::from_snapshot(&counter)));
        }
        for histogram in snapshot.histograms {
            let Ok(key) = MetricKey::try_from(histogram.key.clone()) else {
                continue;
            };
            self.series.histograms.remove(&key);
            self.series
                .histograms
                .insert(key, Arc::new(HistogramSeries::from_snapshot(&histogram)));
        }
    }

    pub fn apply_branch_target_snapshot(
        &self,
        branch_key: &str,
        domain: &DomainName,
        kind: ModelKind,
        target: &ModelName,
        physical_node_id: &ClusterNodeName,
        snapshot: RuntimeMetricsSnapshot,
    ) {
        let target_kind = kind.as_str().to_ascii_uppercase();
        let counter_keys = self
            .series
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
            self.series.branch_counters.remove(&key);
        }
        let histogram_keys = self
            .series
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
            self.series.branch_histograms.remove(&key);
        }

        for counter in snapshot.counters {
            let Ok(key) = MetricKey::try_from(counter.key.clone()) else {
                continue;
            };
            self.series.branch_counters.insert(
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
            self.series.branch_histograms.insert(
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
        domain: &DomainName,
        kind: &str,
        target: impl Into<ModelName>,
    ) -> Vec<String> {
        let target = target.into();
        let mut lines = Vec::new();
        let mut counters = self
            .series
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
            .series
            .histograms
            .iter()
            .filter(|entry| {
                entry.key().domain == domain.as_str()
                    && entry.key().target_kind == kind
                    && entry.key().target == target.as_str()
                    && entry.value().was_observed()
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

    pub fn describe_domain_statistics(&self, domain: &DomainName) -> Vec<String> {
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

    pub fn dataflow_domain_statistics(&self, domain: &DomainName) -> DataflowStatistics {
        self.dataflow_statistics_for_global_keys(|key| key.domain == domain.as_str())
    }

    pub fn dataflow_node_statistics(
        &self,
        domain: &DomainName,
        kind: &str,
        target: &ModelName,
    ) -> DataflowStatistics {
        self.dataflow_statistics_for_global_keys(|key| {
            key.domain == domain.as_str()
                && key.target_kind == kind
                && key.target == target.as_str()
        })
    }

    pub fn dataflow_edge_statistics(
        &self,
        domain: &DomainName,
        metric: &DataflowMetricRef,
    ) -> DataflowStatistics {
        self.dataflow_statistics_for_global_keys(|key| {
            key.matches_dataflow_metric_ref(domain, metric)
        })
    }

    pub fn dataflow_relay_buffer_statistics(
        &self,
        domain: &DomainName,
        relay: &RelayName,
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
        domain: &DomainName,
        kind: &str,
        target: &ModelName,
    ) -> Vec<DataflowBranchStatistics> {
        let mut branches = Vec::<(String, DataflowStatistics)>::new();
        for entry in self.series.branch_counters.iter() {
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
        for entry in self.series.branch_histograms.iter() {
            if !entry.value().was_observed() {
                continue;
            }
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
        domain: &DomainName,
        metric: &DataflowMetricRef,
    ) -> Vec<DataflowBranchStatistics> {
        let mut branches = Vec::<(String, DataflowStatistics)>::new();
        for entry in self.series.branch_counters.iter() {
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
        for entry in self.series.branch_histograms.iter() {
            if !entry.value().was_observed() {
                continue;
            }
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
        for entry in self.series.counters.iter() {
            if !include(entry.key()) {
                continue;
            }
            if let Some(counter_statistics) =
                counter_dataflow_statistics(entry.key(), &entry.value().summary())
            {
                add_dataflow_statistics(&mut statistics, counter_statistics);
            }
        }
        for entry in self.series.histograms.iter() {
            if !entry.value().was_observed() {
                continue;
            }
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
        domain: &DomainName,
        scope: DomainMetricScope,
    ) -> Vec<(MetricKey, AggregatedCounterSummary)> {
        let mut counters = Vec::<(MetricKey, AggregatedCounterSummary)>::new();
        for entry in self.series.counters.iter() {
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
        domain: &DomainName,
        scope: DomainMetricScope,
    ) -> Vec<(MetricKey, HistogramSummary)> {
        let mut histograms = Vec::<(MetricKey, AggregatedRollingHistograms)>::new();
        for entry in self.series.histograms.iter() {
            if !entry.value().was_observed() {
                continue;
            }
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

    fn register_counter(&self, key: MetricKey) {
        drop(self.resolve_global_counter(key));
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
    domain: &DomainName,
    target_kind: &str,
    target: &ModelName,
    physical_node_id: &ClusterNodeName,
) -> bool {
    key.domain == domain.as_str()
        && key.target_kind == target_kind
        && key.target == target.as_str()
        && key.physical_node_id.as_ref() == Some(physical_node_id)
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

/// The wall clock as Unix nanoseconds, or `None` when it cannot be expressed as one.
///
/// A host clock set before 1970, or past the year 2262, has no `i64` nanosecond reading. Rates and
/// ages derived from it are simply not reported for as long as that holds.
fn current_wall_unix_nanos() -> Option<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(duration.as_nanos()).ok()
}

/// The wall-clock reading of a monotonic `instant`, or `None` when the wall clock has none.
///
/// The conversion inherits [`current_wall_unix_nanos`]'s range, and it also declines an instant so
/// far in the past that its offset does not fit. Both mean the same thing to the caller: this
/// sample carries no wall-clock timestamp.
fn wall_unix_nanos_from_instant(instant: Instant) -> Option<i64> {
    let now = current_wall_unix_nanos()?;
    let elapsed = i64::try_from(instant.elapsed().as_nanos()).ok()?;
    now.checked_sub(elapsed)
}

/// The monotonic instant a wall-clock reading corresponds to, or `None` when it has none.
///
/// It has none when the wall clock is out of range, and when the reading is older than the process
/// itself, which is what a persisted timestamp restored on a freshly started node looks like.
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
        value.approx_into::<f64>() / elapsed
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

/// The per-second rate `value` was observed at on a domain clock, or `None` when there is no rate
/// to report yet.
///
/// A domain clock that has not moved, or has moved backwards because a paced domain restarted, has
/// no elapsed span to divide by. The metric is left unreported rather than shown as zero, which
/// would read as a domain that stopped.
fn domain_rate(value: u64, started_at_nanos: &AtomicI64, last_at_nanos: &AtomicI64) -> Option<f64> {
    let started_at_nanos = started_at_nanos.load(AtomicOrdering::Relaxed);
    let last_at_nanos = last_at_nanos.load(AtomicOrdering::Relaxed);
    if started_at_nanos == NO_DOMAIN_TIMESTAMP || last_at_nanos == NO_DOMAIN_TIMESTAMP {
        return None;
    }
    let elapsed_nanos = last_at_nanos.checked_sub(started_at_nanos)?;
    let elapsed = Duration::from_nanos(u64::try_from(elapsed_nanos).ok()?).as_secs_f64();
    if elapsed <= 0.0 {
        return None;
    }
    Some(value.approx_into::<f64>() / elapsed)
}

fn ema_value_bits(value: Option<f64>) -> u64 {
    match value {
        Some(value) => value.to_bits(),
        None => NO_EMA_VALUE_BITS,
    }
}

fn load_ema_value(value_bits: &AtomicU64) -> Option<f64> {
    let bits = value_bits.load(AtomicOrdering::Relaxed);
    if bits == NO_EMA_VALUE_BITS {
        None
    } else {
        Some(f64::from_bits(bits))
    }
}

fn observe_ema_sample(value_bits: &AtomicU64, sample: f64, elapsed_seconds: f64, tau_seconds: f64) {
    let mut current_bits = value_bits.load(AtomicOrdering::Relaxed);
    loop {
        let next = if current_bits == NO_EMA_VALUE_BITS {
            sample
        } else {
            let current = f64::from_bits(current_bits);
            let alpha = time_decay_alpha(elapsed_seconds, tau_seconds);
            current + alpha * (sample - current)
        };
        match value_bits.compare_exchange_weak(
            current_bits,
            next.to_bits(),
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => return,
            Err(observed) => current_bits = observed,
        }
    }
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

fn rate_decay_tau_seconds(window: Duration) -> f64 {
    window.as_secs_f64() / RATE_DECAY_TAU_FRACTION
}

/// Scales a sample into the fixed-point unit the HDR histograms record in.
///
/// Returns `None` for a sample that has no such unit: a non-finite observation, or one whose
/// scaled magnitude leaves the `u64` range. Neither belongs in a histogram, so the caller drops
/// it rather than recording a saturated stand-in.
fn scaled_histogram_value(value: f64) -> Option<u64> {
    (value * HISTOGRAM_VALUE_SCALE)
        .round()
        .max(0.0)
        .checked_approx_into()
}

fn unscale_histogram_value(value: u64) -> f64 {
    value.approx_into::<f64>() / HISTOGRAM_VALUE_SCALE
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
        // Clamped for the same reason as `TimeRollingHistogram::record`: a snapshot written when
        // the ladder reached further still describes observations that belong in this histogram's
        // top bucket, and dropping them would silently lower the restored percentiles.
        histogram
            .record_n(value.value.min(config.highest_trackable_value), value.count)
            .assured("the value was just clamped to the histogram's own maximum");
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
    i64::try_from(duration.as_nanos()).assured(
        "every caller passes a rolling-window constant of minutes, far inside the i64 nanosecond \
         range",
    )
}

fn format_counter_metric_line(prefix: &str, key: &MetricKey, summary: &CounterSummary) -> String {
    format!(
        "{prefix}{} {} relay={} physical_node={} total={} wall_rate_per_sec={} \
         domain_rate_per_sec={} wall_rate_ema_1m_per_sec={} wall_rate_ema_15m_per_sec={} \
         domain_rate_ema_1m_per_sec={} domain_rate_ema_15m_per_sec={}",
        key.metric,
        key.direction,
        empty_as_dash(&key.relay),
        physical_node_label(key.physical_node_id.as_ref()),
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
    const OBSERVED_TOTALS: &str = "both totals count dataflow this cluster already observed";

    target.messages_total = target
        .messages_total
        .checked_add(source.messages_total)
        .assured(OBSERVED_TOTALS);
    target.bytes_total = target
        .bytes_total
        .checked_add(source.bytes_total)
        .assured(OBSERVED_TOTALS);
    target.batches_total = target
        .batches_total
        .checked_add(source.batches_total)
        .assured(OBSERVED_TOTALS);
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
        physical_node_label(key.physical_node_id.as_ref()),
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
    let capacity = match summary.capacity {
        Some(capacity) => format!(" capacity={capacity}"),
        None => String::new(),
    };
    format!(
        "{prefix}{} {} relay={} physical_node={}{} p50_1m={} p90_1m={} p99_1m={} p50_15m={} \
         p90_15m={} p99_15m={} domain_p50_1m={} domain_p90_1m={} domain_p99_1m={} \
         domain_p50_15m={} domain_p90_15m={} domain_p99_15m={}",
        key.metric,
        key.direction,
        empty_as_dash(&key.relay),
        physical_node_label(key.physical_node_id.as_ref()),
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
        physical_node_label(key.physical_node_id.as_ref()),
        key.direction.as_str(),
        empty_as_dash(&key.relay),
        empty_as_dash(&key.peer_kind),
        empty_as_dash(&key.peer),
    ]
}

fn empty_as_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

/// How the owning cluster node is spelled as a label value. A metric observed on a node that owns
/// nothing placed still carries the label, and it uses the same `-` absent marker as the other
/// optional labels.
fn physical_node_label(physical_node_id: Option<&ClusterNodeName>) -> &str {
    match physical_node_id {
        Some(physical_node_id) => physical_node_id.as_str(),
        None => "-",
    }
}

fn format_optional(value: Option<f64>) -> String {
    match value {
        Some(value) => format_number(value),
        None => "-".to_string(),
    }
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
    match value {
        Some(value) => format_histogram_number(value),
        None => "-".to_string(),
    }
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

/// The series of one client ingestor on this node: its attached producers, the batches and bytes
/// they have outstanding, the batches holding a slot of its acknowledgement window, and the
/// batches it answered.
pub(crate) struct ClientIngestorSeries {
    producers: IntGauge,
    forwarded_producers: IntGauge,
    outstanding_batches: IntGauge,
    outstanding_bytes: IntGauge,
    admitted_batches: IntGauge,
    /// Answered batches, whose outcome and cause labels vary per batch.
    submissions: IntCounterVec,
    domain: DomainName,
    ingestor: IngestorName,
}

impl ClientIngestorSeries {
    /// Counts one batch the ingestor answered with `outcome`.
    pub(crate) fn count(&self, outcome: &nervix_models::ClientSubmissionOutcome) {
        self.submissions
            .with_label_values(&[
                self.domain.as_str(),
                self.ingestor.as_str(),
                outcome.class_label(),
                outcome.cause_label(),
            ])
            .inc();
    }

    pub(crate) fn set(&self, gauges: crate::runtime::ClientIngestorGauges) {
        self.producers.set(
            i64::try_from(gauges.producers)
                .assured("every attached producer occupies memory, so the count fits in i64"),
        );
        self.forwarded_producers.set(
            i64::try_from(gauges.forwarded_producers)
                .assured("every attached producer occupies memory, so the count fits in i64"),
        );
        self.outstanding_batches.set(
            i64::try_from(gauges.outstanding_batches)
                .assured("every outstanding batch occupies memory, so the count fits in i64"),
        );
        self.outstanding_bytes
            .set(i64::try_from(gauges.outstanding_bytes).assured(
                "outstanding bytes are held in memory within the node's producer budget, so they \
                 fit in i64",
            ));
        self.admitted_batches
            .set(i64::try_from(gauges.admitted_batches).assured(
                "every admitted batch holds its ACK root in memory, so the count fits in i64",
            ));
    }
}

/// Public metrics for one volatile client emitter delivery owner. Gauges describe only current
/// in-memory work; counters retain outcomes across endpoint restarts on the same node.
#[derive(Clone)]
pub(crate) struct ClientEmitterSeries {
    consumers: IntGauge,
    forwarded_consumers: IntGauge,
    forwarded_credit_bytes: IntGauge,
    forwarded_retained_batches: IntGauge,
    forwarded_retained_bytes: IntGauge,
    retained_batches: IntGauge,
    retained_bytes: IntGauge,
    incomplete_batches: IntGauge,
    retries: IntCounter,
    acks: IntCounter,
    rejections: IntCounter,
}

impl ClientEmitterSeries {
    pub(crate) fn reset_gauges(&self) {
        self.consumers.set(0);
        self.forwarded_consumers.set(0);
        self.forwarded_credit_bytes.set(0);
        self.forwarded_retained_batches.set(0);
        self.forwarded_retained_bytes.set(0);
        self.retained_batches.set(0);
        self.retained_bytes.set(0);
        self.incomplete_batches.set(0);
    }

    pub(crate) fn attach(&self, forwarded: bool, credit: u64) {
        self.consumers.inc();
        if forwarded {
            self.forwarded_consumers.inc();
            self.forwarded_credit_bytes.add(
                i64::try_from(credit)
                    .assured("consumer credit is bounded by the 128 MiB node budget"),
            );
        }
    }

    pub(crate) fn detach(&self, forwarded: bool, credit: u64) {
        self.consumers.dec();
        if forwarded {
            self.forwarded_consumers.dec();
            self.forwarded_credit_bytes.sub(
                i64::try_from(credit)
                    .assured("consumer credit is bounded by the 128 MiB node budget"),
            );
        }
    }

    pub(crate) fn retain(&self, bytes: usize) {
        self.retained_batches.inc();
        self.retained_bytes.add(
            i64::try_from(bytes).assured("a retained IPC payload fits in the 128 MiB node budget"),
        );
    }

    pub(crate) fn release(&self, bytes: usize) {
        self.retained_batches.dec();
        self.retained_bytes.sub(
            i64::try_from(bytes).assured("a retained IPC payload fits in the 128 MiB node budget"),
        );
    }

    pub(crate) fn assign(&self, bytes: usize, forwarded: bool) {
        self.incomplete_batches.inc();
        if forwarded {
            self.forwarded_retained_batches.inc();
            self.forwarded_retained_bytes.add(
                i64::try_from(bytes)
                    .assured("a forwarded IPC payload fits in the 128 MiB node budget"),
            );
        }
    }

    pub(crate) fn unassign(&self, bytes: usize, forwarded: bool) {
        self.incomplete_batches.dec();
        if forwarded {
            self.forwarded_retained_batches.dec();
            self.forwarded_retained_bytes.sub(
                i64::try_from(bytes)
                    .assured("a forwarded IPC payload fits in the 128 MiB node budget"),
            );
        }
    }

    pub(crate) fn retry(&self) {
        self.retries.inc();
    }
    pub(crate) fn ack(&self) {
        self.acks.inc();
    }
    pub(crate) fn reject(&self) {
        self.rejections.inc();
    }

    pub(crate) fn describe_lines(&self) -> Vec<String> {
        vec![
            format!("consumers: {}", self.consumers.get()),
            format!("forwarded consumers: {}", self.forwarded_consumers.get()),
            format!(
                "forwarded credit: {} bytes",
                self.forwarded_credit_bytes.get()
            ),
            format!(
                "forwarded retained batches: {}",
                self.forwarded_retained_batches.get()
            ),
            format!(
                "forwarded retained bytes: {}",
                self.forwarded_retained_bytes.get()
            ),
            format!("retained batches: {}", self.retained_batches.get()),
            format!("retained bytes: {}", self.retained_bytes.get()),
            format!(
                "incomplete application batches: {}",
                self.incomplete_batches.get()
            ),
            format!("retries: {}", self.retries.get()),
            format!("application ACKs: {}", self.acks.get()),
            format!("application rejections: {}", self.rejections.get()),
        ]
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn record_value_at(histogram: &mut TimeRollingHistogram, value: f64, now_nanos: i64) {
        let samples = HistogramSamples::one(value)
            .expect("a finite, non-negative test value has recorded units");
        histogram.record_at(samples, now_nanos);
    }

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

    fn branch_relay_batch_metrics(
        metrics: &RuntimeMetrics,
        domain: &DomainName,
        relay: &RelayName,
        physical_node_id: &ClusterNodeName,
        branch_key: &str,
    ) -> BatchMetricsHandle {
        metrics.resolve_batch_metrics(
            MetricKey::relay(
                domain,
                relay,
                Some(physical_node_id),
                "received",
                MESSAGES_TOTAL,
            ),
            RecordingScope::Branch(branch_key),
        )
    }

    #[test]
    fn local_summary_reports_rates_and_percentiles() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let node = ModelName::parse("dedupe").expect("valid identifier");
        let relay = RelayName::parse("input").expect("valid identifier");
        let physical_node = ClusterNodeName::parse("node-1").expect("valid name");
        let input_metrics = metrics.resolve_node_input_metrics(
            &domain,
            ModelKind::Deduplicator,
            &node,
            &relay,
            Some(&physical_node),
            None,
        );
        input_metrics.observe_delivery(&DeliveryObservation {
            messages: 3,
            bytes: 128,
            delivered_at: Timestamp::from_unix_nanos(1_250_000_000),
            ingested_at: &[1_000_000_000; 3],
        });

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
        let domain = DomainName::parse("main").expect("valid domain");
        let node = ModelName::parse("dedupe").expect("valid identifier");
        let relay = RelayName::parse("input").expect("valid identifier");
        let physical_node = ClusterNodeName::parse("node-1").expect("valid name");
        metrics
            .resolve_node_input_metrics(
                &domain,
                ModelKind::Deduplicator,
                &node,
                &relay,
                Some(&physical_node),
                None,
            )
            .observe_batch(3, 128, None);
        metrics
            .resolve_batch_metrics(
                MetricKey::node(
                    &domain,
                    ModelKind::Deduplicator,
                    &node,
                    Some(&physical_node),
                    &relay,
                    "received",
                    MESSAGES_TOTAL,
                ),
                RecordingScope::Branch(r#"{"tenant":"alpha"}"#),
            )
            .observe(2, 64, None);

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
        let domain = DomainName::parse("main").expect("valid domain");
        let ingestor = IngestorName::parse("ing").expect("valid identifier");
        let physical_node = ClusterNodeName::parse("node-1").expect("valid name");
        metrics
            .resolve_global_node_message_metrics(
                &domain,
                ModelKind::Ingestor,
                &ModelName::from(&ingestor),
                Some(&physical_node),
                "received",
            )
            .observe(2, 34, None);
        metrics
            .resolve_branch_node_message_metrics(
                &domain,
                ModelKind::Ingestor,
                &ModelName::from(&ingestor),
                Some(&physical_node),
                "received",
                r#"{"tenant":"alpha"}"#,
            )
            .observe(2, 34, None);

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
                .checked_mul(std::mem::size_of::<u64>())
                .assured("a histogram's distinct value count is bounded by its configured buckets")
                <= MAX_INTERNAL_BUCKET_BYTES,
            "messages_per_batch histogram count storage is too large: {} values",
            messages.distinct_values()
        );

        let relay_buffer = HistogramConfig::for_buckets(RELAY_BUFFER_LEN_BUCKETS).new_histogram();
        assert!(
            relay_buffer
                .distinct_values()
                .checked_mul(std::mem::size_of::<u64>())
                .assured("a histogram's distinct value count is bounded by its configured buckets")
                <= MAX_INTERNAL_BUCKET_BYTES,
            "relay_buffer_len histogram count storage is too large: {} values",
            relay_buffer.distinct_values()
        );
    }

    #[test]
    fn messages_per_batch_percentiles_follow_observed_values_not_bucket_boundaries() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let node = ModelName::parse("dedupe").expect("valid identifier");
        let relay = RelayName::parse("events").expect("valid identifier");
        let physical_node = ClusterNodeName::parse("node-1").expect("valid name");
        let input_metrics = metrics.resolve_node_input_metrics(
            &domain,
            ModelKind::Deduplicator,
            &node,
            &relay,
            Some(&physical_node),
            None,
        );

        for _ in 0..100 {
            input_metrics.observe_batch(2, 64, None);
        }
        input_metrics.observe_batch(500, 64, None);

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
    fn messages_per_batch_percentiles_preserve_batches_through_65536_messages() {
        let mut histogram = TimeRollingHistogram::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            INTERNAL_MESSAGE_BATCH_BUCKETS,
        );
        record_value_at(&mut histogram, 65_536.0, 0);

        let summary = histogram.summary_at(1_000_000_000);
        assert_histogram_percentile_near(summary.p50, 65_536.0);
        assert_histogram_percentile_near(summary.p90, 65_536.0);
        assert_histogram_percentile_near(summary.p99, 65_536.0);
    }

    #[test]
    fn relay_buffer_len_reports_capacity_and_dataflow_statistics() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let relay = RelayName::parse("events").expect("valid identifier");
        let physical_node = ClusterNodeName::parse("node-1").expect("valid name");
        let relay_metrics =
            metrics.resolve_relay_metrics(&domain, &relay, Some(&physical_node), "concrete", None);
        relay_metrics.observe_buffer(2, 3);
        relay_metrics.observe_batch(2, 64, None);

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
        record_value_at(&mut histogram, 10.0, 0);
        record_value_at(&mut histogram, 20.0, 0);

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
            record_value_at(&mut histogram, 2.0, 0);
        }
        record_value_at(&mut histogram, 500.0, 0);

        let summary = histogram.summary_at(1_000_000_000);
        assert_histogram_percentile_near(summary.p50, 2.0);
        assert_histogram_percentile_near(summary.p90, 2.0);
        assert_histogram_percentile_near(summary.p99, 2.0);
    }

    #[test]
    fn an_observation_past_the_bucket_ladder_lands_in_the_top_bucket() {
        // LATENCY_BUCKETS tops out at 30 seconds. A request slower than that is exactly the
        // observation a latency percentile exists to expose, so it has to be counted at the
        // maximum rather than dropped for being out of range.
        let mut histogram = TimeRollingHistogram::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            LATENCY_BUCKETS,
        );
        for _ in 0..90 {
            record_value_at(&mut histogram, 0.001, 0);
        }
        for _ in 0..10 {
            record_value_at(&mut histogram, 3_600.0, 0);
        }

        let summary = histogram.summary_at(1_000_000_000);
        assert_histogram_percentile_near(summary.p50, 0.001);
        assert!(
            summary.p99.is_some_and(|p99| p99 >= 30.0),
            "the slowest observation must reach the top bucket, got {:?}",
            summary.p99
        );
    }

    #[test]
    fn describe_renders_expired_one_minute_histogram_percentiles_as_absent() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let node = ModelName::parse("dedupe").expect("valid identifier");
        let relay = RelayName::parse("events").expect("valid identifier");
        let key = MetricKey::node(
            &domain,
            ModelKind::Deduplicator,
            &node,
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            &relay,
            "received",
            MESSAGES_PER_BATCH,
        );
        let histogram = HistogramSeries::new(MESSAGE_BATCH_BUCKETS);
        let now_wall = current_wall_unix_nanos().expect("wall clock should be available");
        let old = now_wall - 2 * 60 * 1_000_000_000;
        {
            let mut rolling = histogram.rolling_histograms.lock();
            record_value_at(&mut rolling.wall_1m.inner, 10.0, old);
            record_value_at(&mut rolling.wall_15m.inner, 10.0, old);
            histogram.observed.store(true, AtomicOrdering::Relaxed);
        }
        metrics.series.histograms.insert(key, Arc::new(histogram));

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
        let now = Instant::now();
        let last = now
            .checked_sub(std::time::Duration::from_secs(5 * 60))
            .expect("old instant should be representable");
        let ema = WallEma::new(last, rate_decay_tau_seconds(ONE_MINUTE));
        ema.value_bits
            .store(100.0_f64.to_bits(), AtomicOrdering::Relaxed);
        ema.last_elapsed_nanos.store(0, AtomicOrdering::Relaxed);

        let decayed = ema
            .value_at(now)
            .expect("ema with value and timestamp should summarize");
        assert!(decayed < 1.0, "expected old EMA to decay, got {decayed}");
    }

    #[test]
    fn one_minute_rate_ema_is_nearly_zero_after_one_minute_without_activity() {
        let now = Instant::now();
        let last = now
            .checked_sub(std::time::Duration::from_secs(60))
            .expect("old instant should be representable");
        let ema = WallEma::new(last, rate_decay_tau_seconds(ONE_MINUTE));
        ema.value_bits
            .store(100.0_f64.to_bits(), AtomicOrdering::Relaxed);
        ema.last_elapsed_nanos.store(0, AtomicOrdering::Relaxed);

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
        let ema = WallEma::new(Instant::now(), rate_decay_tau_seconds(ONE_MINUTE));
        ema.value_bits
            .store(10.0_f64.to_bits(), AtomicOrdering::Relaxed);
        observe_ema_sample(&ema.value_bits, 100.0, 5.0, ema.tau_seconds);

        let value = load_ema_value(&ema.value_bits).expect("sampled EMA should have a value");
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
            rate_decay_tau_seconds(ONE_MINUTE),
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
        let _ = histogram.record(
            scaled_histogram_value(10.0).expect("ten seconds has a scaled histogram value"),
        );
        let snapshot = WallRollingHistogramSnapshot {
            buckets: vec![RollingHistogramBucketSnapshot {
                start_at_nanos: bucket_start(old, WALL_HISTOGRAM_1M_STEP),
                values: hdr_histogram_to_snapshot(&histogram),
            }],
        };

        let restored = WallRollingHistogram::from_snapshot(
            &snapshot,
            ONE_MINUTE,
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
        let domain = DomainName::parse("main").expect("valid domain");
        let relay = RelayName::parse("events").expect("valid identifier");
        metrics
            .resolve_relay_metrics(
                &domain,
                &relay,
                Some(&ClusterNodeName::parse("node-1").expect("valid name")),
                "concrete",
                None,
            )
            .observe_batch(2, 64, None);

        let rendered = metrics.prometheus_text();
        assert!(rendered.contains("nervix_messages_total"));
        assert!(rendered.contains("domain=\"main\""));
        assert!(rendered.contains("target_kind=\"RELAY\""));
        assert!(rendered.contains("relay=\"events\""));
        assert!(rendered.contains("physical_node_id=\"node-1\""));
        assert!(rendered.contains(" 2"));
    }

    #[test]
    fn client_emitter_metrics_track_live_work_and_monotonic_application_results() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let emitter = EmitterName::parse("output").expect("valid emitter");
        let series = metrics.client_emitter_series(&domain, &emitter);
        series.reset_gauges();
        series.attach(false, 1024);
        series.attach(true, 2048);
        series.retain(512);
        series.assign(512, true);
        series.retry();
        series.ack();
        series.reject();
        let rendered = metrics.prometheus_text();
        for (name, expected) in [
            ("consumers", 2.0),
            ("forwarded_consumers", 1.0),
            ("forwarded_credit_bytes", 2048.0),
            ("forwarded_retained_batches", 1.0),
            ("forwarded_retained_bytes", 512.0),
            ("retained_batches", 1.0),
            ("retained_bytes", 512.0),
            ("incomplete_batches", 1.0),
            ("retries_total", 1.0),
            ("acks_total", 1.0),
            ("rejections_total", 1.0),
        ] {
            let metric = format!("nervix_client_emitter_{name}");
            assert_eq!(
                prometheus_sample(&rendered, &metric, "domain=\"main\",emitter=\"output\""),
                Some(expected),
                "{metric}"
            );
        }
        series.unassign(512, true);
        series.release(512);
        series.detach(true, 2048);
        series.detach(false, 1024);
        series.reset_gauges();
        assert!(
            series
                .describe_lines()
                .iter()
                .any(|line| line == "retained batches: 0")
        );
        assert!(
            series
                .describe_lines()
                .iter()
                .any(|line| line == "application ACKs: 1")
        );
    }

    #[test]
    fn relinquishing_relay_ownership_removes_its_local_metrics() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let relay = RelayName::parse("events").expect("valid identifier");
        let relay_metrics = metrics.resolve_relay_metrics(
            &domain,
            &relay,
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            "concrete",
            Some(r#"{"tenant":"acme"}"#),
        );
        relay_metrics.observe_batch(2, 64, None);
        relay_metrics.observe_buffer(1, 2);

        metrics.remove_relay(&domain, &relay);

        assert!(
            metrics
                .series
                .counters
                .iter()
                .all(|entry| entry.key().target != relay.as_str())
        );
        assert!(
            metrics
                .series
                .histograms
                .iter()
                .all(|entry| entry.key().target != relay.as_str())
        );
        assert!(
            metrics
                .series
                .branch_counters
                .iter()
                .all(|entry| entry.key().key.target != relay.as_str())
        );
        assert!(
            metrics
                .series
                .branch_histograms
                .iter()
                .all(|entry| entry.key().key.target != relay.as_str())
        );
        assert!(!metrics.prometheus_text().contains("target=\"events\""));
    }

    #[test]
    fn branch_lifecycle_metrics_count_concrete_keys_once_per_node() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let branch = BranchName::parse("by_tenant").expect("valid identifier");
        let concrete_key = r#"{"tenant":"acme"}"#;
        let has_sample = |rendered: &str, metric: &str, label_fragments: &[&str], value: u64| {
            let expected_suffix = format!(" {value}");
            rendered.lines().any(|line| {
                line.starts_with(metric)
                    && label_fragments
                        .iter()
                        .all(|fragment| line.contains(fragment))
                    && line.ends_with(&expected_suffix)
            })
        };

        metrics.register_branch(
            &domain,
            &branch,
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
        );
        metrics.observe_branch_instance_created(
            &domain,
            &branch,
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            concrete_key,
        );
        metrics.observe_branch_instance_created(
            &domain,
            &branch,
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            concrete_key,
        );

        let rendered = metrics.prometheus_text();
        assert!(has_sample(
            &rendered,
            "nervix_branch_instances",
            &[
                "branch=\"by_tenant\"",
                "domain=\"main\"",
                "physical_node_id=\"node-1\"",
            ],
            1,
        ));
        assert!(!rendered.contains(concrete_key));

        metrics.observe_branch_instance_removed(
            &domain,
            &branch,
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            concrete_key,
            BranchEvictionReason::Lru,
        );
        let rendered = metrics.prometheus_text();
        assert!(has_sample(
            &rendered,
            "nervix_branch_instances",
            &[
                "branch=\"by_tenant\"",
                "domain=\"main\"",
                "physical_node_id=\"node-1\"",
            ],
            1,
        ));
        assert!(has_sample(
            &rendered,
            "nervix_branch_evictions_total",
            &[
                "branch=\"by_tenant\"",
                "domain=\"main\"",
                "physical_node_id=\"node-1\"",
                "reason=\"lru\"",
            ],
            1,
        ));

        metrics.observe_branch_instance_removed(
            &domain,
            &branch,
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            concrete_key,
            BranchEvictionReason::Ttl,
        );

        let rendered = metrics.prometheus_text();
        assert!(has_sample(
            &rendered,
            "nervix_branch_instances",
            &[
                "branch=\"by_tenant\"",
                "domain=\"main\"",
                "physical_node_id=\"node-1\"",
            ],
            0,
        ));
        assert!(has_sample(
            &rendered,
            "nervix_branch_evictions_total",
            &[
                "branch=\"by_tenant\"",
                "domain=\"main\"",
                "physical_node_id=\"node-1\"",
                "reason=\"lru\"",
            ],
            1,
        ));
        assert!(has_sample(
            &rendered,
            "nervix_branch_evictions_total",
            &[
                "branch=\"by_tenant\"",
                "domain=\"main\"",
                "physical_node_id=\"node-1\"",
                "reason=\"ttl\"",
            ],
            0,
        ));
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
        let domain = DomainName::parse("main").expect("valid domain");
        let node = ModelName::parse("dedupe").expect("valid identifier");
        let relay = RelayName::parse("events").expect("valid identifier");
        metrics
            .resolve_node_input_metrics(
                &domain,
                ModelKind::Deduplicator,
                &node,
                &relay,
                Some(&ClusterNodeName::parse("node-1").expect("valid name")),
                None,
            )
            .observe_batch(3, 96, None);

        let prometheus = metrics.prometheus_text();
        assert!(prometheus.contains("nervix_messages_per_batch_bucket"));
        assert!(prometheus.contains("nervix_messages_per_batch_count"));

        let snapshot = metrics.snapshot_global_target(
            &domain,
            ModelKind::Deduplicator,
            &node,
            &ClusterNodeName::parse("node-1").expect("valid name"),
        );
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
            &ClusterNodeName::parse("node-1").expect("valid name"),
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
    fn resolved_unobserved_histograms_are_not_exported() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let node = ModelName::parse("dedupe").expect("valid identifier");
        let relay = RelayName::parse("events").expect("valid identifier");

        let _handle = metrics.resolve_node_input_metrics(
            &domain,
            ModelKind::Deduplicator,
            &node,
            &relay,
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            None,
        );

        let prometheus = metrics.prometheus_text();
        assert!(!prometheus.contains("nervix_messages_per_batch_count"));
        assert!(!prometheus.contains("nervix_delivery_latency_seconds_count"));
    }

    #[test]
    fn prometheus_export_uses_global_metrics_only() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let relay = RelayName::parse("events").expect("valid identifier");
        branch_relay_batch_metrics(
            &metrics,
            &domain,
            &relay,
            &ClusterNodeName::parse("node-1").expect("valid name"),
            r#"{"tenant":"acme"}"#,
        )
        .observe(9, 128, None);

        let rendered = metrics.prometheus_text();
        assert!(!rendered.contains(r#"{"tenant":"acme"}"#));
        assert!(!has_graph_prometheus_samples(&rendered));
    }

    #[test]
    fn global_snapshot_uses_global_metrics_only() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("main").expect("valid domain");
        let relay = RelayName::parse("events").expect("valid identifier");
        let physical_node = ClusterNodeName::parse("node-1").expect("valid name");
        branch_relay_batch_metrics(
            &metrics,
            &domain,
            &relay,
            &physical_node,
            r#"{"tenant":"acme"}"#,
        )
        .observe(9, 128, None);
        metrics
            .resolve_relay_metrics(&domain, &relay, Some(&physical_node), "concrete", None)
            .observe_batch(2, 64, None);

        let snapshot = metrics.snapshot_global_target(
            &domain,
            ModelKind::Relay,
            &ModelName::from(&relay),
            &ClusterNodeName::parse("node-1").expect("valid name"),
        );
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
        let domain = DomainName::parse("main").expect("valid domain");
        let relay = RelayName::parse("events").expect("valid identifier");
        let physical_node = ClusterNodeName::parse("node-1").expect("valid name");
        branch_relay_batch_metrics(
            &metrics,
            &domain,
            &relay,
            &physical_node,
            r#"{"tenant":"acme"}"#,
        )
        .observe(9, 128, None);
        metrics
            .resolve_relay_metrics(&domain, &relay, Some(&physical_node), "concrete", None)
            .observe_batch(2, 64, None);

        let snapshot = metrics.snapshot_branch_target(
            r#"{"tenant":"acme"}"#,
            &domain,
            ModelKind::Relay,
            &ModelName::from(&relay),
            &ClusterNodeName::parse("node-1").expect("valid name"),
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
            &ModelName::from(&relay),
            &ClusterNodeName::parse("node-1").expect("valid name"),
            snapshot,
        );
        assert!(!has_graph_prometheus_samples(&restored.prometheus_text()));
        let restored_branch = restored.snapshot_branch_target(
            r#"{"tenant":"acme"}"#,
            &domain,
            ModelKind::Relay,
            &ModelName::from(&relay),
            &ClusterNodeName::parse("node-1").expect("valid name"),
        );
        assert!(
            restored_branch
                .counters
                .iter()
                .any(|counter| { counter.key.metric == MESSAGES_TOTAL && counter.value == 9 })
        );
    }

    fn recorded_values(histogram: &HdrHistogram<u64>) -> Vec<(u64, u64)> {
        histogram
            .iter_recorded()
            .map(|value| (value.value_iterated_to(), value.count_at_value()))
            .collect()
    }

    fn deduplicator_input(
        metrics: &RuntimeMetrics,
        branch_key: Option<&str>,
    ) -> NodeInputMetricsHandle {
        metrics.resolve_node_input_metrics(
            &DomainName::parse("main").expect("valid domain"),
            ModelKind::Deduplicator,
            &ModelName::parse("dedupe").expect("valid identifier"),
            &RelayName::parse("events").expect("valid identifier"),
            Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            branch_key,
        )
    }

    fn delivery_latency_line(metrics: &RuntimeMetrics) -> Option<String> {
        metrics
            .describe_global_target(
                &DomainName::parse("main").expect("valid domain"),
                "DEDUPLICATOR",
                ModelName::parse("dedupe").expect("valid identifier"),
            )
            .into_iter()
            .find(|line| line.contains("delivery_latency_seconds received relay=events"))
    }

    fn prometheus_sample(rendered: &str, series: &str, fragment: &str) -> Option<f64> {
        rendered
            .lines()
            .find(|line| line.starts_with(series) && line.contains(fragment))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse().ok())
    }

    #[test]
    fn a_batch_of_latencies_fills_the_buckets_recording_each_latency_would() {
        let config =
            HistogramConfig::for_buckets(internal_buckets_for_metric(DELIVERY_LATENCY_SECONDS));
        let layout = config.delivery_latency_layout();
        let now = 0_i64;
        let mut instants = vec![i64::MIN, -i64::MAX, 1];
        for millis in 0..=31_000_i64 {
            for remainder in [0, 1, 499_999, 500_001, 999_999] {
                instants.push(now - (millis * 1_000_000 + remainder));
            }
        }

        let mut one_at_a_time = config.new_histogram();
        for elapsed in elapsed_nanos(now, &instants) {
            let seconds = Duration::from_nanos(elapsed).as_secs_f64();
            HistogramSamples::one(seconds)
                .expect("an elapsed time has recorded units")
                .record_into(&mut one_at_a_time);
        }
        let mut per_batch = config.new_histogram();
        let batch = ElapsedHistogram::new(&layout, now, &instants);
        HistogramSamples::Elapsed(&batch).record_into(&mut per_batch);

        assert_eq!(batch.total(), one_at_a_time.len());
        assert_eq!(recorded_values(&per_batch), recorded_values(&one_at_a_time));
    }

    #[test]
    fn a_delivered_batch_records_each_series_once_with_its_latest_watermark() {
        let metrics = RuntimeMetrics::default();
        let input = deduplicator_input(&metrics, Some(r#"{"tenant":"acme"}"#));

        input.observe_delivery(&DeliveryObservation {
            messages: 4,
            bytes: 64,
            delivered_at: Timestamp::from_unix_nanos(10_000_000_000),
            ingested_at: &[9_900_000_000, 7_500_000_000, 10_500_000_000, 10_000_000_000],
        });

        let line = delivery_latency_line(&metrics).expect("delivery latency should be rendered");
        assert!(line.contains(" p50_1m=0.1 "), "{line}");
        assert!(line.contains(" p90_1m=2.5 "), "{line}");
        assert!(line.contains(" domain_p50_1m=0.1 "), "{line}");
        assert!(line.contains(" domain_p99_15m=2.5"), "{line}");

        let rendered = metrics.prometheus_text();
        let count = prometheus_sample(&rendered, "nervix_delivery_latency_seconds_count", "dedupe");
        assert_eq!(count, Some(3.0), "{rendered}");
        let zero = prometheus_sample(
            &rendered,
            "nervix_delivery_latency_seconds_bucket",
            "le=\"0.001\"",
        );
        assert_eq!(zero, Some(1.0), "{rendered}");
        let tenth = prometheus_sample(
            &rendered,
            "nervix_delivery_latency_seconds_bucket",
            "le=\"0.1\"",
        );
        assert_eq!(tenth, Some(2.0), "{rendered}");
        let five = prometheus_sample(
            &rendered,
            "nervix_delivery_latency_seconds_bucket",
            "le=\"5\"",
        );
        assert_eq!(five, Some(3.0), "{rendered}");
        let sum = prometheus_sample(&rendered, "nervix_delivery_latency_seconds_sum", "dedupe")
            .expect("the latency sum is exported");
        assert!((sum - 2.6).abs() < 1e-9, "{rendered}");

        let node_id = ClusterNodeName::parse("node-1").expect("valid name");
        let domain = DomainName::parse("main").expect("valid domain");
        let node = ModelName::parse("dedupe").expect("valid identifier");
        let branch = metrics.snapshot_branch_target(
            r#"{"tenant":"acme"}"#,
            &domain,
            ModelKind::Deduplicator,
            &node,
            &node_id,
        );
        let global =
            metrics.snapshot_global_target(&domain, ModelKind::Deduplicator, &node, &node_id);
        for snapshot in [branch, global] {
            let latency = snapshot
                .histograms
                .iter()
                .find(|histogram| histogram.key.metric == DELIVERY_LATENCY_SECONDS)
                .expect("each latency series records the batch");
            assert_eq!(latency.domain_started_at_nanos, Some(10_500_000_000));
            assert_eq!(latency.domain_last_at_nanos, Some(10_500_000_000));
            let messages = snapshot
                .counters
                .iter()
                .find(|counter| counter.key.metric == MESSAGES_TOTAL)
                .expect("each traffic series records the batch");
            assert_eq!(messages.value, 4);
            assert_eq!(messages.domain_last_at_nanos, Some(10_500_000_000));
        }
    }

    #[test]
    fn a_batch_ingested_after_its_delivery_records_traffic_but_no_latency() {
        let metrics = RuntimeMetrics::default();
        let input = deduplicator_input(&metrics, None);

        input.observe_delivery(&DeliveryObservation {
            messages: 2,
            bytes: 32,
            delivered_at: Timestamp::from_unix_nanos(1_000),
            ingested_at: &[1_001, 2_000],
        });

        assert_eq!(delivery_latency_line(&metrics), None);
        let rendered = metrics.prometheus_text();
        assert!(
            !rendered.contains("nervix_delivery_latency_seconds_count"),
            "{rendered}"
        );
        let messages = prometheus_sample(&rendered, "nervix_messages_total", "dedupe");
        assert_eq!(messages, Some(2.0), "{rendered}");
    }

    #[test]
    fn an_older_batch_leaves_the_latency_domain_time_at_the_latest_watermark() {
        let metrics = RuntimeMetrics::default();
        let input = deduplicator_input(&metrics, None);

        input.observe_delivery(&DeliveryObservation {
            messages: 1,
            bytes: 16,
            delivered_at: Timestamp::from_unix_nanos(60_000_000_000),
            ingested_at: &[59_900_000_000],
        });
        input.observe_delivery(&DeliveryObservation {
            messages: 1,
            bytes: 16,
            delivered_at: Timestamp::from_unix_nanos(60_000_000_000),
            ingested_at: &[15_000_000_000],
        });

        let snapshot = metrics.snapshot_global_target(
            &DomainName::parse("main").expect("valid domain"),
            ModelKind::Deduplicator,
            &ModelName::parse("dedupe").expect("valid identifier"),
            &ClusterNodeName::parse("node-1").expect("valid name"),
        );
        let latency = snapshot
            .histograms
            .iter()
            .find(|histogram| histogram.key.metric == DELIVERY_LATENCY_SECONDS)
            .expect("the latency series records both batches");
        assert_eq!(latency.domain_started_at_nanos, Some(15_000_000_000));
        assert_eq!(latency.domain_last_at_nanos, Some(59_900_000_000));
        let line = delivery_latency_line(&metrics).expect("delivery latency should be rendered");
        assert!(line.contains(" p99_1m=30.1 "), "{line}");
        assert!(line.contains(" domain_p99_1m=0.1 "), "{line}");
    }
}
