//! The interconnection series a node exposes: its transport pools, the execution admission every
//! variable-size operation passes through, its consensus retention, and the reactor delay they all
//! share.
//!
//! Layer: engines and infrastructure, with the same edge inside it the parent module declares.
//!
//! - **Owns.** The metric families that describe a node's own interconnection, the bounded
//!   dimensions they are labelled by, and the timer that samples how late this node's reactor is
//!   polling work that is already runnable.
//! - **Depends on.** The transport, execution and consensus snapshots it reads at scrape time.
//! - **Must not know.** Which peer, domain, branch or operation identity produced a value. Every
//!   label value here comes from an enumeration fixed at compile time, so no payload can widen a
//!   series, and the exporting node is the node the values belong to.
//!
//! Levels are read from the state that owns them each time the endpoint is scraped, rather than
//! mirrored into a counter that a missed decrement could desynchronise. Only events that leave no
//! standing state behind — a connection that failed, a stream that reset, a request that finished
//! — are counted as they happen.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_consensus::Observer;
use nervix_execution::{Executor, MemoryBudgetSnapshot, WorkerClassSnapshot};
use nervix_interconnect::{
    ConnectionDirection, ConnectionFailureReason, PoolClass, RelayAdmissionOutcome, RequestOutcome,
    RequestSubquota, StreamResetReason, TransferDirection, Transport, TransportSnapshot,
};
use parking_lot::RwLock;
use prometheus::{
    CounterVec, GaugeVec, Opts,
    core::{Collector, Desc},
    proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
};
use strum::IntoEnumIterator as _;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use triomphe::Arc;

/// How often the reactor delay probe asks to be woken. Short enough that one blocked poll is
/// visible in a scrape interval, long enough that the probe itself is not the load.
const SCHEDULER_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// The live node state the interconnection series are read from.
///
/// One node installs this once, when its transport, its executor and its consensus observer all
/// exist. Until then the collector reports nothing, because there is nothing yet to report.
#[derive(Clone)]
pub struct NodeObservations {
    inner: Arc<NodeObservationsInner>,
}

struct NodeObservationsInner {
    executor: Executor,
    transport: Transport,
    consensus: Observer,
    scheduler: SchedulerDelay,
}

impl NodeObservations {
    pub fn new(executor: Executor, transport: Transport, consensus: Observer) -> Self {
        Self {
            inner: Arc::new(NodeObservationsInner {
                executor,
                transport,
                consensus,
                scheduler: SchedulerDelay::default(),
            }),
        }
    }

    /// Sample how late this node's reactor is until the node shuts down.
    ///
    /// A timer that asks to be woken every [`SCHEDULER_SAMPLE_INTERVAL`] is woken later than that
    /// exactly when something else held an async worker past its yield point. The excess is
    /// therefore the delay every other task on that worker paid, which is what an operator reads
    /// when a control answer arrives late while a bulk transfer is running.
    pub async fn sample_scheduler_delay(self, shutdown: CancellationToken) {
        let mut ticks = interval(SCHEDULER_SAMPLE_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticks.tick().await;
        let mut expected_at = Instant::now();
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticks.tick() => {}
            }
            let woken_at = Instant::now();
            let due_at = match expected_at.checked_add(SCHEDULER_SAMPLE_INTERVAL) {
                Some(due_at) => due_at,
                None => break,
            };
            self.inner
                .scheduler
                .observe(woken_at.saturating_duration_since(due_at));
            expected_at = woken_at;
        }
    }
}

/// How late this node's reactor has been polling work that was already runnable.
#[derive(Debug, Default)]
struct SchedulerDelay {
    observations: AtomicU64,
    total_nanos: AtomicU64,
    peak_nanos: AtomicU64,
}

impl SchedulerDelay {
    fn observe(&self, delay: Duration) {
        let delay_nanos = u64::try_from(delay.as_nanos())
            .assured("a probe woken 584 years late has outlived the node it was measuring");
        self.observations.fetch_add(1, Ordering::AcqRel);
        self.total_nanos.fetch_add(delay_nanos, Ordering::AcqRel);
        self.peak_nanos.fetch_max(delay_nanos, Ordering::AcqRel);
    }

    fn read(&self) -> SchedulerDelaySnapshot {
        SchedulerDelaySnapshot {
            observations: self.observations.load(Ordering::Acquire),
            total: Duration::from_nanos(self.total_nanos.load(Ordering::Acquire)),
            peak: Duration::from_nanos(self.peak_nanos.load(Ordering::Acquire)),
        }
    }
}

struct SchedulerDelaySnapshot {
    observations: u64,
    total: Duration,
    peak: Duration,
}

/// The Prometheus collector that turns those snapshots into this node's interconnection series.
pub(crate) struct InterconnectionCollector {
    /// Installed once, when the node's transport, executor and consensus observer exist. Read on
    /// every scrape, which is why it is a lock rather than a construction argument.
    sources: RwLock<Option<NodeObservations>>,
    descs: Vec<Desc>,
}

/// The descriptor Prometheus registers one series under.
///
/// A metric vector is built only to take the descriptor off it, because that is the constructor
/// Prometheus offers for a descriptor with no constant labels.
fn describe(series: &SeriesSpec) -> Desc {
    let options = Opts::new(series.name, series.help);
    let naming = "the series names, help text and label names are constants that satisfy \
                  Prometheus naming rules";
    let vector: Box<dyn Collector> = match series.kind {
        SeriesKind::Counter => Box::new(CounterVec::new(options, series.labels).assured(naming)),
        SeriesKind::Gauge => Box::new(GaugeVec::new(options, series.labels).assured(naming)),
    };
    let descs = vector.desc();
    let desc = descs
        .first()
        .assured("a metric vector declares exactly one descriptor");
    (*desc).clone()
}

/// One series this collector declares, named, described and typed once.
struct SeriesSpec {
    name: &'static str,
    help: &'static str,
    kind: SeriesKind,
    labels: &'static [&'static str],
}

/// The two kinds of series this collector has: a level that can fall again, and a total that only
/// rises. Declaring only these two is what lets a sample take its shape from the family it is
/// added to, so a family can never hold a sample of the wrong kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeriesKind {
    Gauge,
    Counter,
}

impl SeriesKind {
    fn metric_type(self) -> MetricType {
        match self {
            Self::Gauge => MetricType::GAUGE,
            Self::Counter => MetricType::COUNTER,
        }
    }
}

const CLASS: &[&str] = &["class"];
const CLASS_DIRECTION: &[&str] = &["class", "direction"];
const CLASS_REASON: &[&str] = &["class", "reason"];
const DIRECTION_OPERATION: &[&str] = &["direction", "operation"];
const OPERATION: &[&str] = &["operation"];
const OPERATION_OUTCOME: &[&str] = &["operation", "outcome"];
const OUTCOME: &[&str] = &["outcome"];
const NO_LABELS: &[&str] = &[];

/// Every series this collector exposes, in the order it produces them.
const SERIES: &[SeriesSpec] = &[
    SeriesSpec {
        name: "nervix_interconnect_connections",
        help: "Physical interconnect connections this node currently holds.",
        kind: SeriesKind::Gauge,
        labels: CLASS_DIRECTION,
    },
    SeriesSpec {
        name: "nervix_interconnect_streams",
        help: "HTTP/2 stream slots leased on this node's outbound interconnect connections.",
        kind: SeriesKind::Gauge,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_interconnect_pending_operations",
        help: "Interconnect requests admitted by this node and not yet resolved.",
        kind: SeriesKind::Gauge,
        labels: DIRECTION_OPERATION,
    },
    SeriesSpec {
        name: "nervix_interconnect_relay_channels",
        help: "Logical relay channels with unresolved work on this node.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_interconnect_relay_attempts",
        help: "Relay attempts whose outcome this node has not retired yet.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_interconnect_relay_grants",
        help: "Relay transfer grants this node has issued and not yet spent.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_interconnect_unresolved_outcome_age_seconds",
        help: "Age of the oldest relay outcome this node has not acknowledged.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_interconnect_connections_established_total",
        help: "Interconnect connections this node has established.",
        kind: SeriesKind::Counter,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_interconnect_connection_failures_total",
        help: "Interconnect connections that failed to establish or ended, by cause.",
        kind: SeriesKind::Counter,
        labels: CLASS_REASON,
    },
    SeriesSpec {
        name: "nervix_interconnect_stream_resets_total",
        help: "Interconnect streams that ended without delivering a result, by cause.",
        kind: SeriesKind::Counter,
        labels: CLASS_REASON,
    },
    SeriesSpec {
        name: "nervix_interconnect_quota_failures_total",
        help: "Interconnect requests refused because their reserved subquota was full.",
        kind: SeriesKind::Counter,
        labels: DIRECTION_OPERATION,
    },
    SeriesSpec {
        name: "nervix_interconnect_requests_total",
        help: "Typed interconnect requests this node completed, by reserved subquota.",
        kind: SeriesKind::Counter,
        labels: OPERATION_OUTCOME,
    },
    SeriesSpec {
        name: "nervix_interconnect_request_seconds_total",
        help: "Round-trip time of typed interconnect requests. The liveness subquota is the peer \
               health probe.",
        kind: SeriesKind::Counter,
        labels: OPERATION,
    },
    SeriesSpec {
        name: "nervix_interconnect_relay_admissions_total",
        help: "Relay attempts this node resolved, by how they left its unresolved set.",
        kind: SeriesKind::Counter,
        labels: OUTCOME,
    },
    SeriesSpec {
        name: "nervix_interconnect_relay_admission_wait_seconds_total",
        help: "Time relay attempts spent between an accepted reservation and their resolution.",
        kind: SeriesKind::Counter,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_interconnect_bulk_bytes_total",
        help: "Bytes carried by streamed interconnect bodies.",
        kind: SeriesKind::Counter,
        labels: CLASS_DIRECTION,
    },
    SeriesSpec {
        name: "nervix_execution_memory_capacity_bytes",
        help: "Transient interconnection memory reserved for one execution class.",
        kind: SeriesKind::Gauge,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_memory_reserved_bytes",
        help: "Transient interconnection memory one execution class is currently holding.",
        kind: SeriesKind::Gauge,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_memory_reservations_total",
        help: "Memory charges one execution class has granted.",
        kind: SeriesKind::Counter,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_memory_rejections_total",
        help: "Memory charges one execution class refused outright.",
        kind: SeriesKind::Counter,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_workers",
        help: "Jobs one execution class may have on the blocking pool at once.",
        kind: SeriesKind::Gauge,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_jobs_running",
        help: "Jobs one execution class currently holds a worker for.",
        kind: SeriesKind::Gauge,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_jobs_pending",
        help: "Jobs waiting in one execution class's bounded queue.",
        kind: SeriesKind::Gauge,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_jobs_total",
        help: "Jobs one execution class has admitted.",
        kind: SeriesKind::Counter,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_jobs_completed_total",
        help: "Jobs that have left a worker of one execution class.",
        kind: SeriesKind::Counter,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_job_rejections_total",
        help: "Jobs refused because one execution class already held its whole wait queue.",
        kind: SeriesKind::Counter,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_job_queue_seconds_total",
        help: "Time admitted jobs spent waiting for a worker of one execution class.",
        kind: SeriesKind::Counter,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_execution_job_work_seconds_total",
        help: "Time completed jobs spent holding a worker of one execution class.",
        kind: SeriesKind::Counter,
        labels: CLASS,
    },
    SeriesSpec {
        name: "nervix_node_scheduler_delay_seconds_total",
        help: "Total delay this node's reactor added to work that was already runnable.",
        kind: SeriesKind::Counter,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_node_scheduler_delay_peak_seconds",
        help: "The longest single reactor delay this node has observed.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_node_scheduler_samples_total",
        help: "Reactor delay samples this node has taken.",
        kind: SeriesKind::Counter,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_consensus_log_last_index",
        help: "The highest Raft index this node's log holds.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_consensus_log_snapshot_index",
        help: "The highest Raft index this node's current snapshot covers.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_consensus_log_purged_index",
        help: "The highest Raft index this node has removed from its log.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_consensus_log_retained_bytes",
        help: "What this node's retained Raft log occupies in node-owned storage.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_consensus_snapshot_pinned_generations",
        help: "Snapshot generations an outgoing transfer is holding against deletion.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_consensus_snapshot_pinned_readers",
        help: "Readers those snapshot pins are held for.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
    SeriesSpec {
        name: "nervix_consensus_snapshot_unreferenced_generations",
        help: "Snapshot generations waiting for the next durable batch to delete them.",
        kind: SeriesKind::Gauge,
        labels: NO_LABELS,
    },
];

impl InterconnectionCollector {
    pub(crate) fn new() -> Self {
        let mut descs = Vec::with_capacity(SERIES.len());
        for series in SERIES {
            descs.push(describe(series));
        }
        Self {
            sources: RwLock::new(None),
            descs,
        }
    }

    /// Give the collector the node state it reports. A node installs its own once.
    pub(crate) fn install(&self, observations: NodeObservations) {
        *self.sources.write() = Some(observations);
    }

    fn families(&self, sources: &NodeObservations) -> Vec<MetricFamily> {
        let transport = sources.inner.transport.snapshot();
        let executor = sources.inner.executor.snapshot();
        let log = sources.inner.consensus.raft_log_retention();
        let snapshots = sources.inner.consensus.snapshot_retention();
        let scheduler = sources.inner.scheduler.read();

        let mut families = Families::new();
        transport_families(&mut families, &transport);
        execution_families(&mut families, &executor);
        scheduler_families(&mut families, &scheduler);
        consensus_families(&mut families, &log, &snapshots);
        families.finish()
    }
}

impl std::fmt::Debug for InterconnectionCollector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InterconnectionCollector")
            .field("installed", &self.sources.read().is_some())
            .finish_non_exhaustive()
    }
}

/// A registry entry over the shared collector, so a node can install its sources after the
/// registry the series are exported through has already been built.
pub(crate) struct InterconnectionCollectorHandle {
    pub(crate) collector: Arc<InterconnectionCollector>,
}

impl Collector for InterconnectionCollectorHandle {
    fn desc(&self) -> Vec<&Desc> {
        self.collector.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.collector.collect()
    }
}

impl Collector for InterconnectionCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let sources = self.sources.read().clone();
        let Some(sources) = sources else {
            return Vec::new();
        };
        self.families(&sources)
    }
}

/// The families a scrape produces, filled in the order [`SERIES`] declares them.
struct Families {
    built: Vec<MetricFamily>,
}

impl Families {
    fn new() -> Self {
        Self {
            built: Vec::with_capacity(SERIES.len()),
        }
    }

    /// Start the next declared series. Callers add samples to it before starting another, so the
    /// produced order matches the declared order and every family is produced exactly once, which
    /// [`produces_every_declared_series_once`] checks. The lookup walks the declared table, whose
    /// length is fixed at compile time and which is only read while a scrape is being answered.
    fn open(&mut self, name: &'static str) -> OpenFamily<'_> {
        let spec = SERIES
            .iter()
            .find(|series| series.name == name)
            .assured("every family opened here is declared in this module's series table");
        let mut family = MetricFamily::new();
        family.set_name(spec.name.to_string());
        family.set_help(spec.help.to_string());
        family.set_field_type(spec.kind.metric_type());
        self.built.push(family);
        OpenFamily {
            family: self
                .built
                .last_mut()
                .verified("the family was pushed on the line above"),
            kind: spec.kind,
        }
    }

    fn finish(self) -> Vec<MetricFamily> {
        self.built
    }
}

/// The family currently being filled, and the kind every sample added to it takes.
struct OpenFamily<'a> {
    family: &'a mut MetricFamily,
    kind: SeriesKind,
}

impl OpenFamily<'_> {
    fn sample(&mut self, labels: &[(&str, &str)], value: f64) {
        let mut metric = Metric::new();
        let mut pairs = Vec::with_capacity(labels.len());
        for (name, label_value) in labels {
            let mut pair = LabelPair::new();
            pair.set_name((*name).to_string());
            pair.set_value((*label_value).to_string());
            pairs.push(pair);
        }
        metric.set_label(pairs);
        match self.kind {
            SeriesKind::Counter => {
                let mut counter = Counter::new();
                counter.set_value(value);
                metric.set_counter(counter);
            }
            SeriesKind::Gauge => {
                let mut gauge = Gauge::new();
                gauge.set_value(value);
                metric.set_gauge(gauge);
            }
        }
        self.family.mut_metric().push(metric);
    }
}

fn transport_families(families: &mut Families, transport: &TransportSnapshot) {
    let mut connections = families.open("nervix_interconnect_connections");
    for direction in ConnectionDirection::iter() {
        for class in PoolClass::ALL {
            let held = transport.connections[direction.index()][class.index()];
            connections.sample(
                &[("class", class.as_ref()), ("direction", direction.as_ref())],
                held.approx_into(),
            );
        }
    }

    let mut streams = families.open("nervix_interconnect_streams");
    for class in PoolClass::ALL {
        let leased = transport.leased_streams[class.index()];
        streams.sample(&[("class", class.as_ref())], leased.approx_into());
    }

    let mut pending = families.open("nervix_interconnect_pending_operations");
    for direction in ConnectionDirection::iter() {
        for operation in RequestSubquota::iter() {
            let held = transport.pending_operations[direction.index()][operation.index()];
            pending.sample(
                &[
                    ("direction", direction.as_ref()),
                    ("operation", operation.as_ref()),
                ],
                held.approx_into(),
            );
        }
    }

    let mut channels = families.open("nervix_interconnect_relay_channels");
    channels.sample(&[], transport.relay_channels.approx_into());
    let mut attempts = families.open("nervix_interconnect_relay_attempts");
    attempts.sample(&[], transport.relay_attempts.approx_into());
    let mut grants = families.open("nervix_interconnect_relay_grants");
    grants.sample(&[], transport.relay_grants.approx_into());
    let mut outcome_age = families.open("nervix_interconnect_unresolved_outcome_age_seconds");
    outcome_age.sample(&[], transport.oldest_unresolved_outcome.as_secs_f64());

    let counters = &transport.counters;
    let mut established = families.open("nervix_interconnect_connections_established_total");
    for class in PoolClass::ALL {
        let count = counters.connections_established[class.index()];
        established.sample(&[("class", class.as_ref())], count.approx_into());
    }

    let mut failures = families.open("nervix_interconnect_connection_failures_total");
    for class in PoolClass::ALL {
        for reason in ConnectionFailureReason::iter() {
            let count = counters.connection_failures[class.index()][reason.index()];
            failures.sample(
                &[("class", class.as_ref()), ("reason", reason.as_ref())],
                count.approx_into(),
            );
        }
    }

    let mut resets = families.open("nervix_interconnect_stream_resets_total");
    for class in PoolClass::ALL {
        for reason in StreamResetReason::iter() {
            let count = counters.stream_resets[class.index()][reason.index()];
            resets.sample(
                &[("class", class.as_ref()), ("reason", reason.as_ref())],
                count.approx_into(),
            );
        }
    }

    let mut quota_failures = families.open("nervix_interconnect_quota_failures_total");
    for direction in ConnectionDirection::iter() {
        for operation in RequestSubquota::iter() {
            let count = counters.quota_failures[direction.index()][operation.index()];
            quota_failures.sample(
                &[
                    ("direction", direction.as_ref()),
                    ("operation", operation.as_ref()),
                ],
                count.approx_into(),
            );
        }
    }

    let mut requests = families.open("nervix_interconnect_requests_total");
    for outcome in RequestOutcome::iter() {
        for operation in RequestSubquota::iter() {
            let count = counters.requests[outcome.index()][operation.index()];
            requests.sample(
                &[
                    ("operation", operation.as_ref()),
                    ("outcome", outcome.as_ref()),
                ],
                count.approx_into(),
            );
        }
    }

    let mut request_time = families.open("nervix_interconnect_request_seconds_total");
    for operation in RequestSubquota::iter() {
        let elapsed = counters.request_time[operation.index()];
        request_time.sample(&[("operation", operation.as_ref())], elapsed.as_secs_f64());
    }

    let mut admissions = families.open("nervix_interconnect_relay_admissions_total");
    for outcome in RelayAdmissionOutcome::iter() {
        let count = counters.relay_outcomes[outcome.index()];
        admissions.sample(&[("outcome", outcome.as_ref())], count.approx_into());
    }

    let mut admission_wait =
        families.open("nervix_interconnect_relay_admission_wait_seconds_total");
    admission_wait.sample(&[], counters.relay_admission_wait.as_secs_f64());

    let mut bulk = families.open("nervix_interconnect_bulk_bytes_total");
    for class in PoolClass::ALL {
        for direction in TransferDirection::iter() {
            let bytes = counters.bulk_bytes[class.index()][direction.index()];
            bulk.sample(
                &[("class", class.as_ref()), ("direction", direction.as_ref())],
                bytes.approx_into(),
            );
        }
    }
}

/// One execution class and the snapshot the executor reports for it.
struct MemoryClassSample {
    class: &'static str,
    budget: MemoryBudgetSnapshot,
}

/// One worker class and the snapshot the executor reports for it.
struct WorkerClassSample {
    class: &'static str,
    workers: WorkerClassSnapshot,
}

fn execution_families(families: &mut Families, executor: &nervix_execution::ExecutorSnapshot) {
    let memory = [
        MemoryClassSample {
            class: "management",
            budget: executor.management_memory,
        },
        MemoryClassSample {
            class: "commands",
            budget: executor.commands_memory,
        },
        MemoryClassSample {
            class: "relay",
            budget: executor.relay_memory,
        },
        MemoryClassSample {
            class: "bulk",
            budget: executor.bulk_memory,
        },
    ];
    let workers = [
        WorkerClassSample {
            class: "control_cpu",
            workers: executor.control_cpu,
        },
        WorkerClassSample {
            class: "data_cpu",
            workers: executor.data_cpu,
        },
        WorkerClassSample {
            class: "bulk_cpu",
            workers: executor.bulk_cpu,
        },
        WorkerClassSample {
            class: "consensus_storage",
            workers: executor.consensus_storage,
        },
        WorkerClassSample {
            class: "filesystem_storage",
            workers: executor.filesystem_storage,
        },
    ];

    let mut capacity = families.open("nervix_execution_memory_capacity_bytes");
    for entry in &memory {
        capacity.sample(
            &[("class", entry.class)],
            entry.budget.capacity_bytes.approx_into(),
        );
    }
    let mut reserved = families.open("nervix_execution_memory_reserved_bytes");
    for entry in &memory {
        reserved.sample(
            &[("class", entry.class)],
            entry.budget.reserved_bytes.approx_into(),
        );
    }
    let mut reservations = families.open("nervix_execution_memory_reservations_total");
    for entry in &memory {
        reservations.sample(
            &[("class", entry.class)],
            entry.budget.granted.approx_into(),
        );
    }
    let mut rejections = families.open("nervix_execution_memory_rejections_total");
    for entry in &memory {
        rejections.sample(
            &[("class", entry.class)],
            entry.budget.refused.approx_into(),
        );
    }

    let mut worker_count = families.open("nervix_execution_workers");
    for entry in &workers {
        worker_count.sample(
            &[("class", entry.class)],
            entry.workers.workers.approx_into(),
        );
    }
    let mut running = families.open("nervix_execution_jobs_running");
    for entry in &workers {
        running.sample(
            &[("class", entry.class)],
            entry.workers.running.approx_into(),
        );
    }
    let mut pending = families.open("nervix_execution_jobs_pending");
    for entry in &workers {
        pending.sample(
            &[("class", entry.class)],
            entry.workers.pending.approx_into(),
        );
    }
    let mut admitted = families.open("nervix_execution_jobs_total");
    for entry in &workers {
        admitted.sample(
            &[("class", entry.class)],
            entry.workers.admitted.approx_into(),
        );
    }
    let mut completed = families.open("nervix_execution_jobs_completed_total");
    for entry in &workers {
        completed.sample(
            &[("class", entry.class)],
            entry.workers.completed.approx_into(),
        );
    }
    let mut refused = families.open("nervix_execution_job_rejections_total");
    for entry in &workers {
        refused.sample(
            &[("class", entry.class)],
            entry.workers.refused.approx_into(),
        );
    }
    let mut queued = families.open("nervix_execution_job_queue_seconds_total");
    for entry in &workers {
        queued.sample(
            &[("class", entry.class)],
            entry.workers.queued.as_secs_f64(),
        );
    }
    let mut worked = families.open("nervix_execution_job_work_seconds_total");
    for entry in &workers {
        worked.sample(
            &[("class", entry.class)],
            entry.workers.worked.as_secs_f64(),
        );
    }
}

fn scheduler_families(families: &mut Families, scheduler: &SchedulerDelaySnapshot) {
    let mut total = families.open("nervix_node_scheduler_delay_seconds_total");
    total.sample(&[], scheduler.total.as_secs_f64());
    let mut peak = families.open("nervix_node_scheduler_delay_peak_seconds");
    peak.sample(&[], scheduler.peak.as_secs_f64());
    let mut samples = families.open("nervix_node_scheduler_samples_total");
    samples.sample(&[], scheduler.observations.approx_into());
}

fn consensus_families(
    families: &mut Families,
    log: &nervix_consensus::RaftLogRetention,
    snapshots: &nervix_consensus::SnapshotRetention,
) {
    let mut last_index = families.open("nervix_consensus_log_last_index");
    last_index.sample(&[], optional_index(log.last_log_index));
    let mut snapshot_index = families.open("nervix_consensus_log_snapshot_index");
    snapshot_index.sample(&[], optional_index(log.snapshot_index));
    let mut purged_index = families.open("nervix_consensus_log_purged_index");
    purged_index.sample(&[], optional_index(log.purged_index));
    let mut retained = families.open("nervix_consensus_log_retained_bytes");
    retained.sample(&[], log.retained_bytes.approx_into());
    let mut pinned = families.open("nervix_consensus_snapshot_pinned_generations");
    pinned.sample(&[], snapshots.pinned_generations.approx_into());
    let mut readers = families.open("nervix_consensus_snapshot_pinned_readers");
    readers.sample(&[], snapshots.pinned_readers.approx_into());
    let mut unreferenced = families.open("nervix_consensus_snapshot_unreferenced_generations");
    unreferenced.sample(&[], snapshots.unreferenced_generations.approx_into());
}

/// A Raft position that does not exist yet reports as `-1`, which no real index can take, rather
/// than as a zero that would read as a position the node actually holds.
fn optional_index(index: Option<u64>) -> f64 {
    match index {
        Some(index) => index.approx_into(),
        None => -1.0,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nervix_consensus::{RaftLogRetention, SnapshotRetention};
    use nervix_execution::Executor;
    use nervix_interconnect::{TransportCounters, TransportSnapshot};
    use strum::EnumCount as _;

    use super::*;

    fn idle_transport() -> TransportSnapshot {
        TransportSnapshot {
            counters: TransportCounters {
                connections_established: [0; PoolClass::COUNT],
                connection_failures: [[0; ConnectionFailureReason::COUNT]; PoolClass::COUNT],
                stream_resets: [[0; StreamResetReason::COUNT]; PoolClass::COUNT],
                quota_failures: [[0; RequestSubquota::COUNT]; ConnectionDirection::COUNT],
                requests: [[0; RequestSubquota::COUNT]; RequestOutcome::COUNT],
                request_time: [Duration::ZERO; RequestSubquota::COUNT],
                relay_outcomes: [0; RelayAdmissionOutcome::COUNT],
                relay_admission_wait: Duration::ZERO,
                bulk_bytes: [[0; TransferDirection::COUNT]; PoolClass::COUNT],
            },
            connections: [[0; PoolClass::COUNT]; ConnectionDirection::COUNT],
            leased_streams: [0; PoolClass::COUNT],
            pending_operations: [[0; RequestSubquota::COUNT]; ConnectionDirection::COUNT],
            relay_channels: 0,
            relay_attempts: 0,
            relay_grants: 0,
            oldest_unresolved_outcome: Duration::ZERO,
        }
    }

    fn idle_families() -> Vec<MetricFamily> {
        let executor = Executor::default().snapshot();
        let log = RaftLogRetention {
            purged_index: None,
            snapshot_index: None,
            last_log_index: None,
            retained_bytes: 0,
        };
        let snapshots = SnapshotRetention {
            active_generation: None,
            pinned_generations: 0,
            pinned_readers: 0,
            obsolete_generations: 0,
            unreferenced_generations: 0,
        };
        let scheduler = SchedulerDelaySnapshot {
            observations: 0,
            total: Duration::ZERO,
            peak: Duration::ZERO,
        };
        let mut families = Families::new();
        transport_families(&mut families, &idle_transport());
        execution_families(&mut families, &executor);
        scheduler_families(&mut families, &scheduler);
        consensus_families(&mut families, &log, &snapshots);
        families.finish()
    }

    #[test]
    fn every_declared_series_has_a_distinct_name() {
        let mut names = BTreeSet::new();
        for series in SERIES {
            assert!(
                names.insert(series.name),
                "series '{}' is declared twice",
                series.name
            );
        }
    }

    #[test]
    fn produces_every_declared_series_once() {
        let families = idle_families();
        let produced: Vec<&str> = families.iter().map(|family| family.name()).collect();
        let declared: Vec<&str> = SERIES.iter().map(|series| series.name).collect();
        assert_eq!(produced, declared);
    }

    #[test]
    fn every_sample_carries_exactly_its_declared_labels() {
        for family in idle_families() {
            let spec = SERIES
                .iter()
                .find(|series| series.name == family.name())
                .expect("every produced family is declared");
            for metric in family.get_metric() {
                let carried: Vec<&str> = metric
                    .get_label()
                    .iter()
                    .map(|label| label.name())
                    .collect();
                assert_eq!(
                    carried, spec.labels,
                    "series '{}' produced a sample with unexpected labels",
                    spec.name
                );
            }
        }
    }

    #[test]
    fn an_uninstalled_collector_reports_nothing() {
        let collector = InterconnectionCollector::new();
        assert_eq!(collector.desc().len(), SERIES.len());
        assert!(collector.collect().is_empty());
    }
}
