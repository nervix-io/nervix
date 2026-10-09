//! Sampling every node's public executor memory gauges while a CLI backup or restore runs.
//!
//! Layer: test harness.
//! - **Owns.** The sampler, the sampled per-node peaks, capacities and refusal counts, and the
//!   measurement evidence a scenario prints.
//! - **Depends on.** Each node's public metrics endpoint and the harness allocator's statistics.
//! - **Must not know.** How the server reserves or releases memory.

use nervix_primitives::task::AbortOnDropHandle;

use super::*;

/// How often a sampler reads every node's gauges.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(20);

/// Reads every node's bulk and restore-metadata executor gauges, and the harness process's heap,
/// from before an operation starts until it is finished.
pub(crate) struct MemorySampler {
    stop: CancellationToken,
    sampler: AbortOnDropHandle<SampledMemory>,
}

/// What a sampler observed. Peaks are sampled observations, not continuous maxima; process heap
/// figures include every in-process node, fixture and harness allocation.
pub(crate) struct SampledMemory {
    pub(crate) bulk_peaks: BTreeMap<String, f64>,
    pub(crate) bulk_capacities: BTreeMap<String, f64>,
    pub(crate) metadata_peaks: BTreeMap<String, f64>,
    /// When each node was first and last sampled holding restore-metadata memory.
    metadata_held: BTreeMap<String, HeldSpan>,
    baseline_refusals: BTreeMap<String, f64>,
    final_refusals: BTreeMap<String, f64>,
    initial_heap: u64,
    peak_heap: u64,
    initial_resident: u64,
    peak_resident: u64,
    samples: u64,
}

/// The first and the last sample at which a node held memory of one class.
#[derive(Clone, Copy)]
struct HeldSpan {
    first: Instant,
    last: Instant,
}

impl MemorySampler {
    /// Starts sampling every node of the scenario's cluster, and returns once the first sample of
    /// every node is taken.
    pub(crate) async fn start(world: &ScenarioWorld) -> Self {
        let mut urls = Vec::new();
        for node in world.cluster().node_ids() {
            let url = world
                .cluster()
                .observability_metrics_url(&node)
                .assured("a measured node exposes metrics");
            urls.push((node, url));
        }
        let (ready, started) = nervix_primitives::sync::oneshot::channel();
        let stop = CancellationToken::new();
        let sampled_stop = stop.clone();
        let initial_usage = nervix_server::memory_pressure::jemalloc_memory_usage()
            .assured("the harness allocator exposes statistics");
        let sampler = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
            let client = reqwest::Client::new();
            let mut sampled = SampledMemory {
                bulk_peaks: BTreeMap::new(),
                bulk_capacities: BTreeMap::new(),
                metadata_peaks: BTreeMap::new(),
                metadata_held: BTreeMap::new(),
                baseline_refusals: BTreeMap::new(),
                final_refusals: BTreeMap::new(),
                initial_heap: initial_usage.allocated,
                peak_heap: initial_usage.allocated,
                initial_resident: initial_usage.resident,
                peak_resident: initial_usage.resident,
                samples: 0,
            };
            let mut ready = Some(ready);
            loop {
                nervix_primitives::task::consume_budget().await;
                for (node, url) in &urls {
                    nervix_primitives::task::consume_budget().await;
                    let response = client
                        .get(url)
                        .send()
                        .await
                        .assured("a measured node's metrics remain reachable");
                    let body = response.text().await.assured("metrics decode as text");
                    sampled.record(node, &body);
                }
                let usage = nervix_server::memory_pressure::jemalloc_memory_usage()
                    .assured("allocator statistics remain available");
                sampled.peak_heap = sampled.peak_heap.max(usage.allocated);
                sampled.peak_resident = sampled.peak_resident.max(usage.resident);
                sampled.samples = sampled
                    .samples
                    .checked_add(1)
                    .assured("one operation has a bounded sample count");
                if let Some(sender) = ready.take() {
                    sender
                        .send(())
                        .assured("the measurement caller waits for its baseline");
                }
                nervix_primitives::select! {
                    () = sampled_stop.cancelled() => break,
                    () = nervix_primitives::time::sleep(SAMPLE_INTERVAL) => {}
                }
            }
            sampled
        }));
        started
            .await
            .assured("every measured node was sampled before the operation starts");
        Self { stop, sampler }
    }

    /// Stops sampling and returns what was observed.
    pub(crate) async fn finish(self) -> SampledMemory {
        self.stop.cancel();
        self.sampler.await.assured("the memory sampler finishes")
    }
}

impl SampledMemory {
    /// Records one node's bulk and restore-metadata gauges from its metrics `body`.
    fn record(&mut self, node: &str, body: &str) {
        for line in body.lines() {
            let metadata = line.contains("class=\"restore_metadata\"");
            if !line.contains("class=\"bulk\"") && !metadata {
                continue;
            }
            let value = line
                .split_whitespace()
                .last()
                .assured("a metric sample contains its value")
                .parse::<f64>()
                .assured("the metric value is numeric");
            if line.starts_with("nervix_execution_memory_reserved_bytes{") {
                let peaks = if metadata {
                    &mut self.metadata_peaks
                } else {
                    &mut self.bulk_peaks
                };
                let peak = peaks.entry(node.to_string()).or_default();
                *peak = peak.max(value);
                if metadata && value > 0.0 {
                    let now = Instant::now();
                    let span = self
                        .metadata_held
                        .entry(node.to_string())
                        .or_insert(HeldSpan {
                            first: now,
                            last: now,
                        });
                    span.last = now;
                }
            } else if !metadata && line.starts_with("nervix_execution_memory_capacity_bytes{") {
                self.bulk_capacities.insert(node.to_string(), value);
            } else if !metadata && line.starts_with("nervix_execution_memory_rejections_total{") {
                self.baseline_refusals
                    .entry(node.to_string())
                    .or_insert(value);
                self.final_refusals.insert(node.to_string(), value);
            }
        }
    }

    /// Asserts that no node's bulk class refused a reservation while the operation ran.
    pub(crate) fn assert_no_bulk_refusal(&self, operation: &str) {
        assert_eq!(self.baseline_refusals, self.final_refusals, "{operation}");
    }

    /// Asserts that no node's sampled bulk reservations exceeded that node's bulk budget.
    pub(crate) fn assert_bulk_within_budget(&self) {
        assert!(!self.bulk_peaks.is_empty(), "every node was sampled");
        for (node, peak) in &self.bulk_peaks {
            let capacity = self
                .bulk_capacities
                .get(node)
                .assured("every sampled node reports its bulk budget");
            assert!(
                peak <= capacity,
                "node '{node}' reserved {peak} bulk bytes of its {capacity}-byte budget"
            );
        }
    }

    /// The measurement evidence of an operation over an archive of `archive_bytes` that took
    /// `elapsed`.
    pub(crate) fn evidence(
        &self,
        world: &ScenarioWorld,
        archive_bytes: u64,
        elapsed: Duration,
    ) -> serde_json::Value {
        let mut metadata_held_milliseconds = BTreeMap::new();
        for (node, span) in &self.metadata_held {
            metadata_held_milliseconds.insert(
                node.clone(),
                span.last.duration_since(span.first).as_millis(),
            );
        }
        serde_json::json!({
            "test_id": world.test_id, "nodes": self.bulk_peaks.len(), "archive_bytes": archive_bytes,
            "milliseconds": elapsed.as_millis(), "sample_interval_milliseconds": SAMPLE_INTERVAL.as_millis(), "samples": self.samples,
            "sampled_bulk_peaks_bytes": self.bulk_peaks, "bulk_budget_bytes": self.bulk_capacities,
            "sampled_restore_metadata_peaks_bytes": self.metadata_peaks,
            "sampled_restore_metadata_held_milliseconds": metadata_held_milliseconds,
            "harness_heap_before_bytes": self.initial_heap, "harness_heap_peak_bytes": self.peak_heap,
            "harness_allocator_resident_before_bytes": self.initial_resident, "harness_allocator_resident_peak_bytes": self.peak_resident,
            "heap_scope": "every in-process cluster and harness allocation; allocator resident excludes mappings outside jemalloc"
        })
    }
}
