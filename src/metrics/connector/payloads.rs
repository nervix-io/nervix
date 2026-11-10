//! Emitter payload preparation and owner-balanced work observations.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Preparation families, resolved children and each work owner's gauge contribution.
//! - **Depends on.** The retained interaction handle, published message buckets and Prometheus.
//! - **Must not know.** Payload values, connector drivers, delivery decisions or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the infrastructure boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry,
};
use strum::{EnumIter, IntoEnumIterator};

use super::{EMITTER_LABELS, EmitterInteractionMetrics};
use crate::metrics::MESSAGE_BATCH_BUCKETS;

// The metric library stores unsigned quantities without narrowing staged row or byte counts.
type WorkGauge = prometheus::core::GenericGauge<prometheus::core::AtomicU64>;
type WorkGaugeVec = prometheus::core::GenericGaugeVec<prometheus::core::AtomicU64>;

#[derive(Debug)]
pub(super) struct PayloadFamilies {
    payloads: IntCounterVec,
    source_messages: IntCounterVec,
    encoded_bytes: IntCounterVec,
    size_bound_bytes: IntCounterVec,
    prepared_bytes: IntCounterVec,
    splits: IntCounterVec,
    messages: HistogramVec,
    work: WorkGaugeVec,
    retained_payloads: WorkGaugeVec,
    retained_bytes: WorkGaugeVec,
}

impl PayloadFamilies {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "registers distinct payload families at node startup"
        )
    )]
    pub(super) fn register(registry: &Registry) -> Self {
        let mut work_labels = EMITTER_LABELS.to_vec();
        work_labels.push("state");
        let messages = HistogramVec::new(
            HistogramOpts::new(
                "nervix_emitter_messages_per_payload",
                "Source messages carried by each newly prepared logical payload.",
            )
            .buckets(MESSAGE_BATCH_BUCKETS.to_vec()),
            &EMITTER_LABELS,
        )
        .assured("the published message buckets and fixed metric identifiers are valid");
        registry
            .register(Box::new(messages.clone()))
            .assured("the node registers this distinct payload histogram once");
        Self {
            payloads: Self::counter(
                registry,
                "nervix_emitter_payloads_total",
                "Newly prepared logical payloads, independent of their delivery outcome.",
            ),
            source_messages: Self::counter(
                registry,
                "nervix_emitter_payload_source_messages_total",
                "Source message memberships in newly prepared logical payloads.",
            ),
            encoded_bytes: Self::counter(
                registry,
                "nervix_emitter_payload_encoded_bytes_total",
                "Encoded bytes at the documented payload size measurement boundary.",
            ),
            size_bound_bytes: Self::counter(
                registry,
                "nervix_emitter_payload_size_bound_bytes_total",
                "Exact encoded size bounds of newly prepared logical payloads.",
            ),
            prepared_bytes: Self::counter(
                registry,
                "nervix_emitter_prepared_bytes_total",
                "Prepared content bytes after connector preparation, before driver framing.",
            ),
            splits: Self::counter(
                registry,
                "nervix_emitter_payload_splits_total",
                "Candidate subdivisions caused by an encoded size limit.",
            ),
            messages,
            work: Self::gauge(
                registry,
                "nervix_emitter_work_messages",
                "Current source messages by overlapping buffer, preparation, write or staging \
                 state.",
                &work_labels,
            ),
            retained_payloads: Self::gauge(
                registry,
                "nervix_emitter_prepared_payloads",
                "Prepared payloads whose content and membership the host retains.",
                &EMITTER_LABELS,
            ),
            retained_bytes: Self::gauge(
                registry,
                "nervix_emitter_prepared_bytes",
                "Prepared content bytes the host retains for a destination answer.",
                &EMITTER_LABELS,
            ),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "registers a distinct fixed payload counter family"
        )
    )]
    fn counter(registry: &Registry, name: &str, help: &str) -> IntCounterVec {
        let family = IntCounterVec::new(Opts::new(name, help), &EMITTER_LABELS)
            .assured("fixed payload family and identity label names satisfy Prometheus rules");
        registry
            .register(Box::new(family.clone()))
            .assured("the node registers each distinct payload family exactly once");
        family
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "registers a distinct fixed current-work gauge family"
        )
    )]
    fn gauge(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> WorkGaugeVec {
        let family = WorkGaugeVec::new(Opts::new(name, help), labels)
            .assured("fixed work family and bounded label names satisfy Prometheus rules");
        registry
            .register(Box::new(family.clone()))
            .assured("the node registers each distinct work family exactly once");
        family
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves all payload children for one starting emitter"
        )
    )]
    pub(super) fn emitter(&self, values: &[&str; 6]) -> PayloadMetrics {
        PayloadMetrics {
            payloads: self.payloads.with_label_values(values),
            source_messages: self.source_messages.with_label_values(values),
            encoded_bytes: self.encoded_bytes.with_label_values(values),
            size_bound_bytes: self.size_bound_bytes.with_label_values(values),
            prepared_bytes: self.prepared_bytes.with_label_values(values),
            splits: self.splits.with_label_values(values),
            messages: self.messages.with_label_values(values),
            buffered: self.work_child(values, "buffered"),
            prepared: self.work_child(values, "prepared"),
            in_flight: self.work_child(values, "in_flight"),
            staged: self.work_child(values, "staged"),
            retained_payloads: self.retained_payloads.with_label_values(values),
            retained_bytes: self.retained_bytes.with_label_values(values),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves one of four fixed source-work states")
    )]
    fn work_child(&self, values: &[&str; 6], state: &str) -> WorkGauge {
        let mut labels = values.to_vec();
        labels.push(state);
        self.work.with_label_values(&labels)
    }
}

#[derive(Debug)]
pub(super) struct PayloadMetrics {
    payloads: IntCounter,
    source_messages: IntCounter,
    encoded_bytes: IntCounter,
    size_bound_bytes: IntCounter,
    prepared_bytes: IntCounter,
    splits: IntCounter,
    messages: Histogram,
    buffered: WorkGauge,
    prepared: WorkGauge,
    in_flight: WorkGauge,
    staged: WorkGauge,
    retained_payloads: WorkGauge,
    retained_bytes: WorkGauge,
}

impl PayloadMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "selects only a retained current-work child")
    )]
    fn gauge(&self, kind: EmitterWorkKind) -> &WorkGauge {
        match kind {
            EmitterWorkKind::Buffered => &self.buffered,
            EmitterWorkKind::Prepared => &self.prepared,
            EmitterWorkKind::InFlight => &self.in_flight,
            EmitterWorkKind::Staged => &self.staged,
            EmitterWorkKind::Payloads => &self.retained_payloads,
            EmitterWorkKind::Bytes => &self.retained_bytes,
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(observer, reason = "renders retained payload and work observations")
    )]
    pub(super) fn describe_lines(&self, selector: &str, lines: &mut Vec<String>) {
        lines.push(format!(
            "payloads: {} source_messages={} encoded_bytes={} size_bound_bytes={} \
             prepared_bytes={} {{{selector}}}",
            self.payloads.get(),
            self.source_messages.get(),
            self.encoded_bytes.get(),
            self.size_bound_bytes.get(),
            self.prepared_bytes.get(),
        ));
        lines.push(format!(
            "payload_splits: {} {{{selector}}}",
            self.splits.get()
        ));
        lines.push(format!(
            "messages_per_payload: count={} source_messages={} {{{selector}}}",
            self.messages.get_sample_count(),
            self.messages.get_sample_sum(),
        ));
        lines.push(format!(
            "work: buffered={} prepared={} in_flight={} staged={} prepared_payloads={} \
             prepared_bytes={} {{{selector}}}",
            self.buffered.get(),
            self.prepared.get(),
            self.in_flight.get(),
            self.staged.get(),
            self.retained_payloads.get(),
            self.retained_bytes.get(),
        ));
    }
}

impl EmitterInteractionMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "records one preparation through pre-resolved children"
        )
    )]
    pub(crate) fn record_payload(
        &self,
        source_messages: u64,
        encoded_bytes: u64,
        prepared_bytes: u64,
    ) {
        let metrics = &self.series.payloads;
        metrics.payloads.inc();
        metrics.source_messages.inc_by(source_messages);
        metrics.encoded_bytes.inc_by(encoded_bytes);
        metrics.size_bound_bytes.inc_by(encoded_bytes);
        metrics.prepared_bytes.inc_by(prepared_bytes);
        metrics.messages.observe(source_messages.approx_into());
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "records the existing divider's size-driven subdivisions"
        )
    )]
    pub(crate) fn record_splits(&self, subdivisions: u64) {
        if subdivisions > 0 {
            self.series.payloads.splits.inc_by(subdivisions);
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "a work owner retains the existing interaction handle once"
        )
    )]
    pub(crate) fn work(&self) -> EmitterWork {
        EmitterWork {
            metrics: self.clone(),
            counts: WorkCounts::default(),
        }
    }
}

/// A gauge contribution belongs to one buffer, prepared store or write scope.
#[derive(Debug, Clone, Copy, EnumIter)]
pub(crate) enum EmitterWorkKind {
    Buffered,
    Prepared,
    InFlight,
    Staged,
    Payloads,
    Bytes,
}

#[derive(Debug, Default)]
struct WorkCounts {
    buffered: u64,
    prepared: u64,
    in_flight: u64,
    staged: u64,
    payloads: u64,
    bytes: u64,
}

impl WorkCounts {
    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "borrows one exclusively owned work count")
    )]
    fn value_mut(&mut self, kind: EmitterWorkKind) -> &mut u64 {
        match kind {
            EmitterWorkKind::Buffered => &mut self.buffered,
            EmitterWorkKind::Prepared => &mut self.prepared,
            EmitterWorkKind::InFlight => &mut self.in_flight,
            EmitterWorkKind::Staged => &mut self.staged,
            EmitterWorkKind::Payloads => &mut self.payloads,
            EmitterWorkKind::Bytes => &mut self.bytes,
        }
    }
}

/// A sole writer's contributions to process-shared gauges. Dropping it removes only its work.
#[derive(Debug)]
pub(crate) struct EmitterWork {
    metrics: EmitterInteractionMetrics,
    counts: WorkCounts,
}

impl EmitterWork {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "creates an independent contribution for a write or transfer"
        )
    )]
    pub(crate) fn scope(&self) -> Self {
        self.metrics.work()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "adds work this owner just retained")
    )]
    pub(crate) fn add(&mut self, kind: EmitterWorkKind, count: u64) {
        let current = *self.counts.value_mut(kind);
        let next = current
            .checked_add(count)
            .assured("the total counts rows or content this owner already holds");
        self.set(kind, next);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "subtracts only work this owner already retained")
    )]
    pub(crate) fn remove(&mut self, kind: EmitterWorkKind, count: u64) {
        let current = *self.counts.value_mut(kind);
        let next = current
            .checked_sub(count)
            .verified("work leaves the same owner that counted its retention");
        self.set(kind, next);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "withdraws every contribution of this one work scope"
        )
    )]
    pub(crate) fn clear(&mut self) {
        for kind in EmitterWorkKind::iter() {
            self.set(kind, 0);
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "updates only this owner's retained gauge contribution"
        )
    )]
    pub(crate) fn set(&mut self, kind: EmitterWorkKind, count: u64) {
        let previous = self.counts.value_mut(kind);
        let gauge = self.metrics.series.payloads.gauge(kind);
        if count > *previous {
            gauge.add(count - *previous);
        } else if count < *previous {
            gauge.sub(*previous - count);
        }
        *previous = count;
    }
}

impl Drop for EmitterWork {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "withdraws this owner's contributions when its scope ends"
        )
    )]
    fn drop(&mut self) {
        self.clear();
    }
}
