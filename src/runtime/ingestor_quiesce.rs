//! Ingestor quiescence and retained-input ownership.
//!
//! Layer: data plane.
//! - **Owns.** In-memory quiescence causes, bounded retained payloads and intake decisions.
//! - **Depends on.** Typed ingestor policy, observed input timestamps and runtime task handles.
//! - **Must not know.** NSPL parsing, consensus decisions or persisted payload state.

use nervix_connector::{IngestMetadataRow, RetainedIngestHeaders};

use super::*;

pub(super) const DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
pub(in crate::runtime) enum IngestorQuiesceCause {
    #[strum(serialize = "entity hold")]
    EntityHold,
    #[strum(serialize = "ownership handoff")]
    OwnershipHandoff,
    #[strum(serialize = "domain pause")]
    DomainPause,
    #[strum(serialize = "memory pressure")]
    MemoryPressure,
    #[strum(serialize = "shutdown")]
    Shutdown,
}

impl IngestorQuiesceCause {
    pub(super) fn as_str(self) -> &'static str {
        self.into()
    }

    /// Whether this cause stops new intake whatever the ingestor's `ON QUIESCE` mode declares.
    ///
    /// An ownership handoff and a graceful shutdown both complete the work an ingestor already
    /// admitted and admit nothing further: polling and endpoint admission stop, a payload already
    /// received still dispatches, and no quiesce buffer or drop policy acts on their behalf.
    fn stops_intake_regardless_of_mode(self) -> bool {
        match self {
            Self::OwnershipHandoff | Self::Shutdown => true,
            Self::EntityHold | Self::DomainPause | Self::MemoryPressure => false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct IngestorQuiesceReasons {
    entity_holds: usize,
    ownership_handoffs: usize,
    domain_pause: bool,
    memory_pressure: bool,
    shutdown: bool,
}

impl IngestorQuiesceReasons {
    fn active(&self) -> Option<IngestorQuiesceCause> {
        if self.shutdown {
            Some(IngestorQuiesceCause::Shutdown)
        } else if self.ownership_handoffs > 0 {
            Some(IngestorQuiesceCause::OwnershipHandoff)
        } else if self.memory_pressure {
            Some(IngestorQuiesceCause::MemoryPressure)
        } else if self.domain_pause {
            Some(IngestorQuiesceCause::DomainPause)
        } else if self.entity_holds > 0 {
            Some(IngestorQuiesceCause::EntityHold)
        } else {
            None
        }
    }

    fn engage(&mut self, cause: IngestorQuiesceCause) {
        match cause {
            IngestorQuiesceCause::EntityHold => {
                self.entity_holds = self.entity_holds.checked_add(1).assured(
                    "a quiesce reason is held once per live entity hold, which is bounded by the \
                     entities resident on this node",
                );
            }
            IngestorQuiesceCause::OwnershipHandoff => {
                self.ownership_handoffs = self.ownership_handoffs.checked_add(1).assured(
                    "a quiesce reason is held once per live ownership handoff, which is bounded \
                     by the entities resident on this node",
                );
            }
            IngestorQuiesceCause::DomainPause => self.domain_pause = true,
            IngestorQuiesceCause::MemoryPressure => self.memory_pressure = true,
            IngestorQuiesceCause::Shutdown => self.shutdown = true,
        }
    }

    fn release(&mut self, cause: IngestorQuiesceCause) {
        match cause {
            IngestorQuiesceCause::EntityHold => {
                self.entity_holds = self.entity_holds.checked_sub(1).verified(
                    "the entity gate hold that engaged this control is released once, by the one \
                     caller that took it out of the hold map",
                );
            }
            IngestorQuiesceCause::OwnershipHandoff => {
                self.ownership_handoffs = self.ownership_handoffs.checked_sub(1).verified(
                    "the entity gate hold that engaged this control is released once, by the one \
                     caller that took it out of the hold map",
                );
            }
            IngestorQuiesceCause::DomainPause => self.domain_pause = false,
            IngestorQuiesceCause::MemoryPressure => self.memory_pressure = false,
            IngestorQuiesceCause::Shutdown => self.shutdown = false,
        }
    }
}

#[derive(Debug, Clone)]
struct IngestorQuiesceModes {
    active: IngestQuiesceMode,
    pending: Option<IngestQuiesceMode>,
    active_supported_by_source: bool,
}

/// How a quiesced ingestor treats its source and the payloads that still reach it.
///
/// Each variant settles polling, endpoint admission and intake together, so those verdicts cannot
/// disagree and reading any of them is a match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestorQuiesceHandling {
    /// The source stops taking payloads, and a payload it already received still dispatches.
    Suspend,
    /// The source stops taking payloads, and a payload that still arrives is shed: an endpoint
    /// rejects it and any other source drops it.
    SuspendAndShed,
    /// Polls stop and every payload that arrives is shed, an endpoint rejecting it with
    /// `retry_after`.
    Shed { retry_after: Option<Duration> },
    /// Every payload that arrives is dropped.
    Drop,
    /// Every payload that arrives is rejected with `retry_after`.
    Reject { retry_after: Option<Duration> },
    /// Payloads are retained per instance within `max_size` bytes, applying `overflow` when full.
    Buffer {
        max_size: usize,
        overflow: IngestQuiesceOverflow,
    },
    /// Endpoint payloads are retained per instance within `max_size` bytes and rejected once full.
    EndpointBuffer { max_size: usize },
}

impl IngestorQuiesceHandling {
    fn new(cause: IngestorQuiesceCause, modes: &IngestorQuiesceModes) -> Self {
        if cause.stops_intake_regardless_of_mode() {
            return Self::Suspend;
        }
        if !modes.active_supported_by_source {
            return Self::SuspendAndShed;
        }
        if cause == IngestorQuiesceCause::MemoryPressure {
            return Self::under_memory_pressure(&modes.active);
        }
        Self::declared_by(&modes.active)
    }

    /// Memory pressure never adds buffered bytes. A suspended source stays suspended, and every
    /// other mode sheds what arrives, keeping the retry delay a `REJECT` declares.
    fn under_memory_pressure(active: &IngestQuiesceMode) -> Self {
        match active {
            IngestQuiesceMode::Suspend => Self::SuspendAndShed,
            IngestQuiesceMode::Reject { retry_after } => Self::Shed {
                retry_after: quiesce_retry_after(retry_after),
            },
            IngestQuiesceMode::Buffer { .. }
            | IngestQuiesceMode::Drop
            | IngestQuiesceMode::EndpointBuffer { .. } => Self::Shed { retry_after: None },
        }
    }

    /// The handling an entity hold or a domain pause takes from a mode the source honors.
    fn declared_by(active: &IngestQuiesceMode) -> Self {
        match active {
            IngestQuiesceMode::Suspend => Self::Suspend,
            IngestQuiesceMode::Drop => Self::Drop,
            IngestQuiesceMode::Reject { retry_after } => Self::Reject {
                retry_after: quiesce_retry_after(retry_after),
            },
            IngestQuiesceMode::Buffer { max_size, overflow } => Self::Buffer {
                max_size: quiesce_max_size_bytes(max_size),
                overflow: *overflow,
            },
            IngestQuiesceMode::EndpointBuffer { max_size } => Self::EndpointBuffer {
                max_size: quiesce_max_size_bytes(max_size),
            },
        }
    }

    fn suspends_intake(self) -> bool {
        match self {
            Self::Suspend | Self::SuspendAndShed => true,
            Self::Shed { .. }
            | Self::Drop
            | Self::Reject { .. }
            | Self::Buffer { .. }
            | Self::EndpointBuffer { .. } => false,
        }
    }

    fn skips_poll(self) -> bool {
        match self {
            Self::Suspend | Self::SuspendAndShed | Self::Shed { .. } => true,
            Self::Drop
            | Self::Reject { .. }
            | Self::Buffer { .. }
            | Self::EndpointBuffer { .. } => false,
        }
    }

    /// The delay a rejected payload or admission tells its sender to wait before retrying.
    fn retry_after(self) -> Option<Duration> {
        match self {
            Self::Shed { retry_after } | Self::Reject { retry_after } => retry_after,
            Self::Suspend
            | Self::SuspendAndShed
            | Self::Drop
            | Self::Buffer { .. }
            | Self::EndpointBuffer { .. } => None,
        }
    }
}

/// What intake does under one publication of an ingestor's quiesce reasons and modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestorQuiesceDecision {
    /// No reason is engaged: sources poll, endpoints admit and every payload dispatches.
    Open,
    Quiesced {
        /// The engaged reason that takes precedence, which `DESCRIBE INGESTOR` reports.
        cause: IngestorQuiesceCause,
        handling: IngestorQuiesceHandling,
    },
}

impl IngestorQuiesceDecision {
    fn new(reasons: IngestorQuiesceReasons, modes: &IngestorQuiesceModes) -> Self {
        let Some(cause) = reasons.active() else {
            return Self::Open;
        };
        let handling = IngestorQuiesceHandling::new(cause, modes);
        Self::Quiesced { cause, handling }
    }

    fn cause(self) -> Option<IngestorQuiesceCause> {
        match self {
            Self::Open => None,
            Self::Quiesced { cause, .. } => Some(cause),
        }
    }

    fn suspends_intake(self) -> bool {
        match self {
            Self::Open => false,
            Self::Quiesced { handling, .. } => handling.suspends_intake(),
        }
    }

    fn skips_poll(self) -> bool {
        match self {
            Self::Open => false,
            Self::Quiesced { handling, .. } => handling.skips_poll(),
        }
    }
}

/// One ingestor's quiesce reasons and modes, with the decision they produce.
///
/// Every engagement, release and declared-mode change publishes a replacement whole, so a reader
/// never sees reasons, modes and a decision that came from different changes.
#[derive(Debug)]
struct IngestorQuiescePublication {
    reasons: IngestorQuiesceReasons,
    modes: IngestorQuiesceModes,
    /// Derived from `reasons` and `modes` when the publication is built. Every message, poll and
    /// endpoint admission reads it; deriving it there instead would resolve the engaged cause and
    /// parse the mode's retry delay and size bound once per message.
    decision: IngestorQuiesceDecision,
}

impl IngestorQuiescePublication {
    fn new(reasons: IngestorQuiesceReasons, modes: IngestorQuiesceModes) -> Self {
        let decision = IngestorQuiesceDecision::new(reasons, &modes);
        Self {
            reasons,
            modes,
            decision,
        }
    }

    fn engaging(&self, cause: IngestorQuiesceCause) -> Self {
        let mut reasons = self.reasons;
        reasons.engage(cause);
        Self::new(reasons, self.modes.clone())
    }

    /// Releasing the last engaged reason makes a mode declared during the hold active.
    fn releasing(&self, cause: IngestorQuiesceCause) -> Self {
        let mut reasons = self.reasons;
        reasons.release(cause);
        let mut modes = self.modes.clone();
        if reasons.active().is_none() {
            if let Some(pending) = modes.pending.take() {
                modes.active = pending;
            }
            modes.active_supported_by_source = true;
        }
        Self::new(reasons, modes)
    }

    /// While a reason is engaged, the mode in effect when the hold began keeps governing it and
    /// `declared` waits for the release.
    fn declaring(&self, declared: &IngestQuiesceMode, active_supported_by_source: bool) -> Self {
        let mut modes = self.modes.clone();
        if self.reasons.active().is_some() {
            modes.active_supported_by_source = active_supported_by_source;
            modes.pending = Some(declared.clone());
        } else {
            modes.active = declared.clone();
            modes.pending = None;
            modes.active_supported_by_source = true;
        }
        Self::new(self.reasons, modes)
    }
}

/// One buffered message's ingest metadata.
///
/// A quiesced ingestor replays its payloads after the source messages are gone, so the
/// buffer owns these values and appends them into the group's builders on replay.
#[derive(Debug, Clone)]
pub(in crate::runtime) enum BufferedIngestMetadata {
    Syslog { peer_addr: std::net::SocketAddr },
    Headers(RetainedIngestHeaders),
}

impl BufferedIngestMetadata {
    /// The metadata of a buffered message whose source carries no transport headers.
    #[cfg(test)]
    pub(in crate::runtime) fn without_headers() -> Self {
        Self::Headers(RetainedIngestHeaders::none())
    }

    pub(super) fn row(&self) -> IngestMetadataRow<'_> {
        match self {
            Self::Syslog { peer_addr } => IngestMetadataRow::Syslog {
                peer_addr: *peer_addr,
            },
            Self::Headers(headers) => IngestMetadataRow::Headers { headers },
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct BufferedIngestPayload {
    pub(super) payloads: Vec<Vec<u8>>,
    /// Row-aligned with `payloads`.
    pub(super) metadata: Vec<BufferedIngestMetadata>,
    observed_at: Timestamp,
}

impl BufferedIngestPayload {
    pub(in crate::runtime) fn new(
        payload: &[u8],
        metadata: BufferedIngestMetadata,
        observed_at: Timestamp,
    ) -> Self {
        Self {
            payloads: vec![payload.to_vec()],
            metadata: vec![metadata],
            observed_at,
        }
    }

    pub(in crate::runtime) fn batch(
        entries: Vec<(Vec<u8>, BufferedIngestMetadata)>,
        observed_at: Timestamp,
    ) -> Self {
        let (payloads, metadata) = entries.into_iter().unzip();
        Self {
            payloads,
            metadata,
            observed_at,
        }
    }

    pub(in crate::runtime) fn payload(&self) -> &[u8] {
        self.payloads.first().verified(
            "both constructors take at least one payload with its metadata, and the batching \
             caller skips an empty set",
        )
    }

    /// The number of source payloads this buffer holds, which is how many payloads the group it
    /// opens accepts.
    pub(in crate::runtime) fn len(&self) -> usize {
        self.payloads.len()
    }

    /// The metadata of the first payload, for bindings that ingest one payload per buffer.
    pub(super) fn first_metadata_row(&self) -> IngestMetadataRow<'_> {
        self.metadata
            .first()
            .verified(
                "both constructors take at least one payload with its metadata, and the batching \
                 caller skips an empty set",
            )
            .row()
    }

    pub(super) fn metadata_rows(&self) -> Vec<IngestMetadataRow<'_>> {
        self.metadata
            .iter()
            .map(BufferedIngestMetadata::row)
            .collect()
    }

    pub(super) fn payloads(&self) -> impl Iterator<Item = &[u8]> {
        self.payloads.iter().map(Vec::as_slice)
    }

    pub(super) const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    pub(super) fn byte_len(&self) -> usize {
        self.payloads.iter().map(Vec::len).sum()
    }
}

#[derive(Debug, Default)]
pub(super) struct IngestorQuiesceBuffer {
    pub(super) payloads: VecDeque<BufferedIngestPayload>,
    pub(super) bytes: usize,
}

impl IngestorQuiesceBuffer {
    /// The bytes this buffer may still admit before it reaches `max_size`.
    ///
    /// Admission is expressed as a remaining budget rather than a projected total so that a
    /// payload is never sized against a sum that could leave `usize`. Saturation is the meaning
    /// here: altering an ingestor may lower `max_size` under an already filled buffer, and such a
    /// buffer has no room left until it drains.
    pub(super) fn remaining_capacity(&self, max_size: usize) -> usize {
        max_size.saturating_sub(self.bytes)
    }

    pub(super) fn admit(&mut self, payload: BufferedIngestPayload, payload_bytes: usize) {
        self.bytes = self
            .bytes
            .checked_add(payload_bytes)
            .assured("both operands count bytes of payloads this node already holds in memory");
        self.payloads.push_back(payload);
    }

    pub(super) fn release(&mut self, payload_bytes: usize) {
        self.bytes = self
            .bytes
            .checked_sub(payload_bytes)
            .verified("the released payload's bytes were added when it was admitted");
    }
}

#[derive(Debug)]
pub(in crate::runtime) enum IngestorQuiesceIntake {
    Dispatch(BufferedIngestPayload),
    Buffered,
    Dropped,
    Rejected { retry_after: Option<Duration> },
}

#[derive(Debug)]
pub(in crate::runtime) struct IngestorQuiesceControl {
    /// Loaded without a lock by every message, poll and admission. Every change replaces it
    /// through `rcu`, deriving the replacement from the publication it replaces, so concurrent
    /// changes never overwrite one another.
    published: ArcSwap<IngestorQuiescePublication>,
    /// Intake reaches the retained payloads only under a buffering decision, and replay only while
    /// `buffered_records` counts some.
    pub(super) buffers: parking_lot::Mutex<HashMap<u64, IngestorQuiesceBuffer>>,
    pub(super) changed: Notify,
    /// Changes only while `buffers` is locked, so whenever that lock is free it counts exactly the
    /// payloads retained.
    pub(super) buffered_records: AtomicUsize,
    pub(super) buffered_bytes: AtomicUsize,
    pub(super) dropped_total: AtomicU64,
    pub(super) rejected_total: AtomicU64,
    pub(super) metrics: RuntimeMetrics,
    pub(super) metric_labels: IngestorQuiesceMetricLabels,
}

impl IngestorQuiesceControl {
    pub(super) fn new(
        mode: IngestQuiesceMode,
        metrics: RuntimeMetrics,
        metric_labels: IngestorQuiesceMetricLabels,
    ) -> Self {
        let modes = IngestorQuiesceModes {
            active: mode,
            pending: None,
            active_supported_by_source: true,
        };
        let publication = IngestorQuiescePublication::new(IngestorQuiesceReasons::default(), modes);
        Self {
            published: ArcSwap::from_pointee(publication),
            buffers: parking_lot::Mutex::new(HashMap::default()),
            changed: Notify::new(),
            buffered_records: AtomicUsize::new(0),
            buffered_bytes: AtomicUsize::new(0),
            dropped_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
            metrics,
            metric_labels,
        }
    }

    pub(super) fn sync_buffered_metrics(&self) {
        self.metrics.set_ingestor_quiesce_buffered(
            &self.metric_labels,
            self.buffered_records.load(Ordering::Relaxed),
            self.buffered_bytes.load(Ordering::Relaxed),
        );
    }

    pub(super) fn record_dropped(&self, count: u64) {
        self.dropped_total.fetch_add(count, Ordering::Relaxed);
        self.metrics
            .increment_ingestor_quiesce_dropped(&self.metric_labels, count);
    }

    pub(super) fn record_rejected(&self, count: u64) {
        self.rejected_total.fetch_add(count, Ordering::Relaxed);
        self.metrics
            .increment_ingestor_quiesce_rejected(&self.metric_labels, count);
    }

    pub(super) fn update_declared_mode(
        &self,
        declared: &IngestQuiesceMode,
        active_supported_by_source: bool,
    ) {
        self.published
            .rcu(|current| current.declaring(declared, active_supported_by_source));
        self.changed.notify_waiters();
    }

    pub(super) fn engage(&self, cause: IngestorQuiesceCause) {
        self.published.rcu(|current| current.engaging(cause));
        self.changed.notify_waiters();
    }

    pub(super) fn release(&self, cause: IngestorQuiesceCause) {
        self.published.rcu(|current| current.releasing(cause));
        self.changed.notify_waiters();
    }

    fn decision(&self) -> IngestorQuiesceDecision {
        self.published.load().decision
    }

    pub(in crate::runtime) fn cause(&self) -> Option<IngestorQuiesceCause> {
        self.decision().cause()
    }

    pub(in crate::runtime) fn is_quiesced(&self) -> bool {
        self.cause().is_some()
    }

    fn mode(&self) -> IngestQuiesceMode {
        self.published.load().modes.active.clone()
    }

    pub(in crate::runtime) fn should_suspend_intake(&self) -> bool {
        self.decision().suspends_intake()
    }

    pub(in crate::runtime) fn should_skip_poll(&self) -> bool {
        self.decision().skips_poll()
    }

    pub(in crate::runtime) async fn wait_until_not_suspended(&self) {
        loop {
            if !self.should_suspend_intake() {
                return;
            }
            let changed = self.changed.notified();
            if !self.should_suspend_intake() {
                return;
            }
            changed.await;
        }
    }

    pub(in crate::runtime) async fn wait_for_change(&self) {
        self.changed.notified().await;
    }

    pub(in crate::runtime) fn intake(
        &self,
        instance: u64,
        payload: BufferedIngestPayload,
        endpoint: bool,
    ) -> IngestorQuiesceIntake {
        let IngestorQuiesceDecision::Quiesced { handling, .. } = self.decision() else {
            return IngestorQuiesceIntake::Dispatch(payload);
        };
        match handling {
            IngestorQuiesceHandling::Suspend => IngestorQuiesceIntake::Dispatch(payload),
            IngestorQuiesceHandling::SuspendAndShed | IngestorQuiesceHandling::Shed { .. } => {
                if endpoint {
                    self.record_rejected(1);
                    return IngestorQuiesceIntake::Rejected {
                        retry_after: handling.retry_after(),
                    };
                }
                self.record_dropped(1);
                IngestorQuiesceIntake::Dropped
            }
            IngestorQuiesceHandling::Drop => {
                self.record_dropped(1);
                IngestorQuiesceIntake::Dropped
            }
            IngestorQuiesceHandling::Reject { retry_after } => {
                self.record_rejected(1);
                IngestorQuiesceIntake::Rejected { retry_after }
            }
            IngestorQuiesceHandling::EndpointBuffer { max_size } => {
                let payload_bytes = payload.byte_len();
                let mut buffers = self.buffers.lock();
                let buffer = buffers.entry(instance).or_default();
                if payload_bytes > buffer.remaining_capacity(max_size) {
                    self.record_rejected(1);
                    return IngestorQuiesceIntake::Rejected { retry_after: None };
                }
                buffer.admit(payload, payload_bytes);
                self.buffered_records.fetch_add(1, Ordering::SeqCst);
                self.buffered_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
                self.sync_buffered_metrics();
                IngestorQuiesceIntake::Buffered
            }
            IngestorQuiesceHandling::Buffer { max_size, overflow } => {
                let payload_bytes = payload.byte_len();
                let mut buffers = self.buffers.lock();
                let buffer = buffers.entry(instance).or_default();
                if payload_bytes > max_size {
                    self.record_dropped(1);
                    return IngestorQuiesceIntake::Dropped;
                }
                if overflow == IngestQuiesceOverflow::DropNewest
                    && payload_bytes > buffer.remaining_capacity(max_size)
                {
                    self.record_dropped(1);
                    return IngestorQuiesceIntake::Dropped;
                }
                while payload_bytes > buffer.remaining_capacity(max_size) {
                    let Some(dropped) = buffer.payloads.pop_front() else {
                        break;
                    };
                    buffer.release(dropped.byte_len());
                    self.buffered_records.fetch_sub(1, Ordering::SeqCst);
                    self.buffered_bytes
                        .fetch_sub(dropped.byte_len(), Ordering::Relaxed);
                    self.record_dropped(1);
                }
                buffer.admit(payload, payload_bytes);
                self.buffered_records.fetch_add(1, Ordering::SeqCst);
                self.buffered_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
                self.sync_buffered_metrics();
                IngestorQuiesceIntake::Buffered
            }
        }
    }

    pub(super) fn endpoint_admission(&self) -> Result<(), Option<Duration>> {
        let IngestorQuiesceDecision::Quiesced { handling, .. } = self.decision() else {
            return Ok(());
        };
        if let IngestorQuiesceHandling::EndpointBuffer { .. } = handling {
            return Ok(());
        }
        self.record_rejected(1);
        Err(handling.retry_after())
    }

    pub(in crate::runtime) fn pop_buffered(&self, instance: u64) -> Option<BufferedIngestPayload> {
        if self.is_quiesced() {
            return None;
        }
        // Every ingestor asks on every loop turn, and payloads are retained only under a buffering
        // decision, so one that retains none answers without the lock. The count changes only
        // under that lock, so it reflects every admission that completed before this read.
        if self.buffered_records.load(Ordering::SeqCst) == 0 {
            return None;
        }
        let mut buffers = self.buffers.lock();
        let buffer = buffers.get_mut(&instance)?;
        let payload = buffer.payloads.pop_front()?;
        buffer.release(payload.byte_len());
        self.buffered_records.fetch_sub(1, Ordering::SeqCst);
        self.buffered_bytes
            .fetch_sub(payload.byte_len(), Ordering::Relaxed);
        self.sync_buffered_metrics();
        Some(payload)
    }

    pub(super) fn counters(&self) -> IngestorQuiesceCounters {
        IngestorQuiesceCounters {
            buffered_records: self.buffered_records.load(Ordering::Relaxed),
            buffered_bytes: self.buffered_bytes.load(Ordering::Relaxed),
            dropped_total: self.dropped_total.load(Ordering::Relaxed),
            rejected_total: self.rejected_total.load(Ordering::Relaxed),
        }
    }

    pub(super) fn terminate(&self) {
        let dropped = {
            let mut buffers = self.buffers.lock();
            let dropped = buffers
                .values()
                .map(|buffer| buffer.payloads.len())
                .sum::<usize>();
            buffers.clear();
            self.buffered_records.store(0, Ordering::SeqCst);
            self.buffered_bytes.store(0, Ordering::Relaxed);
            dropped
        };
        self.sync_buffered_metrics();
        self.record_dropped(dropped.arch_into());
    }
}

pub(super) fn quiesce_max_size_bytes(value: &str) -> usize {
    let Ok(size) = value.parse::<ubyte::ByteUnit>() else {
        return 0;
    };
    size.as_u64().arch_into()
}

fn quiesce_retry_after(value: &str) -> Option<Duration> {
    humantime::parse_duration(value).ok()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct IngestorQuiesceCounters {
    pub(crate) buffered_records: usize,
    pub(crate) buffered_bytes: usize,
    pub(crate) dropped_total: u64,
    pub(crate) rejected_total: u64,
}

#[derive(Debug)]
pub(super) struct IngestorReadiness {
    pub(super) expected_instances: NonZeroU64,
    pub(super) ready_instances: BTreeSet<u64>,
}

impl IngestorReadiness {
    pub(super) fn new(expected_instances: NonZeroU64) -> Self {
        Self {
            expected_instances,
            ready_instances: BTreeSet::new(),
        }
    }

    pub(super) fn is_ready(&self) -> bool {
        let ready_instances: u64 = self.ready_instances.len().arch_into();
        ready_instances >= self.expected_instances.get()
    }
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeReconnectStatus {
    pub(super) backoff: Duration,
    pub(super) retry_at: Instant,
}

impl Runtime {
    pub(in crate::runtime) fn record_ingestor_transient_error(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        error: impl Into<String>,
    ) {
        self.inner.ingestor_transient_errors.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
            error.into(),
        );
    }

    pub(in crate::runtime) fn record_ingestor_transient_error_with_backoff(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        error: impl Into<String>,
        backoff: Duration,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        self.inner
            .ingestor_transient_errors
            .insert(key.clone(), error.into());
        self.inner.ingestor_reconnect_backoffs.insert(
            key,
            RuntimeReconnectStatus {
                backoff,
                retry_at: Instant::now() + backoff,
            },
        );
    }

    pub(in crate::runtime) fn clear_ingestor_transient_error(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        self.clear_ingestor_transient_error_for(&key);
    }

    pub(in crate::runtime) fn clear_ingestor_transient_error_for(&self, key: &DomainNodeRef) {
        self.inner.ingestor_transient_errors.remove(key);
        self.inner.ingestor_reconnect_backoffs.remove(key);
    }

    pub(in crate::runtime) fn prepare_ingestor_readiness(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        expected_instances: NonZeroU64,
    ) {
        self.inner.ingestor_readiness.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
            IngestorReadiness::new(expected_instances),
        );
    }

    pub(in crate::runtime) fn prepare_ingestor_quiescence(
        &self,
        domain: &DomainName,
        ingestor: &IngestorSpec,
    ) -> Arc<IngestorQuiesceControl> {
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if let Some(control) = self.inner.ingestor_quiescence.get(&key) {
            let active_supported_by_source = ingestor.quiesce.supports(&control.mode());
            control.update_declared_mode(ingestor.quiesce.mode(), active_supported_by_source);
            return control.clone();
        }
        let dispatcher = self.inner.remote_dispatcher.load();
        let metric_labels = self.inner.metrics.register_ingestor_quiesce(
            domain,
            &ingestor.name,
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
        );
        let control = Arc::new(IngestorQuiesceControl::new(
            ingestor.quiesce.mode().clone(),
            self.inner.metrics.clone(),
            metric_labels,
        ));
        if self.ingestors_paused_for_memory_pressure() {
            control.engage(IngestorQuiesceCause::MemoryPressure);
        }
        self.inner.ingestor_quiescence.insert(key, control.clone());
        // Read after the control is visible, so a local intake boundary closing concurrently
        // either engages this control itself or is observed here.
        if self.local_intake_is_closed() {
            control.engage(IngestorQuiesceCause::Shutdown);
        }
        control
    }

    pub(in crate::runtime) fn ingestor_quiesce_control(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> Option<Arc<IngestorQuiesceControl>> {
        self.inner
            .ingestor_quiescence
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                ingestor.clone(),
            ))
            .map(|control| control.clone())
    }

    /// Engages one quiesce reason and returns the control it was engaged on, so the caller can
    /// release that same control rather than whichever one the ingestor holds later.
    pub(super) fn engage_ingestor_quiesce(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        cause: IngestorQuiesceCause,
    ) -> Option<Arc<IngestorQuiesceControl>> {
        let control = self.ingestor_quiesce_control(domain, ingestor)?;
        control.engage(cause);
        info!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            cause = cause.as_str(),
            "ingestor entered quiesce"
        );
        Some(control)
    }

    pub(super) fn release_ingestor_quiesce(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        cause: IngestorQuiesceCause,
    ) -> bool {
        let Some(control) = self.ingestor_quiesce_control(domain, ingestor) else {
            return false;
        };
        control.release(cause);
        info!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            cause = cause.as_str(),
            "ingestor left quiesce"
        );
        true
    }

    pub(super) fn engage_domain_ingestor_quiesce(&self, domain: &DomainName) {
        let ingestors = self
            .inner
            .ingestors
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.key().identifier().clone())
            .collect::<Vec<_>>();
        for ingestor in ingestors {
            self.engage_ingestor_quiesce(
                domain,
                &IngestorName::from(&ingestor),
                IngestorQuiesceCause::DomainPause,
            );
        }
    }

    pub(super) fn release_domain_ingestor_quiesce(&self, domain: &DomainName) {
        let ingestors = self
            .inner
            .ingestor_quiescence
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.key().identifier().clone())
            .collect::<Vec<_>>();
        for ingestor in ingestors {
            self.release_ingestor_quiesce(
                domain,
                &IngestorName::from(&ingestor),
                IngestorQuiesceCause::DomainPause,
            );
        }
    }

    pub(super) fn remove_ingestor_quiescence(&self, domain: &DomainName, ingestor: &IngestorName) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        if let Some((_, control)) = self.inner.ingestor_quiescence.remove(&key) {
            control.terminate();
        }
    }

    pub(super) fn clear_domain_ingestor_quiescence(&self, domain: &DomainName) {
        let ingestors = self
            .inner
            .ingestor_quiescence
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.key().identifier().clone())
            .collect::<Vec<_>>();
        for ingestor in ingestors {
            self.remove_ingestor_quiescence(domain, &IngestorName::from(&ingestor));
        }
    }

    pub(in crate::runtime) fn mark_ingestor_instance_ready(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        instance_idx: u64,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        if let Some(mut readiness) = self.inner.ingestor_readiness.get_mut(&key) {
            readiness.ready_instances.insert(instance_idx);
        }
    }

    pub(in crate::runtime) fn mark_ingestor_instance_unready(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        instance_idx: u64,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        if let Some(mut readiness) = self.inner.ingestor_readiness.get_mut(&key) {
            readiness.ready_instances.remove(&instance_idx);
        }
    }

    pub(in crate::runtime) fn clear_ingestor_readiness(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) {
        self.inner
            .ingestor_readiness
            .remove(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                ingestor.clone(),
            ));
    }

    pub(super) fn ingestor_ready(&self, domain: &DomainName, ingestor: &IngestorName) -> bool {
        self.inner
            .ingestor_readiness
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                ingestor.clone(),
            ))
            .is_none_or(|readiness| readiness.is_ready())
    }

    pub(super) fn ingestor_transient_error(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> Option<String> {
        self.inner
            .ingestor_transient_errors
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                ingestor.clone(),
            ))
            .map(|error| error.value().clone())
    }

    pub(super) fn ingestor_reconnect_backoff(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> Option<String> {
        self.inner
            .ingestor_reconnect_backoffs
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                ingestor.clone(),
            ))
            .map(|status| humantime::format_duration(status.value().backoff).to_string())
    }

    pub(super) fn ingestor_reconnect_wait_millis(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> Option<u64> {
        self.inner
            .ingestor_reconnect_backoffs
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                ingestor.clone(),
            ))
            .map(|status| {
                u64::try_from(
                    status
                        .value()
                        .retry_at
                        .saturating_duration_since(Instant::now())
                        .as_millis(),
                )
                .unwrap_or(u64::MAX)
            })
    }

    pub(in crate::runtime) async fn wait_if_ingestor_faulted(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> bool {
        if !self.inner.fault_injection.ingestor_is_failed(ingestor) {
            return false;
        }
        self.record_ingestor_transient_error_with_backoff(
            domain,
            ingestor,
            "ingestor fault injector failed source",
            Duration::from_millis(250),
        );
        tokio::select! {
            changed = shutdown_rx.changed() => changed.is_err() || *shutdown_rx.borrow(),
            _ = sleep(Duration::from_millis(250)) => false,
        }
    }

    pub(crate) async fn pause_ingestors_for_memory_pressure(&self) -> usize {
        self.inner
            .ingestors_paused_for_memory_pressure
            .store(true, Ordering::SeqCst);
        let ingestors = self
            .inner
            .ingestors
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();

        let mut quiesced = 0;
        for key in ingestors {
            tokio::task::consume_budget().await;
            if self
                .engage_ingestor_quiesce(
                    &key.domain,
                    &IngestorName::from(key.identifier()),
                    IngestorQuiesceCause::MemoryPressure,
                )
                .is_some()
            {
                quiesced += 1;
            }
        }
        quiesced
    }

    pub(crate) async fn resume_one_ingestor_after_memory_pressure(
        &self,
    ) -> Result<bool, RuntimeError> {
        let mut keys = self
            .inner
            .ingestor_quiescence
            .iter()
            .filter_map(|entry| {
                (entry.value().cause() == Some(IngestorQuiesceCause::MemoryPressure))
                    .then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        keys.sort();
        let Some(key) = keys.first() else {
            self.inner
                .ingestors_paused_for_memory_pressure
                .store(false, Ordering::SeqCst);
            return Ok(false);
        };
        self.release_ingestor_quiesce(
            &key.domain,
            &IngestorName::from(key.identifier()),
            IngestorQuiesceCause::MemoryPressure,
        );
        info!(
            domain = key.domain.as_str(),
            ingestor = key.identifier().as_str(),
            "resumed ingestor after memory pressure"
        );
        Ok(true)
    }

    pub(crate) fn ingestors_paused_for_memory_pressure(&self) -> bool {
        self.inner
            .ingestors_paused_for_memory_pressure
            .load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use nervix_models::{IngestQuiesceMode, IngestQuiesceOverflow, IngestorName, ModelKind};
    use tokio::sync::watch;
    use triomphe::Arc;

    use super::*;

    #[test]
    fn ownership_handoff_stops_new_intake_and_dispatches_already_admitted_payloads() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named("source");
        let control =
            test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Drop);
        control.engage(IngestorQuiesceCause::OwnershipHandoff);

        assert!(control.should_skip_poll());
        assert!(control.should_suspend_intake());
        assert_eq!(control.endpoint_admission(), Err(None));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"admitted",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(1),
                ),
                false,
            ),
            IngestorQuiesceIntake::Dispatch(_)
        ));
        assert_eq!(control.counters().dropped_total, 0);

        control.release(IngestorQuiesceCause::OwnershipHandoff);
        assert!(!control.should_skip_poll());
    }

    #[test]
    fn shutdown_stops_new_intake_whatever_the_declared_quiesce_mode() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named("source");
        let control = test_ingestor_quiesce_control(
            &runtime,
            &domain,
            &ingestor,
            IngestQuiesceMode::Buffer {
                max_size: "1MiB".to_string(),
                overflow: IngestQuiesceOverflow::DropOldest,
            },
        );
        control.engage(IngestorQuiesceCause::EntityHold);
        control.engage(IngestorQuiesceCause::Shutdown);

        assert_eq!(control.cause(), Some(IngestorQuiesceCause::Shutdown));
        assert!(control.should_skip_poll());
        assert!(control.should_suspend_intake());
        assert_eq!(control.endpoint_admission(), Err(None));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"admitted",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(1),
                ),
                false,
            ),
            IngestorQuiesceIntake::Dispatch(_)
        ));
        assert_eq!(control.counters().buffered_records, 0);

        control.release(IngestorQuiesceCause::EntityHold);
        assert_eq!(control.cause(), Some(IngestorQuiesceCause::Shutdown));
    }

    #[test]
    fn quiesce_buffer_enforces_drop_oldest_per_instance() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named("source");
        let control = test_ingestor_quiesce_control(
            &runtime,
            &domain,
            &ingestor,
            IngestQuiesceMode::Buffer {
                max_size: "5B".to_string(),
                overflow: IngestQuiesceOverflow::DropOldest,
            },
        );
        control.engage(IngestorQuiesceCause::EntityHold);

        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"one",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(1),
                ),
                false,
            ),
            IngestorQuiesceIntake::Buffered
        ));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"two",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(2),
                ),
                false,
            ),
            IngestorQuiesceIntake::Buffered
        ));
        assert_eq!(control.counters().buffered_records, 1);
        assert_eq!(control.counters().buffered_bytes, 3);
        assert_eq!(control.counters().dropped_total, 1);

        control.release(IngestorQuiesceCause::EntityHold);
        let retained = control
            .pop_buffered(0)
            .assured("drop-oldest intake retains the newest payload");
        assert_eq!(retained.payload(), b"two");
        assert_eq!(retained.observed_at(), Timestamp::from_unix_nanos(2));
    }

    #[test]
    fn endpoint_quiesce_buffer_rejects_overflow_without_discarding_acknowledged_payloads() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named("source");
        let control = test_ingestor_quiesce_control(
            &runtime,
            &domain,
            &ingestor,
            IngestQuiesceMode::EndpointBuffer {
                max_size: "5B".to_string(),
            },
        );
        control.engage(IngestorQuiesceCause::EntityHold);

        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"kept",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(1),
                ),
                true,
            ),
            IngestorQuiesceIntake::Buffered
        ));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"no",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(2),
                ),
                true,
            ),
            IngestorQuiesceIntake::Rejected { retry_after: None }
        ));
        assert_eq!(control.counters().buffered_records, 1);
        assert_eq!(control.counters().rejected_total, 1);
        assert_eq!(control.counters().dropped_total, 0);

        control.release(IngestorQuiesceCause::EntityHold);
        assert_eq!(
            control
                .pop_buffered(0)
                .expect("the acknowledged payload must remain buffered")
                .payload(),
            b"kept"
        );
    }

    #[test]
    fn source_replacement_waits_when_the_active_hold_mode_is_not_supported() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named("source");
        let control = test_ingestor_quiesce_control(
            &runtime,
            &domain,
            &ingestor,
            IngestQuiesceMode::EndpointBuffer {
                max_size: "1KiB".to_string(),
            },
        );
        control.engage(IngestorQuiesceCause::EntityHold);
        control.update_declared_mode(&IngestQuiesceMode::Suspend, false);

        assert!(control.should_suspend_intake());
        control.release(IngestorQuiesceCause::EntityHold);
        assert_eq!(control.mode(), IngestQuiesceMode::Suspend);
        assert!(!control.should_suspend_intake());
    }

    #[test]
    fn memory_pressure_turns_buffer_into_zero_capacity_without_losing_existing_payloads() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named("source");
        let control = test_ingestor_quiesce_control(
            &runtime,
            &domain,
            &ingestor,
            IngestQuiesceMode::Buffer {
                max_size: "1KiB".to_string(),
                overflow: IngestQuiesceOverflow::DropNewest,
            },
        );
        control.engage(IngestorQuiesceCause::EntityHold);
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"retained",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(1),
                ),
                false,
            ),
            IngestorQuiesceIntake::Buffered
        ));

        control.engage(IngestorQuiesceCause::MemoryPressure);
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"discarded",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(2),
                ),
                false,
            ),
            IngestorQuiesceIntake::Dropped
        ));
        assert_eq!(control.counters().buffered_records, 1);
        assert_eq!(control.counters().dropped_total, 1);

        control.release(IngestorQuiesceCause::MemoryPressure);
        control.release(IngestorQuiesceCause::EntityHold);
        assert_eq!(
            control
                .pop_buffered(0)
                .expect("pre-pressure payload should remain")
                .payload(),
            b"retained"
        );
    }

    #[test]
    fn reject_quiesce_hints_its_retry_delay_through_a_pause_and_memory_pressure() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named("source");
        let control = test_ingestor_quiesce_control(
            &runtime,
            &domain,
            &ingestor,
            IngestQuiesceMode::Reject {
                retry_after: "7s".to_string(),
            },
        );
        let retry_after = Duration::from_secs(7);

        control.engage(IngestorQuiesceCause::DomainPause);
        assert!(!control.should_suspend_intake());
        assert!(!control.should_skip_poll());
        assert_eq!(control.endpoint_admission(), Err(Some(retry_after)));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"paused",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(1),
                ),
                true,
            ),
            IngestorQuiesceIntake::Rejected { retry_after: Some(delay) } if delay == retry_after
        ));

        control.engage(IngestorQuiesceCause::MemoryPressure);
        assert!(!control.should_suspend_intake());
        assert!(control.should_skip_poll());
        assert_eq!(control.endpoint_admission(), Err(Some(retry_after)));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"pressured",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(2),
                ),
                true,
            ),
            IngestorQuiesceIntake::Rejected { retry_after: Some(delay) } if delay == retry_after
        ));
        assert_eq!(control.counters().rejected_total, 4);
        assert_eq!(control.counters().dropped_total, 0);
    }

    #[test]
    fn memory_pressure_keeps_a_suspended_source_suspended_and_sheds_what_still_arrives() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named("source");
        let control =
            test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend);

        control.engage(IngestorQuiesceCause::MemoryPressure);
        assert!(control.should_suspend_intake());
        assert!(control.should_skip_poll());
        assert_eq!(control.endpoint_admission(), Err(None));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"pressured",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(1),
                ),
                false,
            ),
            IngestorQuiesceIntake::Dropped
        ));
        assert_eq!(control.counters().dropped_total, 1);

        control.release(IngestorQuiesceCause::MemoryPressure);
        control.engage(IngestorQuiesceCause::EntityHold);
        assert!(control.should_suspend_intake());
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(
                    b"held",
                    BufferedIngestMetadata::without_headers(),
                    Timestamp::from_unix_nanos(2),
                ),
                false,
            ),
            IngestorQuiesceIntake::Dispatch(_)
        ));
        assert_eq!(control.counters().dropped_total, 1);
    }

    #[tokio::test]
    async fn memory_pressure_quiesces_registered_ingestors_without_stopping_them() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named::<IngestorName>("source");
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let stopped = Arc::new(AtomicBool::new(false));
        let task_stopped = stopped.clone();
        let task = tokio::spawn(async move {
            let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
            task_stopped.store(true, Ordering::SeqCst);
        });

        runtime.inner.ingestors.insert(
            key.clone(),
            IngestorRuntime {
                shutdown: shutdown_tx,
                branched: Vec::new(),
                tasks: vec![task],
            },
        );
        runtime.inner.ingestor_quiescence.insert(
            key.clone(),
            test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend),
        );

        assert_eq!(runtime.pause_ingestors_for_memory_pressure().await, 1);
        assert!(runtime.ingestors_paused_for_memory_pressure());
        assert!(!stopped.load(Ordering::SeqCst));
        assert!(runtime.inner.ingestors.get(&key).is_some());
        assert_eq!(
            runtime
                .inner
                .ingestor_quiescence
                .get(&key)
                .and_then(|control| control.cause()),
            Some(IngestorQuiesceCause::MemoryPressure)
        );
        assert!(
            runtime
                .resume_one_ingestor_after_memory_pressure()
                .await
                .expect("resume should succeed")
        );
        assert!(
            !runtime
                .resume_one_ingestor_after_memory_pressure()
                .await
                .expect("pause should clear after the last ingestor resumes")
        );
        runtime
            .stop_ingestor(&domain, &ingestor)
            .await
            .expect("test ingestor should stop");
    }

    #[tokio::test]
    async fn memory_pressure_resume_clears_pause_when_no_ingestors_are_pending() {
        let runtime = Runtime::default();

        assert_eq!(runtime.pause_ingestors_for_memory_pressure().await, 0);
        assert!(runtime.ingestors_paused_for_memory_pressure());
        assert!(
            !runtime
                .resume_one_ingestor_after_memory_pressure()
                .await
                .expect("resume should succeed")
        );
        assert!(!runtime.ingestors_paused_for_memory_pressure());
    }

    #[cfg(feature = "shuttle")]
    mod shuttle_checks {
        use shuttle::{sync::mpsc, thread};

        use super::*;
        use crate::shuttle_test::check_interleavings;

        /// What a control answers to each intake check an ingestor makes.
        #[derive(Debug, PartialEq, Eq)]
        struct IntakeAnswers {
            suspends_intake: bool,
            skips_poll: bool,
            endpoint_admission: Result<(), Option<Duration>>,
            dispatches: bool,
            replays: bool,
        }

        impl IntakeAnswers {
            /// Ask `control` every intake check a message, a poll and a loop turn make.
            fn of(control: &IngestorQuiesceControl) -> Self {
                let suspends_intake = control.should_suspend_intake();
                let skips_poll = control.should_skip_poll();
                let endpoint_admission = control.endpoint_admission();
                let intake = control.intake(
                    0,
                    BufferedIngestPayload::new(
                        b"live",
                        BufferedIngestMetadata::without_headers(),
                        Timestamp::from_unix_nanos(1),
                    ),
                    false,
                );
                let replays = control.pop_buffered(0).is_some();
                Self {
                    suspends_intake,
                    skips_poll,
                    endpoint_admission,
                    dispatches: matches!(intake, IngestorQuiesceIntake::Dispatch(_)),
                    replays,
                }
            }
        }

        /// An open buffering control answers every intake check while another thread holds the lock
        /// that owns retained payloads, which that thread releases only after every check answered.
        fn open_control_intake_under_held_retention() {
            // A whole runtime is far heavier than one execution of this model needs, so the control
            // takes the metrics it records into directly.
            let metrics = RuntimeMetrics::default();
            let metric_labels =
                metrics.register_ingestor_quiesce(&domain("default"), &named("source"), None);
            let control = Arc::new(IngestorQuiesceControl::new(
                IngestQuiesceMode::Buffer {
                    max_size: "1MiB".to_string(),
                    overflow: IngestQuiesceOverflow::DropOldest,
                },
                metrics,
                metric_labels,
            ));
            let (held_tx, held_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel::<()>();
            let retention = thread::spawn({
                let control = control.clone();
                move || {
                    let retained = control.buffers.lock();
                    held_tx
                        .send(())
                        .assured("the model keeps the receiver until retention is held");
                    release_rx.recv().assured(
                        "the model releases retention once every intake check has answered",
                    );
                    drop(retained);
                }
            });
            held_rx
                .recv()
                .assured("the retention thread reports once it holds the lock");

            // Retention and replay own this lock, and it stays held until every check has
            // answered, so a check that reached it from a message, a poll or a loop turn would
            // leave every thread blocked, which Shuttle reports as a deadlock.
            let answers = IntakeAnswers::of(&control);
            release_tx
                .send(())
                .assured("the retention thread waits for its release");
            retention.join().assured(
                "Shuttle fails the whole execution when a model thread panics, so no join \
                 observes one",
            );

            assert_eq!(
                answers,
                IntakeAnswers {
                    suspends_intake: false,
                    skips_poll: false,
                    endpoint_admission: Ok(()),
                    dispatches: true,
                    replays: false,
                }
            );
        }

        /// An ingestor that is neither quiesced nor retaining payloads answers every message, poll
        /// and loop turn without the lock that owns retained payloads.
        #[test]
        fn shuttle_an_open_control_answers_intake_without_waiting_on_retained_payloads() {
            check_interleavings(open_control_intake_under_held_retention);
        }
    }
}
