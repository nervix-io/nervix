use super::*;

pub(super) const DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL: Duration = Duration::from_secs(1);

pub(super) enum IngestorRuntime {
    Background {
        shutdown: watch::Sender<bool>,
        branched: Vec<Arc<IngestorRouteRuntime>>,
        tasks: Vec<JoinHandle<()>>,
    },
    Endpoint {
        route_keys: Vec<HttpRouteKey>,
        branched: Vec<Arc<IngestorRouteRuntime>>,
        shutdown: watch::Sender<bool>,
        tasks: Vec<JoinHandle<()>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
pub(crate) enum IngestorQuiesceCause {
    #[strum(serialize = "entity hold")]
    EntityHold,
    #[strum(serialize = "ownership handoff")]
    OwnershipHandoff,
    #[strum(serialize = "domain pause")]
    DomainPause,
    #[strum(serialize = "memory pressure")]
    MemoryPressure,
}

impl IngestorQuiesceCause {
    pub(super) fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Default)]
pub(super) struct IngestorQuiesceReasons {
    pub(super) entity_holds: usize,
    pub(super) ownership_handoffs: usize,
    pub(super) domain_pause: bool,
    pub(super) memory_pressure: bool,
}

impl IngestorQuiesceReasons {
    pub(super) fn active(&self) -> Option<IngestorQuiesceCause> {
        if self.ownership_handoffs > 0 {
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
}

#[derive(Debug)]
pub(super) struct IngestorQuiesceModes {
    pub(super) active: IngestQuiesceMode,
    pub(super) pending: Option<IngestQuiesceMode>,
    pub(super) active_supported_by_source: bool,
}

/// One buffered message's ingest metadata.
///
/// A quiesced ingestor replays its payloads after the source messages are gone, so the
/// buffer owns these values and appends them into the group's builders on replay.
#[derive(Debug, Clone)]
pub(crate) enum BufferedIngestMetadata {
    Syslog { peer_addr: std::net::SocketAddr },
    Headers(RetainedIngestHeaders),
}

impl BufferedIngestMetadata {
    /// The metadata of a buffered message whose source carries no transport headers.
    pub(crate) fn without_headers() -> Self {
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
pub(crate) struct BufferedIngestPayload {
    pub(super) payloads: Vec<Vec<u8>>,
    /// Row-aligned with `payloads`.
    pub(super) metadata: Vec<BufferedIngestMetadata>,
}

impl BufferedIngestPayload {
    pub(crate) fn new(payload: &[u8], metadata: BufferedIngestMetadata) -> Self {
        Self {
            payloads: vec![payload.to_vec()],
            metadata: vec![metadata],
        }
    }

    pub(crate) fn batch(entries: Vec<(Vec<u8>, BufferedIngestMetadata)>) -> Self {
        let (payloads, metadata) = entries.into_iter().unzip();
        Self { payloads, metadata }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        self.payloads.first().verified(
            "both constructors take at least one payload with its metadata, and the batching \
             caller skips an empty set",
        )
    }

    /// The number of source payloads, which is the row count of the group this buffer opens.
    pub(crate) fn len(&self) -> usize {
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
pub(crate) enum IngestorQuiesceIntake {
    Dispatch(BufferedIngestPayload),
    Buffered,
    Dropped,
    Rejected { retry_after: Option<Duration> },
}

#[derive(Debug)]
pub(crate) struct IngestorQuiesceControl {
    pub(super) modes: RwLock<IngestorQuiesceModes>,
    pub(super) reasons: RwLock<IngestorQuiesceReasons>,
    pub(super) buffers: parking_lot::Mutex<HashMap<u64, IngestorQuiesceBuffer>>,
    pub(super) changed: Notify,
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
        Self {
            modes: RwLock::new(IngestorQuiesceModes {
                active: mode,
                pending: None,
                active_supported_by_source: true,
            }),
            reasons: RwLock::new(IngestorQuiesceReasons::default()),
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
        let quiesced = self.reasons.read().active().is_some();
        let mut modes = self.modes.write();
        if quiesced {
            modes.active_supported_by_source = active_supported_by_source;
            modes.pending = Some(declared.clone());
        } else {
            modes.active = declared.clone();
            modes.pending = None;
            modes.active_supported_by_source = true;
        }
        drop(modes);
        self.changed.notify_waiters();
    }

    pub(super) fn engage(&self, cause: IngestorQuiesceCause) {
        let mut reasons = self.reasons.write();
        match cause {
            IngestorQuiesceCause::EntityHold => {
                reasons.entity_holds = reasons.entity_holds.checked_add(1).assured(
                    "a quiesce reason is held once per live entity hold, which is bounded by the \
                     entities resident on this node",
                );
            }
            IngestorQuiesceCause::OwnershipHandoff => {
                reasons.ownership_handoffs = reasons.ownership_handoffs.checked_add(1).assured(
                    "a quiesce reason is held once per live ownership handoff, which is bounded \
                     by the entities resident on this node",
                );
            }
            IngestorQuiesceCause::DomainPause => reasons.domain_pause = true,
            IngestorQuiesceCause::MemoryPressure => reasons.memory_pressure = true,
        }
        drop(reasons);
        self.changed.notify_waiters();
    }

    pub(super) fn release(&self, cause: IngestorQuiesceCause) {
        let now_active = {
            let mut reasons = self.reasons.write();
            match cause {
                IngestorQuiesceCause::EntityHold => {
                    reasons.entity_holds = reasons.entity_holds.checked_sub(1).verified(
                        "the entity gate hold that engaged this control is released once, by the \
                         one caller that took it out of the hold map",
                    );
                }
                IngestorQuiesceCause::OwnershipHandoff => {
                    reasons.ownership_handoffs =
                        reasons.ownership_handoffs.checked_sub(1).verified(
                            "the entity gate hold that engaged this control is released once, by \
                             the one caller that took it out of the hold map",
                        );
                }
                IngestorQuiesceCause::DomainPause => reasons.domain_pause = false,
                IngestorQuiesceCause::MemoryPressure => reasons.memory_pressure = false,
            }
            reasons.active()
        };
        if now_active.is_none() {
            let mut modes = self.modes.write();
            if let Some(pending) = modes.pending.take() {
                modes.active = pending;
            }
            modes.active_supported_by_source = true;
        }
        self.changed.notify_waiters();
    }

    pub(crate) fn cause(&self) -> Option<IngestorQuiesceCause> {
        self.reasons.read().active()
    }

    pub(crate) fn is_quiesced(&self) -> bool {
        self.cause().is_some()
    }

    pub(crate) fn mode(&self) -> IngestQuiesceMode {
        self.modes.read().active.clone()
    }

    pub(super) fn active_mode_is_supported(&self) -> bool {
        self.modes.read().active_supported_by_source
    }

    pub(crate) fn should_suspend_intake(&self) -> bool {
        if self.cause() == Some(IngestorQuiesceCause::OwnershipHandoff) {
            true
        } else {
            self.is_quiesced()
                && (!self.active_mode_is_supported()
                    || matches!(self.mode(), IngestQuiesceMode::Suspend))
        }
    }

    pub(crate) fn should_skip_poll(&self) -> bool {
        match self.cause() {
            Some(IngestorQuiesceCause::OwnershipHandoff) => true,
            Some(IngestorQuiesceCause::MemoryPressure) => true,
            Some(_) if !self.active_mode_is_supported() => true,
            Some(_) => matches!(self.mode(), IngestQuiesceMode::Suspend),
            None => false,
        }
    }

    pub(crate) async fn wait_until_not_suspended(&self) {
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

    pub(crate) async fn wait_for_change(&self) {
        self.changed.notified().await;
    }

    pub(crate) fn intake(
        &self,
        instance: u64,
        payload: BufferedIngestPayload,
        endpoint: bool,
    ) -> IngestorQuiesceIntake {
        let Some(cause) = self.cause() else {
            return IngestorQuiesceIntake::Dispatch(payload);
        };
        if cause == IngestorQuiesceCause::OwnershipHandoff {
            return IngestorQuiesceIntake::Dispatch(payload);
        }
        let mode = self.mode();
        if !self.active_mode_is_supported() {
            if endpoint {
                self.record_rejected(1);
                return IngestorQuiesceIntake::Rejected { retry_after: None };
            }
            self.record_dropped(1);
            return IngestorQuiesceIntake::Dropped;
        }
        if cause == IngestorQuiesceCause::MemoryPressure {
            if endpoint {
                self.record_rejected(1);
                return IngestorQuiesceIntake::Rejected {
                    retry_after: match mode {
                        IngestQuiesceMode::Reject { retry_after } => {
                            humantime::parse_duration(&retry_after).ok()
                        }
                        _ => None,
                    },
                };
            }
            self.record_dropped(1);
            return IngestorQuiesceIntake::Dropped;
        }

        match mode {
            IngestQuiesceMode::Suspend => IngestorQuiesceIntake::Dispatch(payload),
            IngestQuiesceMode::Drop => {
                self.record_dropped(1);
                IngestorQuiesceIntake::Dropped
            }
            IngestQuiesceMode::Reject { retry_after } => {
                self.record_rejected(1);
                IngestorQuiesceIntake::Rejected {
                    retry_after: humantime::parse_duration(&retry_after).ok(),
                }
            }
            IngestQuiesceMode::EndpointBuffer { max_size } => {
                let max_size = quiesce_max_size_bytes(&max_size);
                let payload_bytes = payload.byte_len();
                let mut buffers = self.buffers.lock();
                let buffer = buffers.entry(instance).or_default();
                if payload_bytes > buffer.remaining_capacity(max_size) {
                    self.record_rejected(1);
                    return IngestorQuiesceIntake::Rejected { retry_after: None };
                }
                buffer.admit(payload, payload_bytes);
                self.buffered_records.fetch_add(1, Ordering::Relaxed);
                self.buffered_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
                self.sync_buffered_metrics();
                IngestorQuiesceIntake::Buffered
            }
            IngestQuiesceMode::Buffer { max_size, overflow } => {
                let max_size = quiesce_max_size_bytes(&max_size);
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
                    self.buffered_records.fetch_sub(1, Ordering::Relaxed);
                    self.buffered_bytes
                        .fetch_sub(dropped.byte_len(), Ordering::Relaxed);
                    self.record_dropped(1);
                }
                buffer.admit(payload, payload_bytes);
                self.buffered_records.fetch_add(1, Ordering::Relaxed);
                self.buffered_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
                self.sync_buffered_metrics();
                IngestorQuiesceIntake::Buffered
            }
        }
    }

    pub(super) fn endpoint_admission(&self) -> Result<(), Option<Duration>> {
        let Some(cause) = self.cause() else {
            return Ok(());
        };
        if cause == IngestorQuiesceCause::OwnershipHandoff {
            self.record_rejected(1);
            return Err(None);
        }
        let mode = self.mode();
        if !self.active_mode_is_supported() {
            self.record_rejected(1);
            return Err(None);
        }
        if cause != IngestorQuiesceCause::MemoryPressure
            && matches!(mode, IngestQuiesceMode::EndpointBuffer { .. })
        {
            return Ok(());
        }
        self.record_rejected(1);
        Err(match mode {
            IngestQuiesceMode::Reject { retry_after } => {
                humantime::parse_duration(&retry_after).ok()
            }
            _ => None,
        })
    }

    pub(crate) fn pop_buffered(&self, instance: u64) -> Option<BufferedIngestPayload> {
        if self.is_quiesced() {
            return None;
        }
        let mut buffers = self.buffers.lock();
        let buffer = buffers.get_mut(&instance)?;
        let payload = buffer.payloads.pop_front()?;
        buffer.release(payload.byte_len());
        self.buffered_records.fetch_sub(1, Ordering::Relaxed);
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
            dropped
        };
        self.buffered_records.store(0, Ordering::Relaxed);
        self.buffered_bytes.store(0, Ordering::Relaxed);
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestorQuiesceCounters {
    pub buffered_records: usize,
    pub buffered_bytes: usize,
    pub dropped_total: u64,
    pub rejected_total: u64,
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
        self.inner
            .ingestor_transient_errors
            .remove(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                ingestor.clone(),
            ));
        self.inner
            .ingestor_reconnect_backoffs
            .remove(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                ingestor.clone(),
            ));
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
        let metric_labels = self.inner.metrics.register_ingestor_quiesce(
            domain,
            &ingestor.name,
            self.inner.remote_dispatch.local_node_id.read().as_ref(),
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

    pub async fn pause_ingestors_for_memory_pressure(&self) -> usize {
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

    pub async fn resume_one_ingestor_after_memory_pressure(&self) -> Result<bool, RuntimeError> {
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

    pub fn ingestors_paused_for_memory_pressure(&self) -> bool {
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
                BufferedIngestPayload::new(b"admitted", BufferedIngestMetadata::without_headers(),),
                false,
            ),
            IngestorQuiesceIntake::Dispatch(_)
        ));
        assert_eq!(control.counters().dropped_total, 0);

        control.release(IngestorQuiesceCause::OwnershipHandoff);
        assert!(!control.should_skip_poll());
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
                BufferedIngestPayload::new(b"one", BufferedIngestMetadata::without_headers(),),
                false,
            ),
            IngestorQuiesceIntake::Buffered
        ));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(b"two", BufferedIngestMetadata::without_headers(),),
                false,
            ),
            IngestorQuiesceIntake::Buffered
        ));
        assert_eq!(control.counters().buffered_records, 1);
        assert_eq!(control.counters().buffered_bytes, 3);
        assert_eq!(control.counters().dropped_total, 1);

        control.release(IngestorQuiesceCause::EntityHold);
        assert_eq!(
            control
                .pop_buffered(0)
                .expect("newest payload should remain")
                .payload(),
            b"two"
        );
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
                BufferedIngestPayload::new(b"kept", BufferedIngestMetadata::without_headers(),),
                true,
            ),
            IngestorQuiesceIntake::Buffered
        ));
        assert!(matches!(
            control.intake(
                0,
                BufferedIngestPayload::new(b"no", BufferedIngestMetadata::without_headers(),),
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
                BufferedIngestPayload::new(b"retained", BufferedIngestMetadata::without_headers(),),
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
            IngestorRuntime::Background {
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
}
