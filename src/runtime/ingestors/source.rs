//! The host-owned broker source loop and its runtime intake adapter.
//!
//! Layer: data plane.
//!
//! - **Owns.** Source task lifecycle, quiesce and readiness, grouping policy, runtime decoding and
//!   dispatch, acknowledgement waiting, retry cadence, and connector error reporting.
//! - **Depends on.** The connector source contract and pre-resolved runtime execution handles.
//! - **Must not know.** A broker driver, connector-specific configuration, NSPL parsing, registry
//!   validation, or placement computation.

use std::{future, time::Duration};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    IngestMetadataRow, ParsedRetryPolicy, RetainedIngestHeaders, SourceAckPolicy,
    SourceAcknowledgement, SourceAcknowledgementOutcome, SourceAcknowledgementServices,
    SourceBatch, SourceBatchRequest, SourceConnector, SourceHost, SourceHostServices,
    SourceIntakeBatch, SourceIntakeError, SourceIntakeMessage, SourceIntakeMode,
    SourceIntakeOutcome, SourceIntakeResult, SourceMessage, SourceResume, next_retry_delay,
    physical_time::actual_utc_now,
};

use super::super::*;

const SOURCE_ERROR_RETRY: Duration = Duration::from_millis(100);

pub(super) struct BrokerSourceHostSpec {
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) timestamp_source: Option<IngestTimestampSource>,
    pub(super) output_routes: RelayProcessorOutputsNode,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) codec: Arc<CompiledCodec>,
    pub(super) metrics: MessageMetricsHandle,
    pub(super) branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    pub(super) quiesce: Arc<IngestorQuiesceControl>,
    pub(super) shutdown: watch::Receiver<bool>,
    pub(super) instance_index: u64,
    pub(super) metadata_kind: IngestMetadataKind,
    pub(super) buffered_intake: bool,
}

pub(super) struct BrokerSourceHost {
    runtime: Runtime,
    domain: DomainName,
    ingestor: IngestorName,
    timestamp_source: Option<IngestTimestampSource>,
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    codec: Arc<CompiledCodec>,
    metrics: MessageMetricsHandle,
    branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    quiesce: Arc<IngestorQuiesceControl>,
    ack_root_trackers: IngestorAckRootTrackers,
    shutdown: watch::Receiver<bool>,
    instance_index: u64,
    collector: IngestRouteCollector,
    buffered_intake: bool,
}

impl BrokerSourceHost {
    pub(super) fn build(spec: BrokerSourceHostSpec) -> SourceHost {
        let ack_root_trackers = spec
            .runtime
            .ingestor_ack_root_trackers(&spec.domain, &spec.ingestor);
        let collector = IngestRouteCollector::new(
            spec.metadata_kind,
            INGEST_GROUP_MAX_ROWS,
            spec.metrics.clone(),
        );
        SourceHost::new(Self {
            runtime: spec.runtime,
            domain: spec.domain,
            ingestor: spec.ingestor,
            timestamp_source: spec.timestamp_source,
            output_routes: spec.output_routes,
            filter_where: spec.filter_where,
            codec: spec.codec,
            metrics: spec.metrics,
            branched_senders: spec.branched_senders,
            quiesce: spec.quiesce,
            ack_root_trackers,
            shutdown: spec.shutdown,
            instance_index: spec.instance_index,
            collector,
            buffered_intake: spec.buffered_intake,
        })
    }
}

struct RuntimeSourceAcknowledgement {
    completion: AckCompletion,
    shutdown: watch::Receiver<bool>,
}

#[async_trait]
impl SourceAcknowledgementServices for RuntimeSourceAcknowledgement {
    async fn wait(mut self: Box<Self>, timeout: Duration) -> SourceAcknowledgementOutcome {
        match Runtime::await_ack_completion(&mut self.shutdown, self.completion, timeout).await {
            Some(AckOutcome::Ack) => SourceAcknowledgementOutcome::Ack,
            Some(AckOutcome::NoAck(reason)) => SourceAcknowledgementOutcome::NoAck(reason),
            None => SourceAcknowledgementOutcome::Shutdown,
        }
    }
}

#[async_trait]
impl SourceHostServices for BrokerSourceHost {
    async fn intake(
        &mut self,
        batch: SourceIntakeBatch<'_>,
    ) -> SourceIntakeResult<SourceIntakeOutcome> {
        let acknowledged = batch.mode == SourceIntakeMode::Acknowledged;
        if self.buffered_intake && self.quiesce.is_quiesced() {
            if acknowledged {
                return Err(Report::new(SourceIntakeError::Retain)
                    .attach_printable("acknowledged source input cannot enter a quiesce buffer"));
            }
            let mut entries = Vec::with_capacity(batch.messages.len());
            for message in batch.messages {
                tokio::task::consume_budget().await;
                let metadata = match message.metadata {
                    IngestMetadataRow::Syslog { peer_addr } => {
                        BufferedIngestMetadata::Syslog { peer_addr }
                    }
                    IngestMetadataRow::Headers { headers } => {
                        BufferedIngestMetadata::Headers(RetainedIngestHeaders::capture(headers))
                    }
                    IngestMetadataRow::Kafka { .. } => {
                        return Err(Report::new(SourceIntakeError::Retain).attach_printable(
                            "Kafka source input supports suspension instead of quiesce buffering",
                        ));
                    }
                };
                entries.push((message.payload.to_vec(), metadata));
            }
            let payload = BufferedIngestPayload::batch(entries, actual_utc_now());
            if let IngestorQuiesceIntake::Dispatch(payload) =
                self.quiesce.intake(self.instance_index, payload, false)
            {
                self.dispatch_buffered_payload(&payload).await?;
            }
            return Ok(SourceIntakeOutcome {
                acknowledgements: Vec::new(),
            });
        }
        let row_bound = if acknowledged {
            batch.messages.len().max(1)
        } else {
            INGEST_GROUP_MAX_ROWS
        };
        let mut acknowledged_collector = acknowledged.then(|| {
            IngestRouteCollector::new(self.collector.kind, row_bound, self.metrics.clone())
        });
        let collector = match acknowledged_collector.as_mut() {
            Some(collector) => collector,
            None => &mut self.collector,
        };

        let mut metadata = Vec::with_capacity(batch.messages.len());
        for message in batch.messages {
            tokio::task::consume_budget().await;
            if let Err(error) = collector
                .decode_payload(&self.codec, std::borrow::Cow::Borrowed(message.payload))
                .await
            {
                collector.discard_undispatched_payloads();
                return Err(Report::new(error).change_context(SourceIntakeError::Decode));
            }
            metadata.push(message.metadata);
        }

        let mut roots = Vec::new();
        let mut completions = Vec::new();
        let acks = if acknowledged {
            roots.reserve(metadata.len());
            completions.reserve(metadata.len());
            let mut acks = Vec::with_capacity(metadata.len());
            for _ in &metadata {
                tokio::task::consume_budget().await;
                let (root, completion) = self.ack_root_trackers.tracked_root();
                if self.branched_senders.is_empty() {
                    acks.push(root.clone());
                } else {
                    acks.push(root.attached());
                }
                roots.push(root);
                completions.push(completion);
            }
            acks
        } else {
            metadata.iter().map(|_| AckSet::empty()).collect()
        };

        self.runtime
            .dispatch_ingested_records(IngestGroupDispatch {
                collector,
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                metadata: &metadata,
                ingested_at: actual_utc_now(),
                acks,
            })
            .await
            .change_context(SourceIntakeError::Dispatch)?;

        if acknowledged || collector.len() >= INGEST_GROUP_MAX_ROWS {
            self.runtime
                .flush_ingest_collector(
                    &self.domain,
                    &self.ingestor,
                    &self.branched_senders,
                    collector,
                )
                .await
                .change_context(SourceIntakeError::Flush)?;
        }

        for root in roots {
            root.ack_success();
        }
        let acknowledgements = completions
            .into_iter()
            .map(|completion| {
                SourceAcknowledgement::new(RuntimeSourceAcknowledgement {
                    completion,
                    shutdown: self.shutdown.clone(),
                })
            })
            .collect();
        Ok(SourceIntakeOutcome { acknowledgements })
    }

    async fn flush(&mut self) -> SourceIntakeResult<()> {
        self.runtime
            .flush_ingest_collector(
                &self.domain,
                &self.ingestor,
                &self.branched_senders,
                &mut self.collector,
            )
            .await
            .change_context(SourceIntakeError::Flush)
    }

    async fn replay_buffered(&mut self) -> SourceIntakeResult<bool> {
        let Some(payload) = self.quiesce.pop_buffered(self.instance_index) else {
            return Ok(false);
        };
        self.dispatch_buffered_payload(&payload).await?;
        Ok(true)
    }

    fn next_flush(&self) -> Option<Instant> {
        self.collector.next_flush()
    }

    fn should_suspend_intake(&self) -> bool {
        self.quiesce.should_suspend_intake()
    }

    async fn wait_for_quiesce_change(&mut self) {
        self.quiesce.wait_for_change().await;
    }

    async fn wait_until_not_suspended(&mut self) {
        self.quiesce.wait_until_not_suspended().await;
    }

    async fn wait_until_active(&mut self) -> bool {
        loop {
            tokio::task::consume_budget().await;
            if !self
                .runtime
                .inner
                .fault_injection
                .ingestor_is_failed(&self.ingestor)
            {
                return true;
            }
            if self
                .runtime
                .wait_if_ingestor_faulted(&self.domain, &self.ingestor, &mut self.shutdown)
                .await
            {
                return false;
            }
        }
    }

    fn mark_ready(&self) {
        self.runtime.mark_ingestor_instance_ready(
            &self.domain,
            &self.ingestor,
            self.instance_index,
        );
    }

    fn mark_unready(&self) {
        self.runtime.mark_ingestor_instance_unready(
            &self.domain,
            &self.ingestor,
            self.instance_index,
        );
    }

    fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.runtime.record_ingestor_transient_error_with_backoff(
            &self.domain,
            &self.ingestor,
            reason,
            retry_after,
        );
    }

    fn clear_transient_error(&self) {
        self.runtime
            .clear_ingestor_transient_error(&self.domain, &self.ingestor);
    }

    fn report_error(&self, message: String) {
        self.runtime.events().report_error(format!(
            "source error for ingestor '{}' in domain '{}': {message}",
            self.ingestor.as_str(),
            self.domain.as_str(),
        ));
    }

    fn handle_ack_failure(&self, reason: String) {
        self.runtime.events().report_error(format!(
            "source ACK chain failed for ingestor '{}' in domain '{}': {reason}",
            self.ingestor.as_str(),
            self.domain.as_str(),
        ));
    }
}

impl BrokerSourceHost {
    async fn dispatch_buffered_payload(
        &mut self,
        payload: &BufferedIngestPayload,
    ) -> SourceIntakeResult<()> {
        self.runtime
            .dispatch_raw_ingest_payload(RawIngestDispatch {
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                branched_senders: &self.branched_senders,
                codec: self.codec.clone(),
                payload,
                collector: &mut self.collector,
                flush: false,
            })
            .await
            .change_context(SourceIntakeError::Dispatch)?;
        if self.collector.len() >= INGEST_GROUP_MAX_ROWS {
            self.flush().await?;
        }
        Ok(())
    }
}

enum BatchDisposition {
    Accepted,
    Retry,
    Shutdown,
}

pub(super) async fn run_source_instance<C>(
    source: C,
    host: SourceHost,
    acknowledgement: SourceAckPolicy,
    shutdown: watch::Receiver<bool>,
) where
    C: SourceConnector,
{
    run_source_instance_with_retry(
        source,
        host,
        acknowledgement,
        acknowledgement.retry(),
        shutdown,
    )
    .await;
}

pub(super) async fn run_source_instance_with_retry<C>(
    mut source: C,
    mut host: SourceHost,
    acknowledgement: SourceAckPolicy,
    retry_policy: ParsedRetryPolicy,
    mut shutdown: watch::Receiver<bool>,
) where
    C: SourceConnector,
{
    let mut retry_delay = retry_policy.backoff;
    let mut ready = false;

    loop {
        tokio::task::consume_budget().await;
        if !host.wait_until_active().await {
            break;
        }
        match host.replay_buffered().await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => host.report_error(error.to_string()),
        }
        if host.should_suspend_intake() {
            flush_for_lifecycle(&mut host).await;
            if ready {
                if let Err(error) = source.suspend().await {
                    host.report_error(error.to_string());
                }
                ready = false;
                host.mark_unready();
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = host.wait_until_not_suspended() => {}
            }
            continue;
        }

        if !ready || source.needs_resume() {
            flush_for_lifecycle(&mut host).await;
            match source.resume().await {
                Ok(SourceResume::Ready) => {
                    ready = true;
                    retry_delay = retry_policy.backoff;
                    host.mark_ready();
                    host.clear_transient_error();
                }
                Ok(SourceResume::Waiting { retry_after }) => {
                    ready = false;
                    host.mark_unready();
                    if !wait_for_retry(&mut host, &mut shutdown, retry_after).await {
                        break;
                    }
                    continue;
                }
                Err(error) => {
                    ready = false;
                    host.mark_unready();
                    let delay = source_retry_delay(retry_delay);
                    host.record_transient_error(error.to_string(), delay);
                    host.report_error(error.to_string());
                    if !wait_for_retry(&mut host, &mut shutdown, delay).await {
                        break;
                    }
                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                    continue;
                }
            }
        }

        let request = batch_request(acknowledgement);
        let next_flush = host.next_flush();
        let batch = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            _ = host.wait_for_quiesce_change() => {
                continue;
            }
            _ = wait_for_flush(next_flush) => {
                flush_for_lifecycle(&mut host).await;
                continue;
            }
            batch = source.next_batch(request) => batch,
        };

        let messages = match batch {
            Ok(SourceBatch::Messages(messages)) => messages,
            Ok(SourceBatch::ResumeRequired) => {
                ready = false;
                host.mark_unready();
                continue;
            }
            Ok(SourceBatch::Closed) => break,
            Err(error) => {
                host.record_transient_error(error.to_string(), SOURCE_ERROR_RETRY);
                host.report_error(error.to_string());
                if !wait_for_retry(&mut host, &mut shutdown, SOURCE_ERROR_RETRY).await {
                    break;
                }
                continue;
            }
        };
        if messages.is_empty() {
            host.report_error("source returned an empty message batch".to_string());
            continue;
        }
        host.clear_transient_error();

        match handle_batch(&mut source, &mut host, acknowledgement, messages).await {
            BatchDisposition::Accepted => {
                retry_delay = retry_policy.backoff;
            }
            BatchDisposition::Retry => {
                let delay = source_retry_delay(retry_delay);
                if !wait_for_retry(&mut host, &mut shutdown, delay).await {
                    break;
                }
                retry_delay = next_retry_delay(retry_delay, retry_policy);
            }
            BatchDisposition::Shutdown => break,
        }
    }

    flush_for_lifecycle(&mut host).await;
    if let Err(error) = source.close().await {
        host.report_error(error.to_string());
    }
    host.mark_unready();
}

fn batch_request(acknowledgement: SourceAckPolicy) -> SourceBatchRequest {
    match acknowledgement {
        SourceAckPolicy::None | SourceAckPolicy::Sequential { .. } => SourceBatchRequest {
            max_messages: NonZeroUsize::MIN,
            batch_timeout: None,
        },
        SourceAckPolicy::Parallel {
            max_in_flight,
            batch_timeout,
            ..
        } => SourceBatchRequest {
            max_messages: max_in_flight,
            batch_timeout: Some(batch_timeout),
        },
    }
}

async fn handle_batch<C>(
    source: &mut C,
    host: &mut SourceHost,
    acknowledgement: SourceAckPolicy,
    messages: Vec<C::Message>,
) -> BatchDisposition
where
    C: SourceConnector,
{
    let positions = messages
        .iter()
        .map(|message| message.position().clone())
        .collect::<Vec<_>>();
    let intake_messages = messages
        .iter()
        .map(|message| SourceIntakeMessage {
            payload: message.payload(),
            metadata: message.metadata(),
        })
        .collect();
    let mode = match acknowledgement {
        SourceAckPolicy::None => SourceIntakeMode::Unacknowledged,
        SourceAckPolicy::Sequential { .. } | SourceAckPolicy::Parallel { .. } => {
            SourceIntakeMode::Acknowledged
        }
    };
    let intake = host
        .intake(SourceIntakeBatch {
            messages: intake_messages,
            mode,
        })
        .await;
    let outcome = match intake {
        Ok(outcome) => outcome,
        Err(error) => {
            host.report_error(error.to_string());
            if mode == SourceIntakeMode::Acknowledged {
                reject_batch(source, host, &positions).await;
                return BatchDisposition::Retry;
            }
            return BatchDisposition::Accepted;
        }
    };

    let timeout = match acknowledgement {
        SourceAckPolicy::None => None,
        SourceAckPolicy::Sequential { timeout, .. } | SourceAckPolicy::Parallel { timeout, .. } => {
            Some(timeout)
        }
    };
    if let Some(timeout) = timeout {
        if outcome.acknowledgements.len() != positions.len() {
            host.report_error(format!(
                "source intake returned {} ACKs for {} messages",
                outcome.acknowledgements.len(),
                positions.len(),
            ));
            reject_batch(source, host, &positions).await;
            return BatchDisposition::Retry;
        }
        for acknowledgement in outcome.acknowledgements {
            tokio::task::consume_budget().await;
            match acknowledgement.wait(timeout).await {
                SourceAcknowledgementOutcome::Ack => {}
                SourceAcknowledgementOutcome::NoAck(reason) => {
                    host.handle_ack_failure(reason);
                    reject_batch(source, host, &positions).await;
                    return BatchDisposition::Retry;
                }
                SourceAcknowledgementOutcome::Shutdown => return BatchDisposition::Shutdown,
            }
        }
    } else if !outcome.acknowledgements.is_empty() {
        host.report_error("unacknowledged source intake returned ACK handles".to_string());
    }

    if let Err(error) = source.acknowledge(&positions).await {
        host.report_error(error.to_string());
        reject_batch(source, host, &positions).await;
        return BatchDisposition::Retry;
    }
    BatchDisposition::Accepted
}

async fn reject_batch<C>(source: &mut C, host: &mut SourceHost, positions: &[C::Position])
where
    C: SourceConnector,
{
    if let Err(error) = source.reject(positions).await {
        host.report_error(error.to_string());
    }
}

async fn flush_for_lifecycle(host: &mut SourceHost) {
    if let Err(error) = host.flush().await {
        host.report_error(error.to_string());
    }
}

fn source_retry_delay(delay: Duration) -> Duration {
    delay.max(SOURCE_ERROR_RETRY)
}

async fn wait_for_retry(
    host: &mut SourceHost,
    shutdown: &mut watch::Receiver<bool>,
    delay: Duration,
) -> bool {
    tokio::select! {
        changed = shutdown.changed() => !(changed.is_err() || *shutdown.borrow()),
        _ = host.wait_for_quiesce_change() => true,
        _ = sleep(delay) => true,
    }
}

async fn wait_for_flush(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, future};

    use nervix_connector::{IngestMessageHeaders, IngestMetadataRow, SourceError, SourceResult};
    use parking_lot::Mutex;

    use super::*;

    #[derive(Default)]
    struct SourceLoopObservations {
        requests: Vec<SourceBatchRequest>,
        intake: Vec<(SourceIntakeMode, usize)>,
        acknowledged: Vec<Vec<u64>>,
        rejected: Vec<Vec<u64>>,
        ack_waits: usize,
        resumes: usize,
        ready: usize,
        unready: usize,
    }

    struct FakeMessage {
        position: u64,
        payload: Vec<u8>,
    }

    impl IngestMessageHeaders for FakeMessage {
        fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
    }

    impl SourceMessage for FakeMessage {
        type Position = u64;

        fn payload(&self) -> &[u8] {
            &self.payload
        }

        fn position(&self) -> &Self::Position {
            &self.position
        }

        fn headers(&self) -> &dyn IngestMessageHeaders {
            self
        }

        fn metadata(&self) -> IngestMetadataRow<'_> {
            IngestMetadataRow::Headers { headers: self }
        }
    }

    struct FakeSource {
        messages: VecDeque<FakeMessage>,
        resume_required: bool,
        observations: Arc<Mutex<SourceLoopObservations>>,
    }

    #[async_trait]
    impl SourceConnector for FakeSource {
        type Plan = ();
        type Message = FakeMessage;
        type Position = u64;

        async fn open(_plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
            Err(Report::new(SourceError::Open { connector: "fake" }))
        }

        async fn next_batch(
            &mut self,
            request: SourceBatchRequest,
        ) -> SourceResult<SourceBatch<Self::Message>> {
            self.observations.lock().requests.push(request);
            if self.resume_required {
                self.resume_required = false;
                return Ok(SourceBatch::ResumeRequired);
            }
            if self.messages.is_empty() {
                return Ok(SourceBatch::Closed);
            }
            let count = self.messages.len().min(request.max_messages.get());
            let mut messages = Vec::with_capacity(count);
            for _ in 0..count {
                messages.push(
                    self.messages
                        .pop_front()
                        .verified("the count is bounded by the queue length above"),
                );
            }
            Ok(SourceBatch::Messages(messages))
        }

        async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
            self.observations
                .lock()
                .acknowledged
                .push(positions.to_vec());
            Ok(())
        }

        async fn reject(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
            self.observations.lock().rejected.push(positions.to_vec());
            Ok(())
        }

        async fn resume(&mut self) -> SourceResult<SourceResume> {
            self.observations.lock().resumes += 1;
            Ok(SourceResume::Ready)
        }
    }

    struct ImmediateAcknowledgement {
        observations: Arc<Mutex<SourceLoopObservations>>,
    }

    #[async_trait]
    impl SourceAcknowledgementServices for ImmediateAcknowledgement {
        async fn wait(self: Box<Self>, _timeout: Duration) -> SourceAcknowledgementOutcome {
            self.observations.lock().ack_waits += 1;
            SourceAcknowledgementOutcome::Ack
        }
    }

    struct FakeHost {
        observations: Arc<Mutex<SourceLoopObservations>>,
    }

    #[async_trait]
    impl SourceHostServices for FakeHost {
        async fn intake(
            &mut self,
            batch: SourceIntakeBatch<'_>,
        ) -> SourceIntakeResult<SourceIntakeOutcome> {
            let count = batch.messages.len();
            self.observations.lock().intake.push((batch.mode, count));
            let acknowledgements = match batch.mode {
                SourceIntakeMode::Unacknowledged => Vec::new(),
                SourceIntakeMode::Acknowledged => (0..count)
                    .map(|_| {
                        SourceAcknowledgement::new(ImmediateAcknowledgement {
                            observations: self.observations.clone(),
                        })
                    })
                    .collect(),
            };
            Ok(SourceIntakeOutcome { acknowledgements })
        }

        async fn flush(&mut self) -> SourceIntakeResult<()> {
            Ok(())
        }

        async fn replay_buffered(&mut self) -> SourceIntakeResult<bool> {
            Ok(false)
        }

        fn next_flush(&self) -> Option<Instant> {
            None
        }

        fn should_suspend_intake(&self) -> bool {
            false
        }

        async fn wait_for_quiesce_change(&mut self) {
            future::pending().await
        }

        async fn wait_until_not_suspended(&mut self) {
            future::pending().await
        }

        async fn wait_until_active(&mut self) -> bool {
            true
        }

        fn mark_ready(&self) {
            self.observations.lock().ready += 1;
        }

        fn mark_unready(&self) {
            self.observations.lock().unready += 1;
        }

        fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}
        fn clear_transient_error(&self) {}
        fn report_error(&self, message: String) {
            panic!("unexpected source loop error: {message}");
        }
        fn handle_ack_failure(&self, reason: String) {
            panic!("unexpected source ACK failure: {reason}");
        }
    }

    async fn run_policy_with_refresh(
        policy: SourceAckPolicy,
        resume_required: bool,
    ) -> Arc<Mutex<SourceLoopObservations>> {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let source = FakeSource {
            messages: (0..3)
                .map(|position| FakeMessage {
                    position,
                    payload: vec![
                        u8::try_from(position).verified("the test positions are all below 256"),
                    ],
                })
                .collect(),
            resume_required,
            observations: observations.clone(),
        };
        let host = SourceHost::new(FakeHost {
            observations: observations.clone(),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        run_source_instance(source, host, policy, shutdown_rx).await;
        drop(shutdown_tx);
        observations
    }

    async fn run_policy(policy: SourceAckPolicy) -> Arc<Mutex<SourceLoopObservations>> {
        run_policy_with_refresh(policy, false).await
    }

    fn retry_policy() -> nervix_connector::ParsedRetryPolicy {
        nervix_connector::ParsedRetryPolicy {
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        }
    }

    #[tokio::test]
    async fn source_loop_accepts_no_ack_messages_one_at_a_time() {
        let observations = run_policy(SourceAckPolicy::None).await;
        let observations = observations.lock();
        assert_eq!(
            observations.intake,
            vec![(SourceIntakeMode::Unacknowledged, 1); 3]
        );
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
        assert_eq!(observations.ack_waits, 0);
        assert!(observations.rejected.is_empty());
    }

    #[tokio::test]
    async fn source_loop_waits_and_commits_each_sequential_ack() {
        let observations = run_policy(SourceAckPolicy::Sequential {
            timeout: Duration::from_secs(1),
            retry: retry_policy(),
        })
        .await;
        let observations = observations.lock();
        assert_eq!(
            observations.intake,
            vec![(SourceIntakeMode::Acknowledged, 1); 3]
        );
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
        assert_eq!(observations.ack_waits, 3);
        assert!(observations.rejected.is_empty());
    }

    #[tokio::test]
    async fn source_loop_commits_one_parallel_ack_batch() {
        let observations = run_policy(SourceAckPolicy::Parallel {
            max_in_flight: NonZeroUsize::new(3).verified("three is a nonzero source batch bound"),
            batch_timeout: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            retry: retry_policy(),
        })
        .await;
        let observations = observations.lock();
        assert_eq!(
            observations.intake,
            vec![(SourceIntakeMode::Acknowledged, 3)]
        );
        assert_eq!(observations.acknowledged, vec![vec![0, 1, 2]]);
        assert_eq!(observations.ack_waits, 3);
        assert_eq!(observations.requests[0].max_messages.get(), 3);
        assert_eq!(
            observations.requests[0].batch_timeout,
            Some(Duration::from_millis(10))
        );
        assert!(observations.rejected.is_empty());
    }

    #[tokio::test]
    async fn source_loop_resumes_after_connector_configuration_changes() {
        let observations = run_policy_with_refresh(SourceAckPolicy::None, true).await;
        let observations = observations.lock();
        assert_eq!(observations.resumes, 2);
        assert_eq!(observations.ready, 2);
        assert_eq!(observations.unready, 2);
        assert_eq!(observations.intake.len(), 3);
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
    }
}
