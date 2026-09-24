//! Publishing an emitter's buffered work through its connector.
//!
//! Layer: data plane.
//! - **Owns.** Whether the emitter holds an open connector, flushing its buffer through that
//!   connector on its cadence, a retry or a drain, committing what the connector staged, the
//!   fault-injection checks and stop deadline that bound every attempt, and applying each write's
//!   per-record outcome to the buffered batches.
//! - **Depends on.** The emitter's buffer and retry schedule, the composition root that opens its
//!   connector, the connector contract's lifecycle hooks and outcomes, and the node's
//!   message-error handling.
//! - **Must not know.** Which sink crate a connector comes from, how its input is encoded or
//!   mapped, or how the emitter task receives its input.

use async_trait::async_trait;
use error_stack::ResultExt as _;
use nervix_connector::{
    PerRecordOutcome, SinkCommitReport, SinkDeadline, SinkLifecycle, SinkPublishError,
    SinkRecordPosition, physical_time::PhysicalDeadlineCapability,
};

use super::*;

pub(super) struct EmitterPublishControl<'a> {
    pub(super) fault_injection: &'a ConfiguredFaultInjection,
    pub(super) shutdown_rx: &'a mut watch::Receiver<bool>,
    pub(super) stop_rx: &'a mut watch::Receiver<Option<Instant>>,
    pub(super) backoff: &'a mut RuntimeReconnectBackoff,
}

async fn await_until_emitter_stop_deadline<T>(
    stop_rx: &mut watch::Receiver<Option<Instant>>,
    future: impl std::future::Future<Output = T>,
) -> Result<T, ()> {
    tokio::pin!(future);
    loop {
        tokio::task::consume_budget().await;
        let stop_deadline = *stop_rx.borrow();
        if let Some(deadline) = stop_deadline {
            return tokio::time::timeout_at(deadline, &mut future)
                .await
                .map_err(|_| ());
        }
        tokio::select! {
            output = &mut future => return Ok(output),
            changed = stop_rx.changed() => {
                if changed.is_err() {
                    return Ok(future.await);
                }
            }
        }
    }
}

fn emitter_stop_deadline_elapsed() -> Report<EmitterRuntimeError> {
    Report::new(EmitterRuntimeError::StopDeadlineElapsed)
        .attach_printable("emitter stop deadline elapsed while publishing or retrying")
}

/// Why the host is asking a sink to publish what it staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkCommitReason {
    /// Only the sink's own commit deadline releases what it staged.
    Cadence,
    /// A retry publishes everything staged, and a shutdown that cuts its backoff short leaves the
    /// rest for the attempt after it.
    Retry,
    /// A drain publishes everything staged, and a shutdown that cuts its backoff short fails it.
    Drain,
}

impl SinkCommitReason {
    fn forces_commit(self) -> bool {
        match self {
            Self::Cadence => false,
            Self::Retry | Self::Drain => true,
        }
    }

    /// This reason once an attempt has already started committing, which every further attempt
    /// finishes whatever the cadence says.
    fn forced(self) -> Self {
        match self {
            Self::Cadence => Self::Retry,
            Self::Retry => Self::Retry,
            Self::Drain => Self::Drain,
        }
    }
}

pub(super) struct RejectedEmitterRecord {
    pub(super) position: SinkRecordPosition,
    pub(super) reason: String,
    pub(super) structured_error: Option<StructuredMessageError>,
}

type EmitterPublishResult = Result<Option<PublishReport>, EmitterPublishFailure>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EmitterPublishBatchOwner {
    Caller,
    Buffer,
    Sink,
}

pub(super) struct EmitterPublishFailure {
    pub(super) error: Report<EmitterRuntimeError>,
    pub(super) batch_owner: EmitterPublishBatchOwner,
}

impl EmitterPublishFailure {
    fn caller(error: Report<EmitterRuntimeError>) -> Self {
        Self {
            error,
            batch_owner: EmitterPublishBatchOwner::Caller,
        }
    }

    fn buffer(error: Report<EmitterRuntimeError>) -> Self {
        Self {
            error,
            batch_owner: EmitterPublishBatchOwner::Buffer,
        }
    }

    fn sink(error: Report<EmitterRuntimeError>) -> Self {
        Self {
            error,
            batch_owner: EmitterPublishBatchOwner::Sink,
        }
    }

    pub(super) fn drain_failed_batches(
        self,
        current: &mut Option<EmitterPublishBatch>,
        buffer: &mut EmitterBatchBuffer,
    ) -> (Report<EmitterRuntimeError>, Vec<EmitterPublishBatch>) {
        let batches = match self.batch_owner {
            EmitterPublishBatchOwner::Caller => {
                let mut batches = buffer.drain_pending();
                batches.extend(current.take());
                batches
            }
            EmitterPublishBatchOwner::Buffer => {
                current.take();
                buffer.drain_pending()
            }
            EmitterPublishBatchOwner::Sink => {
                current.take();
                Vec::new()
            }
        };
        (self.error, batches)
    }
}

pub(super) async fn await_emitter_confirmation<F>(
    acks: &impl AcknowledgementKeepalive,
    future: F,
) -> F::Output
where
    F: std::future::Future,
{
    tokio::pin!(future);
    loop {
        tokio::task::consume_budget().await;
        acks.keep_alive();
        tokio::select! {
            result = &mut future => return result,
            _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {}
        }
    }
}

/// A connector as the emitter task drives it, whichever sink contract it implements.
///
/// The composition root pairs every connector with what the host prepares its input with, so the
/// task neither names a sink crate nor learns which contract a sink implements. The task makes one
/// call per flush, and the connector receives one virtual call per batch, never one per row.
#[async_trait]
pub(super) trait EmitterSink: Send {
    /// The hooks through which the host drives the connector's lifecycle, which both sink
    /// contracts share.
    fn lifecycle(&self) -> &dyn SinkLifecycle;

    fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle;

    /// Prepares every batch the emitter released for this connector's contract and writes it.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()>;
}

/// This emitter's connector, as its last attempt to open one left it.
pub(super) enum EmitterSinkState {
    /// The connector this emitter publishes through.
    Open(Box<dyn EmitterSink>),
    /// No connector could be opened, for this reason. The emitter retries on its declared
    /// backoff, and until then a publish attempt fails as one without a client.
    Unavailable { reason: String },
}

impl EmitterSinkState {
    /// Opens this emitter's connector, unless its task is told to stop first.
    pub(super) async fn open_until_cancelled(
        plan: &EmitterStartPlan,
        context: &EmitterSinkContext,
        input_schema: &CompiledSchema,
        codec: Option<&Arc<CompiledCodec>>,
        work_cancel_rx: &mut watch::Receiver<bool>,
    ) -> Self {
        tokio::select! {
            biased;
            _ = wait_for_emitter_work_cancel(work_cancel_rx) => Self::Unavailable {
                reason: "emitter sink initialization canceled while stopping".to_string(),
            },
            sink = Self::open(plan, context, input_schema, codec) => sink,
        }
    }

    /// Opens this emitter's connector through the composition root, reporting a connector that
    /// could not be opened as this emitter's initialization failure.
    async fn open(
        plan: &EmitterStartPlan,
        context: &EmitterSinkContext,
        input_schema: &CompiledSchema,
        codec: Option<&Arc<CompiledCodec>>,
    ) -> Self {
        match EmitterSinkStarter::start(plan, context, input_schema, codec).await {
            Ok(sink) => Self::Open(sink),
            Err(error) => {
                let reason = emitter_error_message(&error);
                context.report_init_error(plan.sink.label(), &reason);
                Self::Unavailable { reason }
            }
        }
    }

    /// The wake that releases this emitter's buffered work on its own flush cadence and its
    /// sink's staged work on that sink's commit boundary.
    pub(super) fn cadence_wake(
        &self,
        clock: &DomainClock,
        buffer: &EmitterBatchBuffer,
    ) -> RuntimeWake {
        let wake = match buffer.deadline() {
            Some(deadline) => RuntimeWake::never().with_buffer(clock, deadline),
            None => RuntimeWake::never(),
        };
        match self.commit_deadline() {
            Some(SinkDeadline::Domain(due_at)) => wake.with_buffer(
                clock,
                BranchBufferDeadline::Logical(clock.deadline_at(due_at)),
            ),
            Some(SinkDeadline::Physical(deadline)) => wake.with_physical(deadline),
            None => wake,
        }
    }

    /// When the staged work this sink holds has to be published.
    fn commit_deadline(&self) -> Option<SinkDeadline> {
        match self {
            Self::Open(sink) => sink.lifecycle().commit_deadline(),
            Self::Unavailable { .. } => None,
        }
    }

    /// Whether this sink publishes what it accepts later, on its own commit boundary.
    ///
    /// Such a sink neither acknowledges a row nor counts it as sent when the host's write returns:
    /// its commit does both.
    fn publishes_on_commit(&self) -> bool {
        match self {
            Self::Open(sink) => sink.lifecycle().retains_acknowledgements(),
            Self::Unavailable { .. } => false,
        }
    }

    /// How many messages this sink staged out of the host's buffer and has not published yet.
    pub(super) fn staged_messages(&self) -> u64 {
        match self {
            Self::Open(sink) => sink.lifecycle().staged_messages(),
            Self::Unavailable { .. } => 0,
        }
    }

    pub(super) fn unavailable_reason(&self) -> Option<&str> {
        match self {
            Self::Open(_) => None,
            Self::Unavailable { reason } => Some(reason.as_str()),
        }
    }

    fn requires_publish_failure_reinitialization(&self) -> bool {
        match self {
            Self::Open(sink) => !sink.lifecycle().keeps_client_on_publish_failure(),
            Self::Unavailable { .. } => true,
        }
    }

    pub(super) fn pending_acks(&self, buffer: &EmitterBatchBuffer) -> EmitterAcknowledgements {
        let sink = match self {
            Self::Open(sink) => sink.lifecycle().pending_acks(),
            Self::Unavailable { .. } => None,
        };
        EmitterAcknowledgements {
            runtime: buffer.pending_acks(),
            sink,
        }
    }

    pub(super) async fn finish_transport(&mut self, deadline: Instant) -> EmitterRuntimeResult<()> {
        match self {
            Self::Open(sink) => sink
                .lifecycle_mut()
                .finish(deadline)
                .await
                .map_err(sink_publish_failure),
            Self::Unavailable { .. } => Ok(()),
        }
    }

    pub(super) fn reconnect_after(&self, error: &Report<EmitterRuntimeError>) -> bool {
        if let EmitterRuntimeError::PublishStalled = error.current_context() {
            false
        } else {
            self.requires_publish_failure_reinitialization()
        }
    }

    pub(super) async fn flush_due(
        &mut self,
        label: &str,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &mut EmitterBatchBuffer,
        retry: bool,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let flushed = if buffer.should_flush(context, retry)? {
            self.flush_buffer(context, control, buffer).await?
        } else {
            None
        };
        let reason = match retry {
            true => SinkCommitReason::Retry,
            false => SinkCommitReason::Cadence,
        };
        let committed = self
            .commit_staged(label, context, control, buffer, reason)
            .await?;
        Ok(PublishReport::merge_optional(flushed, committed))
    }

    pub(super) async fn flush_all(
        &mut self,
        label: &str,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &mut EmitterBatchBuffer,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let flushed;
        loop {
            tokio::task::consume_budget().await;
            match self.flush_buffer(context, control, buffer).await {
                Ok(report) => {
                    control.backoff.reset();
                    context
                        .runtime
                        .clear_emitter_transient_error(&context.domain, &context.emitter);
                    flushed = report;
                    break;
                }
                Err(error)
                    if error.current_context() == &EmitterRuntimeError::StopDeadlineElapsed =>
                {
                    return Err(error);
                }
                Err(error) if emitter_publish_error_is_retryable(&error) => {
                    let reason = emitter_error_message(&error);
                    let wait = emitter_retry_delay(control.backoff, &error);
                    context.runtime.record_emitter_transient_error_with_backoff(
                        &context.domain,
                        &context.emitter,
                        reason.clone(),
                        wait,
                    );
                    context.report_flush_error(label, &reason);
                    let waited = await_until_emitter_stop_deadline(
                        control.stop_rx,
                        RuntimeReconnectBackoff::wait_duration_with_ack_alive(
                            wait,
                            control.shutdown_rx,
                            &buffer.pending_acks(),
                        ),
                    )
                    .await
                    .map_err(|()| emitter_stop_deadline_elapsed())?;
                    if !waited {
                        return Err(Report::new(EmitterRuntimeError::ShutdownWhileStalled));
                    }
                }
                Err(error) => {
                    context.report_flush_error(label, &emitter_error_message(&error));
                    return Err(error);
                }
            }
        }
        let committed = self
            .commit_staged(label, context, control, buffer, SinkCommitReason::Drain)
            .await?;
        Ok(PublishReport::merge_optional(flushed, committed))
    }

    pub(super) async fn publish_batch(
        &mut self,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &mut EmitterBatchBuffer,
        batch: EmitterPublishBatch,
    ) -> EmitterPublishResult {
        if !buffer
            .push(context, batch)
            .map_err(EmitterPublishFailure::caller)?
        {
            return Ok(None);
        }
        let flushed = self
            .flush_buffer(context, control, buffer)
            .await
            .map_err(EmitterPublishFailure::buffer)?;
        // The write may have reached the sink's commit boundary, which publishes here rather than
        // waiting for the next wake. Its failure belongs to the sink: the rows it staged are no
        // longer in this buffer.
        let committed = self
            .commit_staged_once(context, control, buffer, SinkCommitReason::Cadence)
            .await
            .map_err(EmitterPublishFailure::sink)?;
        Ok(PublishReport::merge_optional(flushed, committed))
    }

    /// Whether a sink-owned deadline has been reached.
    fn sink_deadline_reached(
        context: &EmitterSinkContext,
        deadline: SinkDeadline,
    ) -> EmitterRuntimeResult<bool> {
        match deadline {
            SinkDeadline::Domain(due_at) => {
                let snapshot = context.execution_snapshot()?;
                context
                    .clock
                    .deadline_reached(&context.clock.deadline_at(due_at), &snapshot)
                    .change_context(EmitterRuntimeError::FlushTiming)
            }
            SinkDeadline::Physical(deadline) => {
                Ok(PhysicalDeadlineCapability::operational().is_reached(deadline))
            }
        }
    }

    /// Publishes what the sink staged when its commit boundary is reached, retrying a failed
    /// commit on the emitter's declared backoff while the acknowledgements it holds stay alive.
    async fn commit_staged(
        &mut self,
        label: &str,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &EmitterBatchBuffer,
        reason: SinkCommitReason,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let mut reason = reason;
        loop {
            tokio::task::consume_budget().await;
            match self
                .commit_staged_once(context, control, buffer, reason)
                .await
            {
                Ok(report) => {
                    control.backoff.reset();
                    context
                        .runtime
                        .clear_emitter_transient_error(&context.domain, &context.emitter);
                    return Ok(report);
                }
                Err(error)
                    if error.current_context() == &EmitterRuntimeError::StopDeadlineElapsed =>
                {
                    return Err(error);
                }
                Err(error) if error.current_context().is_retryable_publish_failure() => {
                    let acks = self.pending_acks(buffer);
                    if !Self::wait_for_commit_retry(label, context, control, &acks, &error).await? {
                        return match reason {
                            SinkCommitReason::Drain => {
                                Err(Report::new(EmitterRuntimeError::ShutdownWhileStalled)
                                    .attach_printable(
                                        "emitter drain stopped while a staged commit remained \
                                         pending",
                                    ))
                            }
                            SinkCommitReason::Cadence | SinkCommitReason::Retry => Ok(None),
                        };
                    }
                    // An attempt the cadence started keeps retrying whatever is staged, exactly as
                    // the failure it is recovering from was already committing it.
                    reason = reason.forced();
                }
                Err(error) => {
                    context.report_flush_error(label, &emitter_error_message(&error));
                    return Err(error);
                }
            }
        }
    }

    /// One commit attempt, bounded by the emitter's stop deadline and keeping every
    /// acknowledgement the sink retained alive while the external system confirms it.
    async fn commit_staged_once(
        &mut self,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &EmitterBatchBuffer,
        reason: SinkCommitReason,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let Some(deadline) = self.commit_deadline() else {
            return Ok(None);
        };
        if !reason.forces_commit() && !Self::sink_deadline_reached(context, deadline)? {
            return Ok(None);
        }
        let acks = self.pending_acks(buffer);
        let committed = {
            let _confirmation_wait = context
                .runtime
                .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
            let commit = Box::pin(self.commit_sink());
            await_until_emitter_stop_deadline(
                control.stop_rx,
                await_emitter_confirmation(&acks, commit),
            )
            .await
            .map_err(|()| emitter_stop_deadline_elapsed())?
        };
        buffer.report_staged_messages(self.staged_messages());
        let report = committed?.map(|report| {
            PublishReport::flushed(report.messages, report.bytes, report.domain_timestamp)
        });
        Ok(report)
    }

    async fn commit_sink(&mut self) -> EmitterRuntimeResult<Option<SinkCommitReport>> {
        match self {
            Self::Open(sink) => sink
                .lifecycle_mut()
                .commit()
                .await
                .map_err(sink_publish_failure),
            Self::Unavailable { .. } => Ok(None),
        }
    }

    async fn flush_buffer(
        &mut self,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &mut EmitterBatchBuffer,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        if buffer.is_empty() {
            return Ok(None);
        }
        self.check_fault_injection(context, control)?;
        // A sink that stages what it accepts has not published anything yet, so its commit counts
        // these messages as sent and this write counts none.
        let report = match self.publishes_on_commit() {
            true => None,
            false => buffer.report(),
        };
        let pending_acks = buffer.pending_acks();
        {
            let _confirmation_wait = context
                .runtime
                .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
            let publish =
                Box::pin(self.publish_buffered_batches(context, buffer.pending.as_mut_slice()));
            let published = await_until_emitter_stop_deadline(
                control.stop_rx,
                await_emitter_confirmation(&pending_acks, publish),
            )
            .await;
            buffer.report_staged_messages(self.staged_messages());
            published.map_err(|()| emitter_stop_deadline_elapsed())??;
        }
        buffer.clear();
        Ok(report)
    }

    async fn publish_buffered_batches(
        &mut self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        match self {
            Self::Open(sink) => sink.publish_batches(context, batches).await,
            Self::Unavailable { .. } => Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable(
                    "emitter has no initialized sink client for its configured sink",
                )),
        }
    }

    fn check_fault_injection(
        &self,
        context: &EmitterSinkContext,
        control: &EmitterPublishControl<'_>,
    ) -> EmitterRuntimeResult<()> {
        if control
            .fault_injection
            .emitter_should_fail(&context.emitter)
        {
            let reason = format!(
                "fault injector failed emitter '{}'",
                context.emitter.as_str()
            );
            context.runtime.events().report_error(format!(
                "{} in domain '{}'",
                reason,
                context.domain.as_str()
            ));
            warn!(
                domain = context.domain.as_str(),
                emitter = context.emitter.as_str(),
                "fault injector failed emitter publish"
            );
            return Err(Report::new(EmitterRuntimeError::FaultInjected).attach_printable(reason));
        }
        if control
            .fault_injection
            .emitter_should_stall(&context.emitter)
        {
            return Err(Report::new(EmitterRuntimeError::PublishStalled)
                .attach_printable("fault injector stalled emitter publish"));
        }
        // An unavailable client is the one injected fault a sink recovers from on its own: the
        // publish fails the way an unreachable external system does, and the emitter retries it on
        // its declared backoff until the fault clears.
        if control
            .fault_injection
            .sink_client_is_unavailable(&context.emitter)
        {
            return Err(Report::new(EmitterRuntimeError::PublishBatch)
                .attach_printable("sink fault injector returned an unavailable client"));
        }
        Ok(())
    }

    async fn wait_for_commit_retry(
        sink: &str,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        acks: &EmitterAcknowledgements,
        error: &Report<EmitterRuntimeError>,
    ) -> EmitterRuntimeResult<bool> {
        let reason = emitter_error_message(error);
        let wait = control.backoff.next_delay();
        context.runtime.record_commit_failure_with_backoff(
            &context.domain,
            &context.emitter,
            reason.clone(),
            wait,
        );
        context.report_flush_error(sink, &reason);
        await_until_emitter_stop_deadline(
            control.stop_rx,
            control
                .backoff
                .wait_with_ack_alive(control.shutdown_rx, acks),
        )
        .await
        .map_err(|()| emitter_stop_deadline_elapsed())
    }
}

pub(super) fn emitter_unavailable_reason(
    sink: &EmitterSinkState,
    fault_injection: &ConfiguredFaultInjection,
    emitter: &EmitterName,
) -> Option<String> {
    if let Some(reason) = sink.unavailable_reason() {
        return Some(reason.to_owned());
    }
    if fault_injection.emitter_should_stall(emitter) {
        Some("fault injector stalled emitter publish".to_string())
    } else {
        None
    }
}

async fn wait_for_emitter_work_cancel(work_cancel_rx: &mut watch::Receiver<bool>) {
    loop {
        tokio::task::consume_budget().await;
        if *work_cancel_rx.borrow() {
            return;
        }
        if work_cancel_rx.changed().await.is_err() {
            return;
        }
    }
}

pub(super) async fn finish_record_sink_publish(
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
    outcome: PerRecordOutcome,
    acknowledgements: DeliveredAcknowledgements,
) -> EmitterRuntimeResult<()> {
    let outcome = outcome.into_parts();
    for SinkRecordPosition {
        batch_index,
        row_index,
    } in outcome.delivered
    {
        let batch = batches.get_mut(batch_index).ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "sink confirmation references missing emitter batch {batch_index}"
            ))
        })?;
        batch.mark_delivered(row_index, acknowledgements)?;
    }
    let rejected = outcome
        .rejected
        .into_iter()
        .map(|rejected| RejectedEmitterRecord {
            position: rejected.position,
            reason: String::new(),
            structured_error: Some(rejected.error),
        })
        .collect();
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Publish).await?;
    match outcome.infrastructure_error {
        Some(error) => Err(sink_publish_failure(error)),
        None => Ok(()),
    }
}

/// A connector's publish failure as this emitter's own, keeping a misconfigured sink out of the
/// retry loop it would never leave.
pub(super) fn sink_publish_failure(error: Report<SinkPublishError>) -> Report<EmitterRuntimeError> {
    match error.current_context() {
        SinkPublishError::Misconfigured { .. } => {
            error.change_context(EmitterRuntimeError::InvalidSinkConfig)
        }
        SinkPublishError::NotInitialized { .. }
        | SinkPublishError::Publish { .. }
        | SinkPublishError::Finish { .. }
        | SinkPublishError::Commit { .. } => {
            error.change_context(EmitterRuntimeError::PublishBatch)
        }
    }
}

pub(super) async fn finish_rejected_records(
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
    rejected: Vec<RejectedEmitterRecord>,
    operation: MessageErrorOperation,
) -> EmitterRuntimeResult<()> {
    for rejected in rejected {
        tokio::task::consume_budget().await;
        let SinkRecordPosition {
            batch_index,
            row_index,
        } = rejected.position;
        let batch = batches.get_mut(batch_index).ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "record rejection references missing emitter batch {batch_index}"
            ))
        })?;
        let execution_now = batch.execution_now;
        let error = if let Some(error) = rejected.structured_error {
            error
        } else {
            structured_message_error(
                execution_now,
                MessageErrorCode::External,
                rejected.reason,
                operation,
                None,
                std::iter::empty(),
            )
        };
        let record = batch.batch.runtime_row(row_index).map_err(|reason| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(reason)
        })?;
        let key = batch.batch.keys.get(row_index).cloned().ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "record rejection row {row_index} has no branch key"
            ))
        })?;
        let acks = batch.batch.acks.get(row_index).cloned().ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "record rejection row {row_index} has no acknowledgment set"
            ))
        })?;
        batch
            .mark_rejected_after_delivery(
                row_index,
                context
                    .runtime
                    .handle_structured_message_error(MessageErrorHandling {
                        domain: &context.domain,
                        node_kind: ModelKind::Emitter,
                        node: &ModelName::from(&context.emitter),
                        source_route: None,
                        policy: &context.error_policies.message,
                        message: RelayMessage { key, record, acks },
                        error,
                        partial_output: None,
                        materialized_state: HashMap::default(),
                        ingest_metadata: None,
                        execution_now,
                    }),
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nervix_connector::ParsedRetryPolicy;

    use super::*;
    use crate::runtime::test_fixtures::{input_batch, input_batch_with, input_value, sink_context};

    #[tokio::test]
    async fn queued_stop_bounds_an_active_infrastructure_retry() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, mut stop_rx) = watch::channel(None);
        let task_stop_signal = stop_signal.clone();
        let retry_started = Arc::new(Notify::new());
        let task_retry_started = retry_started.clone();
        let task = tokio::spawn(async move {
            let mut backoff = RuntimeReconnectBackoff::from_policy(ParsedRetryPolicy {
                backoff: Duration::from_secs(30),
                max_backoff: Duration::from_secs(30),
            });
            let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let (acks, _completion) = AckSet::root();
            task_retry_started.notify_one();
            let retry = backoff.wait_with_ack_alive(&mut shutdown_rx, &acks);
            assert!(
                await_until_emitter_stop_deadline(&mut stop_rx, retry)
                    .await
                    .is_err(),
                "the queued stop deadline must interrupt the active retry wait"
            );

            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("the stop command must remain queued while retrying");
            };
            task_stop_signal.send_replace(None);
            let _ = response.send(Err(Report::new(EmitterRuntimeError::StopDeadlineElapsed)
                .attach_printable("infrastructure retry exceeded drain deadline")));
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };
        retry_started.notified().await;

        let started = Instant::now();
        let failed = scheduled
            .stop(Duration::from_millis(40))
            .await
            .expect_err("the active infrastructure retry must fail the bounded drain");
        assert_eq!(
            failed.reason(),
            "infrastructure retry exceeded drain deadline"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a 30 second retry backoff must not hide a 40 millisecond drain deadline"
        );
        assert!(
            failed
                .into_task()
                .expect("the failed drain must retain the old task")
                .stop_signal
                .borrow()
                .is_none(),
            "a recoverable stop failure must clear its stop signal"
        );
    }

    /// A connector whose every write fails with a failure the emitter does not retry, so a test
    /// drives the publish path up to the connector and observes what the host keeps afterwards.
    struct UnencodableSink;

    impl SinkLifecycle for UnencodableSink {}

    #[async_trait]
    impl EmitterSink for UnencodableSink {
        fn lifecycle(&self) -> &dyn SinkLifecycle {
            self
        }

        fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle {
            self
        }

        async fn publish_batches(
            &mut self,
            _context: &EmitterSinkContext,
            _batches: &mut [EmitterPublishBatch],
        ) -> EmitterRuntimeResult<()> {
            Err(Report::new(EmitterRuntimeError::EncodeBatch)
                .attach_printable("the test sink encodes no record"))
        }
    }

    #[tokio::test]
    async fn retry_wake_attempts_a_buffer_before_its_ordinary_deadline() {
        let fault_injection = ConfiguredFaultInjection::default();
        let mut backoff = RuntimeReconnectBackoff::default();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_stop_tx, mut stop_rx) = watch::channel(None);
        let mut control = EmitterPublishControl {
            fault_injection: &fault_injection,
            shutdown_rx: &mut shutdown_rx,
            stop_rx: &mut stop_rx,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let mut sink = EmitterSinkState::Open(Box::new(UnencodableSink));
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("retry batch must buffer");

        assert!(
            sink.flush_due("nats", &context, &mut control, &mut buffer, false)
                .await
                .expect("ordinary wake must remain idle")
                .is_none()
        );
        let error = match sink
            .flush_due("nats", &context, &mut control, &mut buffer, true)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("retry wake must attempt the buffered batch"),
        };

        assert_eq!(*error.current_context(), EmitterRuntimeError::EncodeBatch);
        assert_eq!(buffer.pending.len(), 1);
    }

    #[test]
    fn a_started_commit_keeps_forcing_itself_through_every_retry() {
        assert!(!SinkCommitReason::Cadence.forces_commit());
        assert!(SinkCommitReason::Retry.forces_commit());
        assert!(SinkCommitReason::Drain.forces_commit());

        assert_eq!(SinkCommitReason::Cadence.forced(), SinkCommitReason::Retry);
        assert_eq!(SinkCommitReason::Retry.forced(), SinkCommitReason::Retry);
        assert_eq!(SinkCommitReason::Drain.forced(), SinkCommitReason::Drain);
    }

    #[tokio::test]
    async fn flush_all_returns_the_failure_and_retains_unpublished_batches() {
        let fault_injection = ConfiguredFaultInjection::default();
        let mut backoff = RuntimeReconnectBackoff::default();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_stop_tx, mut stop_rx) = watch::channel(None);
        let mut control = EmitterPublishControl {
            fault_injection: &fault_injection,
            shutdown_rx: &mut shutdown_rx,
            stop_rx: &mut stop_rx,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let mut sink = EmitterSinkState::Open(Box::new(UnencodableSink));
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer
            .push(
                &sink_context(),
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("batch must buffer");

        let error = match sink
            .flush_all("nats", &context, &mut control, &mut buffer)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("an unencodable buffered batch must fail final flush"),
        };

        assert_eq!(*error.current_context(), EmitterRuntimeError::EncodeBatch);
        assert_eq!(buffer.pending.len(), 1);
        assert_eq!(buffer.pending[0].message_count(), 1);
    }

    #[test]
    fn publish_failure_drains_exactly_the_batches_owned_by_the_buffer() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        let context = sink_context();
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(1, 0, AckSet::empty()),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("older batch must buffer");
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(2, 0, AckSet::empty()),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("current clone must buffer");
        let mut current = Some(EmitterPublishBatch::from_batch(
            input_batch_with(2, 0, AckSet::empty()),
            Timestamp::from_unix_nanos(100),
        ));
        let failure = EmitterPublishFailure::buffer(Report::new(EmitterRuntimeError::EncodeBatch));

        let (error, failed) = failure.drain_failed_batches(&mut current, &mut buffer);

        assert_eq!(*error.current_context(), EmitterRuntimeError::EncodeBatch);
        assert!(current.is_none());
        assert!(buffer.is_empty());
        assert_eq!(
            failed.len(),
            2,
            "the current caller clone must not be duplicated"
        );
        assert_eq!(input_value(&failed[0].batch), 1);
        assert_eq!(input_value(&failed[1].batch), 2);
    }

    #[test]
    fn caller_owned_publish_failure_includes_current_after_older_buffered_batches() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer
            .push(
                &sink_context(),
                EmitterPublishBatch::from_batch(
                    input_batch_with(1, 0, AckSet::empty()),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("older batch must buffer");
        let mut current = Some(EmitterPublishBatch::from_batch(
            input_batch_with(2, 0, AckSet::empty()),
            Timestamp::from_unix_nanos(100),
        ));
        let failure = EmitterPublishFailure::caller(Report::new(
            EmitterRuntimeError::FlushPolicyNotInitialized,
        ));

        let (_error, failed) = failure.drain_failed_batches(&mut current, &mut buffer);

        assert!(current.is_none());
        assert!(buffer.is_empty());
        assert_eq!(failed.len(), 2);
        assert_eq!(input_value(&failed[0].batch), 1);
        assert_eq!(input_value(&failed[1].batch), 2);
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn buffering_does_not_wait_for_sink_fault_until_a_flush_is_required() {
        let fault_injection = ConfiguredFaultInjection::default();
        fault_injection.fail_emitter("output");
        let mut backoff = RuntimeReconnectBackoff::default();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_stop_tx, mut stop_rx) = watch::channel(None);
        let mut control = EmitterPublishControl {
            fault_injection: &fault_injection,
            shutdown_rx: &mut shutdown_rx,
            stop_rx: &mut stop_rx,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let mut sink = EmitterSinkState::Unavailable {
            reason: "test sink intentionally has no client".to_string(),
        };
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });

        let published = match sink
            .publish_batch(
                &context,
                &mut control,
                &mut buffer,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .await
        {
            Ok(published) => published,
            Err(failure) => panic!(
                "a batch below the flush boundary must only be buffered: {}",
                emitter_error_message(&failure.error)
            ),
        };

        assert!(published.is_none());
        assert_eq!(buffer.pending.len(), 1);
        assert_eq!(buffer.pending[0].message_count(), 1);
    }
}
