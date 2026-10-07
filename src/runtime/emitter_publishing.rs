//! Publishing an emitter's buffered work through its connector.
//!
//! Layer: data plane.
//! - **Owns.** Whether the emitter holds an open connector, flushing its buffer through that
//!   connector on its cadence, a retry or a drain, the commit cadence of what the connector staged
//!   and committing it, the fault-injection checks and stop deadline that bound every attempt, and
//!   delivering the message errors of the rows a write rejected.
//! - **Depends on.** The emitter's buffer and retry schedule, the composition root that opens its
//!   connector, the connector contract's lifecycle hooks and outcomes, and the node's
//!   message-error handling.
//! - **Must not know.** Which sink crate a connector comes from, how its input is encoded or
//!   mapped, or how the emitter task receives its input.

use async_trait::async_trait;
use error_stack::ResultExt as _;
use nervix_connector::{
    PerRecordOutcome, RejectedSinkRecord, SinkLifecycle, SinkPublishError, SinkRecordPosition,
    SinkStagedCommit,
};

use super::{emitter_supervision::EmitterConfirmationWaitGuard, *};

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
        nervix_primitives::task::consume_budget().await;
        let stop_deadline = *stop_rx.borrow();
        if let Some(deadline) = stop_deadline {
            return nervix_primitives::time::timeout_at(deadline, &mut future)
                .await
                .map_err(|_| ());
        }
        nervix_primitives::select! {
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
    /// Only the commit cadence of what the sink staged, or the sink reaching its size boundary,
    /// releases it.
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

#[derive(Debug)]
pub(super) struct RejectedEmitterRecord {
    pub(super) position: SinkRecordPosition,
    pub(super) reason: String,
    pub(super) structured_error: Option<StructuredMessageError>,
}

/// What the message error of a record rejected after its batch was admitted reads besides the
/// error itself.
pub(super) struct RejectedRecordInput {
    /// The record the error handler reads as `input`.
    pub(super) record: RuntimeRow,
    /// The attempted output the error handler reads as `partial_output`, when there is one.
    pub(super) partial_output: Option<RuntimeRecordBatch>,
    /// The materialized state the error handler reads.
    pub(super) materialized_state: HashMap<String, RuntimeValue>,
}

pub(super) type EmitterPublishResult = Result<Option<PublishReport>, EmitterPublishFailure>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EmitterPublishBatchOwner {
    Caller,
    Buffer,
    Sink,
}

pub(super) struct EmitterPublishFailure {
    error: Report<EmitterRuntimeError>,
    batch_owner: EmitterPublishBatchOwner,
}

impl EmitterPublishFailure {
    pub(super) fn caller(error: Report<EmitterRuntimeError>) -> Self {
        Self {
            error,
            batch_owner: EmitterPublishBatchOwner::Caller,
        }
    }

    pub(super) fn buffer(error: Report<EmitterRuntimeError>) -> Self {
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

    pub(super) fn error(&self) -> &Report<EmitterRuntimeError> {
        &self.error
    }

    pub(super) fn into_parts(self) -> (Report<EmitterRuntimeError>, EmitterPublishBatchOwner) {
        (self.error, self.batch_owner)
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
        nervix_primitives::task::consume_budget().await;
        acks.keep_alive();
        nervix_primitives::select! {
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
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained sink host invokes publication, retry, acknowledgement and commit \
                  callbacks"
    )
)]
pub(super) trait EmitterSink: Send {
    /// The hooks through which the host drives the connector's lifecycle, which both sink
    /// contracts share.
    fn lifecycle(&self) -> &dyn SinkLifecycle;

    fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle;

    /// Prepares every batch the emitter released for this connector's contract and writes it,
    /// together with the batch payloads earlier attempts retained.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()>;
}

/// This emitter's connector, as its last attempt to open one left it.
pub(super) enum EmitterSinkState {
    /// The connector this emitter publishes through.
    Open(OpenEmitterSink),
    /// No connector could be opened, for this reason. The emitter retries on its declared
    /// backoff, and until then a publish attempt fails as one without a client.
    Unavailable { reason: String },
}

/// A connector the emitter holds open, and the commit cadence the host measures for what it staged.
///
/// The cadence belongs to the rows this connector staged, so a connector opened in its place,
/// which has staged nothing, starts without one.
pub(super) struct OpenEmitterSink {
    sink: Box<dyn EmitterSink>,
    /// The `COMMIT EACH` deadline of the staged rows, armed by the first cadence check that finds
    /// them staged and cleared once the connector holds nothing.
    commit_cadence: BranchBufferTimer,
}

impl EmitterSinkState {
    /// The state of an emitter whose connector just opened and has staged nothing yet.
    pub(super) fn opened(sink: Box<dyn EmitterSink>) -> Self {
        Self::Open(OpenEmitterSink {
            sink,
            commit_cadence: BranchBufferTimer::default(),
        })
    }

    /// Opens this emitter's connector, unless its task is told to stop first.
    pub(super) async fn open_until_cancelled(
        plan: &EmitterStartPlan,
        context: &EmitterSinkContext,
        input_schema: &CompiledSchema,
        output_schema: &Arc<CompiledSchema>,
        codec: Option<&Arc<CompiledCodec>>,
        work_cancel_rx: &mut watch::Receiver<bool>,
    ) -> Self {
        nervix_primitives::select! {
            biased;
            _ = wait_for_emitter_work_cancel(work_cancel_rx) => Self::Unavailable {
                reason: "emitter sink initialization canceled while stopping".to_string(),
            },
            sink = Self::open(plan, context, input_schema, output_schema, codec) => sink,
        }
    }

    /// Opens this emitter's connector through the composition root, reporting a connector that
    /// could not be opened as this emitter's initialization failure.
    async fn open(
        plan: &EmitterStartPlan,
        context: &EmitterSinkContext,
        input_schema: &CompiledSchema,
        output_schema: &Arc<CompiledSchema>,
        codec: Option<&Arc<CompiledCodec>>,
    ) -> Self {
        match EmitterSinkStarter::start(plan, context, input_schema, output_schema, codec).await {
            Ok(sink) => Self::opened(sink),
            Err(error) => {
                let reason = emitter_error_message(&error);
                context.report_init_error(plan.sink.label(), &reason);
                Self::Unavailable { reason }
            }
        }
    }

    /// The wake that releases this emitter's buffered work on its own flush cadence and its
    /// sink's staged work on its commit cadence.
    pub(super) fn cadence_wake(
        &self,
        clock: &DomainClock,
        buffer: &EmitterBatchBuffer,
    ) -> RuntimeWake {
        let wake = match buffer.deadline() {
            Some(deadline) => RuntimeWake::never().with_buffer(clock, deadline),
            None => RuntimeWake::never(),
        };
        match self.commit_cadence_deadline() {
            Some(deadline) => wake.with_buffer(clock, deadline),
            None => wake,
        }
    }

    /// When the commit cadence of the rows this sink staged ends, once a cadence check armed it.
    fn commit_cadence_deadline(&self) -> Option<BranchBufferDeadline> {
        match self {
            Self::Open(open) => open.commit_cadence.deadline(),
            Self::Unavailable { .. } => None,
        }
    }

    /// The domain time the commit cadence of the staged rows ends at, once a cadence check armed
    /// it.
    #[cfg(test)]
    fn commit_cadence_due_at(&self) -> Option<Timestamp> {
        let deadline = self.commit_cadence_deadline()?;
        match deadline {
            BranchBufferDeadline::Logical(deadline) => Some(deadline.due_at()),
            BranchBufferDeadline::Physical(_) => None,
        }
    }

    /// How many messages this sink staged out of the host's buffer and has not published yet.
    pub(super) fn staged_messages(&self) -> u64 {
        match self {
            Self::Open(open) => open.sink.lifecycle().staged_messages(),
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
            Self::Open(open) => !open.sink.lifecycle().keeps_client_on_publish_failure(),
            Self::Unavailable { .. } => true,
        }
    }

    pub(super) fn pending_acks(&self, buffer: &EmitterBatchBuffer) -> EmitterAcknowledgements {
        match self {
            Self::Open(open) => open.pending_acks(buffer),
            Self::Unavailable { .. } => EmitterAcknowledgements::from(buffer.pending_acks()),
        }
    }

    pub(super) async fn finish_transport(&mut self, deadline: Instant) -> EmitterRuntimeResult<()> {
        match self {
            Self::Open(open) => open
                .sink
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
            nervix_primitives::task::consume_budget().await;
            match self.flush_buffer(context, control, buffer).await {
                Ok(report) => {
                    control.backoff.reset();
                    context.status.clear();
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
                    context.record_retry(reason.clone(), wait, EmitterRetryKind::Infrastructure);
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
            nervix_primitives::task::consume_budget().await;
            match self
                .commit_staged_once(context, control, buffer, reason)
                .await
            {
                Ok(report) => {
                    control.backoff.reset();
                    context.status.clear();
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
        match self {
            Self::Open(open) => {
                open.commit_staged_once(context, control, buffer, reason)
                    .await
            }
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
        let pending_acks = buffer.pending_acks();
        {
            let _confirmation_wait =
                EmitterConfirmationWaitGuard::begin(&context.confirmation_waits);
            let publish =
                Box::pin(self.publish_buffered_batches(context, buffer.publication_mut()));
            let published = await_until_emitter_stop_deadline(
                control.stop_rx,
                await_emitter_confirmation(&pending_acks, publish),
            )
            .await;
            buffer.report_staged_messages(self.staged_messages());
            published.map_err(|()| emitter_stop_deadline_elapsed())??;
        }
        // Every buffered row is resolved now, and the rows the sink delivered are sent, however
        // many attempts delivered them. A sink that stages what it accepts has not published those
        // rows yet, so its commit counts them as sent and this write counts none.
        let report = buffer.delivered_report();
        buffer.clear();
        Ok(report)
    }

    async fn publish_buffered_batches(
        &mut self,
        context: &EmitterSinkContext,
        publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()> {
        match self {
            Self::Open(open) => open.sink.publish_batches(context, publication).await,
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
        context.record_retry(reason.clone(), wait, EmitterRetryKind::Commit);
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

impl OpenEmitterSink {
    fn pending_acks(&self, buffer: &EmitterBatchBuffer) -> EmitterAcknowledgements {
        EmitterAcknowledgements {
            runtime: buffer.pending_acks(),
            sink: self.sink.lifecycle().pending_acks(),
        }
    }

    /// One commit attempt of what the connector staged, once its commit is due or `reason` forces
    /// it.
    async fn commit_staged_once(
        &mut self,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &EmitterBatchBuffer,
        reason: SinkCommitReason,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let Some(staged) = self.sink.lifecycle().staged_commit() else {
            self.commit_cadence.clear();
            return Ok(None);
        };
        if !reason.forces_commit() && !self.commit_is_due(context, staged)? {
            return Ok(None);
        }
        let acks = self.pending_acks(buffer);
        let committed = {
            let _confirmation_wait =
                EmitterConfirmationWaitGuard::begin(&context.confirmation_waits);
            let commit = Box::pin(self.sink.lifecycle_mut().commit());
            await_until_emitter_stop_deadline(
                control.stop_rx,
                await_emitter_confirmation(&acks, commit),
            )
            .await
            .map_err(|()| emitter_stop_deadline_elapsed())?
        };
        buffer.report_staged_messages(self.sink.lifecycle().staged_messages());
        let committed = committed.map_err(sink_publish_failure)?;
        // The commit published everything staged, so the next rows staged start a cadence of
        // their own.
        self.commit_cadence.clear();
        let report = committed.map(|report| {
            PublishReport::flushed(report.messages, report.bytes, report.domain_timestamp)
        });
        Ok(report)
    }

    /// Whether the commit of what the connector staged is due: at once when it reached its size
    /// boundary, and otherwise once its cadence has passed since the first check that found it
    /// staged.
    ///
    /// The cadence is armed here rather than by the write that staged the rows: this check follows
    /// every write the emitter's own cadence releases, and a retry or a drain forces its commit
    /// without asking, so neither of them reads the domain clock.
    fn commit_is_due(
        &mut self,
        context: &EmitterSinkContext,
        staged: SinkStagedCommit,
    ) -> EmitterRuntimeResult<bool> {
        match staged {
            SinkStagedCommit::SizeReached => Ok(true),
            SinkStagedCommit::Cadence(interval) => {
                let snapshot = context.execution_snapshot()?;
                self.commit_cadence
                    .arm_logical(&context.clock, &snapshot, interval);
                self.commit_cadence
                    .is_due(&context.clock, &snapshot)
                    .change_context(EmitterRuntimeError::FlushTiming)
            }
        }
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

pub(super) async fn wait_for_emitter_work_cancel(work_cancel_rx: &mut watch::Receiver<bool>) {
    loop {
        nervix_primitives::task::consume_budget().await;
        if *work_cancel_rx.borrow() {
            return;
        }
        if work_cancel_rx.changed().await.is_err() {
            return;
        }
    }
}

impl EmitterSinkContext {
    /// One write's outcome as it reaches the emitter.
    ///
    /// A test build can stall the sink after it resolved the first records of a write: the
    /// answers for every later record are lost, as they are from a broker that stops answering
    /// after it accepted them, and the write fails as one whose outcome the emitter never
    /// learned. Any other build receives the outcome exactly as the sink reported it.
    pub(super) fn received_outcome<Id: Copy + Ord>(
        &self,
        records: usize,
        outcome: PerRecordOutcome<Id>,
    ) -> PerRecordOutcome<Id> {
        if records == 0 {
            return outcome;
        }
        let Some(resolved) = self
            .runtime
            .inner
            .fault_injection
            .take_emitter_sink_stall(&self.emitter)
        else {
            return outcome;
        };
        stalled_after_resolving(outcome, resolved)
    }
}

/// One answer a sink gave for one record of a write.
enum SinkAnswer<Id> {
    Delivered(Id),
    Rejected(RejectedSinkRecord<Id>),
}

impl<Id: Copy> SinkAnswer<Id> {
    fn id(&self) -> Id {
        match self {
            Self::Delivered(id) => *id,
            Self::Rejected(rejected) => rejected.id,
        }
    }
}

/// `outcome` as it arrives from a sink that stalled after it resolved `resolved` records: the
/// answers for every record after the first `resolved`, in the order the host handed them over,
/// never arrive, and the write fails as one whose outcome is unknown.
fn stalled_after_resolving<Id: Copy + Ord>(
    outcome: PerRecordOutcome<Id>,
    resolved: usize,
) -> PerRecordOutcome<Id> {
    let parts = outcome.into_parts();
    let mut answers = Vec::with_capacity(
        parts
            .delivered
            .len()
            .checked_add(parts.rejected.len())
            .assured("both counts total answers this node already holds in memory"),
    );
    for id in parts.delivered {
        answers.push(SinkAnswer::Delivered(id));
    }
    for rejected in parts.rejected {
        answers.push(SinkAnswer::Rejected(rejected));
    }
    answers.sort_by_key(SinkAnswer::id);
    let mut stalled = PerRecordOutcome::with_capacity(resolved);
    for answer in answers.into_iter().take(resolved) {
        match answer {
            SinkAnswer::Delivered(id) => stalled.deliver(id),
            SinkAnswer::Rejected(rejected) => stalled.reject(rejected),
        }
    }
    stalled.fail(
        Report::new(SinkPublishError::Publish { sink: "stalled" }).attach_printable(format!(
            "fault injector stalled the sink after it resolved {resolved} records"
        )),
    );
    stalled
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
        nervix_primitives::task::consume_budget().await;
        let SinkRecordPosition {
            batch_index,
            row_index,
        } = rejected.position;
        let batch = batches.get_mut(batch_index).ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "record rejection references missing emitter batch {batch_index}"
            ))
        })?;
        let execution_now = batch.execution_now();
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
        let RejectedRecordInput {
            record,
            partial_output,
            materialized_state,
        } = batch.rejected_record_input(row_index)?;
        let key = batch
            .relay_batch()
            .keys
            .get(row_index)
            .cloned()
            .ok_or_else(|| {
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "record rejection row {row_index} has no branch key"
                ))
            })?;
        let acks = batch
            .relay_batch()
            .acks
            .get(row_index)
            .cloned()
            .ok_or_else(|| {
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
                        routing: Some(&context.routing.load()),
                        domain: &context.domain,
                        node_kind: ModelKind::Emitter,
                        node: &ModelName::from(&context.emitter),
                        source_route: None,
                        policy: &context.error_policies.message,
                        message: RelayMessage { key, record, acks },
                        error,
                        partial_output,
                        materialized_state,
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
    use nervix_connector::{ParsedRetryPolicy, SinkCommitReport, SinkPublishResult, SinkRecordId};

    use super::*;
    use crate::runtime::test_fixtures::{
        input_batch, input_batch_with, input_value, sink_context, test_domain_clock_authority,
        unpaced_domain_state,
    };

    #[nervix_primitives::test]
    async fn queued_stop_bounds_an_active_infrastructure_retry() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, mut stop_rx) = watch::channel(None);
        let task_stop_signal = stop_signal.clone();
        let retry_started = Arc::new(Notify::new());
        let task_retry_started = retry_started.clone();
        let task = nervix_primitives::task::spawn(async move {
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
            failed.drain_description().as_deref(),
            Some("infrastructure retry exceeded drain deadline")
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a 30 second retry backoff must not hide a 40 millisecond drain deadline"
        );
        let (_, retained) = failed.into_parts();
        assert!(
            retained.stop_signal.borrow().is_none(),
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
            _publication: EmitterPublication<'_>,
        ) -> EmitterRuntimeResult<()> {
            Err(Report::new(EmitterRuntimeError::EncodeBatch)
                .attach_printable("the test sink encodes no record"))
        }
    }

    /// A connector that stages every row it is handed and publishes the rows only on its own
    /// commit, which is due once `size_boundary` rows are staged and otherwise after `cadence`.
    struct StagingSink {
        cadence: Duration,
        size_boundary: u64,
        staged: u64,
    }

    impl StagingSink {
        fn new(cadence: Duration, size_boundary: u64) -> Self {
            Self {
                cadence,
                size_boundary,
                staged: 0,
            }
        }
    }

    #[async_trait]
    impl SinkLifecycle for StagingSink {
        fn staged_commit(&self) -> Option<SinkStagedCommit> {
            if self.staged == 0 {
                return None;
            }
            if self.staged >= self.size_boundary {
                return Some(SinkStagedCommit::SizeReached);
            }
            Some(SinkStagedCommit::Cadence(self.cadence))
        }

        fn staged_messages(&self) -> u64 {
            self.staged
        }

        async fn commit(&mut self) -> SinkPublishResult<Option<SinkCommitReport>> {
            let messages = std::mem::take(&mut self.staged);
            Ok(Some(SinkCommitReport {
                messages,
                bytes: 0,
                domain_timestamp: Timestamp::from_unix_nanos(100),
            }))
        }
    }

    #[async_trait]
    impl EmitterSink for StagingSink {
        fn lifecycle(&self) -> &dyn SinkLifecycle {
            self
        }

        fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle {
            self
        }

        async fn publish_batches(
            &mut self,
            _context: &EmitterSinkContext,
            publication: EmitterPublication<'_>,
        ) -> EmitterRuntimeResult<()> {
            for batch in publication.batches.iter_mut() {
                for row in batch.pending_record_rows() {
                    batch.mark_delivered(row, DeliveredAcknowledgements::Sink)?;
                    self.staged = self
                        .staged
                        .checked_add(1)
                        .expect("a test stages a handful of rows");
                }
            }
            Ok(())
        }
    }

    /// A buffer that releases every batch the moment it takes it, so the write that stages a batch
    /// follows its acceptance at once.
    fn releasing_buffer() -> EmitterBatchBuffer {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: 1,
        });
        buffer
    }

    /// Hands `batch` to the emitter through its publish path and returns what that published.
    async fn publish(
        sink: &mut EmitterSinkState,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &mut EmitterBatchBuffer,
        batch: EmitterPublishBatch,
    ) -> Option<PublishReport> {
        match sink.publish_batch(context, control, buffer, batch).await {
            Ok(published) => published,
            Err(failure) => panic!(
                "the staging write must succeed: {}",
                emitter_error_message(failure.error())
            ),
        }
    }

    #[nervix_primitives::test]
    async fn staged_rows_wait_one_commit_cadence_from_the_write_that_staged_them() {
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
        let cadence = Duration::from_millis(20);
        let mut sink = EmitterSinkState::opened(Box::new(StagingSink::new(cadence, u64::MAX)));
        let mut buffer = releasing_buffer();

        let before = context
            .execution_snapshot()
            .expect("the fixture clock is installed")
            .now();
        // The batch was accepted at a domain time far older than the write that stages it, which
        // must not shorten the cadence its staged rows wait.
        let published = publish(
            &mut sink,
            &context,
            &mut control,
            &mut buffer,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .await;
        let after = context
            .execution_snapshot()
            .expect("the fixture clock is installed")
            .now();

        assert!(
            published.is_none(),
            "a staged row is sent only by its commit"
        );
        assert_eq!(sink.staged_messages(), 1);
        let due_at = sink
            .commit_cadence_due_at()
            .expect("the check after the staging write arms the commit cadence");
        let earliest = before
            .checked_add(cadence)
            .expect("the fixture clock reads a recent domain time");
        let latest = after
            .checked_add(cadence)
            .expect("the fixture clock reads a recent domain time");
        assert!(
            earliest <= due_at && due_at <= latest,
            "the commit cadence must start when the rows are staged: due at {due_at:?}, staged \
             between {before:?} and {after:?}"
        );

        sink.cadence_wake(&context.clock, &buffer)
            .wait()
            .await
            .expect("the fixture clock reaches the commit cadence");
        let committed = sink
            .flush_due("staging", &context, &mut control, &mut buffer, false)
            .await
            .expect("the commit due on its cadence succeeds");

        assert_eq!(committed.map(|report| report.messages), Some(1));
        assert_eq!(sink.staged_messages(), 0);
        assert!(
            sink.commit_cadence_due_at().is_none(),
            "the rows staged next start a commit cadence of their own"
        );
    }

    #[nervix_primitives::test]
    async fn rows_staged_up_to_the_size_boundary_commit_with_the_write_that_staged_them() {
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
        let mut sink =
            EmitterSinkState::opened(Box::new(StagingSink::new(Duration::from_secs(60), 1)));
        let mut buffer = releasing_buffer();

        let published = publish(
            &mut sink,
            &context,
            &mut control,
            &mut buffer,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .await;

        assert_eq!(published.map(|report| report.messages), Some(1));
        assert_eq!(sink.staged_messages(), 0);
        assert!(
            sink.commit_cadence_due_at().is_none(),
            "a commit due at the size boundary arms no cadence"
        );
    }

    #[nervix_primitives::test]
    async fn a_drain_commits_staged_rows_without_reading_the_domain_clock() {
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
        let mut context = sink_context();
        let domain = unpaced_domain_state(context.domain.as_str());
        let lifecycle = DomainClockLifecycle::new(context.domain.clone());
        lifecycle.synchronize(&domain, &test_domain_clock_authority());
        context.clock = lifecycle
            .bind()
            .expect("the fixture installs an unpaced domain clock");
        let mut sink = EmitterSinkState::opened(Box::new(StagingSink::new(
            Duration::from_secs(60),
            u64::MAX,
        )));
        let mut buffer = releasing_buffer();
        let published = publish(
            &mut sink,
            &context,
            &mut control,
            &mut buffer,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .await;
        assert!(published.is_none());
        assert!(sink.commit_cadence_due_at().is_some());

        // Stopping a domain stops its clock before its emitters drain.
        lifecycle.stop(domain.start_version);
        assert!(
            context.execution_snapshot().is_err(),
            "a stopped domain has no domain time to read"
        );
        let drained = sink
            .flush_all("staging", &context, &mut control, &mut buffer)
            .await
            .expect("a drain commits what is staged without the domain clock");

        assert_eq!(drained.map(|report| report.messages), Some(1));
        assert_eq!(sink.staged_messages(), 0);
        assert!(sink.commit_cadence_due_at().is_none());
    }

    #[test]
    fn a_stalled_sink_keeps_only_its_first_answers_in_the_order_they_were_handed_over() {
        let mut outcome = PerRecordOutcome::with_capacity(3);
        outcome.deliver(SinkRecordId::new(2));
        outcome.reject(RejectedSinkRecord::external(
            SinkRecordId::new(1),
            Timestamp::from_unix_nanos(1),
            "refused".to_string(),
        ));
        outcome.deliver(SinkRecordId::new(0));

        let stalled = stalled_after_resolving(outcome, 2).into_parts();

        assert_eq!(stalled.delivered, vec![SinkRecordId::new(0)]);
        assert_eq!(
            stalled
                .rejected
                .iter()
                .map(|rejected| rejected.id)
                .collect::<Vec<_>>(),
            vec![SinkRecordId::new(1)]
        );
        let error = sink_publish_failure(
            stalled
                .infrastructure_error
                .expect("a stalled sink leaves the write unresolved"),
        );
        assert!(emitter_publish_error_is_retryable(&error));
        assert_eq!(
            emitter_error_message(&error),
            "fault injector stalled the sink after it resolved 2 records"
        );
    }

    #[test]
    fn an_empty_write_never_consumes_an_armed_stall() {
        let context = sink_context();
        let mut outcome = PerRecordOutcome::<SinkRecordId>::with_capacity(0);
        outcome.deliver(SinkRecordId::new(0));

        let received = context.received_outcome(0, outcome).into_parts();

        assert_eq!(received.delivered, vec![SinkRecordId::new(0)]);
        assert!(received.infrastructure_error.is_none());
    }

    #[nervix_primitives::test]
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
        let mut sink = EmitterSinkState::opened(Box::new(UnencodableSink));
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Each {
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
        assert_eq!(buffer.pending().len(), 1);
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

    #[nervix_primitives::test]
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
        let mut sink = EmitterSinkState::opened(Box::new(UnencodableSink));
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Immediate);
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
        assert_eq!(buffer.pending().len(), 1);
        assert_eq!(buffer.pending()[0].message_count(), 1);
    }

    #[test]
    fn publish_failure_drains_exactly_the_batches_owned_by_the_buffer() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Each {
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
        assert_eq!(input_value(failed[0].relay_batch()), 1);
        assert_eq!(input_value(failed[1].relay_batch()), 2);
    }

    #[test]
    fn caller_owned_publish_failure_includes_current_after_older_buffered_batches() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Immediate);
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
        assert_eq!(input_value(failed[0].relay_batch()), 1);
        assert_eq!(input_value(failed[1].relay_batch()), 2);
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
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
        buffer.set_flush_policy(RuntimeFlushPolicy::Each {
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
                emitter_error_message(failure.error())
            ),
        };

        assert!(published.is_none());
        assert_eq!(buffer.pending().len(), 1);
        assert_eq!(buffer.pending()[0].message_count(), 1);
    }
}
