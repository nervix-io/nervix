//! The batches an emitter holds between receiving and publishing them.
//!
//! Layer: data plane.
//! - **Owns.** The emitter's buffer of released batches, their message and byte accounting, the
//!   delivery state of every row they carry, and the flush cadence that releases them.
//! - **Depends on.** Relay batches and their acknowledgements, the emitter's flush policy, and the
//!   domain clock its cadence is resolved against.
//! - **Must not know.** Which connector publishes the batches, how their rows are encoded or
//!   mapped, or how a failed publish is retried.

use error_stack::ResultExt as _;

use super::*;

/// What one emitter still holds: the batches its own buffer collected, and the rows a sink that
/// publishes on its commit boundary staged out of that buffer.
#[derive(Debug)]
pub(super) struct EmitterBufferedMessages {
    reported: Arc<AtomicUsize>,
    buffered: AtomicUsize,
    staged: AtomicUsize,
}

impl EmitterBufferedMessages {
    pub(super) fn new(reported: Arc<AtomicUsize>) -> Self {
        Self {
            reported,
            buffered: AtomicUsize::new(0),
            staged: AtomicUsize::new(0),
        }
    }

    fn set_buffered(&self, messages: usize) {
        self.buffered.store(messages, Ordering::Release);
        self.report_total();
    }

    fn set_staged(&self, messages: usize) {
        self.staged.store(messages, Ordering::Release);
        self.report_total();
    }

    fn report_total(&self) {
        self.reported.store(
            self.buffered
                .load(Ordering::Acquire)
                .checked_add(self.staged.load(Ordering::Acquire))
                .assured("both counts total messages this node already holds in memory"),
            Ordering::Release,
        );
    }
}

impl Default for EmitterBufferedMessages {
    fn default() -> Self {
        Self::new(Arc::new(AtomicUsize::new(0)))
    }
}

#[derive(Clone)]
pub(super) struct EmitterPublishBatch {
    pub(super) batch: RelayRecordBatch,
    pub(super) execution_now: Timestamp,
    headers: Option<Vec<EmitterHeaders>>,
    /// The ordering group of every row, absent when the emitter declares none.
    ordering_groups: Option<OrderingGroups>,
    pub(super) delivered: Vec<bool>,
}

/// The bound every byte estimate in this module relies on: each term counts bytes of a batch,
/// header, or group identifier this node already holds in memory, so their total is bounded by the
/// address space those values occupy.
const BYTES_IN_MEMORY: &str =
    "every term counts bytes of a value this node already holds in memory";

impl EmitterPublishBatch {
    pub(super) fn from_batch(batch: RelayRecordBatch, execution_now: Timestamp) -> Self {
        let row_count = batch.batch.batch().num_rows();
        Self {
            batch,
            execution_now,
            headers: None,
            ordering_groups: None,
            delivered: vec![false; row_count],
        }
    }

    pub(super) fn new(
        batch: RelayRecordBatch,
        headers: Option<Vec<EmitterHeaders>>,
        execution_now: Timestamp,
    ) -> EmitterRuntimeResult<Self> {
        let row_count = batch.batch.batch().num_rows();
        if let Some(headers) = &headers
            && row_count != headers.len()
        {
            return Err(Report::new(EmitterRuntimeError::HeaderCountMismatch {
                header_count: headers.len(),
                row_count,
            }));
        }
        Ok(Self {
            batch,
            execution_now,
            headers,
            ordering_groups: None,
            delivered: vec![false; row_count],
        })
    }

    /// This batch with the ordering group of each of its rows.
    pub(super) fn with_ordering_groups(
        mut self,
        groups: OrderingGroups,
    ) -> EmitterRuntimeResult<Self> {
        let row_count = self.batch.batch.batch().num_rows();
        if let Some(group_count) = groups.row_count()
            && group_count != row_count
        {
            return Err(Report::new(
                EmitterRuntimeError::OrderingGroupCountMismatch {
                    group_count,
                    row_count,
                },
            ));
        }
        self.ordering_groups = Some(groups);
        Ok(self)
    }

    /// The ordering group row `row` is published under, absent when the emitter declares none, or
    /// why the row has none.
    pub(super) fn ordering_group(
        &self,
        row: usize,
    ) -> EmitterRuntimeResult<Result<Option<String>, OrderingGroupError>> {
        let Some(groups) = &self.ordering_groups else {
            return Ok(Ok(None));
        };
        match groups.group(row) {
            Some(Ok(group)) => Ok(Ok(Some(group.to_string()))),
            Some(Err(failure)) => Ok(Err(failure.clone())),
            None => Err(
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "emitter batch row {row} has no ordering group entry"
                )),
            ),
        }
    }

    fn estimated_bytes(&self) -> u64 {
        let header_bytes = self
            .headers
            .iter()
            .flatten()
            .flatten()
            .map(|(name, value)| {
                let name_len: u64 = name.len().arch_into();
                let value_len: u64 = value.len().arch_into();
                name_len.checked_add(value_len).assured(BYTES_IN_MEMORY)
            })
            .try_fold(0_u64, u64::checked_add)
            .assured(BYTES_IN_MEMORY);
        let group_bytes = match &self.ordering_groups {
            Some(groups) => groups.estimated_bytes(),
            None => 0,
        };
        self.batch
            .estimated_bytes()
            .checked_add(header_bytes)
            .assured(BYTES_IN_MEMORY)
            .checked_add(group_bytes)
            .assured(BYTES_IN_MEMORY)
    }

    pub(super) fn headers_for_row(&self, row: usize) -> Option<&EmitterHeaders> {
        static EMPTY: EmitterHeaders = Vec::new();

        match &self.headers {
            Some(headers) => headers.get(row),
            None if row < self.batch.keys.len() => Some(&EMPTY),
            None => None,
        }
    }

    pub(super) fn message_count(&self) -> u64 {
        self.batch.message_count()
    }

    fn domain_timestamp(&self) -> Option<Timestamp> {
        self.batch.domain_timestamp()
    }

    pub(super) fn merged_acks(&self) -> AckSet {
        self.batch.merged_acks()
    }

    pub(super) fn is_delivered(&self, row: usize) -> bool {
        self.delivered.get(row).copied().unwrap_or(false)
    }

    pub(super) fn mark_delivered(
        &mut self,
        row: usize,
        acknowledgements: DeliveredAcknowledgements,
    ) -> EmitterRuntimeResult<()> {
        let delivered_rows = self.delivered.len();
        let delivered = self.delivered.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::DeliveryRowOutOfBounds {
                row,
                row_count: delivered_rows,
            })
        })?;
        if !*delivered {
            let ack_rows = self.batch.acks.len();
            let acks = self.batch.acks.get(row).ok_or_else(|| {
                Report::new(EmitterRuntimeError::AcknowledgementRowOutOfBounds {
                    row,
                    row_count: ack_rows,
                })
            })?;
            match acknowledgements {
                DeliveredAcknowledgements::Host => acks.ack_success(),
                DeliveredAcknowledgements::Sink => {}
            }
            *delivered = true;
        }
        Ok(())
    }

    fn mark_rejected(&mut self, row: usize) -> EmitterRuntimeResult<()> {
        let delivered_rows = self.delivered.len();
        let delivered = self.delivered.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::RejectionRowOutOfBounds {
                row,
                row_count: delivered_rows,
            })
        })?;
        *delivered = true;
        Ok(())
    }

    pub(super) async fn mark_rejected_after_delivery(
        &mut self,
        row: usize,
        delivery: impl std::future::Future<Output = ()>,
    ) -> EmitterRuntimeResult<()> {
        delivery.await;
        self.mark_rejected(row)
    }

    pub(super) fn pending_record_rows(&self) -> Vec<usize> {
        (0..self.batch.batch.batch().num_rows())
            .filter(|row| !self.delivered.get(*row).copied().unwrap_or(false))
            .collect()
    }

    /// The acknowledgements of `rows`, for a sink that takes them with the write.
    pub(super) fn acks_for_rows(&self, rows: &[usize]) -> AckSet {
        AckSet::merged(
            rows.iter()
                .filter_map(|row| self.batch.acks.get(*row).cloned()),
        )
    }
}

/// Who resolves the acknowledgements of the rows one write delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveredAcknowledgements {
    /// The host resolves them as the write returns.
    Host,
    /// The sink took them with the rows and resolves them when its own commit succeeds.
    Sink,
}

pub(in crate::runtime) struct PublishReport {
    pub(super) messages: u64,
    pub(super) bytes: u64,
    pub(super) domain_timestamp: Timestamp,
}

impl PublishReport {
    pub(super) fn flushed(messages: u64, bytes: u64, domain_timestamp: Timestamp) -> Self {
        Self {
            messages,
            bytes,
            domain_timestamp,
        }
    }

    fn merge(self, other: Self) -> Self {
        Self {
            messages: self
                .messages
                .checked_add(other.messages)
                .assured("both counts total messages this node already published"),
            bytes: self.bytes.checked_add(other.bytes).assured(BYTES_IN_MEMORY),
            domain_timestamp: self.domain_timestamp.max(other.domain_timestamp),
        }
    }

    pub(super) fn merge_optional(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        match (left, right) {
            (Some(left), Some(right)) => Some(left.merge(right)),
            (Some(report), None) | (None, Some(report)) => Some(report),
            (None, None) => None,
        }
    }
}

/// The emitter's own buffer of published batches and the cadence deadline that releases them.
///
/// The cadence is the emitter's explicit `FLUSH EACH` or `FLUSH IMMEDIATE` policy and nothing
/// else. Retry backoff and acknowledgement keepalive are owned by [`EmitterRetrySchedule`], so a
/// failed publish attempt leaves this buffer's pending batches, acknowledgements and cadence
/// deadline exactly as they were.
#[derive(Default)]
pub(super) struct EmitterBatchBuffer {
    pub(super) flush_policy: Option<RuntimeFlushPolicy>,
    pub(super) pending: Vec<EmitterPublishBatch>,
    pending_messages: u64,
    pending_bytes: u64,
    cadence: BranchBufferTimer,
    buffered_messages: Arc<EmitterBufferedMessages>,
}

impl EmitterBatchBuffer {
    pub(super) fn new(
        context: &EmitterSinkContext,
        flush_policy: &FlushPolicy,
        buffered_messages: Arc<EmitterBufferedMessages>,
    ) -> Self {
        Self {
            flush_policy: context.parse_flush_policy("emitter", flush_policy),
            pending: Vec::new(),
            pending_messages: 0,
            pending_bytes: 0,
            cadence: BranchBufferTimer::default(),
            buffered_messages,
        }
    }

    fn update_buffered_messages(&self) {
        self.buffered_messages
            .set_buffered(self.pending_messages.arch_into());
    }

    /// Publishes what a sink that commits separately still holds, which a drain reads together
    /// with this buffer's own pending batches.
    pub(super) fn report_staged_messages(&self, messages: u64) {
        self.buffered_messages.set_staged(messages.arch_into());
    }

    pub(super) fn reconfigure(
        &mut self,
        context: &EmitterSinkContext,
        flush_policy: &FlushPolicy,
    ) -> EmitterRuntimeResult<()> {
        self.flush_policy = context.parse_flush_policy("emitter", flush_policy);
        self.cadence.clear();
        if self.pending.is_empty() {
            return Ok(());
        }
        let Some(flush_policy) = self.flush_policy else {
            return Ok(());
        };
        self.arm_cadence(context, flush_policy)
    }

    /// Starts the cadence for a buffer that just stopped being empty.
    ///
    /// An armed cadence is left alone, so a later batch joining the same buffer neither brings the
    /// deadline forward nor pays for a clock read.
    fn arm_cadence(
        &mut self,
        context: &EmitterSinkContext,
        flush_policy: RuntimeFlushPolicy,
    ) -> EmitterRuntimeResult<()> {
        if self.cadence.is_armed() {
            return Ok(());
        }
        let snapshot = context.execution_snapshot()?;
        self.cadence
            .arm_flush(flush_policy, &context.clock, &snapshot)
            .change_context(EmitterRuntimeError::FlushTiming)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn deadline(&self) -> Option<BranchBufferDeadline> {
        self.cadence.deadline()
    }

    pub(super) fn push(
        &mut self,
        context: &EmitterSinkContext,
        batch: EmitterPublishBatch,
    ) -> EmitterRuntimeResult<bool> {
        let Some(flush_policy) = self.flush_policy else {
            return Err(Report::new(EmitterRuntimeError::FlushPolicyNotInitialized));
        };
        self.retain(batch);
        self.arm_cadence(context, flush_policy)?;
        Ok(flush_policy.size_boundary_reached(self.pending_bytes))
    }

    /// Retains a batch whose release the caller already owns.
    ///
    /// A drain publishes everything the emitter holds, and a batch put back after a failed
    /// transfer is released by the retry that failure schedules. Neither needs a cadence deadline,
    /// and leaving the clock out keeps both paths working for a domain that has already
    /// uninstalled it.
    pub(super) fn retain_without_cadence(
        &mut self,
        batch: EmitterPublishBatch,
    ) -> EmitterRuntimeResult<()> {
        if self.flush_policy.is_none() {
            return Err(Report::new(EmitterRuntimeError::FlushPolicyNotInitialized));
        }
        self.retain(batch);
        Ok(())
    }

    fn retain(&mut self, batch: EmitterPublishBatch) {
        self.pending_messages = self
            .pending_messages
            .checked_add(batch.message_count())
            .assured("both counts total messages this emitter already holds in memory");
        self.pending_bytes = self
            .pending_bytes
            .checked_add(batch.estimated_bytes())
            .assured("both counts estimate bytes of batches this node already holds in memory");
        self.pending.push(batch);
        self.update_buffered_messages();
    }

    fn is_due(&self, context: &EmitterSinkContext) -> EmitterRuntimeResult<bool> {
        let snapshot = context.execution_snapshot()?;
        self.cadence
            .is_due(&context.clock, &snapshot)
            .change_context(EmitterRuntimeError::FlushTiming)
    }

    pub(super) fn should_flush(
        &self,
        context: &EmitterSinkContext,
        force: bool,
    ) -> EmitterRuntimeResult<bool> {
        if self.pending.is_empty() {
            return Ok(false);
        }
        if force {
            return Ok(true);
        }
        self.is_due(context)
    }

    pub(super) fn pending_acks(&self) -> AckSet {
        AckSet::merged(self.pending.iter().map(EmitterPublishBatch::merged_acks))
    }

    pub(super) fn drain_pending(&mut self) -> Vec<EmitterPublishBatch> {
        let pending = std::mem::take(&mut self.pending);
        self.pending_messages = 0;
        self.pending_bytes = 0;
        self.cadence.clear();
        self.update_buffered_messages();
        pending
    }

    pub(super) fn clear(&mut self) {
        self.pending.clear();
        self.pending_messages = 0;
        self.pending_bytes = 0;
        self.cadence.clear();
        self.update_buffered_messages();
    }

    pub(super) fn report(&self) -> Option<PublishReport> {
        if self.pending.is_empty() {
            return None;
        }
        let bytes = self.pending_bytes;
        let domain_timestamp = self
            .pending
            .iter()
            .map(|batch| batch.domain_timestamp().unwrap_or(batch.execution_now))
            .max()
            .verified("a non-empty emitter buffer has an observation time for every batch");
        Some(PublishReport::flushed(
            self.pending_messages,
            bytes,
            domain_timestamp,
        ))
    }
}

impl Drop for EmitterBatchBuffer {
    fn drop(&mut self) {
        self.pending_acks().no_ack("emitter dropped buffered batch");
        self.buffered_messages.set_buffered(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{
        emitter_ordering_group::EvaluatedOrderingGroups,
        test_fixtures::{input_batch, input_batch_with, input_schema, named, sink_context},
    };

    #[tokio::test]
    async fn cancelled_message_error_delivery_keeps_the_record_pending() {
        let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
            name: named("events"),
            fields: vec![nervix_models::SchemaField {
                name: named("value"),
                ty: nervix_models::ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }));
        let (acks, _completion) = AckSet::root();
        let batch = RelayRecordBatch::single(
            schema,
            None,
            test_runtime_row([(
                "value".to_string(),
                RuntimeValue::String("poison".to_string()),
            )]),
            acks,
        )
        .expect("test emitter batch must build");
        let mut batch = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100));

        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                batch.mark_rejected_after_delivery(0, std::future::pending()),
            )
            .await
            .is_err(),
            "the simulated message-error delivery must remain pending"
        );
        assert!(
            !batch.delivered[0],
            "cancelling message-error delivery must leave the poison record retryable"
        );

        batch
            .mark_rejected_after_delivery(0, std::future::ready(()))
            .await
            .expect("completed message-error delivery must account for the record");
        assert!(batch.delivered[0]);
    }

    #[tokio::test]
    async fn rejected_record_ack_remains_held_for_message_error_delivery() {
        let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
            name: named("events"),
            fields: vec![nervix_models::SchemaField {
                name: named("value"),
                ty: nervix_models::ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }));
        let (acks, mut completion) = AckSet::root();
        let message_error_acks = acks.clone();
        let batch = RelayRecordBatch::single(
            schema,
            None,
            test_runtime_row([(
                "value".to_string(),
                RuntimeValue::String("poison".to_string()),
            )]),
            acks,
        )
        .expect("test emitter batch must build");
        let mut batch = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100));
        batch
            .mark_rejected(0)
            .expect("test record must be marked rejected");
        let mut buffer = EmitterBatchBuffer::default();
        buffer.pending.push(batch);

        buffer.clear();

        assert!(
            tokio::time::timeout(Duration::from_millis(20), completion.wait_for_progress(),)
                .await
                .is_err(),
            "clearing an accounted poison record must not release its source ACK"
        );
        message_error_acks.ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
    }

    #[test]
    fn publish_batch_requires_one_header_row_per_record() {
        let batch = input_batch();
        let from_batch =
            EmitterPublishBatch::from_batch(batch.clone(), Timestamp::from_unix_nanos(100));
        assert!(from_batch.headers.is_none());
        assert_eq!(from_batch.message_count(), 1);
        assert_eq!(
            from_batch.domain_timestamp(),
            Some(Timestamp::from_unix_nanos(0))
        );

        let headers = vec![vec![("route".to_string(), "fast".to_string())]];
        let with_headers = EmitterPublishBatch::new(
            batch.clone(),
            Some(headers.clone()),
            Timestamp::from_unix_nanos(100),
        )
        .expect("row-aligned headers must build");
        assert_eq!(with_headers.headers.as_ref(), Some(&headers));
        let header_bytes: u64 = "routefast".len().arch_into();
        assert_eq!(
            with_headers.estimated_bytes(),
            batch.estimated_bytes() + header_bytes
        );

        let error = match EmitterPublishBatch::new(
            batch,
            Some(Vec::new()),
            Timestamp::from_unix_nanos(100),
        ) {
            Err(error) => error,
            Ok(_) => panic!("missing row headers must be rejected"),
        };
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::HeaderCountMismatch {
                header_count: 0,
                row_count: 1,
            }
        );
    }

    #[test]
    fn publish_batch_rejects_misaligned_groups_and_row_updates() {
        let groups =
            match EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100))
                .with_ordering_groups(OrderingGroups::Evaluated(
                    EvaluatedOrderingGroups::from_rows([]),
                )) {
                Ok(_) => panic!("ordering groups must stay aligned with source rows"),
                Err(error) => error,
            };
        assert_eq!(
            *groups.current_context(),
            EmitterRuntimeError::OrderingGroupCountMismatch {
                group_count: 0,
                row_count: 1,
            }
        );

        let mut delivered =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
        let error = delivered
            .mark_delivered(1, DeliveredAcknowledgements::Host)
            .expect_err("delivery cannot address a missing row");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::DeliveryRowOutOfBounds {
                row: 1,
                row_count: 1,
            }
        );

        delivered.batch.acks.clear();
        let error = delivered
            .mark_delivered(0, DeliveredAcknowledgements::Host)
            .expect_err("delivery requires the row's acknowledgement set");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::AcknowledgementRowOutOfBounds {
                row: 0,
                row_count: 0,
            }
        );

        let mut rejected =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
        let error = rejected
            .mark_rejected(1)
            .expect_err("rejection cannot address a missing row");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::RejectionRowOutOfBounds {
                row: 1,
                row_count: 1,
            }
        );
    }

    #[test]
    fn publish_batch_reads_each_row_group_only_where_the_emitter_declares_one() {
        let undeclared =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
        assert_eq!(
            undeclared
                .ordering_group(0)
                .expect("an emitter without a group reads none"),
            Ok(None)
        );

        let batch = input_batch();
        let batch_bytes = batch.estimated_bytes();
        let evaluated = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100))
            .with_ordering_groups(OrderingGroups::Evaluated(
                EvaluatedOrderingGroups::from_rows([Ok("tenant-a")]),
            ))
            .expect("one group for one row must attach");
        assert_eq!(
            evaluated
                .ordering_group(0)
                .expect("the row has a group entry"),
            Ok(Some("tenant-a".to_string()))
        );
        let group_bytes: u64 = "tenant-a".len().arch_into();
        assert_eq!(evaluated.estimated_bytes(), batch_bytes + group_bytes);
        let error = evaluated
            .ordering_group(1)
            .expect_err("a row outside the batch has no group entry");
        assert_eq!(*error.current_context(), EmitterRuntimeError::EncodeBatch);

        let unavailable =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100))
                .with_ordering_groups(OrderingGroups::Unavailable(
                    OrderingGroupError::UnbranchedRecord,
                ))
                .expect("a group every row shares attaches to any batch");
        assert_eq!(
            unavailable
                .ordering_group(0)
                .expect("the row has a group entry"),
            Err(OrderingGroupError::UnbranchedRecord)
        );
        assert_eq!(unavailable.estimated_bytes(), batch_bytes);
    }
    #[tokio::test]
    async fn publish_batch_ack_helpers_preserve_and_complete_all_roots() {
        let (acks, completion) = AckSet::root();
        let batch = EmitterPublishBatch::from_batch(
            input_batch_with(1, 0, acks),
            Timestamp::from_unix_nanos(100),
        );

        assert!(!batch.merged_acks().is_empty());
        batch.merged_acks().ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
    }

    #[test]
    fn batch_buffer_rejects_input_without_an_initialized_flush_policy() {
        let mut buffer = EmitterBatchBuffer::default();
        let error = buffer
            .push(
                &sink_context(),
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect_err("an unconfigured buffer must reject input");

        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::FlushPolicyNotInitialized
        );
        assert!(buffer.is_empty());
        assert_eq!(buffer.pending_bytes, 0);
        assert!(buffer.deadline().is_none());
    }

    #[test]
    fn batch_buffer_tracks_size_messages_deadline_and_latest_timestamp() {
        let reported_messages = Arc::new(AtomicUsize::new(0));
        let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer.buffered_messages = buffered_messages.clone();
        let first = EmitterPublishBatch::from_batch(
            input_batch_with(1, 10, AckSet::empty()),
            Timestamp::from_unix_nanos(100),
        );
        let second_batch = RelayRecordBatch::from_messages(
            input_schema(),
            vec![
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("value".to_string(), RuntimeValue::I64(2))])
                        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(20)),
                    acks: AckSet::empty(),
                },
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("value".to_string(), RuntimeValue::I64(3))])
                        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(20)),
                    acks: AckSet::empty(),
                },
            ],
        )
        .expect("valid multi-row emitter input batch");
        let second = EmitterPublishBatch::new(
            second_batch,
            Some(vec![
                vec![("name".to_string(), "value".to_string())],
                Vec::new(),
            ]),
            Timestamp::from_unix_nanos(100),
        )
        .expect("headers must align");
        let expected_bytes = first
            .estimated_bytes()
            .checked_add(second.estimated_bytes())
            .assured("the two test batches are far smaller than the u64 byte range");

        let context = sink_context();
        assert!(
            !buffer
                .push(&context, first)
                .expect("first batch must buffer")
        );
        assert_eq!(buffer.pending_messages, 1);
        assert!(
            buffer.deadline().is_some(),
            "the first push arms the cadence"
        );
        assert!(
            !buffer
                .push(&context, second)
                .expect("second batch must buffer")
        );

        assert!(
            !buffer
                .is_due(&context)
                .expect("the fixture clock stays installed"),
            "a second push does not bring the cadence forward"
        );
        assert_eq!(reported_messages.load(Ordering::Acquire), 3);
        assert_eq!(buffer.pending_messages, 3);
        let report = buffer
            .report()
            .expect("pending batches must produce a report");
        assert_eq!(report.messages, 3);
        assert_eq!(report.bytes, expected_bytes);
        assert_eq!(report.domain_timestamp, Timestamp::from_unix_nanos(20));
        assert_eq!(buffer.pending_bytes, expected_bytes);
    }

    #[test]
    fn batch_buffer_honors_its_size_boundary_and_logical_cadence() {
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: 1,
        });

        assert!(
            buffer
                .push(
                    &context,
                    EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100),)
                )
                .expect("batch must buffer")
        );
        assert!(matches!(
            buffer.deadline(),
            Some(BranchBufferDeadline::Logical(_))
        ));
        assert!(
            !buffer
                .is_due(&context)
                .expect("the fixture clock stays installed")
        );

        let mut immediate = EmitterBatchBuffer::default();
        immediate.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        immediate
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("batch must buffer");
        assert!(matches!(
            immediate.deadline(),
            Some(BranchBufferDeadline::Physical(_))
        ));
    }

    #[test]
    fn forced_retry_ignores_the_ordinary_buffer_deadline() {
        let context = sink_context();
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
            !buffer
                .is_due(&context)
                .expect("the fixture clock stays installed")
        );

        assert!(
            buffer
                .should_flush(&context, true)
                .expect("a forced flush needs no clock read")
        );
        assert!(
            !buffer
                .should_flush(&context, false)
                .expect("the fixture clock stays installed")
        );
    }

    #[tokio::test]
    async fn a_sink_that_commits_separately_takes_the_rows_it_accepted() {
        let (first_acks, mut first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        let mut batch = EmitterPublishBatch::from_batch(
            RelayRecordBatch::from_messages(
                input_schema(),
                vec![
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
                        acks: first_acks,
                    },
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([("value".to_string(), RuntimeValue::I64(2))]),
                        acks: second_acks,
                    },
                ],
            )
            .expect("valid two-row emitter batch"),
            Timestamp::from_unix_nanos(100),
        );

        let retained = batch.acks_for_rows(&[0]);
        batch
            .mark_delivered(0, DeliveredAcknowledgements::Sink)
            .expect("a staged row must be marked delivered");
        batch
            .mark_delivered(1, DeliveredAcknowledgements::Host)
            .expect("a published row must be marked delivered");

        assert!(batch.pending_record_rows().is_empty());
        assert_eq!(second_completion.wait().await, AckOutcome::Ack);
        // The staged row is the sink's until its commit resolves it, so the host left it alone.
        retained.ack_alive();
        assert_eq!(
            first_completion.wait_for_progress().await,
            AckProgress::Alive
        );
        retained.ack_success();
        assert_eq!(first_completion.wait().await, AckOutcome::Ack);
    }

    #[test]
    fn buffered_and_staged_message_counts_are_summed_independently() {
        let reported = Arc::new(AtomicUsize::new(0));
        let buffered = EmitterBufferedMessages::new(reported.clone());

        buffered.set_buffered(2);
        buffered.set_staged(3);
        assert_eq!(reported.load(Ordering::Acquire), 5);

        buffered.set_buffered(0);
        assert_eq!(reported.load(Ordering::Acquire), 3);
        buffered.set_staged(0);
        assert_eq!(reported.load(Ordering::Acquire), 0);
    }

    #[test]
    fn batch_buffer_drain_clear_reconfigure_and_drop_reset_accounting() {
        let context = sink_context();
        let reported_messages = Arc::new(AtomicUsize::new(0));
        let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
        let mut buffer = EmitterBatchBuffer::new(
            &context,
            &FlushPolicy::Each {
                interval: "10s".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            buffered_messages.clone(),
        );
        assert!(buffer.flush_policy.is_some());
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("configured buffer must accept input");
        assert_eq!(buffer.pending_messages, 1);
        buffer
            .reconfigure(&context, &FlushPolicy::Immediate)
            .expect("the fixture clock stays installed");
        assert_eq!(buffer.flush_policy, Some(RuntimeFlushPolicy::Immediate));
        assert!(
            matches!(buffer.deadline(), Some(BranchBufferDeadline::Physical(_))),
            "a reconfigured Immediate cadence re-arms the retained batches physically"
        );

        let drained = buffer.drain_pending();
        assert_eq!(drained.len(), 1);
        assert!(buffer.is_empty());
        assert_eq!(buffer.pending_bytes, 0);
        assert_eq!(buffer.pending_messages, 0);
        assert!(buffer.deadline().is_none());
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);

        buffer
            .push(
                &sink_context(),
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("reconfigured buffer must accept input");
        assert_eq!(buffer.pending_messages, 1);
        buffer.clear();
        assert!(buffer.report().is_none());
        assert_eq!(buffer.pending_messages, 0);
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);

        buffered_messages.set_buffered(7);
        drop(buffer);
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn batch_buffer_ack_helpers_merge_and_complete_pending_batches() {
        let (first_acks, first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(1, 0, first_acks),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("first batch must buffer");
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(2, 0, second_acks),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("second batch must buffer");

        assert!(!buffer.pending_acks().is_empty());
        buffer.pending_acks().ack_success();
        assert_eq!(first_completion.wait().await, AckOutcome::Ack);
        assert_eq!(second_completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn retained_batch_clone_owns_exactly_one_attached_ack_share() {
        let (root, mut completion) = AckSet::root();
        let attached = root.attached();
        let batch = EmitterPublishBatch::from_batch(
            input_batch_with(1, 0, attached),
            Timestamp::from_unix_nanos(100),
        );
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);

        buffer
            .push(&sink_context(), batch.clone())
            .expect("retry buffer must retain the batch");
        root.ack_success();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), completion.wait_for_progress())
                .await
                .is_err(),
            "the retained attached share must keep the root pending"
        );

        buffer.pending_acks().ack_success();
        buffer.clear();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        drop(batch);
    }

    #[tokio::test]
    async fn batch_buffer_drop_no_acks_every_retained_batch() {
        let (first_acks, first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        let reported_messages = Arc::new(AtomicUsize::new(0));
        let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer.buffered_messages = buffered_messages.clone();
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(1, 0, first_acks),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("first batch must buffer");
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(2, 0, second_acks),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("second batch must buffer");

        drop(buffer);

        assert_eq!(
            first_completion.wait().await,
            AckOutcome::NoAck("emitter dropped buffered batch".to_string())
        );
        assert_eq!(
            second_completion.wait().await,
            AckOutcome::NoAck("emitter dropped buffered batch".to_string())
        );
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);
    }
}
