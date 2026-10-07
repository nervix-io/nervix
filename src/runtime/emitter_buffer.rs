//! The batches an emitter holds between receiving and publishing them.
//!
//! Layer: data plane.
//! - **Owns.** The emitter's buffer of released batches with their source relay and branch, the
//!   request an HTTP emitter admitted each row with and what a later rejection of the row reads,
//!   message and byte accounting, where every row stands in its publication and which rows its sink
//!   delivered, what a flush reports as sent, the batch payloads, HTTP requests and prepared row
//!   requests retained for the rows they carry, and flush cadence.
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

    /// Reports that the emitter holds nothing: its buffer and the rows its sink staged are gone
    /// with its task.
    fn clear(&self) {
        self.buffered.store(0, Ordering::Release);
        self.staged.store(0, Ordering::Release);
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
    /// A relay has one fixed named branch declaration, so this name and `batch.key` together
    /// identify the exact source branch even when another relay has an equal concrete key.
    source_relay: RelayName,
    batch: RelayRecordBatch,
    execution_now: Timestamp,
    headers: Option<Vec<EmitterHeaders>>,
    /// The ordering group of every row, absent when the emitter declares none.
    ordering_groups: Option<OrderingGroups>,
    /// The request every row was admitted with, present only for an emitter that publishes through
    /// an HTTP sink.
    http_requests: Option<AdmittedHttpRequests>,
    /// Where every row stands in its publication.
    rows: Vec<BufferedRow>,
}

/// Where one buffered row stands between the emitter receiving it and the emitter owning nothing
/// of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferedRow {
    /// No attempt has prepared the row for its sink yet, or the attempt that did left it for the
    /// next one.
    Pending,
    /// A batch payload, HTTP request or prepared row request the emitter retains carries the row,
    /// so only the sink's answer for it resolves the row.
    Prepared,
    /// The sink delivered the row, which the emitter's sent counters count once.
    Delivered,
    /// A sink that publishes on its own commit accepted the row and took its acknowledgements, so
    /// that commit, not this buffer, counts it as sent.
    Staged,
    /// The row was rejected once its message error was delivered.
    Rejected,
}

impl BufferedRow {
    /// Whether the row is resolved, so no attempt carries it again.
    fn is_resolved(self) -> bool {
        match self {
            Self::Pending | Self::Prepared => false,
            Self::Delivered | Self::Staged | Self::Rejected => true,
        }
    }
}

/// The bound every byte estimate in this module relies on: each term counts bytes of a batch,
/// header, or group identifier this node already holds in memory, so their total is bounded by the
/// address space those values occupy.
const BYTES_IN_MEMORY: &str =
    "every term counts bytes of a value this node already holds in memory";

/// The guarantee measuring the delivered rows of a batch relies on: they are read from the batch's
/// own row states, which it holds one for each of its rows, so they are distinct rows of it in row
/// order, and a batch of some of its rows rebuilds from its own columns under its own schema.
const DELIVERED_ROWS: &str =
    "delivered rows are distinct rows of their batch, read from its states one per row";

impl EmitterPublishBatch {
    pub(super) fn from_input(
        source_relay: RelayName,
        batch: RelayRecordBatch,
        execution_now: Timestamp,
    ) -> Self {
        let row_count = batch.batch.batch().num_rows();
        Self {
            source_relay,
            batch,
            execution_now,
            headers: None,
            ordering_groups: None,
            http_requests: None,
            rows: vec![BufferedRow::Pending; row_count],
        }
    }

    pub(super) fn new(
        source_relay: RelayName,
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
            source_relay,
            batch,
            execution_now,
            headers,
            ordering_groups: None,
            http_requests: None,
            rows: vec![BufferedRow::Pending; row_count],
        })
    }

    #[cfg(test)]
    pub(super) fn from_batch(batch: RelayRecordBatch, execution_now: Timestamp) -> Self {
        Self::from_input(
            RelayName::parse("test_relay")
                .assured("the fixed test relay name satisfies the name grammar"),
            batch,
            execution_now,
        )
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

    /// This batch with the request each of its rows was admitted with.
    pub(super) fn with_http_requests(
        mut self,
        requests: AdmittedHttpRequests,
    ) -> EmitterRuntimeResult<Self> {
        let row_count = self.batch.batch.batch().num_rows();
        let request_count = requests.request_count();
        if request_count != row_count {
            return Err(Report::new(EmitterRuntimeError::HttpRequestCountMismatch {
                request_count,
                row_count,
            }));
        }
        self.http_requests = Some(requests);
        Ok(self)
    }

    /// The request fields row `row` was admitted with.
    pub(super) fn http_request(&self, row: usize) -> EmitterRuntimeResult<&HttpRequestFields> {
        let requests = match &self.http_requests {
            Some(requests) => requests,
            None => {
                return Err(Report::new(EmitterRuntimeError::MissingHttpRequest { row }));
            }
        };
        requests
            .request(row)
            .ok_or_else(|| Report::new(EmitterRuntimeError::MissingHttpRequest { row }))
    }

    /// What the message error of row `row` reads besides the error once the row is rejected after
    /// admission. An HTTP request's error reads its original source record, its attempted codec
    /// record and the materialized state its batch was admitted with; any other row's reads the
    /// row as this batch holds it.
    pub(super) fn rejected_record_input(
        &self,
        row: usize,
    ) -> EmitterRuntimeResult<RejectedRecordInput> {
        if let Some(requests) = &self.http_requests {
            return requests
                .rejected_record(&self.batch, row)
                .change_context(EmitterRuntimeError::EncodeBatch);
        }
        let record = self
            .batch
            .runtime_row(row)
            .change_context(EmitterRuntimeError::EncodeBatch)?;
        Ok(RejectedRecordInput {
            record,
            partial_output: None,
            materialized_state: HashMap::default(),
        })
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
        let mut header_bytes = 0_u64;
        if let Some(headers) = &self.headers {
            for row_headers in headers {
                header_bytes = header_bytes
                    .checked_add(Self::header_bytes(row_headers))
                    .assured(BYTES_IN_MEMORY);
            }
        }
        let group_bytes = match &self.ordering_groups {
            Some(groups) => groups.estimated_bytes(),
            None => 0,
        };
        let request_bytes = match &self.http_requests {
            Some(requests) => requests.estimated_bytes(),
            None => 0,
        };
        self.batch
            .estimated_bytes()
            .checked_add(header_bytes)
            .assured(BYTES_IN_MEMORY)
            .checked_add(group_bytes)
            .assured(BYTES_IN_MEMORY)
            .checked_add(request_bytes)
            .assured(BYTES_IN_MEMORY)
    }

    /// The name and value bytes of `headers`, which one row is published with.
    fn header_bytes(headers: &EmitterHeaders) -> u64 {
        let mut bytes = 0_u64;
        for (name, value) in headers {
            let name_len: u64 = name.len().arch_into();
            let value_len: u64 = value.len().arch_into();
            bytes = bytes
                .checked_add(name_len)
                .assured(BYTES_IN_MEMORY)
                .checked_add(value_len)
                .assured(BYTES_IN_MEMORY);
        }
        bytes
    }

    /// The rows of this batch its sink delivered, in row order.
    fn delivered_rows(&self) -> Vec<usize> {
        let mut delivered = Vec::with_capacity(self.rows.len());
        for (row, state) in self.rows.iter().enumerate() {
            if *state == BufferedRow::Delivered {
                delivered.push(row);
            }
        }
        delivered
    }

    /// What this batch adds to its emitter's sent counters: every row its sink delivered, counted
    /// once however many attempts it took, and the payload bytes those rows carry. A row the sink
    /// rejected is not sent, and a row a sink staged is counted by its commit, so a batch the sink
    /// delivered none of reports nothing.
    pub(super) fn delivered_report(&self) -> Option<PublishReport> {
        let delivered = self.delivered_rows();
        if delivered.is_empty() {
            return None;
        }
        let messages: u64 = delivered.len().arch_into();
        let bytes = self.delivered_payload_bytes(&delivered);
        let domain_timestamp = self.domain_timestamp().unwrap_or(self.execution_now);
        Some(PublishReport::flushed(messages, bytes, domain_timestamp))
    }

    /// The payload bytes `delivered`, rows of this batch in row order, carry: the Arrow data of
    /// their records with the headers and ordering groups they were published with, which is how
    /// the buffer measures the whole batch.
    ///
    /// An HTTP request's method, target and headers are request metadata rather than payload, so
    /// only the record a codec body encodes counts, and a request without a body carries none.
    fn delivered_payload_bytes(&self, delivered: &[usize]) -> u64 {
        if let Some(requests) = &self.http_requests {
            return requests
                .body_bytes(&self.batch, delivered)
                .assured(DELIVERED_ROWS);
        }
        if delivered.len() == self.rows.len() {
            return self.estimated_bytes();
        }
        let mut header_bytes = 0_u64;
        if let Some(headers) = &self.headers {
            for row in delivered {
                let row_headers = headers
                    .get(*row)
                    .verified("the batch was built with one set of headers for each of its rows");
                header_bytes = header_bytes
                    .checked_add(Self::header_bytes(row_headers))
                    .assured(BYTES_IN_MEMORY);
            }
        }
        let group_bytes = match &self.ordering_groups {
            Some(groups) => groups.estimated_bytes_of_rows(delivered),
            None => 0,
        };
        self.batch
            .payload_bytes_of_rows(delivered)
            .assured(DELIVERED_ROWS)
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

    pub(super) fn source_relay(&self) -> &RelayName {
        &self.source_relay
    }

    pub(super) fn relay_batch(&self) -> &RelayRecordBatch {
        &self.batch
    }

    pub(super) fn into_relay_batch(self) -> RelayRecordBatch {
        self.batch
    }

    pub(super) fn execution_now(&self) -> Timestamp {
        self.execution_now
    }

    /// Whether each row is resolved, so a failure that takes the batch back routes only the rest.
    pub(super) fn resolved_rows(&self) -> Vec<bool> {
        self.rows.iter().map(|row| row.is_resolved()).collect()
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

    /// Resolves `row` as delivered. A row resolves once, so delivering it again changes nothing
    /// and never resolves its acknowledgement a second time.
    pub(super) fn mark_delivered(
        &mut self,
        row: usize,
        acknowledgements: DeliveredAcknowledgements,
    ) -> EmitterRuntimeResult<()> {
        let row_count = self.rows.len();
        let state = self.rows.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::DeliveryRowOutOfBounds { row, row_count })
        })?;
        if state.is_resolved() {
            return Ok(());
        }
        let ack_rows = self.batch.acks.len();
        let acks = self.batch.acks.get(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::AcknowledgementRowOutOfBounds {
                row,
                row_count: ack_rows,
            })
        })?;
        let resolved = match acknowledgements {
            DeliveredAcknowledgements::Host => {
                acks.ack_success();
                BufferedRow::Delivered
            }
            DeliveredAcknowledgements::Sink => BufferedRow::Staged,
        };
        *state = resolved;
        Ok(())
    }

    /// Resolves `row` as rejected. A row resolves once, so rejecting a row already resolved
    /// changes nothing, and a delivered row stays delivered.
    fn mark_rejected(&mut self, row: usize) -> EmitterRuntimeResult<()> {
        let row_count = self.rows.len();
        let state = self.rows.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::RejectionRowOutOfBounds { row, row_count })
        })?;
        if state.is_resolved() {
            return Ok(());
        }
        *state = BufferedRow::Rejected;
        Ok(())
    }

    /// Records that a retained batch payload, HTTP request or prepared row request carries `row`,
    /// which only the sink's answer for it resolves from now on.
    pub(super) fn mark_prepared(&mut self, row: usize) -> EmitterRuntimeResult<()> {
        let row_count = self.rows.len();
        let state = self.rows.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::PreparedRowOutOfBounds { row, row_count })
        })?;
        if *state != BufferedRow::Pending {
            return Err(Report::new(EmitterRuntimeError::RowAlreadyPrepared { row }));
        }
        *state = BufferedRow::Prepared;
        Ok(())
    }

    /// Returns a row whose payload the sink definitively rejected to the rows still pending, until
    /// its own message error is delivered.
    ///
    /// No payload carries the row any more, so a delivery the emitter's stop deadline cuts short
    /// leaves it for the next attempt to prepare again rather than for a payload that is gone.
    pub(super) fn release_prepared(&mut self, row: usize) -> EmitterRuntimeResult<()> {
        let row_count = self.rows.len();
        let state = self.rows.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::RejectionRowOutOfBounds { row, row_count })
        })?;
        if *state == BufferedRow::Prepared {
            *state = BufferedRow::Pending;
        }
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

    /// Every unresolved row in row order, as batch packing reads them: a pending row it may pack,
    /// or a row a retained payload carries, which a payload packed now must not span.
    pub(super) fn rows_to_pack(&self) -> Vec<RowToPack> {
        let mut rows = Vec::with_capacity(self.rows.len());
        for (row, state) in self.rows.iter().enumerate() {
            match state {
                BufferedRow::Pending => rows.push(RowToPack::Pending(row)),
                BufferedRow::Prepared => rows.push(RowToPack::Retained),
                BufferedRow::Delivered | BufferedRow::Staged | BufferedRow::Rejected => {}
            }
        }
        rows
    }

    /// The rows no attempt has prepared yet and nothing has resolved.
    pub(super) fn pending_record_rows(&self) -> Vec<usize> {
        let mut pending = Vec::with_capacity(self.rows.len());
        for (row, state) in self.rows.iter().enumerate() {
            if *state == BufferedRow::Pending {
                pending.push(row);
            }
        }
        pending
    }

    /// The acknowledgements of `rows`, for a sink that takes them with the write.
    pub(super) fn acks_for_rows(&self, rows: &[usize]) -> AckSet {
        AckSet::merged(
            rows.iter()
                .filter_map(|row| self.batch.acks.get(*row).cloned()),
        )
    }
}

/// One unresolved row of a buffered batch, as batch packing reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RowToPack {
    /// A row no payload carries yet, by its index in the batch.
    Pending(usize),
    /// A row a retained payload carries.
    Retained,
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
///
/// The buffer is the one owner of everything a failed attempt leaves behind: the batches, whose
/// acknowledgements the retry keeps alive, where each of their rows stands, and the batch payloads,
/// HTTP requests or prepared row requests an attempt already offered to the sink without learning
/// their outcome. Its connector may be reopened between attempts, so none of that lives with the
/// connector.
#[derive(Default)]
pub(super) struct EmitterBatchBuffer {
    flush_policy: Option<RuntimeFlushPolicy>,
    pending: Vec<EmitterPublishBatch>,
    /// The batch payloads a record sink was handed and has not answered for.
    payloads: PreparedPayloads<EncodedPayload>,
    /// Native Arrow IPC payloads retained with their members until application ACK.
    client_payloads: PreparedPayloads<ClientPayload>,
    /// The requests an HTTP sink was handed and has not answered for.
    requests: PreparedPayloads<PreparedHttpRequest>,
    /// The requests a row request sink prepared and has not answered for. An emitter publishes
    /// through one sink, so at most one of the three holds anything.
    row_requests: PreparedPayloads<RowRequestBody>,
    pending_messages: u64,
    pending_bytes: u64,
    cadence: BranchBufferTimer,
    buffered_messages: Arc<EmitterBufferedMessages>,
}

/// What one flush hands the emitter's sink: the buffered batches, and the batch payloads, HTTP
/// requests or row requests earlier attempts prepared from their rows and retained.
pub(super) struct EmitterPublication<'a> {
    pub(super) batches: &'a mut [EmitterPublishBatch],
    pub(super) payloads: &'a mut PreparedPayloads<EncodedPayload>,
    pub(super) client_payloads: &'a mut PreparedPayloads<ClientPayload>,
    pub(super) requests: &'a mut PreparedPayloads<PreparedHttpRequest>,
    pub(super) row_requests: &'a mut PreparedPayloads<RowRequestBody>,
}

impl EmitterBatchBuffer {
    pub(super) fn new(
        context: &EmitterSinkContext,
        flush_policy: &FlushPolicy,
        buffered_messages: Arc<EmitterBufferedMessages>,
    ) -> Self {
        Self {
            flush_policy: context.parse_flush_policy(flush_policy),
            pending: Vec::new(),
            payloads: PreparedPayloads::default(),
            client_payloads: PreparedPayloads::default(),
            requests: PreparedPayloads::default(),
            row_requests: PreparedPayloads::default(),
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
        self.flush_policy = context.parse_flush_policy(flush_policy);
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

    pub(super) fn publication_mut(&mut self) -> EmitterPublication<'_> {
        EmitterPublication {
            batches: self.pending.as_mut_slice(),
            payloads: &mut self.payloads,
            client_payloads: &mut self.client_payloads,
            requests: &mut self.requests,
            row_requests: &mut self.row_requests,
        }
    }

    #[cfg(test)]
    pub(super) fn pending(&self) -> &[EmitterPublishBatch] {
        self.pending.as_slice()
    }

    #[cfg(test)]
    pub(super) fn set_flush_policy(&mut self, flush_policy: RuntimeFlushPolicy) {
        self.flush_policy = Some(flush_policy);
    }

    /// An empty buffer flushing on `flush_policy` that reports what it holds to
    /// `buffered_messages`, as the node's drain reads it.
    #[cfg(all(test, feature = "shuttle"))]
    pub(super) fn reporting_to(
        buffered_messages: Arc<EmitterBufferedMessages>,
        flush_policy: RuntimeFlushPolicy,
    ) -> Self {
        Self {
            flush_policy: Some(flush_policy),
            pending: Vec::new(),
            payloads: PreparedPayloads::default(),
            client_payloads: PreparedPayloads::default(),
            requests: PreparedPayloads::default(),
            row_requests: PreparedPayloads::default(),
            pending_messages: 0,
            pending_bytes: 0,
            cadence: BranchBufferTimer::default(),
            buffered_messages,
        }
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

    /// Takes every batch back, together with the rows the retained payloads and requests carry,
    /// which are still unresolved in the batches returned.
    pub(super) fn drain_pending(&mut self) -> Vec<EmitterPublishBatch> {
        let pending = std::mem::take(&mut self.pending);
        self.payloads.clear();
        self.client_payloads.clear();
        self.requests.clear();
        self.row_requests.clear();
        self.pending_messages = 0;
        self.pending_bytes = 0;
        self.cadence.clear();
        self.update_buffered_messages();
        pending
    }

    pub(super) fn clear(&mut self) {
        self.pending.clear();
        self.payloads.clear();
        self.client_payloads.clear();
        self.requests.clear();
        self.row_requests.clear();
        self.pending_messages = 0;
        self.pending_bytes = 0;
        self.cadence.clear();
        self.update_buffered_messages();
    }

    /// What a flush that just completed published: every row its sink delivered while this buffer
    /// held it, over every attempt the flush took, each counted once, and the payload bytes they
    /// carry. Absent when the sink delivered none of them.
    pub(super) fn delivered_report(&self) -> Option<PublishReport> {
        let mut report = None;
        for batch in &self.pending {
            report = PublishReport::merge_optional(report, batch.delivered_report());
        }
        report
    }
}

impl Drop for EmitterBatchBuffer {
    /// The buffer lives as long as its emitter task, so dropping it ends what the task held: the
    /// buffered batches, and the rows its sink staged, which the sink's own end abandons.
    fn drop(&mut self) {
        self.pending_acks().no_ack("emitter dropped buffered batch");
        self.buffered_messages.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{
        emitter_ordering_group::EvaluatedOrderingGroups,
        test_fixtures::{input_batch, input_batch_with, input_schema, named, sink_context},
    };

    #[nervix_primitives::test]
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
            nervix_primitives::time::timeout(
                Duration::from_millis(20),
                batch.mark_rejected_after_delivery(0, std::future::pending()),
            )
            .await
            .is_err(),
            "the simulated message-error delivery must remain pending"
        );
        assert_eq!(
            batch.rows[0],
            BufferedRow::Pending,
            "cancelling message-error delivery must leave the poison record retryable"
        );

        batch
            .mark_rejected_after_delivery(0, std::future::ready(()))
            .await
            .expect("completed message-error delivery must account for the record");
        assert_eq!(batch.rows[0], BufferedRow::Rejected);
        assert!(
            batch.delivered_report().is_none(),
            "a rejected record is never sent"
        );
    }

    #[nervix_primitives::test]
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
            nervix_primitives::time::timeout(
                Duration::from_millis(20),
                completion.wait_for_progress(),
            )
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
            named("test_relay"),
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
            named("test_relay"),
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
    fn a_batch_counts_the_request_fields_of_its_rows() {
        let fields = request_fields();
        let expected: u64 =
            ("POST".len() + "/events".len() + "x-key".len() + "abc".len()).arch_into();
        assert_eq!(fields.estimated_bytes(), expected);
        let request_bytes = fields.estimated_bytes();
        let plain = EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
        let with_requests =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100))
                .with_http_requests(AdmittedHttpRequests::published(vec![fields]))
                .expect("one request for the one row");

        assert_eq!(
            with_requests.estimated_bytes(),
            plain.estimated_bytes() + request_bytes
        );
        assert_eq!(
            with_requests
                .http_request(0)
                .expect("the row has its request")
                .target
                .as_str(),
            "/events"
        );
    }

    /// The request fields `POST /events` with one `X-Key` header, which a row is admitted with.
    fn request_fields() -> HttpRequestFields {
        let origin = nervix_models::HttpOrigin::parse("https://api.example.com")
            .expect("the test origin has an HTTPS scheme and a host");
        let mut headers = nervix_models::HttpApplicationHeaders::default();
        headers
            .insert(
                nervix_models::HttpHeaderName::parse("X-Key").expect("a valid field name"),
                nervix_models::HttpHeaderValue::parse("abc").expect("a valid field value"),
            )
            .expect("one short header is within the envelope");
        HttpRequestFields {
            method: nervix_models::HttpMethod::parse(
                "POST",
                nervix_models::HttpBodyMode::WithoutBody,
            )
            .expect("POST is a valid method"),
            target: origin.target("/events").expect("an origin-relative target"),
            headers,
        }
    }

    #[test]
    fn a_delivered_http_request_counts_its_codec_record_and_never_its_request_fields() {
        let published = batch_of(&[1, 2]);
        let without_body =
            EmitterPublishBatch::from_batch(published.clone(), Timestamp::from_unix_nanos(100))
                .with_http_requests(AdmittedHttpRequests::published(vec![
                    request_fields(),
                    request_fields(),
                ]))
                .expect("one request for each row");
        let with_body =
            EmitterPublishBatch::from_batch(published.clone(), Timestamp::from_unix_nanos(100))
                .with_http_requests(AdmittedHttpRequests::encoded(
                    vec![request_fields(), request_fields()],
                    &published,
                    vec![0, 1],
                ))
                .expect("one request for each row");

        for mut batch in [without_body.clone(), with_body.clone()] {
            batch
                .mark_delivered(0, DeliveredAcknowledgements::Host)
                .expect("the first request can be delivered");
            batch
                .mark_rejected(1)
                .expect("the second request can be refused");
            let report = batch.delivered_report().expect("one request was delivered");
            assert_eq!(report.messages, 1);
        }

        let mut bodyless = without_body;
        let mut encoded = with_body;
        for row in 0..2 {
            bodyless
                .mark_delivered(row, DeliveredAcknowledgements::Host)
                .expect("every request can be delivered");
            encoded
                .mark_delivered(row, DeliveredAcknowledgements::Host)
                .expect("every request can be delivered");
        }
        let bodyless = bodyless
            .delivered_report()
            .expect("both requests were delivered");
        assert_eq!(bodyless.messages, 2);
        assert_eq!(bodyless.bytes, 0, "a request without a body carries none");
        let encoded = encoded
            .delivered_report()
            .expect("both requests were delivered");
        assert_eq!(encoded.messages, 2);
        assert_eq!(
            encoded.bytes,
            published.estimated_bytes(),
            "the method, target and header bytes are request metadata, not payload"
        );

        let mut partly =
            EmitterPublishBatch::from_batch(published.clone(), Timestamp::from_unix_nanos(100))
                .with_http_requests(AdmittedHttpRequests::encoded(
                    vec![request_fields(), request_fields()],
                    &published,
                    vec![0, 1],
                ))
                .expect("one request for each row");
        partly
            .mark_rejected(0)
            .expect("the first request can be refused");
        partly
            .mark_delivered(1, DeliveredAcknowledgements::Host)
            .expect("the second request can be delivered");
        let partly = partly
            .delivered_report()
            .expect("one request was delivered");
        assert_eq!(partly.bytes, batch_of(&[2]).estimated_bytes());
    }

    #[test]
    fn a_delivered_row_counts_only_its_own_ordering_group() {
        let mut batch =
            EmitterPublishBatch::from_batch(batch_of(&[1, 2, 3]), Timestamp::from_unix_nanos(100))
                .with_ordering_groups(OrderingGroups::Evaluated(
                    EvaluatedOrderingGroups::from_rows([
                        Ok("tenant-a"),
                        Ok("tenant-bb"),
                        Err(OrderingGroupError::Null),
                    ]),
                ))
                .expect("one group for each row must attach");
        batch
            .mark_delivered(0, DeliveredAcknowledgements::Host)
            .expect("the first row can be delivered");
        batch
            .mark_rejected(1)
            .expect("the second row can be rejected");
        batch
            .mark_delivered(2, DeliveredAcknowledgements::Host)
            .expect("the third row can be delivered");

        let report = batch.delivered_report().expect("two rows were delivered");

        let group_bytes: u64 = "tenant-a".len().arch_into();
        assert_eq!(report.messages, 2);
        assert_eq!(
            report.bytes,
            batch_of(&[1, 3]).estimated_bytes() + group_bytes,
            "a row without a group adds none, and a refused row's group is not sent"
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

    #[nervix_primitives::test]
    async fn a_row_resolves_once_however_often_it_is_answered() {
        let (root, mut completion) = AckSet::root();
        let mut batch = EmitterPublishBatch::from_batch(
            input_batch_with(1, 0, root.attached()),
            Timestamp::from_unix_nanos(100),
        );
        batch
            .mark_prepared(0)
            .expect("a pending row can be prepared");
        let error = batch
            .mark_prepared(0)
            .expect_err("a prepared row cannot be prepared again");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::RowAlreadyPrepared { row: 0 }
        );
        assert!(batch.pending_record_rows().is_empty());

        batch
            .mark_delivered(0, DeliveredAcknowledgements::Host)
            .expect("a prepared row can be delivered");
        batch
            .mark_delivered(0, DeliveredAcknowledgements::Host)
            .expect("delivering a resolved row changes nothing");
        batch
            .release_prepared(0)
            .expect("releasing a resolved row changes nothing");

        assert_eq!(batch.resolved_rows(), vec![true]);
        assert!(
            nervix_primitives::time::timeout(
                Duration::from_millis(20),
                completion.wait_for_progress()
            )
            .await
            .is_err(),
            "a second delivery must not resolve the row's share of the root again"
        );
        root.ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        let error = batch
            .mark_prepared(1)
            .expect_err("preparing needs a row of the batch");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::PreparedRowOutOfBounds {
                row: 1,
                row_count: 1,
            }
        );
    }

    #[test]
    fn packing_reads_a_retained_row_as_a_boundary_and_skips_a_resolved_one() {
        let mut batch = EmitterPublishBatch::from_batch(
            RelayRecordBatch::from_messages(
                input_schema(),
                (1..=4)
                    .map(|value| RelayMessage {
                        key: None,
                        record: test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
                        acks: AckSet::empty(),
                    })
                    .collect(),
            )
            .expect("valid four-row emitter batch"),
            Timestamp::from_unix_nanos(100),
        );
        batch
            .mark_prepared(1)
            .expect("a pending row can be prepared");
        batch
            .mark_delivered(3, DeliveredAcknowledgements::Host)
            .expect("a pending row can be delivered");

        assert_eq!(
            batch.rows_to_pack(),
            vec![
                RowToPack::Pending(0),
                RowToPack::Retained,
                RowToPack::Pending(2),
            ]
        );
    }

    #[test]
    fn a_released_row_waits_for_the_next_attempt() {
        let mut batch =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
        batch
            .mark_prepared(0)
            .expect("a pending row can be prepared");
        batch
            .release_prepared(0)
            .expect("a prepared row can be released");

        assert_eq!(batch.pending_record_rows(), vec![0]);
        assert_eq!(batch.resolved_rows(), vec![false]);
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
    #[nervix_primitives::test]
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
            named("test_relay"),
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
        assert!(
            buffer.delivered_report().is_none(),
            "nothing is sent before the sink delivers it"
        );
        for batch in &mut buffer.pending {
            for row in 0..batch.rows.len() {
                batch
                    .mark_delivered(row, DeliveredAcknowledgements::Host)
                    .expect("every buffered row can be delivered");
            }
        }
        let report = buffer
            .delivered_report()
            .expect("delivered batches must produce a report");
        assert_eq!(report.messages, 3);
        assert_eq!(report.bytes, expected_bytes);
        assert_eq!(report.domain_timestamp, Timestamp::from_unix_nanos(20));
        assert_eq!(buffer.pending_bytes, expected_bytes);
    }

    /// A batch of `values` whose every record has one acknowledgement-free message.
    fn batch_of(values: &[i64]) -> RelayRecordBatch {
        let mut messages = Vec::with_capacity(values.len());
        for value in values {
            messages.push(RelayMessage {
                key: None,
                record: test_runtime_row([("value".to_string(), RuntimeValue::I64(*value))])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(30)),
                acks: AckSet::empty(),
            });
        }
        RelayRecordBatch::from_messages(input_schema(), messages)
            .expect("the test records match the emitter input schema")
    }

    #[test]
    fn a_flush_reports_each_delivered_row_once_and_no_rejected_row() {
        let headers = vec![
            vec![("a".to_string(), "1".to_string())],
            vec![("b".to_string(), "22".to_string())],
            vec![("c".to_string(), "333".to_string())],
        ];
        let mut mixed = EmitterPublishBatch::new(
            named("test_relay"),
            batch_of(&[1, 2, 3]),
            Some(headers),
            Timestamp::from_unix_nanos(100),
        )
        .expect("headers must align");
        mixed
            .mark_delivered(0, DeliveredAcknowledgements::Host)
            .expect("the first row can be delivered");
        mixed
            .mark_rejected(1)
            .expect("the second row can be rejected");
        mixed
            .mark_delivered(2, DeliveredAcknowledgements::Host)
            .expect("the third row can be delivered");
        mixed
            .mark_rejected(2)
            .expect("rejecting a delivered row changes nothing");
        let mut refused =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
        refused
            .mark_rejected(0)
            .expect("the only row can be rejected");
        assert!(
            refused.delivered_report().is_none(),
            "a batch whose every row was refused sends nothing"
        );
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Immediate);
        buffer
            .retain_without_cadence(mixed)
            .expect("the configured buffer retains the batch");
        buffer
            .retain_without_cadence(refused)
            .expect("the configured buffer retains the batch");

        let report = buffer.delivered_report().expect("two rows were delivered");

        // The delivered rows carry their own records and headers, and nothing of the refused ones.
        let header_bytes: u64 = "a1c333".len().arch_into();
        assert_eq!(report.messages, 2);
        assert_eq!(
            report.bytes,
            batch_of(&[1, 3]).estimated_bytes() + header_bytes
        );
        assert_eq!(report.domain_timestamp, Timestamp::from_unix_nanos(30));
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

    #[nervix_primitives::test]
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
        assert_eq!(batch.resolved_rows(), vec![true, true]);
        // The sink's commit counts the staged row as sent, so the batch counts only the other.
        let report = batch.delivered_report().expect("the published row is sent");
        assert_eq!(report.messages, 1);
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

    /// A sink that commits on its own cadence still holds staged messages when its emitter task is
    /// torn down. Nothing holds them once the task is gone, so the count drains read for the
    /// emitter drops to zero instead of keeping them until a restarted task first changes its
    /// buffer.
    #[test]
    fn a_torn_down_emitter_keeps_none_of_its_staged_messages_counted() {
        let context = sink_context();
        let reported_messages = Arc::new(AtomicUsize::new(0));
        let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
        let buffer =
            EmitterBatchBuffer::new(&context, &FlushPolicy::Immediate, buffered_messages.clone());
        buffer.report_staged_messages(3);
        assert_eq!(reported_messages.load(Ordering::Acquire), 3);

        drop(buffer);

        assert_eq!(
            reported_messages.load(Ordering::Acquire),
            0,
            "a torn-down emitter still reports the messages its sink had staged"
        );
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
        assert!(buffer.delivered_report().is_none());
        assert_eq!(buffer.pending_messages, 0);
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);

        buffered_messages.set_buffered(7);
        drop(buffer);
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);
    }

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
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
            nervix_primitives::time::timeout(
                Duration::from_millis(20),
                completion.wait_for_progress()
            )
            .await
            .is_err(),
            "the retained attached share must keep the root pending"
        );

        buffer.pending_acks().ack_success();
        buffer.clear();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        drop(batch);
    }

    #[nervix_primitives::test]
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
