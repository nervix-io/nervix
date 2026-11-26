//! The batches an emitter holds between receiving and publishing them.
//!
//! Layer: data plane.
//! - **Owns.** The emitter's buffer of released batches with their source relay and branch, the
//!   request an HTTP emitter admitted each row with, what the error route of a row that fails after
//!   the emitter's route ran reads (the source row the route built it from, the record it built and
//!   the materialized state its batch was admitted with) and the bound on what the buffer keeps for
//!   it, message and byte accounting, where every row stands in its publication and which rows its
//!   emitter delivered, what a flush reports as sent, the batch payloads, HTTP requests and prepared
//!   row requests retained for the rows they carry, and flush cadence.
//! - **Depends on.** Relay batches and their acknowledgements, the emitter's flush policy, and the
//!   domain clock its cadence is resolved against.
//! - **Must not know.** Which connector publishes the batches, how their rows are encoded or
//!   mapped, or how a failed publish is retried.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::{RecordEnvelope, physical_time::PhysicalDeadline};

use super::{emitter_task::branch_retention::EmitterBranchRetirements, *};
use crate::metrics::{EmitterWork, EmitterWorkKind};

/// What one emitter still holds: the batches its own buffer collected, and the rows an emitter that
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

    /// Reports that the emitter holds nothing: its buffer and the rows its emitter staged are gone
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
    /// The envelope of every row, absent when the emitter writes none.
    envelopes: Option<RowEnvelopes>,
    /// The request every row was admitted with, present only for an emitter that publishes through
    /// an HTTP emitter.
    http_requests: Option<AdmittedHttpRequests>,
    /// What the error route of every row reads once the row fails after the emitter's route ran,
    /// present only for an emitter whose message-error policy sends to an error relay and whose
    /// rows need more than they hold themselves.
    error_route_inputs: Option<ErrorRouteInputs>,
    /// Where every row stands in its publication.
    rows: Vec<BufferedRow>,
    /// When the emitter's `GIVE UP AFTER` stops retrying this batch.
    give_up: GiveUpClock,
}

/// When an emitter stops retrying a buffered batch: its `GIVE UP AFTER` measured from the first
/// failed attempt that held the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GiveUpClock {
    /// No attempt that held the batch has failed since the emitter received it, or the emitter
    /// declares no `GIVE UP AFTER`.
    NotStarted,
    /// The emitter gives up on the batch once this physical deadline is reached.
    Until(PhysicalDeadline),
    /// The limit ends beyond every instant the monotonic clock can report, so the emitter retries
    /// the batch until its destination takes it, as it would without a limit.
    Unreachable,
    /// The destination's own delivery owner, which retries a payload by itself, measured the
    /// limit from the payload's first failed attempt and gave the payload up, so the emitter gives
    /// up every batch the payload carries.
    RanOut,
}

impl GiveUpClock {
    /// The deadline this clock gives up at, while it has one.
    pub(super) fn deadline(self) -> Option<PhysicalDeadline> {
        match self {
            Self::Until(deadline) => Some(deadline),
            Self::NotStarted | Self::Unreachable | Self::RanOut => None,
        }
    }
}

/// Where one buffered row stands between the emitter receiving it and the emitter owning nothing
/// of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferedRow {
    /// No attempt has prepared the row for its emitter yet, or the attempt that did left it for the
    /// next one.
    Pending,
    /// A batch payload, HTTP request or prepared row request the emitter retains carries the row,
    /// so only the emitter's answer for it resolves the row.
    Prepared,
    /// The emitter delivered the row, which the emitter's sent counters count once.
    Delivered,
    /// An emitter that publishes on its own commit accepted the row and took its acknowledgements, so
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

/// The source rows a route built the published rows of one batch from, one for each published row
/// in row order. They share the source batch's columns rather than copying them.
#[derive(Clone)]
pub(in crate::runtime) struct RouteSourceRows {
    records: SourceRecords,
    /// The source row of each published row, in row order.
    rows: Vec<usize>,
}

impl RouteSourceRows {
    pub(in crate::runtime) fn new(records: SourceRecords, rows: Vec<usize>) -> Self {
        Self { records, rows }
    }

    /// The source row published row `row` was built from, without copying its values.
    fn source_row(&self, row: usize) -> EmitterRuntimeResult<RuntimeRow> {
        let Some(source_row) = self.rows.get(row) else {
            return Err(Report::new(EmitterRuntimeError::MissingSourceRow { row }));
        };
        self.records
            .row(*source_row)
            .change_context(EmitterRuntimeError::MissingSourceRow { row })
    }
}

/// What the error route of each row of one batch reads besides the error once the row fails after
/// the emitter's route ran: the source row the route built the row from, when the route built
/// one, and the materialized state the batch was admitted with. The record the route built is the
/// row the batch publishes, so it is never kept twice.
#[derive(Clone)]
pub(in crate::runtime) struct ErrorRouteInputs {
    /// Absent when the published rows are their own source rows.
    built_from: Option<RouteSourceRows>,
    materialized_state: HashMap<String, RuntimeValue>,
}

impl ErrorRouteInputs {
    pub(in crate::runtime) fn new(
        built_from: Option<RouteSourceRows>,
        materialized_state: HashMap<String, RuntimeValue>,
    ) -> Self {
        Self {
            built_from,
            materialized_state,
        }
    }

    /// Whether these inputs name nothing beyond what the published rows hold themselves.
    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.built_from.is_none() && self.materialized_state.is_empty()
    }

    /// The bytes these inputs keep alive: the Arrow data of the whole source batch the route built
    /// from, which they hold however few of its rows they name, and the state snapshot.
    fn retained_bytes(&self) -> u64 {
        let source_bytes = match &self.built_from {
            Some(built_from) => built_from.records.estimated_bytes(),
            None => 0,
        };
        source_bytes
            .checked_add(state_bytes(&self.materialized_state))
            .assured(BYTES_IN_MEMORY)
    }

    /// What the error route of row `row` of `published`, the batch these inputs were kept with,
    /// reads besides the error: the source row the route built it from and the built row as
    /// `partial_output`, or the row itself when the route built none, with the state snapshot.
    pub(in crate::runtime) fn rejected_record(
        &self,
        published: &RelayRecordBatch,
        row: usize,
    ) -> EmitterRuntimeResult<RejectedRecordInput> {
        let (record, partial_output) = match &self.built_from {
            Some(built_from) => (
                built_from.source_row(row)?,
                finalized_partial_output(&published.batch, row),
            ),
            None => (
                published
                    .runtime_row(row)
                    .change_context(EmitterRuntimeError::EncodeBatch)?,
                None,
            ),
        };
        Ok(RejectedRecordInput {
            record,
            partial_output,
            materialized_state: self.materialized_state.clone(),
        })
    }
}

/// The bytes a materialized-state snapshot holds: each name and the data of each value.
fn state_bytes(state: &HashMap<String, RuntimeValue>) -> u64 {
    let mut bytes = 0_u64;
    for (name, value) in state {
        let name_bytes: u64 = name.len().arch_into();
        bytes = bytes
            .checked_add(name_bytes)
            .assured(BYTES_IN_MEMORY)
            .checked_add(value_bytes(value))
            .assured(BYTES_IN_MEMORY);
    }
    bytes
}

/// The bytes one value holds: the text of a string, the values of a list, and the width a column
/// of its type stores it in otherwise.
fn value_bytes(value: &RuntimeValue) -> u64 {
    match value {
        RuntimeValue::String(text) => text.len().arch_into(),
        RuntimeValue::Array(values) | RuntimeValue::Vec(values) => {
            let mut bytes = 0_u64;
            for value in values {
                bytes = bytes
                    .checked_add(value_bytes(value))
                    .assured(BYTES_IN_MEMORY);
            }
            bytes
        }
        RuntimeValue::U8(_) | RuntimeValue::I8(_) | RuntimeValue::Bool(_) => 1,
        RuntimeValue::U16(_) | RuntimeValue::I16(_) => 2,
        RuntimeValue::U32(_) | RuntimeValue::I32(_) | RuntimeValue::F32(_) => 4,
        RuntimeValue::U64(_)
        | RuntimeValue::I64(_)
        | RuntimeValue::F64(_)
        | RuntimeValue::Datetime(_)
        | RuntimeValue::Duration(_) => 8,
    }
}

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
            envelopes: None,
            http_requests: None,
            error_route_inputs: None,
            rows: vec![BufferedRow::Pending; row_count],
            give_up: GiveUpClock::NotStarted,
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
            envelopes: None,
            http_requests: None,
            error_route_inputs: None,
            rows: vec![BufferedRow::Pending; row_count],
            give_up: GiveUpClock::NotStarted,
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

    /// This batch with the envelope `envelope` evaluates for each of its rows, reading `input` as
    /// the source records of its rows.
    ///
    /// Fails only when the envelope cannot be evaluated over the batch at all. A row whose own
    /// envelope cannot be evaluated keeps its reason, and the emitter rejects it when it publishes.
    pub(super) async fn with_evaluated_envelopes(
        mut self,
        envelope: &CompiledEnvelope,
        executor: &Executor,
        ack_timing: &SharedRemoteAckTiming,
        input: EnvelopeInput<'_>,
        side_inputs: &HashMap<String, RuntimeValue>,
        execution_now: Timestamp,
    ) -> Result<Self, PlannedGeneralFailure> {
        let envelopes = envelope
            .evaluate(
                ProgramRun {
                    executor,
                    now: execution_now,
                    ack_timing,
                },
                &self.source_relay,
                &mut self.batch,
                input,
                side_inputs,
            )
            .await?;
        self.envelopes = Some(envelopes);
        Ok(self)
    }

    /// This batch with `envelopes` as the envelope of each of its rows.
    #[cfg(test)]
    pub(super) fn with_envelopes(mut self, envelopes: RowEnvelopes) -> Self {
        if let Some(envelope_count) = envelopes.row_count() {
            assert_eq!(
                envelope_count,
                self.batch.batch.batch().num_rows(),
                "a test gives every row of the batch one envelope"
            );
        }
        self.envelopes = Some(envelopes);
        self
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

    /// This batch with what the error route of each of its rows reads once the row fails after the
    /// emitter's route ran.
    pub(super) fn with_error_route_inputs(
        mut self,
        inputs: ErrorRouteInputs,
    ) -> EmitterRuntimeResult<Self> {
        let row_count = self.batch.batch.batch().num_rows();
        if let Some(built_from) = &inputs.built_from
            && built_from.rows.len() != row_count
        {
            return Err(Report::new(EmitterRuntimeError::SourceRowCountMismatch {
                source_rows: built_from.rows.len(),
                row_count,
            }));
        }
        self.error_route_inputs = Some(inputs);
        Ok(self)
    }

    /// What the message error of row `row` reads besides the error once the row is rejected or
    /// fails after admission: what the emitter kept for its error route, or, for an emitter that
    /// kept nothing because no policy of it reads more, the row as this batch holds it.
    pub(super) fn rejected_record_input(
        &self,
        row: usize,
    ) -> EmitterRuntimeResult<RejectedRecordInput> {
        if let Some(inputs) = &self.error_route_inputs {
            return inputs.rejected_record(&self.batch, row);
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

    /// What the message error of each row reads once the emitter fails this batch as a whole, in
    /// row order, as [`Self::rejected_record_input`] names it: nothing for a row already resolved,
    /// which the failure leaves alone.
    pub(super) fn unresolved_row_inputs(
        &self,
    ) -> EmitterRuntimeResult<Vec<Option<RejectedRecordInput>>> {
        let mut inputs = Vec::with_capacity(self.rows.len());
        for (row, state) in self.rows.iter().enumerate() {
            let input = match state.is_resolved() {
                true => None,
                false => Some(self.rejected_record_input(row)?),
            };
            inputs.push(input);
        }
        Ok(inputs)
    }

    /// The bytes this batch keeps for its error route beside what it publishes. The buffer bounds
    /// them apart from the batch's own bytes, and they are never part of what the emitter reports
    /// as sent.
    fn retained_bytes(&self) -> u64 {
        match &self.error_route_inputs {
            Some(inputs) => inputs.retained_bytes(),
            None => 0,
        }
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

    /// The envelope row `row` is published with, absent when the emitter writes none, or why the
    /// row has none. The row shares it with this batch rather than copying it.
    pub(super) fn envelope(
        &self,
        row: usize,
    ) -> EmitterRuntimeResult<Result<Option<Arc<RecordEnvelope>>, EnvelopeError>> {
        let Some(envelopes) = &self.envelopes else {
            return Ok(Ok(None));
        };
        match envelopes.envelope(row) {
            Some(Ok(envelope)) => Ok(Ok(Some(Arc::clone(envelope)))),
            Some(Err(failure)) => Ok(Err(failure.clone())),
            None => Err(Report::new(EmitterRuntimeError::EncodeBatch)
                .attach_printable(format!("emitter batch row {row} has no envelope entry"))),
        }
    }

    pub(super) fn destination(
        &self,
        row: usize,
    ) -> Option<Arc<nervix_connector::RecordDestination>> {
        self.envelopes.as_ref()?.destination(row).cloned()
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
        let envelope_bytes = match &self.envelopes {
            Some(envelopes) => envelopes.estimated_bytes(),
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
            .checked_add(envelope_bytes)
            .assured(BYTES_IN_MEMORY)
            .checked_add(request_bytes)
            .assured(BYTES_IN_MEMORY)
    }

    /// The name and value bytes of `headers`, which one row is published with.
    fn header_bytes(headers: &EmitterHeaders) -> u64 {
        let mut bytes = 0_u64;
        match headers {
            EmitterHeaders::Text(headers) => {
                for (name, value) in headers {
                    let name_len: u64 = name.len().arch_into();
                    let value_len: u64 = value.len().arch_into();
                    bytes = bytes
                        .checked_add(name_len)
                        .assured(BYTES_IN_MEMORY)
                        .checked_add(value_len)
                        .assured(BYTES_IN_MEMORY);
                }
            }
            EmitterHeaders::Typed(headers) => {
                for header in headers {
                    bytes = bytes
                        .checked_add(header.held_bytes())
                        .assured(BYTES_IN_MEMORY);
                }
            }
        }
        bytes
    }

    /// The rows of this batch its emitter delivered, in row order.
    fn delivered_rows(&self) -> Vec<usize> {
        let mut delivered = Vec::with_capacity(self.rows.len());
        for (row, state) in self.rows.iter().enumerate() {
            if *state == BufferedRow::Delivered {
                delivered.push(row);
            }
        }
        delivered
    }

    /// What this batch adds to its emitter's sent counters: every row its emitter delivered, counted
    /// once however many attempts it took, and the payload bytes those rows carry. A row the emitter
    /// rejected is not sent, and a row an emitter staged is counted by its commit, so a batch the emitter
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
    /// their records with the headers, ordering groups and envelopes they were published with,
    /// which is how the buffer measures the whole batch.
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
        let envelope_bytes = match &self.envelopes {
            Some(envelopes) => envelopes.estimated_bytes_of_rows(delivered),
            None => 0,
        };
        self.batch
            .payload_bytes_of_rows(delivered)
            .assured(DELIVERED_ROWS)
            .checked_add(header_bytes)
            .assured(BYTES_IN_MEMORY)
            .checked_add(group_bytes)
            .assured(BYTES_IN_MEMORY)
            .checked_add(envelope_bytes)
            .assured(BYTES_IN_MEMORY)
    }

    pub(super) fn headers_for_row(&self, row: usize) -> Option<&EmitterHeaders> {
        // The headers of a row whose emitter's route makes no write.
        static EMPTY: EmitterHeaders = EmitterHeaders::Text(Vec::new());

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
    #[cfg(test)]
    pub(super) fn resolved_rows(&self) -> Vec<bool> {
        self.rows.iter().map(|row| row.is_resolved()).collect()
    }

    pub(super) fn message_count(&self) -> u64 {
        self.batch.message_count()
    }

    fn unresolved_messages(&self) -> u64 {
        let mut messages = 0;
        for row in &self.rows {
            if !row.is_resolved() {
                messages += 1;
            }
        }
        messages
    }

    fn domain_timestamp(&self) -> Option<Timestamp> {
        self.batch.domain_timestamp()
    }

    /// Starts this batch's give-up clock as `clock`, unless an earlier failed attempt already
    /// started it: the emitter gives up measuring from the first failed attempt that held the
    /// batch.
    fn start_give_up_clock(&mut self, clock: GiveUpClock) {
        if self.give_up == GiveUpClock::NotStarted {
            self.give_up = clock;
        }
    }

    /// Whether the emitter has retried this batch for as long as its `GIVE UP AFTER` allows, at the
    /// instant `now` marks.
    fn has_given_up(&self, now: PhysicalDeadline) -> bool {
        match self.give_up {
            GiveUpClock::Until(deadline) => deadline <= now,
            GiveUpClock::RanOut => true,
            GiveUpClock::NotStarted | GiveUpClock::Unreachable => false,
        }
    }

    /// Gives this batch up at once, because the destination's own delivery owner gave up a payload
    /// that carries one of its rows after measuring the limit itself.
    pub(super) fn give_up_now(&mut self) {
        self.give_up = GiveUpClock::RanOut;
    }

    pub(super) fn merged_acks(&self) -> AckSet {
        self.batch.merged_acks()
    }

    /// Releases every row the emitter has not resolved as done, as the drop that destroys the
    /// emitter does, and answers how many they were. Staged rows belong to the connector, which
    /// releases them itself.
    fn release_unresolved_as_done(&self) -> u64 {
        let mut released = 0_u64;
        for (row, state) in self.rows.iter().enumerate() {
            if state.is_resolved() {
                continue;
            }
            self.batch
                .acks
                .get(row)
                .verified("a buffered row retains the ACK validated with its source batch")
                .ack_success();
            released = released
                .checked_add(1)
                .assured("the rows of one buffered batch are fewer than u64::MAX");
        }
        released
    }

    /// Staged rows already belong to the connector; retirement abandons only host-owned rows.
    fn retire_unresolved(&self) {
        for (row, state) in self.rows.iter().enumerate() {
            if !state.is_resolved() {
                self.batch
                    .acks
                    .get(row)
                    .verified("a buffered row retains the ACK validated with its source batch")
                    .no_ack("emitter source branch retired");
            }
        }
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
            DeliveredAcknowledgements::Connector => BufferedRow::Staged,
        };
        *state = resolved;
        Ok(())
    }

    /// Resolves `row` as rejected. A row resolves once, so rejecting a row already resolved
    /// changes nothing, and a delivered row stays delivered.
    fn mark_rejected(&mut self, row: usize) -> EmitterRuntimeResult<bool> {
        let row_count = self.rows.len();
        let state = self.rows.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::RejectionRowOutOfBounds { row, row_count })
        })?;
        if state.is_resolved() {
            return Ok(false);
        }
        *state = BufferedRow::Rejected;
        Ok(true)
    }

    /// Records that a retained batch payload, HTTP request or prepared row request carries `row`,
    /// which only the emitter's answer for it resolves from now on.
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

    /// Returns a row whose payload the emitter definitively rejected to the rows still pending, until
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
    ) -> EmitterRuntimeResult<bool> {
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

    /// The acknowledgements of `rows`, for an emitter that takes them with the write.
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
    /// The emitter took them with the rows and resolves them when its own commit succeeds.
    Connector,
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
/// HTTP requests or prepared row requests an attempt already offered to the emitter without learning
/// their outcome. Its connector may be reopened between attempts, so none of that lives with the
/// connector.
#[derive(Default)]
pub(super) struct EmitterBatchBuffer {
    flush_policy: Option<RuntimeFlushPolicy>,
    pending: Vec<EmitterPublishBatch>,
    /// The batch payloads a record emitter was handed and has not answered for.
    payloads: PreparedPayloads<EncodedPayload>,
    /// Native Arrow IPC payloads retained with their members until application ACK.
    client_payloads: PreparedPayloads<ClientPayload>,
    /// The requests an HTTP emitter was handed and has not answered for.
    requests: PreparedPayloads<PreparedHttpRequest>,
    /// The requests a row request emitter prepared and has not answered for. An emitter publishes
    /// through one connector, so at most one prepared store holds anything.
    row_requests: PreparedPayloads<RowRequestBody>,
    pending_messages: u64,
    pending_bytes: u64,
    /// The bytes the pending batches keep for their error routes, which the flush policy's
    /// `MAX BATCH SIZE` bounds apart from `pending_bytes`.
    retained_bytes: u64,
    cadence: BranchBufferTimer,
    buffered_messages: Arc<EmitterBufferedMessages>,
    observations: Option<EmitterWork>,
}

/// What one flush hands the emitter's connector: the buffered batches, and the batch payloads, HTTP
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
        context: &EmitterConnectorContext,
        flush_policy: &FlushPolicy,
        buffered_messages: Arc<EmitterBufferedMessages>,
    ) -> Self {
        Self {
            flush_policy: context.parse_flush_policy(flush_policy),
            pending: Vec::new(),
            payloads: PreparedPayloads::observed(&context.interactions),
            client_payloads: PreparedPayloads::observed(&context.interactions),
            requests: PreparedPayloads::observed(&context.interactions),
            row_requests: PreparedPayloads::observed(&context.interactions),
            pending_messages: 0,
            pending_bytes: 0,
            retained_bytes: 0,
            cadence: BranchBufferTimer::default(),
            buffered_messages,
            observations: Some(context.interactions.work()),
        }
    }

    fn update_buffered_messages(&mut self) {
        self.buffered_messages
            .set_buffered(self.pending_messages.arch_into());
        if let Some(work) = &mut self.observations {
            work.set(EmitterWorkKind::Buffered, self.pending_messages);
        }
    }

    /// Publishes what an emitter that commits separately still holds, which a drain reads together
    /// with this buffer's own pending batches.
    pub(super) fn report_staged_messages(&mut self, messages: u64) {
        self.buffered_messages.set_staged(messages.arch_into());
        if let Some(work) = &mut self.observations {
            work.set(EmitterWorkKind::Staged, messages);
        }
    }

    /// Counts only rows still owned by a publish, without allocating a resolution bitmap.
    pub(super) fn unresolved_messages(&self) -> u64 {
        let mut messages = 0;
        for batch in &self.pending {
            messages += batch.unresolved_messages();
        }
        messages
    }

    pub(super) fn reconfigure(
        &mut self,
        context: &EmitterConnectorContext,
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
        context: &EmitterConnectorContext,
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
            retained_bytes: 0,
            cadence: BranchBufferTimer::default(),
            buffered_messages,
            observations: None,
        }
    }

    pub(super) fn deadline(&self) -> Option<BranchBufferDeadline> {
        self.cadence.deadline()
    }

    pub(super) fn push(
        &mut self,
        context: &EmitterConnectorContext,
        batch: EmitterPublishBatch,
    ) -> EmitterRuntimeResult<bool> {
        let Some(flush_policy) = self.flush_policy else {
            return Err(Report::new(EmitterRuntimeError::FlushPolicyNotInitialized));
        };
        self.retain(batch);
        self.arm_cadence(context, flush_policy)?;
        // What the batches keep for their error routes has an allowance of its own, so a buffer
        // holds at most about twice the bound, and batching changes only where the kept rows
        // outweigh the records the emitter publishes.
        Ok(flush_policy.size_boundary_reached(self.pending_bytes)
            || flush_policy.size_boundary_reached(self.retained_bytes))
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
        self.retained_bytes = self
            .retained_bytes
            .checked_add(batch.retained_bytes())
            .assured("both counts estimate bytes of batches this node already holds in memory");
        self.pending.push(batch);
        self.update_buffered_messages();
    }

    fn is_due(&self, context: &EmitterConnectorContext) -> EmitterRuntimeResult<bool> {
        let snapshot = context.execution_snapshot()?;
        self.cadence
            .is_due(&context.clock, &snapshot)
            .change_context(EmitterRuntimeError::FlushTiming)
    }

    pub(super) fn should_flush(
        &self,
        context: &EmitterConnectorContext,
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

    /// Releases every row the buffer holds unresolved as done, as the drop that destroys the
    /// emitter does, and answers how many they were. The buffer holds nothing afterwards.
    pub(super) fn release_as_done(&mut self) -> u64 {
        let mut released = 0_u64;
        for batch in self.drain_pending() {
            released = released
                .checked_add(batch.release_unresolved_as_done())
                .assured("the rows one emitter buffers are fewer than u64::MAX");
        }
        released
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
        self.retained_bytes = 0;
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
        self.retained_bytes = 0;
        self.cadence.clear();
        self.update_buffered_messages();
    }

    /// Ends only the named source branches. Stable in-place compaction keeps carrier order and
    /// remaps every prepared store before publishing the reduced buffer depth. The task owns
    /// this operation exclusively; yielding between carriers lets terminal teardown cancel it.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the task supplies a synchronous observer of rows already \
                                   delivered; local observer bodies retain their \
                                   recurring-execution checks")
    )]
    pub(super) async fn retire_branches(
        &mut self,
        retired: &EmitterBranchRetirements,
        observe_sent: &mut impl FnMut(&PublishReport),
    ) {
        if retired.is_empty() {
            return;
        }
        let Some(first) = self
            .pending
            .iter()
            .position(|batch| retired.contains(batch.source_relay(), &batch.relay_batch().key))
        else {
            return;
        };
        let Self {
            flush_policy: _,
            pending,
            payloads,
            client_payloads,
            requests,
            row_requests,
            pending_messages,
            pending_bytes,
            retained_bytes,
            cadence,
            buffered_messages,
            observations,
        } = self;
        let mut positions = (0..first).map(Some).collect::<Vec<_>>();
        positions.reserve(
            pending
                .len()
                .checked_sub(first)
                .verified("first is an index returned by this pending sequence"),
        );
        let mut next = first;
        for index in first..pending.len() {
            nervix_primitives::task::consume_budget().await;
            let batch = &pending[index];
            if retired.contains(batch.source_relay(), &batch.relay_batch().key) {
                if let Some(report) = batch.delivered_report() {
                    observe_sent(&report);
                }
                batch.retire_unresolved();
                *pending_messages = pending_messages
                    .checked_sub(batch.message_count())
                    .assured("retired messages were included when their batch entered this buffer");
                *pending_bytes = pending_bytes
                    .checked_sub(batch.estimated_bytes())
                    .assured("retired bytes were included when their batch entered this buffer");
                *retained_bytes = retained_bytes
                    .checked_sub(batch.retained_bytes())
                    .assured("retired bytes were included when their batch entered this buffer");
                positions.push(None);
            } else {
                positions.push(Some(next));
                pending.swap(index, next);
                next = next
                    .checked_add(1)
                    .assured("surviving carriers fit in the buffer already allocated");
            }
        }
        pending.truncate(next);
        payloads.remap_batches(&positions).await;
        client_payloads.remap_batches(&positions).await;
        requests.remap_batches(&positions).await;
        row_requests.remap_batches(&positions).await;
        if pending.is_empty() {
            cadence.clear();
        }
        buffered_messages.set_buffered((*pending_messages).arch_into());
        if let Some(work) = observations {
            work.set(EmitterWorkKind::Buffered, *pending_messages);
        }
    }

    /// Moves from `retired` into `kept` every branch this buffer still holds an unresolved row of:
    /// the emitter holds that branch's work, so the branch stays.
    pub(super) fn keep_branches_with_held_work(
        &self,
        retired: &mut EmitterBranchRetirements,
        kept: &mut EmitterBranchRetirements,
    ) {
        for batch in &self.pending {
            if batch.unresolved_messages() == 0 {
                continue;
            }
            let key = &batch.relay_batch().key;
            if retired.remove(batch.source_relay(), key)
                && let Some(key) = key
            {
                kept.insert(batch.source_relay().clone(), [key.clone()]);
            }
        }
    }

    /// Starts the give-up clock of every held batch whose clock has not started yet as `clock`,
    /// because an attempt that held them just failed. The clock of a batch an earlier failed
    /// attempt already held keeps counting from that first failure.
    pub(super) fn start_give_up_clocks(&mut self, clock: GiveUpClock) {
        for batch in &mut self.pending {
            batch.start_give_up_clock(clock);
        }
    }

    /// The earliest physical deadline at which this buffer gives a held batch up, absent while no
    /// held batch has a give-up clock running.
    pub(super) fn next_give_up(&self) -> Option<PhysicalDeadline> {
        let mut next: Option<PhysicalDeadline> = None;
        for batch in &self.pending {
            let Some(deadline) = batch.give_up.deadline() else {
                continue;
            };
            next = match next {
                Some(earlier) => Some(earlier.min(deadline)),
                None => Some(deadline),
            };
        }
        next
    }

    /// Takes out every held batch the emitter has retried for as long as its `GIVE UP AFTER`
    /// allows by the instant `now` marks, so that their unresolved rows can fail.
    ///
    /// A batch payload, HTTP request or prepared row request carries its rows as one unit: it is
    /// written again with exactly the bytes and members it was first written with, so none of its
    /// members can be retried while another one is given up. A batch whose rows share a retained
    /// payload with a given-up batch is therefore given up with it. The batches that stay keep their
    /// order, and every prepared store is remapped onto them.
    pub(super) async fn take_given_up(
        &mut self,
        now: PhysicalDeadline,
    ) -> Vec<EmitterPublishBatch> {
        let mut taken = Vec::with_capacity(self.pending.len());
        for batch in &self.pending {
            taken.push(batch.has_given_up(now));
        }
        if !taken.contains(&true) {
            return Vec::new();
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut spread = self.payloads.spread_over_members(&mut taken);
            spread |= self.client_payloads.spread_over_members(&mut taken);
            spread |= self.requests.spread_over_members(&mut taken);
            spread |= self.row_requests.spread_over_members(&mut taken);
            if !spread {
                break;
            }
        }
        let Self {
            flush_policy: _,
            pending,
            payloads,
            client_payloads,
            requests,
            row_requests,
            pending_messages,
            pending_bytes,
            retained_bytes,
            cadence,
            buffered_messages,
            observations,
        } = self;
        let mut positions = Vec::with_capacity(pending.len());
        let mut kept = Vec::with_capacity(pending.len());
        let mut given_up = Vec::new();
        let mut next = 0_usize;
        for (batch, taken) in std::mem::take(pending).into_iter().zip(taken) {
            nervix_primitives::task::consume_budget().await;
            if taken {
                *pending_messages = pending_messages
                    .checked_sub(batch.message_count())
                    .assured("given-up messages were counted when their batch entered this buffer");
                *pending_bytes = pending_bytes
                    .checked_sub(batch.estimated_bytes())
                    .assured("given-up bytes were counted when their batch entered this buffer");
                *retained_bytes = retained_bytes
                    .checked_sub(batch.retained_bytes())
                    .assured("given-up bytes were counted when their batch entered this buffer");
                positions.push(None);
                given_up.push(batch);
            } else {
                positions.push(Some(next));
                kept.push(batch);
                next = next
                    .checked_add(1)
                    .assured("kept carriers fit in the buffer already allocated");
            }
        }
        *pending = kept;
        payloads.remap_batches(&positions).await;
        client_payloads.remap_batches(&positions).await;
        requests.remap_batches(&positions).await;
        row_requests.remap_batches(&positions).await;
        if pending.is_empty() {
            cadence.clear();
        }
        buffered_messages.set_buffered((*pending_messages).arch_into());
        if let Some(work) = observations {
            work.set(EmitterWorkKind::Buffered, *pending_messages);
        }
        given_up
    }

    /// What a flush that just completed published: every row its emitter delivered while this buffer
    /// held it, over every attempt the flush took, each counted once, and the payload bytes they
    /// carry. Absent when the emitter delivered none of them.
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
    /// buffered batches, and the rows its emitter staged, which the emitter's own end abandons.
    fn drop(&mut self) {
        self.pending_acks().no_ack("emitter dropped buffered batch");
        self.buffered_messages.clear();
    }
}

#[cfg(test)]
#[path = "emitter_buffer_tests.rs"]
mod tests;
