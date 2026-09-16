//! Columnar ingestion group execution.
//!
//! Layer: data plane.
//! - **Owns.** Group decoding, timestamped row metadata, route execution and in-memory ACKs.
//! - **Depends on.** Installed domain capabilities, compiled codecs and branch-local routes.
//! - **Must not know.** NSPL parsing, consensus decisions or source transport lifecycle.

use std::borrow::Cow;

use ahash::RandomState;
use bytes::Bytes;
use indexmap::{Equivalent, IndexMap};

use super::*;

/// Why an ingestor that keeps running discards the summary a flush returns.
///
/// See [`Runtime::flush_ingest_collector`], which routes every failure through the ingestor's
/// error policy before returning that summary.
pub(in crate::runtime) const INGEST_FLUSH_FAILURES_ARE_HANDLED: &str =
    "the ingestor's error policy already handled every failure this flush produced";

/// Chosen operational bound for how many decoded source messages accumulate before an ingest group
/// executes and becomes one Arrow batch per (relay, branch key). A group closes once it holds this
/// many or more: every message of one payload joins the same group, so a payload that unfolds past
/// the remaining room carries the group beyond the bound instead of being split. This is
/// intentionally independent of an NSPL route's flush policy.
pub(in crate::runtime) const INGEST_GROUP_MAX_ROWS: usize = 1024;

/// Chosen operational bound for how long a partial source group waits when the source
/// goes quiet. This is intentionally independent of an NSPL route's flush policy.
pub(in crate::runtime) const INGEST_GROUP_IDLE_FLUSH: Duration = Duration::from_millis(5);

pub(super) struct IngestorDependencies {
    pub(super) output_routes: RelayProcessorOutputsNode,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) codec: Arc<CompiledCodec>,
    pub(super) branched_templates: HashMap<RelayName, (SharedActiveGraph, IngestorRouteTemplate)>,
    pub(super) metrics: MessageMetricsHandle,
}

pub(super) struct IngestGroupContext {
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) timestamp_source: Option<IngestTimestampSource>,
    pub(super) output_routes: RelayProcessorOutputsNode,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
}

/// The payloads a source has decoded into its ingest group and is now accepting.
///
/// The payloads have already been decoded into the group's record builder by
/// [`IngestRouteCollector::decode_payload`], each as the zero or more messages it unfolded into;
/// this call is what accepts them, by giving each payload the metadata row and ACK set that belong
/// to it. A source may contribute one poll batch or several consecutive single-record polls. The
/// collector owns the actual group boundary: request-scoped sources flush at the end of the
/// request, while streaming sources flush at the message or idle-time bound.
pub(super) struct IngestGroupDispatch<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) ingestor: &'a IngestorName,
    pub(super) timestamp_source: Option<&'a IngestTimestampSource>,
    pub(super) output_routes: &'a RelayProcessorOutputsNode,
    pub(super) filter_where: Option<&'a CompiledProgramWithMaterializedInterest>,
    /// One entry per decoded payload, read from the borrowed source messages and appended into the
    /// group's own metadata builders once for every message the payload unfolded into.
    pub(super) metadata: &'a [IngestMetadataRow<'a>],
    /// Payload-aligned with `metadata`. Every message of a payload takes one share of its set, and
    /// an empty share is replaced by a tracked ack root.
    pub(super) acks: Vec<AckSet>,
    pub(super) ingested_at: Timestamp,
    /// Sources differ only in when they flush this group: stream sources use the
    /// size/idle-time bounds, while request-scoped sources flush at request completion.
    pub(super) collector: &'a mut IngestRouteCollector,
}

pub(super) struct IngestGroupContribution<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) ingestor: &'a IngestorName,
    pub(super) timestamp_source: Option<&'a IngestTimestampSource>,
    pub(super) output_routes: &'a RelayProcessorOutputsNode,
    pub(super) filter_where: Option<&'a CompiledProgramWithMaterializedInterest>,
    pub(super) metadata: &'a [IngestMetadataRow<'a>],
    pub(super) acks: Vec<AckSet>,
    pub(super) ingested_at: Timestamp,
}

pub(super) struct RawIngestDispatch<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) ingestor: &'a IngestorName,
    pub(super) timestamp_source: Option<&'a IngestTimestampSource>,
    pub(super) output_routes: &'a RelayProcessorOutputsNode,
    pub(super) filter_where: Option<&'a CompiledProgramWithMaterializedInterest>,
    pub(super) branched_senders: &'a HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    pub(super) codec: Arc<CompiledCodec>,
    pub(super) payload: &'a BufferedIngestPayload,
    pub(super) collector: &'a mut IngestRouteCollector,
    pub(super) flush: bool,
}

/// Columnar ingest group state.
///
/// Record columns and the shared ingest-metadata view are selected together. ACKs remain
/// row-indexed hot-path state so a message error and its ACK identity stay attributable
/// to the record that produced them after filtering.
pub(super) struct IngestGroupRows {
    pub(super) batch: Arc<RuntimeRecordBatch>,
    pub(super) record_metadata: Vec<RuntimeRecordMetadata>,
    pub(super) ingest_metadata: IngestFilterMapMetadata,
    pub(super) acks: Vec<AckSet>,
}

/// One ingest group's messages before the group closes.
///
/// The group owns exactly one record builder and exactly one set of metadata builders: it opens
/// each with its first message, appends one row per decoded message, and finishes both once in
/// `into_rows`. Nothing here is built per message, so a group of `n` messages is one set of Arrow
/// columns rather than `n` single-row batches and a concatenation.
///
/// Decoding and accepting are two steps, because a payload that fails to decode has to stay
/// attributable to the message that carried it. `decode_payload` appends a payload's messages to
/// the group's builder, all of them or none, and remembers how many there were; `append` then
/// accepts the payloads that did decode, giving every message of each payload the payload's
/// metadata row and a share of its ACK set.
pub(super) struct PendingIngestGroup {
    pub(super) kind: IngestMetadataKind,
    /// The number of rows the group is expected to reach, used to size its builders.
    pub(super) row_bound: usize,
    pub(super) records: Option<RuntimeRecordBatchBuilder>,
    /// How many messages each decoded payload unfolded into, oldest first, for the payloads the
    /// source has not accepted yet.
    pub(super) undispatched_payloads: VecDeque<usize>,
    pub(super) metadata: Option<IngestMetadataBuilders>,
    /// One ACK set per accepted message.
    pub(super) acks: Vec<AckSet>,
    /// One ingestion instant per accepted message.
    pub(super) ingested_at: Vec<Timestamp>,
}

impl PendingIngestGroup {
    pub(super) fn new(kind: IngestMetadataKind, row_bound: usize) -> Self {
        Self {
            kind,
            row_bound,
            records: None,
            undispatched_payloads: VecDeque::new(),
            metadata: None,
            acks: Vec::new(),
            ingested_at: Vec::new(),
        }
    }

    /// Decodes one payload into the group's record builder, opened for the codec's schema on the
    /// first payload the group decodes, and holds its messages until the source accepts it.
    ///
    /// A payload that fails to decode leaves the group's rows exactly as they were. When the group
    /// then holds no row at all, its builder is dropped, so the rows a rejected payload abandoned do
    /// not stay allocated while the group waits for a message it keeps.
    pub(super) async fn decode_payload(
        &mut self,
        codec: &Arc<CompiledCodec>,
        payload: Cow<'_, [u8]>,
    ) -> Result<(), CodecError> {
        let row_bound = self.row_bound;
        let records = self
            .records
            .get_or_insert_with(|| codec.schema().batch_builder(row_bound));
        match decode_ingested_payload(codec, payload, records).await {
            Ok(messages) => {
                self.undispatched_payloads.push_back(messages);
                Ok(())
            }
            Err(error) => {
                if self.decoded_rows() == 0 {
                    self.records = None;
                }
                Err(error)
            }
        }
    }

    /// Payloads the group has decoded but the source has not accepted yet.
    pub(super) fn undispatched_payloads(&self) -> usize {
        self.undispatched_payloads.len()
    }

    fn decoded_rows(&self) -> usize {
        match self.records.as_ref() {
            Some(records) => records.rows(),
            None => 0,
        }
    }

    /// Drops the decoded payloads the caller could not accept, so the group keeps only the
    /// messages it accepted.
    pub(super) fn discard_undispatched_payloads(&mut self) {
        self.undispatched_payloads.clear();
        let accepted = self.acks.len();
        if let Some(records) = self.records.as_mut() {
            records.abandon_rows_after(accepted);
        }
    }

    /// Accepts the oldest decoded payloads, one for each metadata row and ACK set.
    ///
    /// Every message a payload unfolded into takes the payload's metadata row and one share of its
    /// ACK set, so the payload's acknowledgement resolves only once all of its messages have. A
    /// payload that unfolded into no message has nothing left to wait for, so its share resolves
    /// here.
    pub(super) fn append(
        &mut self,
        metadata: &[IngestMetadataRow<'_>],
        acks: Vec<AckSet>,
        ingested_at: Timestamp,
    ) -> Result<(), String> {
        let payload_count = metadata.len();
        if acks.len() != payload_count {
            return Err(format!(
                "received {} ack sets for {payload_count} ingest metadata rows",
                acks.len()
            ));
        }
        // Payloads are accepted in the order they decoded, and a source may accept them one at a
        // time, so a contribution may cover a prefix of what the group has decoded but never more.
        if payload_count > self.undispatched_payloads.len() {
            return Err(format!(
                "received {payload_count} ingest metadata rows for {} decoded payloads",
                self.undispatched_payloads.len()
            ));
        }

        let (kind, row_bound) = (self.kind, self.row_bound);
        let builders = self
            .metadata
            .get_or_insert_with(|| IngestMetadataBuilders::new(kind, row_bound));
        for (row, payload_acks) in metadata.iter().zip(acks) {
            let messages = self
                .undispatched_payloads
                .pop_front()
                .verified("the check above admits at most one metadata row per decoded payload");
            for _ in 0..messages {
                builders.append(row).map_err(|error| error.to_string())?;
            }
            payload_acks.split_into(messages, &mut self.acks);
            self.ingested_at
                .extend(std::iter::repeat_n(ingested_at, messages));
        }
        Ok(())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.acks.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.acks.len()
    }

    pub(super) fn into_rows(self) -> Result<IngestGroupRows, String> {
        let row_count = self.acks.len();
        if self.ingested_at.len() != row_count {
            return Err(format!(
                "ingest group has {row_count} ack sets and {} ingest timestamps",
                self.ingested_at.len()
            ));
        }
        let ingest_metadata = self
            .metadata
            .ok_or_else(|| "ingest group closed without opening its metadata builders".to_string())?
            .finish()
            .map_err(|error| error.to_string())?;
        if ingest_metadata.len() != row_count {
            return Err(format!(
                "ingest group has {row_count} records and {} ingest metadata rows",
                ingest_metadata.len()
            ));
        }
        let batch = self
            .records
            .ok_or_else(|| "ingest group closed without opening its record builder".to_string())?
            .finish()
            .map_err(|error| error.to_string())?;
        if batch.batch().num_rows() != row_count {
            return Err(format!(
                "ingest group has {row_count} records and {} decoded rows",
                batch.batch().num_rows()
            ));
        }
        Ok(IngestGroupRows {
            batch: Arc::new(batch),
            record_metadata: self
                .ingested_at
                .into_iter()
                .map(|ingested_at| {
                    RuntimeRecordMetadata::from_ingested_at_watermarks(ingested_at, ingested_at)
                })
                .collect(),
            ingest_metadata,
            acks: self.acks,
        })
    }
}

impl IngestGroupRows {
    pub(super) fn len(&self) -> usize {
        self.batch.batch().num_rows()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.batch.batch().num_rows() == 0
    }

    pub(super) fn metadata_row(&self, row: usize) -> Option<IngestFilterMapMetadata> {
        self.ingest_metadata.row(row)
    }

    pub(super) fn metadata_rows(&self) -> Option<&IngestFilterMapMetadata> {
        Some(&self.ingest_metadata)
    }

    pub(super) fn row(&self, row: usize) -> Result<RuntimeRow, String> {
        let metadata = self.record_metadata.get(row).cloned().ok_or_else(|| {
            format!(
                "ingest row {row} is outside metadata with {} rows",
                self.record_metadata.len()
            )
        })?;
        RuntimeRow::new(self.batch.clone(), row, metadata).map_err(|error| error.to_string())
    }

    /// Keeps only the rows selected by `keep`, moving records, metadata and acks
    /// together so the three stay row-aligned.
    pub(super) fn select(self, keep: &[bool]) -> Result<Self, String> {
        let selected = |row: usize| keep.get(row).copied().unwrap_or(false);
        let predicate = BooleanArray::from_iter((0..self.len()).map(|row| Some(selected(row))));
        Ok(Self {
            batch: Arc::new(
                self.batch
                    .filter(&predicate)
                    .map_err(|error| error.to_string())?,
            ),
            record_metadata: self
                .record_metadata
                .into_iter()
                .enumerate()
                .filter_map(|(row, metadata)| selected(row).then_some(metadata))
                .collect(),
            ingest_metadata: self
                .ingest_metadata
                .select(keep)
                .map_err(|error| error.to_string())?,
            acks: self
                .acks
                .into_iter()
                .enumerate()
                .filter_map(|(row, acks)| selected(row).then_some(acks))
                .collect(),
        })
    }
}

/// An ingestor `FILTER WHERE` message error, with the row it came from.
pub(super) struct IngestorFilterWhereError<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) ingestor: &'a IngestorName,
    pub(super) output_routes: &'a RelayProcessorOutputsNode,
    pub(super) record: &'a RuntimeRow,
    pub(super) ingest_metadata: Option<IngestFilterMapMetadata>,
    pub(super) acks: AckSet,
    pub(super) error: StructuredMessageError,
    pub(super) materialized_state: HashMap<String, RuntimeValue>,
    pub(super) execution_now: Timestamp,
}

/// Accumulates one source ingest group before program execution, then holds its routed
/// messages long enough to build one Arrow batch per (relay, branch key).
///
/// A source decodes each payload into the open group with `decode_payload` and accepts the payloads
/// that decoded with `collect`, so the group's records, metadata and ACKs stay row-aligned. The
/// metadata schema is fixed by the source kind when the ingestor starts, so every group the
/// collector opens builds the same columns.
pub(super) struct IngestRouteCollector {
    pub(super) kind: IngestMetadataKind,
    /// The number of rows a group is expected to reach, used to size its metadata builders.
    pub(super) row_bound: usize,
    pub(super) context: Option<IngestGroupContext>,
    metrics: MessageMetricsHandle,
    routing: Option<DomainRoutingCache>,
    pub(super) pending: PendingIngestGroup,
    pub(super) routed: IndexMap<RoutedGroupKey, Vec<RelayMessage>, RandomState>,
    pub(super) flush_at: Option<Instant>,
}

impl IngestRouteCollector {
    /// Opens a collector for a source of `kind` whose groups close at `row_bound` rows.
    ///
    /// Sources that flush one group per message or per poll pass their own bound so a
    /// short group does not size its builders for a streaming one.
    pub(super) fn new(
        kind: IngestMetadataKind,
        row_bound: usize,
        metrics: MessageMetricsHandle,
    ) -> Self {
        Self {
            kind,
            row_bound,
            context: None,
            metrics,
            routing: None,
            pending: PendingIngestGroup::new(kind, row_bound),
            routed: IndexMap::with_hasher(RandomState::default()),
            flush_at: None,
        }
    }

    /// Decodes one source payload into the group's record builder.
    ///
    /// The builder belongs to the group, so consecutive payloads share one set of Arrow columns,
    /// and every message a payload unfolds into is appended there. A payload that fails to decode
    /// leaves the group exactly as it was, and the error names that payload alone.
    pub(super) async fn decode_payload(
        &mut self,
        codec: &Arc<CompiledCodec>,
        payload: Cow<'_, [u8]>,
    ) -> Result<(), CodecError> {
        self.pending.decode_payload(codec, payload).await
    }

    /// Drops decoded payloads a caller could not accept, so a failed dispatch leaves no stray row.
    pub(super) fn discard_undispatched_payloads(&mut self) {
        self.pending.discard_undispatched_payloads();
    }

    pub(super) fn collect(
        &mut self,
        contribution: IngestGroupContribution<'_>,
    ) -> Result<(), String> {
        let IngestGroupContribution {
            domain,
            ingestor,
            timestamp_source,
            output_routes,
            filter_where,
            metadata,
            acks,
            ingested_at,
        } = contribution;
        if metadata.is_empty() && self.pending.undispatched_payloads() == 0 {
            return Ok(());
        }
        if let Some(existing) = self.context.as_ref()
            && (existing.domain != *domain || existing.ingestor != *ingestor)
        {
            self.pending.discard_undispatched_payloads();
            return Err(format!(
                "ingest group for '{}.{}' cannot collect rows for '{}.{}'",
                existing.domain.as_str(),
                existing.ingestor.as_str(),
                domain.as_str(),
                ingestor.as_str()
            ));
        }
        let accepted_before = self.pending.len();
        if let Err(error) = self.pending.append(metadata, acks, ingested_at) {
            self.pending.discard_undispatched_payloads();
            return Err(error);
        }
        // Payloads that unfolded into no message leave nothing to deliver, so they neither open
        // the group nor push its idle close back.
        if self.pending.len() == accepted_before {
            return Ok(());
        }
        if self.context.is_none() {
            self.context = Some(IngestGroupContext {
                domain: domain.clone(),
                ingestor: ingestor.clone(),
                timestamp_source: timestamp_source.cloned(),
                output_routes: output_routes.clone(),
                filter_where: filter_where.cloned(),
            });
        }
        self.flush_at = Some(Instant::now() + INGEST_GROUP_IDLE_FLUSH);
        Ok(())
    }

    pub(super) fn take_pending(
        &mut self,
    ) -> Result<Option<(IngestGroupContext, IngestGroupRows)>, String> {
        // A decoded payload the source never accepted would put the records out of step with the
        // metadata and ACKs, so the group says so rather than closing over the mismatch.
        let undispatched = self.pending.undispatched_payloads();
        if undispatched != 0 {
            self.pending.discard_undispatched_payloads();
            return Err(format!(
                "ingest group closed with {undispatched} decoded payloads that were never accepted"
            ));
        }
        if self.pending.is_empty() {
            return Ok(None);
        }
        self.flush_at = None;
        let context = self.context.take().verified(
            "the empty check above already returned, and a group keeps its context while it holds \
             rows",
        );
        let pending = std::mem::replace(
            &mut self.pending,
            PendingIngestGroup::new(self.kind, self.row_bound),
        );
        Ok(Some((context, pending.into_rows()?)))
    }

    fn routing_snapshot(
        &mut self,
        runtime: &Runtime,
        domain: &DomainName,
    ) -> Result<StdArc<DomainRoutingSnapshot>, Report<DomainRoutingError>> {
        if self.routing.is_none() {
            self.routing = runtime.domain_routing_cache(domain);
        }
        let Some(routing) = self.routing.as_mut() else {
            return Err(Report::new(DomainRoutingError::DomainNotInstantiated {
                domain: domain.clone(),
            }));
        };
        Ok(routing.load().clone())
    }

    pub(super) fn push(&mut self, relay: &RelayName, message: RelayMessage) {
        let lookup = RoutedGroupLookup {
            relay,
            key: &message.key,
        };
        if let Some(messages) = self.routed.get_mut(&lookup) {
            messages.push(message);
            return;
        }

        let group_key = RoutedGroupKey {
            relay: relay.clone(),
            key: message.key.clone(),
        };
        self.routed.insert(group_key, vec![message]);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.routed.is_empty()
    }

    /// The messages the open group has accepted.
    ///
    /// Sources close the group once this reaches [`INGEST_GROUP_MAX_ROWS`]. It counts messages
    /// rather than payloads, and a payload that unfolds past the remaining room overshoots the
    /// bound rather than being split.
    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn next_flush(&self) -> Option<Instant> {
        self.flush_at
    }

    /// Groups by relay and branch key, preserving arrival order within each group.
    /// `RelayRecordBatch::from_messages` requires a uniform key per batch.
    pub(super) fn drain_groups(&mut self) -> Vec<RoutedGroup> {
        let routed = std::mem::take(&mut self.routed);
        let mut groups = Vec::with_capacity(routed.len());
        for (key, messages) in routed {
            groups.push(RoutedGroup {
                relay: key.relay,
                messages,
            });
        }
        groups
    }
}

/// The relay and branch key a routed message is grouped under. Branch identity is part of the key
/// because a relay batch carries exactly one branch key.
#[derive(PartialEq, Eq, Hash)]
pub(super) struct RoutedGroupKey {
    pub(super) relay: RelayName,
    pub(super) key: Option<BranchKey>,
}

/// A borrowed grouping key avoids reference-count traffic while an existing group is found.
#[derive(Hash)]
struct RoutedGroupLookup<'a> {
    relay: &'a RelayName,
    key: &'a Option<BranchKey>,
}

impl Equivalent<RoutedGroupKey> for RoutedGroupLookup<'_> {
    fn equivalent(&self, key: &RoutedGroupKey) -> bool {
        self.relay == &key.relay && self.key == &key.key
    }
}

/// Messages routed to one relay under one branch key, in arrival order.
pub(super) struct RoutedGroup {
    pub(super) relay: RelayName,
    pub(super) messages: Vec<RelayMessage>,
}

#[derive(Clone, Default)]
pub(super) struct IngestorRouteRuntimes {
    pub(super) runtimes: Vec<Arc<IngestorRouteRuntime>>,
    pub(super) senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
}

pub(super) type BranchedEntrypointInput = RelayRecordBatch;

pub(super) struct BranchedEntrypointBatch {
    pub(super) batch: RuntimeRecordBatch,
    pub(super) metadata: Vec<RuntimeRecordMetadata>,
    pub(super) keys: Vec<Option<BranchKey>>,
    pub(super) acks: Vec<AckSet>,
}

#[derive(Clone)]
pub(super) struct BranchedBranchSelection {
    pub(super) key: Option<BranchKey>,
    pub(super) rows: Vec<usize>,
}

impl BranchedEntrypointBatch {
    pub(super) fn from_inputs(
        inputs: Vec<BranchedEntrypointInput>,
    ) -> Result<Self, (String, Vec<AckSet>)> {
        if inputs.is_empty() {
            return Err((
                "cannot build branch batch from zero inputs".to_string(),
                Vec::new(),
            ));
        }
        let mut batches = Vec::<Arc<RuntimeRecordBatch>>::new();
        let mut metadata = Vec::<RuntimeRecordMetadata>::new();
        let mut keys = Vec::<Option<BranchKey>>::new();
        let mut acks = Vec::<AckSet>::new();

        for input in inputs {
            let parts = input.into_unkeyed_parts();
            batches.push(parts.batch);
            metadata.extend(parts.metadata);
            keys.extend(parts.keys);
            acks.extend(parts.acks);
        }
        let batch_refs = batches.iter().map(Arc::as_ref).collect::<Vec<_>>();
        let batch = RuntimeRecordBatch::concat(&batch_refs).map_err(|error| {
            (
                format!("failed to concatenate branch input batches: {error}"),
                acks.clone(),
            )
        })?;
        let row_count = batch.batch().num_rows();
        if metadata.len() != row_count || keys.len() != row_count || acks.len() != row_count {
            return Err((
                format!(
                    "branch input batch row count {row_count} does not match metadata {}, branch \
                     keys {}, acks {}",
                    metadata.len(),
                    keys.len(),
                    acks.len()
                ),
                acks,
            ));
        }

        Ok(Self {
            batch,
            metadata,
            keys,
            acks,
        })
    }

    pub(super) fn branch_selections(&self) -> Result<Vec<BranchedBranchSelection>, String> {
        let mut selections = Vec::<BranchedBranchSelection>::new();
        let mut positions = HashMap::<Option<BranchKey>, usize>::default();
        for index in 0..self.metadata.len() {
            let key = self.keys.get(index).cloned().flatten();
            if let Some(position) = positions.get(&key).copied() {
                selections[position].rows.push(index);
                continue;
            }
            positions.insert(key.clone(), selections.len());
            selections.push(BranchedBranchSelection {
                key,
                rows: vec![index],
            });
        }

        Ok(selections)
    }

    pub(super) fn filter_branch(
        &self,
        selection: BranchedBranchSelection,
        ack_boundary: BranchInstanceAckBoundary,
    ) -> Result<RelayRecordBatch, (String, Vec<AckSet>)> {
        let predicate = self
            .branch_predicate(&selection)
            .map_err(|error| (error, self.acks.clone()))?;
        let selected_rows = selected_rows(&predicate);
        let filtered_batch = self
            .batch
            .filter(&predicate)
            .map_err(|error| (error.to_string(), self.acks.clone()))?;
        let mut metadata = Vec::with_capacity(selected_rows.len());
        let mut acks = Vec::with_capacity(selected_rows.len());
        for row in selected_rows {
            metadata.push(self.metadata[row].clone());
            acks.push(match ack_boundary {
                BranchInstanceAckBoundary::Preserve => self.acks[row].clone(),
                BranchInstanceAckBoundary::Reingestor(AckMode::Attached) => {
                    let forwarded = self.acks[row].attached();
                    self.acks[row].ack_success();
                    forwarded
                }
                BranchInstanceAckBoundary::Reingestor(AckMode::Detached) => {
                    self.acks[row].ack_success();
                    AckSet::empty()
                }
            });
        }
        RelayRecordBatch::from_filtered_parts(selection.key, filtered_batch, metadata, acks)
            .map_err(|error| (error, self.acks.clone()))
    }

    pub(super) fn branch_predicate(
        &self,
        selection: &BranchedBranchSelection,
    ) -> Result<BooleanArray, String> {
        let row_count = self.batch.batch().num_rows();
        let mut selected = vec![false; row_count];
        for row in &selection.rows {
            let Some(value) = selected.get_mut(*row) else {
                return Err(format!(
                    "branch selection row {row} is outside batch with {row_count} rows"
                ));
            };
            *value = true;
        }
        Ok(BooleanArray::from(selected))
    }
}

pub(super) fn selected_rows(predicate: &BooleanArray) -> Vec<usize> {
    (0..predicate.len())
        .filter(|row| predicate.is_valid(*row) && predicate.value(*row))
        .collect()
}

pub(super) fn branched_entrypoint_inputs_acks(inputs: &[BranchedEntrypointInput]) -> Vec<AckSet> {
    inputs
        .iter()
        .flat_map(|input| input.acks.iter().cloned())
        .collect()
}

pub(super) async fn branched_entrypoint_batch_from_inputs_blocking(
    inputs: Vec<BranchedEntrypointInput>,
) -> Result<Arc<BranchedEntrypointBatch>, (String, Vec<AckSet>)> {
    let acks = branched_entrypoint_inputs_acks(&inputs);
    match tokio::task::spawn_blocking(move || BranchedEntrypointBatch::from_inputs(inputs)).await {
        Ok(Ok(batch)) => Ok(Arc::new(batch)),
        Ok(Err(error)) => Err(error),
        Err(error) => Err((
            format!("branch input batch build task failed: {error}"),
            acks,
        )),
    }
}

pub(super) async fn branched_branch_plan_blocking(
    input: Arc<BranchedEntrypointBatch>,
) -> Result<Vec<BranchedBranchSelection>, String> {
    input.branch_selections()
}

pub(super) async fn branched_branch_filter_blocking(
    input: Arc<BranchedEntrypointBatch>,
    selection: BranchedBranchSelection,
    ack_boundary: BranchInstanceAckBoundary,
) -> Result<(Option<BranchKey>, RelayRecordBatch), (String, Vec<AckSet>)> {
    let failure_input = input.clone();
    let key = selection.key.clone();
    match tokio::task::spawn_blocking(move || {
        input
            .filter_branch(selection, ack_boundary)
            .map(|batch| (key, batch))
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err((
            format!("branch filter task failed: {error}"),
            failure_input.acks.clone(),
        )),
    }
}

/// Decodes one payload into `builder` and answers how many messages it decoded into.
///
/// A schemaful codec decodes a payload into exactly one message, and a JAQ-backed codec unfolds it
/// into zero or more. jaq and protobuf decoding is CPU-bound, so the unfolding half runs off the
/// reactor and hands back the messages the append consumes. The append itself always runs here,
/// which keeps the builder on the task that owns it, and it keeps all of a payload's messages or
/// none of them.
pub(super) async fn decode_ingested_payload(
    codec: &Arc<CompiledCodec>,
    payload: Cow<'_, [u8]>,
    builder: &mut RuntimeRecordBatchBuilder,
) -> Result<usize, CodecError> {
    if !codec.requires_blocking_decode() {
        return decode_with_codec(codec, payload, builder);
    }

    // Only the unfolding leaves the reactor. The Arrow append that consumes its result stays here,
    // with the batch builder the decoded rows join.
    let codec_name = codec.name.as_str().to_string();
    let blocking_codec = codec.clone();
    let payload = Bytes::from(payload.into_owned());
    let unfolded = tokio::task::spawn_blocking(move || blocking_codec.unfold_on_ingestion(payload))
        .await
        .map_err(|error| CodecError::InvalidCodec {
            codec: codec_name,
            reason: format!("blocking decode task failed: {error}"),
        })??;
    unfolded.append_to(codec, builder)
}

impl Runtime {
    pub(in crate::runtime) async fn ingest_stream_boundary_message(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        _registry: &RelayRegistry,
        services: &RelayBoundaryServices,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        if !self.owns_relay(services) {
            return services.dispatch_to_owner(domain, relay, batch).await;
        }
        services.enqueue_owner_batch(batch).await
    }

    /// Executes the collected ingest group, then builds one Arrow batch per (relay,
    /// branch key) and forwards each batch to its branch entrypoint.
    ///
    /// This is the counterpart to `IngestGroupDispatch::collector`. Building the batch once per
    /// group replaces N single-row batch constructions, N channel sends, and the
    /// `spawn_blocking` hop the route task pays per message.
    ///
    /// Every failure below is handled before it is returned: the affected acknowledgements go to
    /// the ingestor's general error policy, which is what decides whether the messages are logged,
    /// routed to a dead-letter relay, or dropped. The returned error is a second copy of the first
    /// such failure, for callers that need to stop rather than continue collecting. A caller that
    /// only continues is therefore right to discard it, and discarding it loses no report. Such
    /// a caller names [`INGEST_FLUSH_FAILURES_ARE_HANDLED`] as its reason.
    pub(in crate::runtime) async fn flush_ingest_collector(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        branched_senders: &HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
        collector: &mut IngestRouteCollector,
    ) -> Result<(), String> {
        if collector.is_empty() {
            return Ok(());
        }
        let routing = collector
            .routing_snapshot(self, domain)
            .map_err(|error| error.to_string())?;
        if let Some((context, rows)) = collector.take_pending()? {
            if context.domain != *domain || context.ingestor != *ingestor {
                return Err(format!(
                    "ingest group for '{}.{}' cannot flush as '{}.{}'",
                    context.domain.as_str(),
                    context.ingestor.as_str(),
                    domain.as_str(),
                    ingestor.as_str()
                ));
            }
            self.execute_ingest_group(&routing, &context, rows, collector)
                .await?;
        }
        if collector.is_empty() {
            return Ok(());
        }
        let groups = collector.drain_groups();
        if routing.passive_only {
            let error = format!("domain '{}' is not running", domain.as_str());
            for group in &groups {
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor,
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    group.messages.iter().map(|message| &message.acks),
                    error.clone(),
                );
            }
            return Err(error);
        }

        let mut first_error = None;
        for RoutedGroup { relay, messages } in groups {
            tokio::task::consume_budget().await;
            let acks = messages
                .iter()
                .map(|message| message.acks.clone())
                .collect::<Vec<_>>();
            let Some(schema) = routing.relay_schemas.get(&relay).cloned() else {
                let error = format!(
                    "stream '{}' schema is not instantiated in domain '{}'",
                    relay.as_str(),
                    domain.as_str()
                );
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor,
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    acks.iter(),
                    error.clone(),
                );
                first_error.get_or_insert(error);
                continue;
            };
            let batch = match RelayRecordBatch::from_messages(schema, messages) {
                Ok(batch) => batch,
                Err(error) => {
                    self.handle_general_error_for_acks(
                        domain,
                        ModelKind::Ingestor,
                        ingestor,
                        &ErrorPolicies::handled_by_log(),
                        acks.iter(),
                        error.clone(),
                    );
                    first_error.get_or_insert(error);
                    continue;
                }
            };
            let Some(sender) = branched_senders.get(&relay) else {
                let error = format!(
                    "ingestor '{}' has no branch entrypoint for relay '{}'",
                    ingestor.as_str(),
                    relay.as_str()
                );
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor,
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    batch.acks.iter(),
                    error.clone(),
                );
                first_error.get_or_insert(error);
                continue;
            };
            if let Err(error) = sender.send(batch).await {
                let batch = error.0;
                let reason = format!(
                    "ingestor '{}' failed to forward batch to branch entrypoint for relay '{}'",
                    ingestor.as_str(),
                    relay.as_str()
                );
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor,
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    batch.acks.iter(),
                    reason.clone(),
                );
                first_error.get_or_insert(reason);
            }
        }
        match first_error {
            Some(reason) => Err(reason),
            None => Ok(()),
        }
    }

    /// Accepts the rows a source decoded into the current ingest group.
    ///
    /// Program execution is deliberately deferred until `flush_ingest_collector`, so
    /// sources that receive one record per poll still enter the columnar VM once for the
    /// collected group rather than once per call to this method.
    pub(in crate::runtime) async fn dispatch_ingested_records(
        &self,
        dispatch: IngestGroupDispatch<'_>,
    ) -> Result<(), String> {
        let IngestGroupDispatch {
            domain,
            ingestor,
            timestamp_source,
            output_routes,
            filter_where,
            metadata,
            acks,
            ingested_at,
            collector,
        } = dispatch;
        collector
            .collect(IngestGroupContribution {
                domain,
                ingestor,
                timestamp_source,
                output_routes,
                filter_where,
                metadata,
                acks,
                ingested_at,
            })
            .map_err(|reason| format!("ingestor '{}' {reason}", ingestor.as_str()))
    }

    /// Executes one collected ingest group.
    ///
    /// The ingestor `FILTER WHERE` runs once over the group and each route's filter-map
    /// runs once over the rows that survived it. Record and ingest-metadata columns use
    /// the same row projection while ACKs retain their corresponding hot-path indices,
    /// keeping errors and acknowledgements attributable to their original records.
    pub(super) async fn execute_ingest_group(
        &self,
        routing: &DomainRoutingSnapshot,
        context: &IngestGroupContext,
        mut rows: IngestGroupRows,
        collector: &mut IngestRouteCollector,
    ) -> Result<(), String> {
        let domain = &context.domain;
        let ingestor = &context.ingestor;
        let timestamp_source = context.timestamp_source.as_ref();
        let output_routes = &context.output_routes;
        let filter_where = context.filter_where.as_ref();

        // Sources that do not track acks themselves still need a root for downstream
        // resolution to land on. Those completions are deliberately never observed.
        let mut _unobserved_completions = Vec::new();
        let ack_root_trackers = if rows.acks.iter().any(AckSet::is_empty) {
            Some(self.ingestor_ack_root_trackers(domain, ingestor))
        } else {
            None
        };
        for slot in rows.acks.iter_mut().filter(|slot| slot.is_empty()) {
            let trackers = ack_root_trackers
                .as_ref()
                .verified("the tracker handle was acquired because this ACK set is empty");
            let (tracked, completion) = trackers.tracked_root();
            *slot = tracked;
            _unobserved_completions.push(completion);
        }
        // One execution clock for the whole group: a batch is evaluated against the
        // state it was admitted with.
        let ingestion_time = self
            .ingestion_time(domain, ingestor)
            .map_err(|error| format!("{error:?}"))?;
        let execution_now = ingestion_time.now();

        if let Some(filter_where) = filter_where {
            let side_inputs = self
                .load_materialized_side_inputs(
                    routing,
                    domain,
                    &None,
                    &filter_where.materialized_interest,
                )
                .await
                .map_err(|error| error.to_string())?;
            let keys = vec![None; rows.len()];
            let outcomes = evaluate_filter_map_on_batch(
                ModelKind::Ingestor.as_str(),
                ingestor,
                filter_where,
                FilterMapOutcomeInputs {
                    carrier: &rows.batch,
                    record_metadata: &rows.record_metadata,
                    keys: &keys,
                    filter_map_metadata: rows.metadata_rows(),
                    side_inputs: &side_inputs,
                },
                execution_now,
            )
            .await?;
            let mut keep = vec![false; rows.len()];
            let mut transformed = Vec::new();
            for (row, outcome) in outcomes.into_iter().enumerate() {
                tokio::task::consume_budget().await;
                match outcome {
                    SingleRecordFilterMapOutcome::Filtered => rows.acks[row].ack_success(),
                    SingleRecordFilterMapOutcome::Output(record) => {
                        keep[row] = true;
                        transformed.push((row, record));
                    }
                    SingleRecordFilterMapOutcome::MessageError {
                        error,
                        materialized_state,
                        ..
                    } => {
                        let acks = std::mem::replace(&mut rows.acks[row], AckSet::empty());
                        self.handle_ingestor_filter_where_error(IngestorFilterWhereError {
                            domain,
                            ingestor,
                            output_routes,
                            record: &rows.row(row)?,
                            ingest_metadata: rows.metadata_row(row),
                            acks,
                            error,
                            materialized_state,
                            execution_now,
                        })
                        .await;
                    }
                }
            }
            rows = rows.select(&keep)?;
            if !transformed.is_empty() {
                transformed.sort_unstable_by_key(|(row, _)| *row);
                let batches = transformed
                    .into_iter()
                    .map(|(_, record)| record.one_row_batch())
                    .collect::<Vec<_>>();
                let batch_refs = batches.iter().collect::<Vec<_>>();
                rows.batch = Arc::new(
                    RuntimeRecordBatch::concat(&batch_refs).map_err(|error| error.to_string())?,
                );
            }
        }
        if rows.is_empty() {
            return Ok(());
        }

        // Timestamp resolution and admission stay per record: `TIMESTAMP AT` reads a
        // field of the record itself, and a paced domain admits each event on its own
        // merits. Either rejection fails the group, exactly as the per-record path did.
        let mut event_timestamps = Vec::with_capacity(rows.len());
        for row in 0..rows.len() {
            let record = rows.row(row)?;
            let event_timestamp = ingestion_time
                .select(timestamp_source, &record)
                .map_err(|error| format!("{error:?}"))?;
            event_timestamps.push(event_timestamp);
        }
        rows.record_metadata = std::mem::take(&mut rows.record_metadata)
            .into_iter()
            .zip(&event_timestamps)
            .map(|(_, event_timestamp)| {
                RuntimeRecordMetadata::from_ingested_at_watermarks(
                    *event_timestamp,
                    *event_timestamp,
                )
            })
            .collect();
        let estimated_bytes = rows.batch.estimated_bytes();
        let row_count: u64 = rows.len().arch_into();
        // One decoded group is one metrics recording unit. Its latest event time gives the
        // rolling domain rate the group's high-water mark.
        let domain_timestamp = event_timestamps.iter().copied().max();
        collector
            .metrics
            .observe(row_count, estimated_bytes, domain_timestamp);
        self.mark_branch_aggregated_metrics_updated(domain, ModelKind::Ingestor, ingestor);

        // Every route filters the same surviving group in one VM execution. Outcomes are
        // transposed back onto their originating row so each record's ack split still
        // counts only the routes that actually took it.
        /// One output route's result for one input row, kept with the branch key that route
        /// constructed so the row's ACK can be split across every route it reached.
        struct RoutedOutcome {
            output_index: usize,
            outcome: SingleRecordFilterMapOutcome,
            key: Option<BranchKey>,
        }

        /// One route that produced a record, waiting for its share of the input row's ACK.
        struct RouteOutput {
            output_index: usize,
            key: Option<BranchKey>,
            record: RuntimeRow,
        }

        /// One route that failed, waiting for its share of the input row's ACK.
        struct RouteError {
            output_index: usize,
            error: StructuredMessageError,
            partial_output: Option<RuntimeRecordBatch>,
            materialized_state: HashMap<String, RuntimeValue>,
        }

        let mut routed = (0..rows.len())
            .map(|_| Vec::<RoutedOutcome>::new())
            .collect::<Vec<_>>();
        for (output_index, output) in output_routes.routes.iter().enumerate() {
            tokio::task::consume_budget().await;
            let outcomes = if let Some(filter_map) = output.compiled_program.as_ref() {
                let side_inputs = self
                    .load_materialized_side_inputs(
                        routing,
                        domain,
                        &None,
                        &filter_map.materialized_interest,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let keys = vec![None; rows.len()];
                evaluate_filter_map_on_batch(
                    ModelKind::Ingestor.as_str(),
                    ingestor,
                    filter_map,
                    FilterMapOutcomeInputs {
                        carrier: &rows.batch,
                        record_metadata: &rows.record_metadata,
                        keys: &keys,
                        filter_map_metadata: rows.metadata_rows(),
                        side_inputs: &side_inputs,
                    },
                    execution_now,
                )
                .await?
            } else {
                (0..rows.len())
                    .map(|row| rows.row(row))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(SingleRecordFilterMapOutcome::Output)
                    .collect()
            };
            let mut route_keys = (0..rows.len()).map(|_| None).collect::<Vec<_>>();
            let mut branch_state_snapshot = HashMap::default();
            if let Some(branch_program) = output.compiled_branch_program.as_ref() {
                let successful = outcomes
                    .iter()
                    .enumerate()
                    .filter_map(|(row, outcome)| {
                        if let SingleRecordFilterMapOutcome::Output(record) = outcome {
                            Some((row, record.one_row_batch()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                if !successful.is_empty() {
                    let input_rows = successful.iter().map(|(row, _)| *row).collect::<Vec<_>>();
                    let output_batches = successful
                        .iter()
                        .map(|(_, batch)| batch)
                        .collect::<Vec<_>>();
                    let output_batch = RuntimeRecordBatch::concat(&output_batches)
                        .map_err(|error| error.to_string())?;
                    let input_batch = rows
                        .batch
                        .take(&input_rows)
                        .map_err(|error| error.to_string())?;
                    let input_keys = vec![None; input_rows.len()];
                    let side_inputs = self
                        .load_materialized_side_inputs(
                            routing,
                            domain,
                            &None,
                            &branch_program.program.materialized_interest,
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    branch_state_snapshot = relay_state_snapshot_from_side_inputs(&side_inputs);
                    let evaluated = evaluate_output_branch_program(
                        ingestor,
                        branch_program,
                        &input_batch,
                        &output_batch,
                        &input_keys,
                        &side_inputs,
                        execution_now,
                    )
                    .await?;
                    for (row, key) in input_rows.into_iter().zip(evaluated) {
                        route_keys[row] = Some(key);
                    }
                }
            } else {
                let key = match output.branch.as_ref() {
                    Some(OutputBranch::Unbranched) | None => None,
                    Some(OutputBranch::BranchedBy { assignments, .. })
                        if assignments.is_empty() =>
                    {
                        None
                    }
                    Some(OutputBranch::BranchedBy { .. }) => {
                        return Err(format!(
                            "ingestor '{}' output '{}' has no compiled branch program",
                            ingestor.as_str(),
                            output.relay.as_str()
                        ));
                    }
                };
                for (row, outcome) in outcomes.iter().enumerate() {
                    if let SingleRecordFilterMapOutcome::Output(_) = outcome {
                        route_keys[row] = Some(Ok(key.clone()));
                    }
                }
            }
            for (row, outcome) in outcomes.into_iter().enumerate() {
                if let SingleRecordFilterMapOutcome::Filtered = outcome {
                    continue;
                }
                match outcome {
                    SingleRecordFilterMapOutcome::Output(record) => {
                        match route_keys[row].take().ok_or_else(|| {
                            format!(
                                "ingestor '{}' output '{}' has no branch result for row {}",
                                ingestor.as_str(),
                                output.relay.as_str(),
                                row
                            )
                        })? {
                            Ok(key) => routed[row].push(RoutedOutcome {
                                output_index,
                                outcome: SingleRecordFilterMapOutcome::Output(record),
                                key,
                            }),
                            Err(reason) => routed[row].push(RoutedOutcome {
                                output_index,
                                outcome: SingleRecordFilterMapOutcome::MessageError {
                                    error: structured_message_error(
                                        execution_now,
                                        MessageErrorCode::Evaluation,
                                        reason,
                                        MessageErrorOperation::Set,
                                        None,
                                        std::iter::empty(),
                                    ),
                                    partial_output: Some(record.one_row_batch()),
                                    materialized_state: branch_state_snapshot.clone(),
                                },
                                key: None,
                            }),
                        }
                    }
                    outcome => routed[row].push(RoutedOutcome {
                        output_index,
                        outcome,
                        key: None,
                    }),
                }
            }
        }

        for (row, outcomes) in routed.into_iter().enumerate() {
            tokio::task::consume_budget().await;
            let acks = std::mem::replace(&mut rows.acks[row], AckSet::empty());
            if outcomes.is_empty() {
                acks.ack_success();
                continue;
            }
            let mut route_errors = Vec::new();
            let mut route_outputs = Vec::new();
            for routed in outcomes {
                match routed.outcome {
                    SingleRecordFilterMapOutcome::Filtered => {}
                    SingleRecordFilterMapOutcome::Output(record) => {
                        route_outputs.push(RouteOutput {
                            output_index: routed.output_index,
                            key: routed.key,
                            record,
                        });
                    }
                    SingleRecordFilterMapOutcome::MessageError {
                        error,
                        partial_output,
                        materialized_state,
                    } => route_errors.push(RouteError {
                        output_index: routed.output_index,
                        error,
                        partial_output,
                        materialized_state,
                    }),
                }
            }
            let routed_count = route_errors.len() + route_outputs.len();
            let mut ack_queue = VecDeque::with_capacity(routed_count);
            for _ in 1..routed_count {
                ack_queue.push_back(acks.attached());
            }
            ack_queue.push_front(acks);
            for route_error in route_errors {
                let acks = ack_queue
                    .pop_front()
                    .verified("the queue above was filled with one ACK entry per route");
                let output = &output_routes.routes[route_error.output_index];
                self.handle_structured_message_error(MessageErrorHandling {
                    domain,
                    node_kind: ModelKind::Ingestor,
                    node: &ModelName::from(ingestor),
                    source_route: Some(&output.relay),
                    policy: &output.message_error_policy,
                    message: RelayMessage {
                        key: None,
                        record: rows.row(row)?,
                        acks,
                    },
                    error: route_error.error,
                    partial_output: route_error.partial_output,
                    materialized_state: route_error.materialized_state,
                    ingest_metadata: rows.metadata_row(row),
                    execution_now,
                })
                .await;
            }
            for route_output in route_outputs {
                let acks = ack_queue
                    .pop_front()
                    .verified("the queue above was filled with one ACK entry per route");
                let output = &output_routes.routes[route_output.output_index];
                output.branch.as_ref().ok_or_else(|| {
                    format!(
                        "ingestor '{}' output '{}' has no branch declaration",
                        ingestor.as_str(),
                        output.relay.as_str()
                    )
                })?;
                collector.push(
                    &output.relay,
                    RelayMessage {
                        key: route_output.key,
                        record: route_output.record,
                        acks,
                    },
                );
            }
        }
        Ok(())
    }

    /// Fans an ingestor `FILTER WHERE` message error out to every output route's error
    /// policy, splitting acks the same way a routed message would have.
    pub(super) async fn handle_ingestor_filter_where_error(
        &self,
        handling: IngestorFilterWhereError<'_>,
    ) {
        let IngestorFilterWhereError {
            domain,
            ingestor,
            output_routes,
            record,
            ingest_metadata,
            acks,
            error,
            materialized_state,
            execution_now,
        } = handling;
        let route_count = output_routes.routes.len();
        if route_count == 0 {
            acks.no_ack(error.message);
            return;
        }
        let mut ack_queue = VecDeque::with_capacity(route_count);
        for _ in 1..route_count {
            ack_queue.push_back(acks.attached());
        }
        ack_queue.push_front(acks);
        for output in &output_routes.routes {
            let acks = ack_queue
                .pop_front()
                .verified("the queue above was filled with one ACK entry per route");
            self.handle_structured_message_error(MessageErrorHandling {
                domain,
                node_kind: ModelKind::Ingestor,
                node: &ModelName::from(ingestor),
                source_route: Some(&output.relay),
                policy: &output.message_error_policy,
                message: RelayMessage {
                    key: None,
                    record: record.clone(),
                    acks,
                },
                error: error.clone(),
                partial_output: None,
                materialized_state: materialized_state.clone(),
                ingest_metadata: ingest_metadata.clone(),
                execution_now,
            })
            .await;
        }
    }

    pub(in crate::runtime) async fn initialize_domain_kafka_consumer_offsets(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        topic: &str,
        consumer: &StreamConsumer,
        state: &KafkaOffsetStateOriginator,
        instance_idx: u64,
    ) -> Result<(u64, bool), String> {
        let (start_version, last_start) = if let Some(domain_state) = self.inner.domains.get(domain)
        {
            (domain_state.start_version, domain_state.last_start.clone())
        } else {
            return Err(format!("domain '{}' is not installed", domain.as_str()));
        };
        let scheduled_partition_schedule = if let Some(execution) =
            self.inner.executions.get(domain)
            && let Some(node) = execution.schedule.nodes.get(&NodeRef::new(
                ModelKind::Ingestor,
                ModelName::from(ingestor),
            )) {
            node.kafka_partition_schedule.clone()
        } else {
            None
        };

        let offsets = if let nervix_models::DomainStartPoint::Resume = &last_start {
            let missing_partition_timestamp = self
                .current_paced_domain_time(domain)
                .map_err(|error| error.to_string())?;
            KafkaIngestor::resume_offsets_from_state(
                consumer,
                topic,
                state.read(),
                missing_partition_timestamp,
            )
            .map_err(|error| error.to_string())?
        } else {
            let timestamp = match &last_start {
                nervix_models::DomainStartPoint::Now { .. } => {
                    return Err(format!(
                        "domain '{}' has an unresolved START AT NOW",
                        domain.as_str()
                    ));
                }
                nervix_models::DomainStartPoint::At { timestamp, .. } => *timestamp,
                nervix_models::DomainStartPoint::Resume => unreachable!("handled above"),
            };
            KafkaIngestor::offsets_by_timestamp(consumer, topic, timestamp)
                .map_err(|error| error.to_string())?
        };
        let has_assignment = KafkaIngestor::assign_offsets_for_instance(
            consumer,
            topic,
            &offsets,
            scheduled_partition_schedule.as_ref(),
            instance_idx,
        )
        .map_err(|error| error.to_string())?;

        if let nervix_models::DomainStartPoint::Resume = &last_start {
            return Ok((start_version, has_assignment));
        }

        let concrete_offsets =
            KafkaIngestor::concrete_next_offsets_from_assignment(consumer, topic, &offsets)
                .map_err(|error| error.to_string())?;
        self.reset_domain_kafka_offsets(state, concrete_offsets)
            .await
            .map_err(|error| error.to_string())?;
        Ok((start_version, has_assignment))
    }

    pub(in crate::runtime) async fn dispatch_raw_ingest_payload(
        &self,
        dispatch: RawIngestDispatch<'_>,
    ) -> Result<(), String> {
        let RawIngestDispatch {
            domain,
            ingestor,
            timestamp_source,
            output_routes,
            filter_where,
            branched_senders,
            codec,
            payload,
            collector,
            flush,
        } = dispatch;
        for source_payload in payload.payloads() {
            tokio::task::consume_budget().await;
            // A request carries all of its payloads or none of them, so a payload that fails to
            // decode takes the payloads decoded before it back out of the group.
            if let Err(error) = collector
                .decode_payload(&codec, Cow::Borrowed(source_payload))
                .await
            {
                collector.discard_undispatched_payloads();
                return Err(error.to_string());
            }
        }
        let metadata = payload.metadata_rows();
        self.dispatch_ingested_records(IngestGroupDispatch {
            collector,
            domain,
            ingestor,
            timestamp_source,
            output_routes,
            filter_where,
            metadata: &metadata,
            ingested_at: payload.observed_at(),
            acks: vec![AckSet::empty(); payload.len()],
        })
        .await
        .map_err(|error| error.to_string())?;
        if flush {
            self.flush_ingest_collector(domain, ingestor, branched_senders, collector)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use ahash::HashMap;
    use arc_swap::ArcSwapOption;
    use nervix_models::{
        AckMode, CodecJaqFormat, CodecJaqTransformations, CodecWireFormat, CreateCodec,
        CreateSchema, CreateWireSchema, ErrorPolicies, JsonType, ModelKind, ParseAsType,
        ResolvedCodecWireFormat, SchemaField, Timestamp, WireSchemaField,
    };
    use tokio::time::{Duration, timeout};
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime::branch_runtime::BranchExecutionRuntime,
        runtime_ack::{AckOutcome, AckRootTracker, AckSet},
        runtime_schema::{
            RECORD_BUILDER_SETS_OPENED, RECORD_COLUMN_SETS_BUILT, RuntimeRecordBatch,
            RuntimeRecordMetadata, RuntimeValue, compile_codec, test_runtime_row,
        },
    };

    /// The one-field schema the ingest-group decode tests below decode into.
    fn grouped_event_schema() -> Arc<CompiledSchema> {
        Arc::new(compile_schema(&CreateSchema {
            name: named("grouped_event"),
            fields: vec![SchemaField {
                name: named("user_id"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }))
    }

    /// A JSON codec over the grouped event schema, for the ingest-group decode tests below.
    fn grouped_event_codec() -> Arc<CompiledCodec> {
        let schema = grouped_event_schema();
        compile_codec(
            &CreateCodec {
                name: named("grouped_event_codec"),
                wire_format: CodecWireFormat::Json {
                    wire_schema: named("grouped_event_wire"),
                },
                schema: named("grouped_event"),
                encoding_rules: Vec::new(),
            },
            schema,
            ResolvedCodecWireFormat::Json(&CreateWireSchema {
                name: named("grouped_event_wire"),
                strictness: Default::default(),
                fields: vec![WireSchemaField {
                    name: named("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                }],
            }),
        )
        .expect("the grouped event codec should compile")
    }

    /// A JSON codec over the grouped event schema whose ON INGESTION program is `program`.
    fn unfolding_event_codec(program: &str) -> Arc<CompiledCodec> {
        let transformations = CodecJaqTransformations {
            on_ingestion: Some(program.to_string()),
            on_emitting: None,
        };
        compile_codec(
            &CreateCodec {
                name: named("unfolding_event_codec"),
                wire_format: CodecWireFormat::JaqNative {
                    format: CodecJaqFormat::Json,
                    transformations: transformations.clone(),
                },
                schema: named("grouped_event"),
                encoding_rules: Vec::new(),
            },
            grouped_event_schema(),
            ResolvedCodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: &transformations,
            },
        )
        .expect("the unfolding event codec should compile")
    }

    fn grouped_event_ingestor_metrics() -> MessageMetricsHandle {
        let ingestor: IngestorName = named("grouped_event_source");
        RuntimeMetrics::default().resolve_global_node_message_metrics(
            &domain("default"),
            ModelKind::Ingestor,
            &ModelName::from(&ingestor),
            None,
            "received",
        )
    }

    /// Accepts the oldest payloads `collector` has decoded, one for each ACK set, as an ingestor
    /// does after a successful decode.
    fn accept_decoded_payloads(
        collector: &mut IngestRouteCollector,
        acks: Vec<AckSet>,
    ) -> Result<(), String> {
        let headers = NoIngestHeaders;
        let metadata = (0..acks.len())
            .map(|_| IngestMetadataRow::Headers { headers: &headers })
            .collect::<Vec<_>>();
        collector.collect(IngestGroupContribution {
            domain: &domain("default"),
            ingestor: &named("grouped_event_source"),
            timestamp_source: None,
            output_routes: &RelayProcessorOutputsNode { routes: Vec::new() },
            filter_where: None,
            metadata: &metadata,
            acks,
            ingested_at: Timestamp::from_unix_nanos(1),
        })
    }

    /// A group of `n` messages must cost one set of Arrow columns, not `n` single-row batches and
    /// a concatenation.
    #[tokio::test]
    async fn ingest_group_builds_one_record_column_set_for_all_of_its_messages() {
        let codec = grouped_event_codec();
        let mut collector = IngestRouteCollector::new(
            IngestMetadataKind::Headers,
            8,
            grouped_event_ingestor_metrics(),
        );

        RECORD_BUILDER_SETS_OPENED.with(|count| count.set(0));
        RECORD_COLUMN_SETS_BUILT.with(|count| count.set(0));

        for user_id in 0..3i64 {
            collector
                .decode_payload(
                    &codec,
                    Cow::Owned(format!(r#"{{"user_id":{user_id}}}"#).into_bytes()),
                )
                .await
                .expect("each payload should decode into the open group");
            accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
                .expect("each decoded payload should be accepted");
        }

        assert_eq!(
            RECORD_BUILDER_SETS_OPENED.with(std::cell::Cell::get),
            1,
            "a group must open exactly one record builder, not one per message"
        );
        assert_eq!(
            RECORD_COLUMN_SETS_BUILT.with(std::cell::Cell::get),
            0,
            "an open group must not build record columns before it closes"
        );

        let (_, rows) = collector
            .take_pending()
            .expect("the group must close")
            .expect("the group holds rows");

        assert_eq!(
            RECORD_COLUMN_SETS_BUILT.with(std::cell::Cell::get),
            1,
            "closing a group must build exactly one record column set"
        );
        assert_eq!(rows.len(), 3);
        for user_id in 0..3usize {
            assert_eq!(
                rows.batch
                    .value(user_id, "user_id")
                    .expect("the decoded column must be readable"),
                Some(RuntimeValue::I64(
                    user_id.try_into().expect("a small test index fits i64")
                ))
            );
        }
    }

    /// An acknowledged poll group decodes its whole batch up front and then accepts the payloads
    /// one at a time, so a contribution covers a prefix of what the group has decoded.
    #[tokio::test]
    async fn ingest_group_accepts_its_decoded_payloads_one_at_a_time() {
        let codec = grouped_event_codec();
        let mut collector = IngestRouteCollector::new(
            IngestMetadataKind::Headers,
            3,
            grouped_event_ingestor_metrics(),
        );

        for user_id in 0..3i64 {
            collector
                .decode_payload(
                    &codec,
                    Cow::Owned(format!(r#"{{"user_id":{user_id}}}"#).into_bytes()),
                )
                .await
                .expect("each payload should decode into the open group");
        }
        for _ in 0..3 {
            accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
                .expect("each decoded payload should be accepted on its own");
        }

        let (_, rows) = collector
            .take_pending()
            .expect("the group must close")
            .expect("the group holds rows");

        assert_eq!(rows.len(), 3);
        for user_id in 0..3usize {
            assert_eq!(
                rows.batch
                    .value(user_id, "user_id")
                    .expect("the decoded column must be readable"),
                Some(RuntimeValue::I64(
                    user_id.try_into().expect("a small test index fits i64")
                ))
            );
        }
    }

    /// A payload the codec rejects stays attributable to its own message: the group keeps the rows
    /// around it, and its records, metadata and ACKs stay row-aligned.
    #[tokio::test]
    async fn ingest_group_keeps_its_other_messages_when_one_payload_fails_to_decode() {
        let codec = grouped_event_codec();
        let mut collector = IngestRouteCollector::new(
            IngestMetadataKind::Headers,
            8,
            grouped_event_ingestor_metrics(),
        );

        collector
            .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":1}"#))
            .await
            .expect("the first payload should decode");
        accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
            .expect("the first payload should be accepted");

        collector
            .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":"two"}"#))
            .await
            .expect_err("a user id of the wrong type should be rejected");

        collector
            .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":3}"#))
            .await
            .expect("the payload after the rejected one should decode");
        accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
            .expect("the third payload should be accepted");

        let (_, rows) = collector
            .take_pending()
            .expect("the group must close")
            .expect("the group holds rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows.acks.len(), 2);
        assert_eq!(rows.record_metadata.len(), 2);
        assert_eq!(
            rows.batch.value(0, "user_id").expect("readable"),
            Some(RuntimeValue::I64(1))
        );
        assert_eq!(
            rows.batch.value(1, "user_id").expect("readable"),
            Some(RuntimeValue::I64(3))
        );
    }

    /// Every message a payload unfolds into takes the payload's metadata and a share of its ACK
    /// set, so the source's acknowledgement waits for all of them.
    #[tokio::test]
    async fn ingest_group_gives_every_unfolded_message_its_payload_metadata_and_an_ack_share() {
        let codec = unfolding_event_codec(".[]");
        let mut collector = IngestRouteCollector::new(
            IngestMetadataKind::Headers,
            1,
            grouped_event_ingestor_metrics(),
        );
        let tracker = Arc::new(AckRootTracker::default());
        let (payload_acks, completion) = AckSet::tracked_root(tracker.clone());

        collector
            .decode_payload(
                &codec,
                Cow::Borrowed(br#"[{"user_id":1},{"user_id":2},{"user_id":3}]"#),
            )
            .await
            .expect("the payload should unfold into the open group");
        accept_decoded_payloads(&mut collector, vec![payload_acks])
            .expect("the unfolded payload should be accepted");

        assert_eq!(
            collector.len(),
            3,
            "the group counts messages, not payloads"
        );
        let (_, rows) = collector
            .take_pending()
            .expect("the group must close")
            .expect("the group holds rows");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.record_metadata.len(), 3);
        assert_eq!(rows.acks.len(), 3);
        assert!(
            rows.metadata_row(2).is_some(),
            "every unfolded message must carry the payload's ingest metadata"
        );
        for (row, user_id) in (1..=3i64).enumerate() {
            assert_eq!(
                rows.batch.value(row, "user_id").expect("readable"),
                Some(RuntimeValue::I64(user_id))
            );
        }

        rows.acks[0].ack_success();
        rows.acks[1].ack_success();
        assert_eq!(
            tracker.outstanding(),
            1,
            "the payload must stay outstanding until its last message is acknowledged"
        );
        rows.acks[2].ack_success();
        assert_eq!(
            timeout(Duration::from_secs(1), completion.wait())
                .await
                .expect("the payload acknowledgement should resolve"),
            AckOutcome::Ack
        );
    }

    /// A payload that unfolds into no message is acknowledged when it is accepted, and it neither
    /// opens the group nor schedules its idle close.
    #[tokio::test]
    async fn ingest_group_acknowledges_a_payload_that_unfolds_into_no_messages() {
        let codec = unfolding_event_codec(".[] | select(.keep)");
        let mut collector = IngestRouteCollector::new(
            IngestMetadataKind::Headers,
            1,
            grouped_event_ingestor_metrics(),
        );
        let tracker = Arc::new(AckRootTracker::default());
        let (payload_acks, completion) = AckSet::tracked_root(tracker.clone());

        collector
            .decode_payload(&codec, Cow::Borrowed(br#"[{"user_id":1,"keep":false}]"#))
            .await
            .expect("a payload the program selects nothing from should decode");
        accept_decoded_payloads(&mut collector, vec![payload_acks])
            .expect("the payload without messages should be accepted");

        assert_eq!(tracker.outstanding(), 0);
        assert_eq!(
            timeout(Duration::from_secs(1), completion.wait())
                .await
                .expect("the payload acknowledgement should resolve"),
            AckOutcome::Ack
        );
        assert!(collector.is_empty());
        assert!(collector.next_flush().is_none());
        assert!(
            collector
                .take_pending()
                .expect("the empty group must close")
                .is_none()
        );
    }

    /// A payload that fails part-way through unfolding contributes no message, and the group keeps
    /// the messages of the payloads around it.
    #[tokio::test]
    async fn ingest_group_keeps_no_message_of_a_payload_that_fails_part_way_through_unfolding() {
        let codec = unfolding_event_codec(".[]");
        let mut collector = IngestRouteCollector::new(
            IngestMetadataKind::Headers,
            8,
            grouped_event_ingestor_metrics(),
        );

        collector
            .decode_payload(&codec, Cow::Borrowed(br#"[{"user_id":1}]"#))
            .await
            .expect("the first payload should unfold");
        accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
            .expect("the first payload should be accepted");

        let error = collector
            .decode_payload(
                &codec,
                Cow::Borrowed(br#"[{"user_id":2},{"user_id":"confidential"}]"#),
            )
            .await
            .expect_err("an element of the wrong type must reject its whole payload");
        let message = error.to_string();
        assert!(message.contains("(input value 0, output 1)"), "{message}");
        assert!(!message.contains("confidential"), "{message}");

        collector
            .decode_payload(&codec, Cow::Borrowed(br#"[{"user_id":3},{"user_id":4}]"#))
            .await
            .expect("the payload after the rejected one should unfold");
        accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
            .expect("the last payload should be accepted");

        let (_, rows) = collector
            .take_pending()
            .expect("the group must close")
            .expect("the group holds rows");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.acks.len(), 3);
        for (row, user_id) in [1i64, 3, 4].into_iter().enumerate() {
            assert_eq!(
                rows.batch.value(row, "user_id").expect("readable"),
                Some(RuntimeValue::I64(user_id))
            );
        }
    }

    /// A rejected payload in an otherwise empty group releases the rows it abandoned instead of
    /// keeping them allocated until a message arrives.
    #[tokio::test]
    async fn ingest_group_releases_the_rows_a_rejected_payload_abandoned_in_an_empty_group() {
        let codec = unfolding_event_codec(".[]");
        let mut collector = IngestRouteCollector::new(
            IngestMetadataKind::Headers,
            8,
            grouped_event_ingestor_metrics(),
        );

        collector
            .decode_payload(
                &codec,
                Cow::Borrowed(br#"[{"user_id":1},{"user_id":"two"}]"#),
            )
            .await
            .expect_err("an element of the wrong type must reject its whole payload");

        assert!(
            collector.pending.records.is_none(),
            "an empty group must drop the builder a rejected payload abandoned rows in"
        );
    }

    #[test]
    fn ingest_group_rows_share_the_group_batch_allocation() {
        let first = test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]);
        let second = test_runtime_row([("value".to_string(), RuntimeValue::I64(2))]);
        let batch =
            RuntimeRecordBatch::from_rows(first.batch().schema(), [&first, &second].into_iter())
                .expect("test rows should form one ingest group");
        let rows = IngestGroupRows {
            batch: Arc::new(batch),
            record_metadata: vec![RuntimeRecordMetadata::test(); 2],
            ingest_metadata: ingest_metadata_for_test(
                IngestMetadataKind::Headers,
                &[
                    IngestMetadataRow::Headers {
                        headers: &NoIngestHeaders,
                    },
                    IngestMetadataRow::Headers {
                        headers: &NoIngestHeaders,
                    },
                ],
            ),
            acks: vec![AckSet::empty(), AckSet::empty()],
        };

        let first = rows.row(0).expect("first row should exist");
        let second = rows.row(1).expect("second row should exist");

        assert!(
            Arc::ptr_eq(first.batch(), second.batch()),
            "row views from one ingest group must retain the same batch allocation"
        );
    }

    #[tokio::test]
    async fn branched_root_without_children_acks_success() {
        let runtime = Runtime::default();
        let root_domain = domain("default");
        install_unpaced_test_domain(&runtime, &root_domain);
        let root_relay = named("tenant_orders");
        let root_registry = RelayRegistry::new();
        let root_services = test_relay_boundary_services();
        let owner_task = runtime.spawn_relay_owner_task(
            &root_domain,
            &root_relay,
            root_registry.clone(),
            root_services.clone(),
            RelayRetention::default(),
        );
        let root_key = Some(concrete_branch_key([(
            named("tenant"),
            RuntimeValue::String("acme".to_string()),
        )]));
        let root_source = named("metric_ingestor");
        let root_metrics = runtime
            .inner
            .metrics
            .resolve_node_batch_metrics(NodeBatchMetricsSpec {
                domain: &root_domain,
                kind: ModelKind::Ingestor,
                node: &ModelName::from(&root_source),
                relay: &root_relay,
                physical_node_id: None,
                direction: "sent",
                branch_key: Some(branch_key_display(&root_key)),
            });
        let mut root = BranchRuntime {
            key: root_key.clone(),
            runtime: runtime.clone(),
            domain: root_domain.clone(),
            routing: None,
            routing_snapshot: None,
            domain_clock: test_domain_clock(&root_domain),
            source_kind: ModelKind::Ingestor,
            source: root_source,
            root_relay: root_relay.clone(),
            error_policies: ErrorPolicies::handled_by_log(),
            relays: [(
                root_relay.clone(),
                ConcreteRelayRuntime::new(ConcreteRelayRuntimeBuild {
                    runtime,
                    domain: root_domain,
                    relay: root_relay,
                    registry: root_registry,
                    services: root_services,
                    key: root_key,
                }),
            )]
            .into_iter()
            .collect(),
            materialized_states: HashMap::default(),
            relay_state_epoch: None,
            processors: HashMap::default(),
            metrics: BranchRuntimeMetrics {
                source: root_metrics,
                source_input: None,
                processor_inputs: HashMap::default(),
                processor_outputs: HashMap::default(),
            },
        };
        let graph = StdArc::new(ArcSwapOption::from(None));
        let (acks, completion) = AckSet::root();
        let schema = test_schema(&[("tenant", ParseAsType::String)]);

        root.dispatch(
            &graph,
            RelayRecordBatch::single(
                schema,
                string_branch_key("tenant", "acme"),
                test_runtime_row([(
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                )]),
                acks,
            )
            .expect("batch should build"),
        )
        .await;

        assert_eq!(
            timeout(Duration::from_secs(1), completion.wait())
                .await
                .expect("ack completion should resolve"),
            AckOutcome::Ack
        );
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[tokio::test]
    async fn branch_entrypoint_dispatches_an_ingestor_prepared_batch_immediately() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let root_relay = named("notifications");
        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(1));
        let mut fan_in =
            RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
        let services = Arc::new(RelayBoundaryServices::new(fanout, 1, 0, Vec::new(), None));
        let registry = RelayRegistry::new();
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &root_relay,
            registry.clone(),
            services.clone(),
            RelayRetention::default(),
        );
        let schema = test_schema(&[("user_id", ParseAsType::U32)]);
        let branched_runtime = BranchExecutionRuntime::new(
            runtime,
            domain,
            named("notifications_ingestor"),
            StdArc::new(ArcSwapOption::from(None)),
            BranchInstanceTemplate {
                source_kind: ModelKind::Ingestor,
                source: named("notifications_ingestor"),
                root_relay: root_relay.clone(),
                branch: None,
                branch_ttl: None,
                branch_max_instances: None,
                error_policies: ErrorPolicies::handled_by_log(),
                relays: [(
                    root_relay,
                    RelayProcessorRelayTemplate {
                        registry,
                        services: services.clone(),
                    },
                )]
                .into_iter()
                .collect(),
                processors: HashMap::default(),
            },
            Duration::from_secs(30),
        );

        branched_runtime
            .sender()
            .send(
                RelayRecordBatch::single(
                    schema,
                    None,
                    test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))]),
                    AckSet::empty(),
                )
                .expect("ingestor output batch should build"),
            )
            .await
            .expect("branch entrypoint should accept an ingestor-prepared batch");

        let batch = timeout(Duration::from_millis(100), fan_in.recv())
            .await
            .expect("branch entrypoint must not apply a second flush delay")
            .expect("runtime consumer should remain open");
        assert_eq!(batch.message_count(), 1);

        branched_runtime.shutdown().await;
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[tokio::test]
    async fn ingestor_and_reingestor_routes_apply_size_boundaries_independently_per_branch() {
        let cases = [
            (
                ModelKind::Ingestor,
                BranchInstanceAckBoundary::Preserve,
                "notifications_ingestor",
            ),
            (
                ModelKind::Reingestor,
                BranchInstanceAckBoundary::Reingestor(AckMode::Attached),
                "notifications_reingestor",
            ),
        ];
        for (source_kind, ack_boundary, source) in cases {
            tokio::task::consume_budget().await;
            let runtime = Runtime::default();
            let domain = domain("default");
            install_unpaced_test_domain(&runtime, &domain);
            let root_relay = named("notifications");
            let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(4));
            let mut fan_in = RelayRuntimeFanIn::new(
                fanout.runtime_consumer_receiver_for_mode(AckMode::Attached),
            );
            let services = Arc::new(RelayBoundaryServices::new(fanout, 1, 0, Vec::new(), None));
            let registry = RelayRegistry::new();
            let owner_task = runtime.spawn_relay_owner_task(
                &domain,
                &root_relay,
                registry.clone(),
                services.clone(),
                RelayRetention::default(),
            );
            let schema = test_schema(&[
                ("tenant", ParseAsType::String),
                ("user_id", ParseAsType::U32),
            ]);
            let batch = |tenant: &str, user_id| {
                RelayRecordBatch::single(
                    schema.clone(),
                    string_branch_key("tenant", tenant),
                    test_runtime_row([
                        (
                            "tenant".to_string(),
                            RuntimeValue::String(tenant.to_string()),
                        ),
                        ("user_id".to_string(), RuntimeValue::U32(user_id)),
                    ]),
                    AckSet::empty(),
                )
                .expect("ingestor output batch should build")
            };
            let acme_one = batch("acme", 1);
            let max_batch_size = acme_one.estimated_bytes() + 1;
            let route_runtime = IngestorRouteRuntime::new(
                runtime,
                domain,
                named(source),
                StdArc::new(ArcSwapOption::from(None)),
                IngestorRouteTemplate {
                    branch: BranchInstanceTemplate {
                        source_kind,
                        source: named(source),
                        root_relay: root_relay.clone(),
                        branch: None,
                        branch_ttl: None,
                        branch_max_instances: None,
                        error_policies: ErrorPolicies::handled_by_log(),
                        relays: [(
                            root_relay,
                            RelayProcessorRelayTemplate {
                                registry,
                                services: services.clone(),
                            },
                        )]
                        .into_iter()
                        .collect(),
                        processors: HashMap::default(),
                    },
                    ack_boundary,
                    flush_policy: RuntimeFlushPolicy::Each {
                        interval: Duration::from_secs(10),
                        max_batch_size,
                    },
                },
                Duration::from_secs(30),
            );

            route_runtime
                .sender()
                .send(acme_one)
                .await
                .expect("acme batch should enter the route");
            route_runtime
                .sender()
                .send(batch("beta", 1))
                .await
                .expect("beta batch should enter the route");
            assert!(
                timeout(Duration::from_millis(50), fan_in.recv())
                    .await
                    .is_err(),
                "different branches must not share a size boundary"
            );

            route_runtime
                .sender()
                .send(batch("acme", 2))
                .await
                .expect("second acme batch should enter the route");
            let acme = timeout(Duration::from_secs(1), fan_in.recv())
                .await
                .expect("acme size boundary should flush")
                .expect("runtime consumer should remain open");
            assert_eq!(key_label(&acme.key), r#"{"tenant":"acme"}"#);
            assert_eq!(acme.message_count(), 2);
            assert!(
                timeout(Duration::from_millis(50), fan_in.recv())
                    .await
                    .is_err(),
                "beta must remain pending until its own size boundary"
            );

            route_runtime
                .sender()
                .send(batch("beta", 2))
                .await
                .expect("second beta batch should enter the route");
            let beta = timeout(Duration::from_secs(1), fan_in.recv())
                .await
                .expect("beta size boundary should flush")
                .expect("runtime consumer should remain open");
            assert_eq!(key_label(&beta.key), r#"{"tenant":"beta"}"#);
            assert_eq!(beta.message_count(), 2);

            route_runtime.shutdown().await;
            owner_task
                .stop(Duration::from_secs(1))
                .await
                .expect("relay owner should stop");
        }
    }
}
