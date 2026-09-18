//! Columnar ingestion group execution.
//!
//! Layer: data plane.
//! - **Owns.** Group decoding, timestamped row metadata, route execution and in-memory ACKs.
//! - **Depends on.** Installed domain capabilities, compiled codecs and branch-local routes.
//! - **Must not know.** NSPL parsing, consensus decisions or source transport lifecycle.

use std::borrow::Cow;

use ahash::RandomState;
use bytes::Bytes;
use error_stack::ResultExt as _;
use indexmap::{Equivalent, IndexMap};
use nervix_connector::IngestMetadataRow;

use super::{ingestors::kafka::KafkaOffsetInitialization, *};

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

#[derive(Debug, Clone, Copy, strum::Display)]
pub(in crate::runtime) enum IngestGroupRecordOperation {
    #[strum(serialize = "finish the decoded ingest batch")]
    FinishDecodedBatch,
    #[strum(serialize = "address an ingest row")]
    AddressRow,
    #[strum(serialize = "filter an ingest batch")]
    FilterBatch,
    #[strum(serialize = "concatenate transformed ingest rows")]
    ConcatenateTransformedRows,
    #[strum(serialize = "concatenate branch output rows")]
    ConcatenateBranchOutputs,
    #[strum(serialize = "select branch input rows")]
    SelectBranchInputs,
    #[strum(serialize = "concatenate branch input batches")]
    ConcatenateBranchInputs,
    #[strum(serialize = "filter a branch input batch")]
    FilterBranchInput,
}

#[derive(Debug, Clone, Copy, strum::Display)]
pub(in crate::runtime) enum IngestMetadataOperation {
    #[strum(serialize = "append ingest metadata")]
    Append,
    #[strum(serialize = "finish ingest metadata")]
    Finish,
    #[strum(serialize = "select ingest metadata")]
    Select,
}

#[derive(Debug, Clone, Copy, strum::Display)]
pub(in crate::runtime) enum KafkaOffsetInitializationOperation {
    #[strum(serialize = "read paced domain time")]
    ReadDomainTime,
    #[strum(serialize = "resolve resume offsets")]
    ResolveResumeOffsets,
    #[strum(serialize = "resolve timestamp offsets")]
    ResolveTimestampOffsets,
    #[strum(serialize = "assign offsets")]
    AssignOffsets,
    #[strum(serialize = "resolve concrete offsets")]
    ResolveConcreteOffsets,
    #[strum(serialize = "reset replicated offsets")]
    ResetOffsets,
}

#[derive(Debug, Clone, Copy, strum::Display)]
pub(in crate::runtime) enum IngestGroupBlockingOperation {
    #[strum(serialize = "build a branch input batch")]
    BuildBranchInput,
    #[strum(serialize = "filter a branch input batch")]
    FilterBranchInput,
}

#[derive(Debug, Error)]
pub(in crate::runtime) enum IngestGroupError {
    #[error("received {ack_sets} ACK sets for {metadata_rows} ingest metadata rows")]
    PayloadSidecarCount {
        metadata_rows: usize,
        ack_sets: usize,
    },
    #[error(
        "received {metadata_rows} ingest metadata rows for {decoded_payloads} decoded payloads"
    )]
    DecodedPayloadCount {
        metadata_rows: usize,
        decoded_payloads: usize,
    },
    #[error("failed to {operation}")]
    Metadata { operation: IngestMetadataOperation },
    #[error("ingest group has {ack_sets} ACK sets and {ingest_timestamps} ingest timestamps")]
    TimestampCount {
        ack_sets: usize,
        ingest_timestamps: usize,
    },
    #[error("ingest group closed without opening its metadata builders")]
    MissingMetadataBuilders,
    #[error("ingest group has {records} records and {metadata_rows} ingest metadata rows")]
    MetadataRowCount {
        records: usize,
        metadata_rows: usize,
    },
    #[error("ingest group closed without opening its record builder")]
    MissingRecordBuilder,
    #[error("ingest group has {records} records and {decoded_rows} decoded rows")]
    DecodedRowCount { records: usize, decoded_rows: usize },
    #[error("ingest row {row} is outside record metadata with {record_metadata_rows} rows")]
    RecordMetadataRowOutOfBounds {
        row: usize,
        record_metadata_rows: usize,
    },
    #[error(
        "ingest group has {arrow_rows} Arrow rows, {record_metadata_rows} record metadata rows, \
         {ingest_metadata_rows} ingest metadata rows, and {ack_sets} ACK sets"
    )]
    RowCount {
        arrow_rows: usize,
        record_metadata_rows: usize,
        ingest_metadata_rows: usize,
        ack_sets: usize,
    },
    #[error("ingest selection has {found} rows for a group with {expected} rows")]
    SelectionLengthMismatch { expected: usize, found: usize },
    #[error("failed to {operation}")]
    RuntimeSchema {
        operation: IngestGroupRecordOperation,
    },
    #[error(
        "ingest group for '{existing_domain}.{existing_ingestor}' cannot collect rows for \
         '{received_domain}.{received_ingestor}'"
    )]
    CollectorIdentity {
        existing_domain: DomainName,
        existing_ingestor: IngestorName,
        received_domain: DomainName,
        received_ingestor: IngestorName,
    },
    #[error("ingest group closed with {payloads} decoded payloads that were never accepted")]
    UndispatchedPayloads { payloads: usize },
    #[error(
        "branch input has {arrow_rows} Arrow rows, {metadata_rows} metadata rows, {branch_keys} \
         branch keys, and {ack_sets} ACK sets"
    )]
    BranchInputRowCount {
        arrow_rows: usize,
        metadata_rows: usize,
        branch_keys: usize,
        ack_sets: usize,
    },
    #[error("a branch input batch must contain at least one input")]
    EmptyBranchInputs,
    #[error("branch selection row {row} is outside batch with {batch_rows} rows")]
    BranchSelectionRowOutOfBounds { row: usize, batch_rows: usize },
    #[error("failed to construct a filtered relay batch")]
    FilteredRelayBatch,
    #[error("failed to {operation} in a blocking task")]
    BlockingTask {
        operation: IngestGroupBlockingOperation,
    },
    #[error("failed to load routing for domain '{domain}'")]
    Routing { domain: DomainName },
    #[error(
        "ingest group for '{group_domain}.{group_ingestor}' cannot flush as \
         '{requested_domain}.{requested_ingestor}'"
    )]
    FlushIdentity {
        group_domain: DomainName,
        group_ingestor: IngestorName,
        requested_domain: DomainName,
        requested_ingestor: IngestorName,
    },
    #[error("domain '{domain}' is not running")]
    DomainNotRunning { domain: DomainName },
    #[error("relay '{relay}' schema is not instantiated in domain '{domain}'")]
    RelaySchemaMissing {
        domain: DomainName,
        relay: RelayName,
    },
    #[error("failed to build an ingest batch for relay '{relay}'")]
    RelayBatch { relay: RelayName },
    #[error("ingestor '{ingestor}' has no branch entrypoint for relay '{relay}'")]
    BranchEntrypointMissing {
        ingestor: IngestorName,
        relay: RelayName,
    },
    #[error("ingestor '{ingestor}' failed to forward a batch to relay '{relay}'")]
    BranchEntrypointClosed {
        ingestor: IngestorName,
        relay: RelayName,
    },
    #[error("failed to collect a group for ingestor '{ingestor}'")]
    Collect { ingestor: IngestorName },
    #[error("failed to establish ingestion time for '{domain}.{ingestor}'")]
    IngestionTime {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error("failed to load materialized state for ingestor '{ingestor}'")]
    MaterializedState { ingestor: IngestorName },
    #[error("failed to load materialized state for ingestor '{ingestor}' route '{relay}'")]
    RouteMaterializedState {
        ingestor: IngestorName,
        relay: RelayName,
    },
    #[error("failed to evaluate the FILTER WHERE program for ingestor '{ingestor}'")]
    FilterWhere { ingestor: IngestorName },
    #[error("failed to evaluate route '{relay}' for ingestor '{ingestor}'")]
    RouteProgram {
        ingestor: IngestorName,
        relay: RelayName,
    },
    #[error("failed to evaluate branch construction for ingestor '{ingestor}' route '{relay}'")]
    BranchProgram {
        ingestor: IngestorName,
        relay: RelayName,
    },
    #[error("ingestor '{ingestor}' route '{relay}' has no compiled branch program")]
    MissingBranchProgram {
        ingestor: IngestorName,
        relay: RelayName,
    },
    #[error("ingestor '{ingestor}' route '{relay}' has no branch result for row {row}")]
    MissingBranchResult {
        ingestor: IngestorName,
        relay: RelayName,
        row: usize,
    },
    #[error("ingestor '{ingestor}' route '{relay}' has no branch declaration")]
    MissingBranchDeclaration {
        ingestor: IngestorName,
        relay: RelayName,
    },
    #[error("domain '{domain}' is not installed")]
    DomainNotInstalled { domain: DomainName },
    #[error("domain '{domain}' has an unresolved START AT NOW")]
    UnresolvedStartNow { domain: DomainName },
    #[error(
        "failed to {operation} for Kafka ingestor '{ingestor}' topic '{topic}' in domain \
         '{domain}'"
    )]
    KafkaOffsets {
        operation: KafkaOffsetInitializationOperation,
        domain: DomainName,
        ingestor: IngestorName,
        topic: String,
    },
    #[error("failed to decode a payload for ingestor '{ingestor}'")]
    DecodePayload { ingestor: IngestorName },
}

#[derive(Debug)]
pub(super) struct IngestGroupFailure<T> {
    pub(super) error: Report<IngestGroupError>,
    pub(super) preserved: T,
}

impl<T> IngestGroupFailure<T> {
    fn new(error: Report<IngestGroupError>, preserved: T) -> Self {
        Self { error, preserved }
    }
}

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
    ) -> error_stack::Result<(), IngestGroupError> {
        let payload_count = metadata.len();
        if acks.len() != payload_count {
            return Err(Report::new(IngestGroupError::PayloadSidecarCount {
                metadata_rows: payload_count,
                ack_sets: acks.len(),
            }));
        }
        // Payloads are accepted in the order they decoded, and a source may accept them one at a
        // time, so a contribution may cover a prefix of what the group has decoded but never more.
        if payload_count > self.undispatched_payloads.len() {
            return Err(Report::new(IngestGroupError::DecodedPayloadCount {
                metadata_rows: payload_count,
                decoded_payloads: self.undispatched_payloads.len(),
            }));
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
                builders
                    .append(row)
                    .change_context(IngestGroupError::Metadata {
                        operation: IngestMetadataOperation::Append,
                    })?;
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

    pub(super) fn into_rows(self) -> error_stack::Result<IngestGroupRows, IngestGroupError> {
        let row_count = self.acks.len();
        if self.ingested_at.len() != row_count {
            return Err(Report::new(IngestGroupError::TimestampCount {
                ack_sets: row_count,
                ingest_timestamps: self.ingested_at.len(),
            }));
        }
        let metadata = self
            .metadata
            .ok_or_else(|| Report::new(IngestGroupError::MissingMetadataBuilders))?;
        let ingest_metadata = metadata
            .finish()
            .change_context(IngestGroupError::Metadata {
                operation: IngestMetadataOperation::Finish,
            })?;
        if ingest_metadata.len() != row_count {
            return Err(Report::new(IngestGroupError::MetadataRowCount {
                records: row_count,
                metadata_rows: ingest_metadata.len(),
            }));
        }
        let records = self
            .records
            .ok_or_else(|| Report::new(IngestGroupError::MissingRecordBuilder))?;
        let batch = records
            .finish()
            .change_context(IngestGroupError::RuntimeSchema {
                operation: IngestGroupRecordOperation::FinishDecodedBatch,
            })?;
        if batch.batch().num_rows() != row_count {
            return Err(Report::new(IngestGroupError::DecodedRowCount {
                records: row_count,
                decoded_rows: batch.batch().num_rows(),
            }));
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

    pub(super) fn row(&self, row: usize) -> error_stack::Result<RuntimeRow, IngestGroupError> {
        let metadata = self.record_metadata.get(row).cloned().ok_or_else(|| {
            Report::new(IngestGroupError::RecordMetadataRowOutOfBounds {
                row,
                record_metadata_rows: self.record_metadata.len(),
            })
        })?;
        RuntimeRow::new(self.batch.clone(), row, metadata).change_context(
            IngestGroupError::RuntimeSchema {
                operation: IngestGroupRecordOperation::AddressRow,
            },
        )
    }

    /// Keeps only the rows selected by `keep`, moving records, metadata and acks
    /// together so the three stay row-aligned.
    pub(super) fn select(self, keep: &[bool]) -> error_stack::Result<Self, IngestGroupError> {
        let row_count = self.len();
        if self.record_metadata.len() != row_count
            || self.ingest_metadata.len() != row_count
            || self.acks.len() != row_count
        {
            return Err(Report::new(IngestGroupError::RowCount {
                arrow_rows: row_count,
                record_metadata_rows: self.record_metadata.len(),
                ingest_metadata_rows: self.ingest_metadata.len(),
                ack_sets: self.acks.len(),
            }));
        }
        if keep.len() != row_count {
            return Err(Report::new(IngestGroupError::SelectionLengthMismatch {
                expected: row_count,
                found: keep.len(),
            }));
        }
        let selected = |row: usize| keep.get(row).copied().unwrap_or(false);
        let predicate = BooleanArray::from_iter((0..row_count).map(|row| Some(selected(row))));
        Ok(Self {
            batch: Arc::new(self.batch.filter(&predicate).change_context(
                IngestGroupError::RuntimeSchema {
                    operation: IngestGroupRecordOperation::FilterBatch,
                },
            )?),
            record_metadata: self
                .record_metadata
                .into_iter()
                .enumerate()
                .filter_map(|(row, metadata)| selected(row).then_some(metadata))
                .collect(),
            ingest_metadata: self.ingest_metadata.select(keep).change_context(
                IngestGroupError::Metadata {
                    operation: IngestMetadataOperation::Select,
                },
            )?,
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
    ) -> error_stack::Result<(), IngestGroupError> {
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
            return Err(Report::new(IngestGroupError::CollectorIdentity {
                existing_domain: existing.domain.clone(),
                existing_ingestor: existing.ingestor.clone(),
                received_domain: domain.clone(),
                received_ingestor: ingestor.clone(),
            }));
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
    ) -> error_stack::Result<Option<(IngestGroupContext, IngestGroupRows)>, IngestGroupError> {
        // A decoded payload the source never accepted would put the records out of step with the
        // metadata and ACKs, so the group says so rather than closing over the mismatch.
        let undispatched = self.pending.undispatched_payloads();
        if undispatched != 0 {
            self.pending.discard_undispatched_payloads();
            return Err(Report::new(IngestGroupError::UndispatchedPayloads {
                payloads: undispatched,
            }));
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
    ) -> Result<Self, IngestGroupFailure<Vec<AckSet>>> {
        if inputs.is_empty() {
            return Err(IngestGroupFailure::new(
                Report::new(IngestGroupError::EmptyBranchInputs),
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
        let batch = match RuntimeRecordBatch::concat(&batch_refs) {
            Ok(batch) => batch,
            Err(error) => {
                return Err(IngestGroupFailure::new(
                    error.change_context(IngestGroupError::RuntimeSchema {
                        operation: IngestGroupRecordOperation::ConcatenateBranchInputs,
                    }),
                    acks,
                ));
            }
        };
        let row_count = batch.batch().num_rows();
        if metadata.len() != row_count || keys.len() != row_count || acks.len() != row_count {
            return Err(IngestGroupFailure::new(
                Report::new(IngestGroupError::BranchInputRowCount {
                    arrow_rows: row_count,
                    metadata_rows: metadata.len(),
                    branch_keys: keys.len(),
                    ack_sets: acks.len(),
                }),
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

    pub(super) fn branch_selections(
        &self,
    ) -> error_stack::Result<Vec<BranchedBranchSelection>, IngestGroupError> {
        let row_count = self.batch.batch().num_rows();
        if self.metadata.len() != row_count
            || self.keys.len() != row_count
            || self.acks.len() != row_count
        {
            return Err(Report::new(IngestGroupError::BranchInputRowCount {
                arrow_rows: row_count,
                metadata_rows: self.metadata.len(),
                branch_keys: self.keys.len(),
                ack_sets: self.acks.len(),
            }));
        }
        let mut selections = Vec::<BranchedBranchSelection>::new();
        let mut positions = HashMap::<Option<BranchKey>, usize>::default();
        for index in 0..row_count {
            let key = self.keys[index].clone();
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
    ) -> Result<RelayRecordBatch, IngestGroupFailure<Vec<AckSet>>> {
        let predicate = match self.branch_predicate(&selection) {
            Ok(predicate) => predicate,
            Err(error) => {
                return Err(IngestGroupFailure::new(error, self.acks.clone()));
            }
        };
        let selected_rows = selected_rows(&predicate);
        let filtered_batch = match self.batch.filter(&predicate) {
            Ok(batch) => batch,
            Err(error) => {
                return Err(IngestGroupFailure::new(
                    error.change_context(IngestGroupError::RuntimeSchema {
                        operation: IngestGroupRecordOperation::FilterBranchInput,
                    }),
                    self.acks.clone(),
                ));
            }
        };
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
        match RelayRecordBatch::from_filtered_parts(selection.key, filtered_batch, metadata, acks) {
            Ok(batch) => Ok(batch),
            Err(error) => Err(IngestGroupFailure::new(
                error.change_context(IngestGroupError::FilteredRelayBatch),
                self.acks.clone(),
            )),
        }
    }

    pub(super) fn branch_predicate(
        &self,
        selection: &BranchedBranchSelection,
    ) -> error_stack::Result<BooleanArray, IngestGroupError> {
        let row_count = self.batch.batch().num_rows();
        let mut selected = vec![false; row_count];
        for row in &selection.rows {
            let Some(value) = selected.get_mut(*row) else {
                return Err(Report::new(
                    IngestGroupError::BranchSelectionRowOutOfBounds {
                        row: *row,
                        batch_rows: row_count,
                    },
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
) -> Result<Arc<BranchedEntrypointBatch>, IngestGroupFailure<Vec<AckSet>>> {
    let acks = branched_entrypoint_inputs_acks(&inputs);
    match tokio::task::spawn_blocking(move || BranchedEntrypointBatch::from_inputs(inputs)).await {
        Ok(Ok(batch)) => Ok(Arc::new(batch)),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(IngestGroupFailure::new(
            Report::new(error).change_context(IngestGroupError::BlockingTask {
                operation: IngestGroupBlockingOperation::BuildBranchInput,
            }),
            acks,
        )),
    }
}

pub(super) async fn branched_branch_plan_blocking(
    input: Arc<BranchedEntrypointBatch>,
) -> error_stack::Result<Vec<BranchedBranchSelection>, IngestGroupError> {
    input.branch_selections()
}

pub(super) async fn branched_branch_filter_blocking(
    input: Arc<BranchedEntrypointBatch>,
    selection: BranchedBranchSelection,
    ack_boundary: BranchInstanceAckBoundary,
) -> Result<(Option<BranchKey>, RelayRecordBatch), IngestGroupFailure<Vec<AckSet>>> {
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
        Err(error) => Err(IngestGroupFailure::new(
            Report::new(error).change_context(IngestGroupError::BlockingTask {
                operation: IngestGroupBlockingOperation::FilterBranchInput,
            }),
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
    ) -> error_stack::Result<(), IngestGroupError> {
        if collector.is_empty() {
            return Ok(());
        }
        let routing =
            collector
                .routing_snapshot(self, domain)
                .change_context(IngestGroupError::Routing {
                    domain: domain.clone(),
                })?;
        if let Some((context, rows)) = collector.take_pending()? {
            if context.domain != *domain || context.ingestor != *ingestor {
                return Err(Report::new(IngestGroupError::FlushIdentity {
                    group_domain: context.domain,
                    group_ingestor: context.ingestor,
                    requested_domain: domain.clone(),
                    requested_ingestor: ingestor.clone(),
                }));
            }
            self.execute_ingest_group(&routing, &context, rows, collector)
                .await?;
        }
        if collector.is_empty() {
            return Ok(());
        }
        let groups = collector.drain_groups();
        if routing.passive_only {
            let error = Report::new(IngestGroupError::DomainNotRunning {
                domain: domain.clone(),
            });
            let reason = error.to_string();
            for group in &groups {
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor,
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    group.messages.iter().map(|message| &message.acks),
                    reason.clone(),
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
                let error = Report::new(IngestGroupError::RelaySchemaMissing {
                    domain: domain.clone(),
                    relay: relay.clone(),
                });
                let reason = error.to_string();
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor,
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    acks.iter(),
                    reason,
                );
                first_error.get_or_insert(error);
                continue;
            };
            let batch = match RelayRecordBatch::from_messages(schema, messages) {
                Ok(batch) => batch,
                Err(error) => {
                    let error = error.change_context(IngestGroupError::RelayBatch {
                        relay: relay.clone(),
                    });
                    let reason = error.to_string();
                    self.handle_general_error_for_acks(
                        domain,
                        ModelKind::Ingestor,
                        ingestor,
                        &ErrorPolicies::handled_by_log(),
                        acks.iter(),
                        reason,
                    );
                    first_error.get_or_insert(error);
                    continue;
                }
            };
            let Some(sender) = branched_senders.get(&relay) else {
                let error = Report::new(IngestGroupError::BranchEntrypointMissing {
                    ingestor: ingestor.clone(),
                    relay: relay.clone(),
                });
                let reason = error.to_string();
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor,
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    batch.acks.iter(),
                    reason,
                );
                first_error.get_or_insert(error);
                continue;
            };
            if let Err(error) = sender.send(batch).await {
                let batch = error.0;
                let error = Report::new(IngestGroupError::BranchEntrypointClosed {
                    ingestor: ingestor.clone(),
                    relay: relay.clone(),
                });
                let reason = error.to_string();
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor,
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    batch.acks.iter(),
                    reason,
                );
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
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
    ) -> error_stack::Result<(), IngestGroupError> {
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
            .change_context(IngestGroupError::Collect {
                ingestor: ingestor.clone(),
            })
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
    ) -> error_stack::Result<(), IngestGroupError> {
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
        let ingestion_time = self.ingestion_time(domain, ingestor).change_context(
            IngestGroupError::IngestionTime {
                domain: domain.clone(),
                ingestor: ingestor.clone(),
            },
        )?;
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
                .change_context(IngestGroupError::MaterializedState {
                    ingestor: ingestor.clone(),
                })?;
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
            .await
            .change_context(IngestGroupError::FilterWhere {
                ingestor: ingestor.clone(),
            })?;
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
                rows.batch = Arc::new(RuntimeRecordBatch::concat(&batch_refs).change_context(
                    IngestGroupError::RuntimeSchema {
                        operation: IngestGroupRecordOperation::ConcatenateTransformedRows,
                    },
                )?);
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
                .change_context(IngestGroupError::IngestionTime {
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                })?;
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
                    .change_context(IngestGroupError::RouteMaterializedState {
                        ingestor: ingestor.clone(),
                        relay: output.relay.clone(),
                    })?;
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
                .await
                .change_context(IngestGroupError::RouteProgram {
                    ingestor: ingestor.clone(),
                    relay: output.relay.clone(),
                })?
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
                    let output_batch = RuntimeRecordBatch::concat(&output_batches).change_context(
                        IngestGroupError::RuntimeSchema {
                            operation: IngestGroupRecordOperation::ConcatenateBranchOutputs,
                        },
                    )?;
                    let input_batch = rows.batch.take(&input_rows).change_context(
                        IngestGroupError::RuntimeSchema {
                            operation: IngestGroupRecordOperation::SelectBranchInputs,
                        },
                    )?;
                    let input_keys = vec![None; input_rows.len()];
                    let side_inputs = self
                        .load_materialized_side_inputs(
                            routing,
                            domain,
                            &None,
                            &branch_program.program.materialized_interest,
                        )
                        .await
                        .change_context(IngestGroupError::RouteMaterializedState {
                            ingestor: ingestor.clone(),
                            relay: output.relay.clone(),
                        })?;
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
                    .await
                    .change_context(IngestGroupError::BranchProgram {
                        ingestor: ingestor.clone(),
                        relay: output.relay.clone(),
                    })?;
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
                        return Err(Report::new(IngestGroupError::MissingBranchProgram {
                            ingestor: ingestor.clone(),
                            relay: output.relay.clone(),
                        }));
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
                        let branch_result = route_keys[row].take().ok_or_else(|| {
                            Report::new(IngestGroupError::MissingBranchResult {
                                ingestor: ingestor.clone(),
                                relay: output.relay.clone(),
                                row,
                            })
                        })?;
                        match branch_result {
                            Ok(key) => routed[row].push(RoutedOutcome {
                                output_index,
                                outcome: SingleRecordFilterMapOutcome::Output(record),
                                key,
                            }),
                            Err(error) => routed[row].push(RoutedOutcome {
                                output_index,
                                outcome: SingleRecordFilterMapOutcome::MessageError {
                                    error: structured_message_error(
                                        execution_now,
                                        MessageErrorCode::Evaluation,
                                        error.to_string(),
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
                    Report::new(IngestGroupError::MissingBranchDeclaration {
                        ingestor: ingestor.clone(),
                        relay: output.relay.clone(),
                    })
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
        initialization: KafkaOffsetInitialization<'_>,
    ) -> error_stack::Result<(u64, bool), IngestGroupError> {
        let KafkaOffsetInitialization {
            topic,
            consumer,
            consumer_assignment,
            state,
            instance_idx,
        } = initialization;
        let (start_version, last_start) = if let Some(domain_state) = self.inner.domains.get(domain)
        {
            (domain_state.start_version, domain_state.last_start.clone())
        } else {
            return Err(Report::new(IngestGroupError::DomainNotInstalled {
                domain: domain.clone(),
            }));
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
                .change_context(IngestGroupError::KafkaOffsets {
                    operation: KafkaOffsetInitializationOperation::ReadDomainTime,
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                    topic: topic.to_string(),
                })?;
            KafkaIngestor::resume_offsets_from_state(
                consumer,
                topic,
                state.read(),
                missing_partition_timestamp,
            )
            .change_context(IngestGroupError::KafkaOffsets {
                operation: KafkaOffsetInitializationOperation::ResolveResumeOffsets,
                domain: domain.clone(),
                ingestor: ingestor.clone(),
                topic: topic.to_string(),
            })?
        } else {
            let timestamp = match &last_start {
                nervix_models::DomainStartPoint::Now { .. } => {
                    return Err(Report::new(IngestGroupError::UnresolvedStartNow {
                        domain: domain.clone(),
                    }));
                }
                nervix_models::DomainStartPoint::At { timestamp, .. } => *timestamp,
                nervix_models::DomainStartPoint::Resume => unreachable!("handled above"),
            };
            KafkaIngestor::offsets_by_timestamp(consumer, topic, timestamp).change_context(
                IngestGroupError::KafkaOffsets {
                    operation: KafkaOffsetInitializationOperation::ResolveTimestampOffsets,
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                    topic: topic.to_string(),
                },
            )?
        };
        let has_assignment = KafkaIngestor::assign_offsets_for_instance(
            consumer,
            topic,
            &offsets,
            scheduled_partition_schedule.as_ref(),
            instance_idx,
            consumer_assignment,
        )
        .change_context(IngestGroupError::KafkaOffsets {
            operation: KafkaOffsetInitializationOperation::AssignOffsets,
            domain: domain.clone(),
            ingestor: ingestor.clone(),
            topic: topic.to_string(),
        })?;

        if let nervix_models::DomainStartPoint::Resume = &last_start {
            return Ok((start_version, has_assignment));
        }

        let concrete_offsets =
            KafkaIngestor::concrete_next_offsets_from_assignment(consumer, topic, &offsets)
                .change_context(IngestGroupError::KafkaOffsets {
                    operation: KafkaOffsetInitializationOperation::ResolveConcreteOffsets,
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                    topic: topic.to_string(),
                })?;
        self.reset_domain_kafka_offsets(state, concrete_offsets)
            .await
            .change_context(IngestGroupError::KafkaOffsets {
                operation: KafkaOffsetInitializationOperation::ResetOffsets,
                domain: domain.clone(),
                ingestor: ingestor.clone(),
                topic: topic.to_string(),
            })?;
        Ok((start_version, has_assignment))
    }

    pub(in crate::runtime) async fn dispatch_raw_ingest_payload(
        &self,
        dispatch: RawIngestDispatch<'_>,
    ) -> error_stack::Result<(), IngestGroupError> {
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
                return Err(
                    Report::new(error).change_context(IngestGroupError::DecodePayload {
                        ingestor: ingestor.clone(),
                    }),
                );
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
        .await?;
        if flush {
            self.flush_ingest_collector(domain, ingestor, branched_senders, collector)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "ingest_group_tests.rs"]
mod tests;
