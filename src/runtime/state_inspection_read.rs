//! Bounded selection from an immutable materialized relay generation.
//!
//! Layer: data plane.
//! - **Owns.** Capturing native row views, complete selected counts and owner-side page selection.
//! - **Depends on.** Typed placements, immutable Arrow generations and bounded execution.
//! - **Must not know.** Models, Plexus, sessions, Row frames or presentation formats.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        observer,
        reason = "operator inspections capture installed row views on explicit request"
    )
)]

use error_stack::{Report, ResultExt as _};
use nervix_execution::{AdmissionError, CpuClass, Executor, MemoryClass, Reservation};
use nervix_models::{
    BranchKeyFingerprint, DomainName, ModelKind, RelayName, StateInspectionBranchSelection,
    StateInspectionCounts, StateInspectionStage, Unit,
};

use super::{
    MaterializedGeneration, MaterializedGenerationRecord, RestoredMaterializedSnapshot, Runtime,
    RuntimeStateKind, materialized_snapshot::ResidentEncodingPlan,
};
use crate::runtime_schema::batch_payload_bytes;

#[cfg(test)]
#[path = "state_inspection_read_publication_tests.rs"]
mod publication_tests;

#[cfg(test)]
#[path = "state_inspection_read_cost_tests.rs"]
mod cost_tests;

#[derive(Debug, thiserror::Error)]
pub(crate) enum StatePageReadError {
    #[error("the materialized state assignment is not ready")]
    NotReady,
    #[error("the state page could not obtain bounded execution or memory")]
    Admission,
    #[error("the stored generation could not be read")]
    Stored,
    #[error("the continuation position does not belong to this generation")]
    Position,
    #[error("one selected row exceeds the page byte bound")]
    RowTooLarge,
    #[error("the materialized key census changed during inspection")]
    CatalogChanged,
    #[error("capturing the unit's state takes more memory than this node could ever hold")]
    CaptureTooLarge,
}

/// Effective owner-side bounds after the coordinator and local capability checks.
pub(crate) struct MaterializedPageBounds {
    pub(crate) rows: usize,
    pub(crate) bytes: u64,
}

/// One owner's bounded row selection and the optional inherited-key census it captures with it.
pub(crate) struct MaterializedPageQuery {
    pub(crate) unit: Unit,
    pub(crate) selection: StateInspectionBranchSelection,
    pub(crate) after: Option<(Option<BranchKeyFingerprint>, u64)>,
    pub(crate) bounds: MaterializedPageBounds,
    pub(crate) catalog_requested: bool,
    pub(crate) filter: Option<nervix_interconnect::MaterializedPartitionFilter>,
    /// What selecting the page's rows also sets aside for the reply that carries them.
    pub(crate) reply: MaterializedPageReply,
}

/// How a materialized page reaches its reader, which decides what selecting its rows also sets
/// aside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaterializedPageReply {
    /// This node presents the page itself and writes its rows as Row frames: nothing is set aside.
    Local,
    /// Another node reads the page, which travels as a descriptor and a native snapshot container.
    /// The page sets aside `descriptor` ahead, for encoding its descriptor and framing its stream,
    /// and every row it selects sets aside what encoding that row into the container adds.
    Remote { descriptor: u64 },
}

impl MaterializedPageReply {
    /// The bulk share a page of at most `max_rows` rows and `max_bytes` bytes starts its capture
    /// from. Every charge a page this node presents, or one that returns no row, takes is
    /// required, so it starts from the least share and a capture whose share has no room for one
    /// runs again within the share it needed. A page another node reads starts from the room the
    /// class has free now ([`Executor::memory`]), up to the most its reply takes by its own
    /// charges: its container's encoding of `max_bytes` of columns and the descriptor it sets
    /// aside ahead. Its rows after the first are optional, so a page whose share has no room for
    /// one ends before it.
    pub(crate) fn initial_share(self, executor: &Executor, max_rows: u64, max_bytes: u64) -> u64 {
        let Self::Remote { descriptor } = self else {
            return 1;
        };
        if max_rows == 0 {
            return 1;
        }
        let free = executor.memory(MemoryClass::Bulk).available_bytes();
        // A bound past what the plan can count is past the class, whose free room alone then
        // bounds the share.
        let container = match ResidentEncodingPlan::bound(executor, max_bytes) {
            Ok(container) => container,
            Err(_past_the_class) => return free.max(1),
        };
        let Some(most) = container.checked_add(descriptor) else {
            return free.max(1);
        };
        most.min(free).max(1)
    }
}

/// What a page another node reads set aside for its reply.
pub(crate) struct MaterializedReplyMemory {
    /// Encodes the descriptor and frames the stream.
    pub(crate) descriptor: Reservation,
    /// Encodes the container of the selected rows: what [`ResidentEncodingPlan`] names for them.
    pub(crate) container: Reservation,
}

pub(crate) struct MaterializedInspectionPage {
    pub(crate) fingerprint: [u8; 32],
    pub(crate) counts: StateInspectionCounts,
    pub(crate) stage: StateInspectionStage,
    pub(crate) generation: MaterializedGeneration,
    pub(crate) offset: u64,
    pub(crate) last_branch: Option<BranchKeyFingerprint>,
    pub(crate) more: bool,
    /// What the page set aside for the reply another node reads it through; absent for a page this
    /// node presents.
    pub(crate) reply: Option<MaterializedReplyMemory>,
    _metadata: Reservation,
    // Stored columns retain their decode admission while the page shares their Arrow arrays.
    _stored: Option<RestoredMaterializedSnapshot>,
}

pub(crate) struct MaterializedStateCapture {
    pub(crate) page: MaterializedInspectionPage,
    pub(crate) catalog: nervix_models::StateInspectionKeyCatalog,
    // Keeps the catalog admitted for the lifetime of this capture.
    _catalog_charge: Option<Reservation>,
}

impl Runtime {
    pub(crate) fn inspection_executor(&self) -> &nervix_execution::Executor {
        &self.inner.executor
    }

    pub(crate) fn state_inspection_settings(&self) -> super::StateInspectionSettings {
        self.inner.inspection_settings
    }

    /// Captures one unit's materialized records and selects a bounded page of them, taking every
    /// charge through `executor`.
    ///
    /// The caller runs the capture within allowances of the state-read and bulk classes
    /// (`Executor::within_allowance`): every charge the capture requires draws from a share the
    /// caller waited for while it held nothing, and a capture whose share has no room for one runs
    /// again within the share it needed. The capture first counts the unit's records, then takes
    /// everything capturing and selecting them requires: the views it copies, the identities of
    /// their states, the largest state's own capture, the selected page's views, the requested key
    /// catalog and, for a page another node reads, the reply's descriptor. Records added after
    /// they were counted grow those charges, as required. A capture the whole bulk class could
    /// never hold is refused at once. A page another node reads also sets aside, for every row it
    /// selects, what encoding that row into its container adds: its first row is required, and
    /// every later row is optional, taken only while the share has room for it now, so the page
    /// ends early, with its continuation and complete counts, where the share's room ends.
    pub(crate) async fn capture_materialized_state_page(
        &self,
        executor: &Executor,
        domain: &DomainName,
        relay: &RelayName,
        query: MaterializedPageQuery,
    ) -> Result<MaterializedStateCapture, Report<StatePageReadError>> {
        let placement = self
            .state_placement_for_unit(
                domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                query.unit,
                None,
            )
            .change_context(StatePageReadError::NotReady)?;
        let routing = self
            .domain_routing(domain)
            .ok_or_else(|| Report::new(StatePageReadError::NotReady))?
            .load_full();
        let schema = routing
            .materialized_stream_specs
            .get(relay)
            .ok_or_else(|| Report::new(StatePageReadError::NotReady))?
            .schema
            .clone();
        let executor = executor.clone();
        let census = UnitCensus::count(&executor, &routing, relay, query.unit).await?;
        let memory = CaptureMemory::admit(&executor, &census, &query).await?;
        let CaptureMemory {
            views,
            sources: source_charge,
            state: state_charge,
            selection: selection_memory,
        } = memory;
        let captured_routing = routing.clone();
        let captured_relay = relay.clone();
        let captured_schema = schema.clone();
        let unit = query.unit;
        let captured = executor
            .run_cpu(CpuClass::Bulk, views, move |mut charge, cancellation| {
                let reads = captured_routing
                    .materialized_stream_reads
                    .get(&captured_relay)
                    .ok_or_else(|| Report::new(StatePageReadError::NotReady))?;
                let mut records = Vec::with_capacity(census.records);
                #[cfg_attr(nervix_lint, expect(nervix::branch_inversion, reason = "materialized or emitter state groups its data by branch; move it to the branch in https://app.clickup.com/t/86bd1z3bd"))]
                let mut sources = Vec::with_capacity(census.states);
                let mut source_charge = source_charge;
                let mut state_charge = Some(state_charge);
                reads.visit_unit_states(unit, |branch, state| {
                    cancellation
                        .check()
                        .change_context(StatePageReadError::Admission)?;
                    let admitted = state_charge
                        .take()
                        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
                    // Keep the generation and its admission in one owner: tuple fields drop in
                    // order, so row-view metadata is freed before its charge is released.
                    let captured = state
                        .capture_for_inspection(admitted)
                        .change_context(StatePageReadError::Admission)?;
                    let generation = &captured.0;
                    reserve_views(&mut records, generation.records().len(), &mut charge)?;
                    records.extend_from_slice(generation.records());
                    reserve_views(&mut sources, 1, &mut source_charge)?;
                    sources.push((
                        branch.map(|branch| branch.fingerprint()),
                        state.inspection_instance(),
                        generation.revision(),
                        generation.fence(),
                        generation.branch_generation(),
                    ));
                    let (generation, admitted) = captured;
                    drop(generation);
                    state_charge = Some(admitted);
                    Ok::<_, Report<StatePageReadError>>(())
                })?;
                if sources.is_empty() {
                    return Ok(None);
                }
                records.sort_unstable_by(|a, b| {
                    super::BranchKey::canonical_order(&a.branch, &b.branch)
                });
                sources.sort_unstable();
                let mut digest = nervix_crypto::digest::Sha256::new();
                digest.update(b"nervix/state-inspection/unit-generations");
                let mut revision = 0;
                let mut fence = 0;
                let mut generation = 0;
                for (branch, instance, rev, source_fence, membership) in sources {
                    match branch {
                        Some(branch) => {
                            digest.update(&[1]);
                            digest.update(branch.fingerprint());
                        }
                        None => digest.update(&[0]),
                    }
                    digest.update(&instance.to_be_bytes());
                    digest.update(&rev.to_be_bytes());
                    digest.update(&source_fence.to_be_bytes());
                    digest.update(&membership.to_be_bytes());
                    revision = revision.max(rev);
                    fence = fence.max(source_fence);
                    generation = generation.max(membership);
                }
                Ok::<_, Report<StatePageReadError>>(Some(PageSource {
                    generation: MaterializedGeneration::new(
                        revision,
                        fence,
                        generation,
                        captured_schema,
                        records,
                    ),
                    views: charge,
                    stored: StoredSource::Live,
                    sources: digest.finish(),
                }))
            })
            .await
            .change_context(StatePageReadError::Admission)??;
        let source = match captured {
            Some(captured) => captured,
            None => {
                self.stored_page_source(&executor, &placement, &schema)
                    .await?
            }
        };
        let selection = PageSelection {
            runtime: self.clone(),
            routing,
            placement,
            schema,
            query,
            memory: selection_memory,
        };
        executor
            .run_cpu(CpuClass::Bulk, source.views, move |views, cancellation| {
                selection.select(
                    source.generation,
                    source.stored,
                    source.sources,
                    views,
                    cancellation,
                )
            })
            .await
            .change_context(StatePageReadError::Admission)?
    }

    /// The records this node last persisted for `placement`, for a unit with no installed state
    /// here.
    async fn stored_page_source(
        &self,
        executor: &Executor,
        placement: &super::RuntimeStatePlacement,
        schema: &nervix_primitives::sync::StdArc<arrow_schema::Schema>,
    ) -> Result<PageSource, Report<StatePageReadError>> {
        let stored = self
            .open_stored_materialized_snapshot(executor, None, placement)
            .await
            .change_context(StatePageReadError::Stored)?;
        let count = match &stored {
            Some(stored) => stored.records.len(),
            None => 0,
        };
        let bytes = count
            .checked_mul(std::mem::size_of::<MaterializedGenerationRecord>())
            .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
        let views = executor
            .reserve(
                MemoryClass::Bulk,
                u64::try_from(bytes)
                    .map_err(|_| Report::new(StatePageReadError::Admission))?
                    .max(1),
            )
            .await
            .change_context(StatePageReadError::Admission)?;
        let generation = match &stored {
            Some(stored) => MaterializedGeneration::new(
                stored.revision,
                stored.fence,
                stored.branch_generation,
                schema.clone(),
                stored
                    .records
                    .iter()
                    .map(|record| MaterializedGenerationRecord {
                        branch: record.branch.clone(),
                        row: record.row.clone(),
                    })
                    .collect(),
            ),
            None => MaterializedGeneration::new(0, 0, 0, schema.clone(), Vec::new()),
        };
        Ok(PageSource {
            generation,
            views,
            stored: StoredSource::Stored(stored),
            sources: [0; 32],
        })
    }
}

/// What one unit's installed states hold, counted before any of them is captured.
#[derive(Debug, Clone, Copy)]
struct UnitCensus {
    /// The unit's states.
    states: usize,
    /// The records they hold together.
    records: usize,
    /// The most records any one of them holds, which capturing that state copies at once.
    largest: usize,
}

impl UnitCensus {
    /// Counts `unit`'s installed states of `relay` and their records, holding nothing but the job.
    async fn count(
        executor: &Executor,
        routing: &nervix_primitives::sync::StdArc<super::DomainRoutingSnapshot>,
        relay: &RelayName,
        unit: Unit,
    ) -> Result<Self, Report<StatePageReadError>> {
        let routing = routing.clone();
        let relay = relay.clone();
        // The census holds nothing else, so its job waits for the room it takes.
        let job = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(StatePageReadError::Admission)?;
        executor
            .run_cpu(CpuClass::Bulk, job, move |_job, cancellation| {
                let reads = routing
                    .materialized_stream_reads
                    .get(&relay)
                    .ok_or_else(|| Report::new(StatePageReadError::NotReady))?;
                let mut census = Self {
                    states: 0,
                    records: 0,
                    largest: 0,
                };
                reads.visit_unit_states(unit, |_branch, state| {
                    cancellation
                        .check()
                        .change_context(StatePageReadError::Admission)?;
                    let records = state.inspection_records();
                    census.states = census
                        .states
                        .checked_add(1)
                        .ok_or_else(|| Report::new(StatePageReadError::CaptureTooLarge))?;
                    census.records = census
                        .records
                        .checked_add(records)
                        .ok_or_else(|| Report::new(StatePageReadError::CaptureTooLarge))?;
                    census.largest = census.largest.max(records);
                    Ok::<_, Report<StatePageReadError>>(())
                })?;
                Ok(census)
            })
            .await
            .change_context(StatePageReadError::Admission)?
    }
}

/// The bytes `count` values of `T` take.
fn bytes_of<T>(count: usize) -> Result<u64, Report<StatePageReadError>> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Report::new(StatePageReadError::CaptureTooLarge))?;
    u64::try_from(bytes).map_err(|_| Report::new(StatePageReadError::CaptureTooLarge))
}

/// What selecting a page of `records` captured records takes: the selected views, at most
/// `rows` of them, and, when it is requested, the key catalog of every record.
struct SelectionNeed {
    selected: u64,
    catalog: u64,
}

impl SelectionNeed {
    /// The need for a page of at most `rows` rows within `max_bytes`. A requested catalog that
    /// alone passes the page's byte bound makes the page too large.
    fn of(
        records: usize,
        rows: usize,
        max_bytes: u64,
        catalog_requested: bool,
    ) -> Result<Self, Report<StatePageReadError>> {
        let too_large = || Report::new(StatePageReadError::CaptureTooLarge);
        let selected = rows
            .min(records)
            .checked_mul(
                std::mem::size_of::<MaterializedGenerationRecord>()
                    + std::mem::size_of::<std::ops::Range<usize>>(),
            )
            .ok_or_else(too_large)?;
        let selected = u64::try_from(selected).map_err(|_| too_large())?.max(1);
        let catalog = if catalog_requested {
            let bytes = records
                .checked_mul(
                    64 * (std::mem::size_of::<BranchKeyFingerprint>()
                        + std::mem::size_of::<Option<nervix_models::InheritedPartitionOrigin>>()),
                )
                .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
            let bytes =
                u64::try_from(bytes).map_err(|_| Report::new(StatePageReadError::Admission))?;
            if bytes > max_bytes {
                return Err(Report::new(StatePageReadError::RowTooLarge));
            }
            bytes.max(1)
        } else {
            0
        };
        Ok(Self { selected, catalog })
    }
}

/// The memory a capture admits ahead, split by what spends it.
struct CaptureMemory {
    /// The views the capture copies.
    views: Reservation,
    /// The identities of the captured states.
    sources: Reservation,
    /// One state's own capture at a time.
    state: Reservation,
    /// What selecting the page takes.
    selection: SelectionMemory,
}

/// The memory selecting a page draws on.
struct SelectionMemory {
    selected: Reservation,
    catalog: Reservation,
    /// For a page another node reads, its descriptor's share.
    descriptor: Option<Reservation>,
}

impl CaptureMemory {
    /// Admits everything capturing the counted unit and selecting its page takes, holding nothing
    /// while it waits for room. A capture the whole class could never hold is refused at once.
    async fn admit(
        executor: &Executor,
        census: &UnitCensus,
        query: &MaterializedPageQuery,
    ) -> Result<Self, Report<StatePageReadError>> {
        let too_large = || Report::new(StatePageReadError::CaptureTooLarge);
        let views = bytes_of::<MaterializedGenerationRecord>(census.records)?;
        // Each captured state's branch, instance, revision, fence and branch generation.
        let sources =
            bytes_of::<(Option<BranchKeyFingerprint>, u64, u64, u64, u64)>(census.states)?;
        let state = bytes_of::<MaterializedGenerationRecord>(census.largest)?;
        let selection = SelectionNeed::of(
            census.records,
            query.bounds.rows,
            query.bounds.bytes,
            query.catalog_requested,
        )?;
        let descriptor = match query.reply {
            MaterializedPageReply::Local => 0,
            MaterializedPageReply::Remote { descriptor } => descriptor,
        };
        let mut total = views;
        for part in [
            sources,
            state,
            selection.selected,
            selection.catalog,
            descriptor,
        ] {
            total = total.checked_add(part).ok_or_else(too_large)?;
        }
        let admitted = match executor.reserve(MemoryClass::Bulk, total.max(1)).await {
            Ok(admitted) => admitted,
            Err(refusal) => {
                if let AdmissionError::ExceedsBudget { .. } = refusal.current_context() {
                    return Err(refusal.change_context(StatePageReadError::CaptureTooLarge));
                }
                return Err(refusal.change_context(StatePageReadError::Admission));
            }
        };
        let split = |charge: Reservation, bytes: u64| {
            charge
                .split(bytes)
                .change_context(StatePageReadError::Admission)
        };
        let (views, rest) = split(admitted, views)?;
        let (sources, rest) = split(rest, sources)?;
        let (state, rest) = split(rest, state)?;
        let (selected, rest) = split(rest, selection.selected)?;
        let (catalog, rest) = split(rest, selection.catalog)?;
        let descriptor = match query.reply {
            MaterializedPageReply::Local => None,
            MaterializedPageReply::Remote { .. } => Some(rest),
        };
        Ok(Self {
            views,
            sources,
            state,
            selection: SelectionMemory {
                selected,
                catalog,
                descriptor,
            },
        })
    }
}

/// Where a page's records come from.
enum StoredSource {
    /// The installed states, captured live.
    Live,
    /// What this node last persisted, kept open while the page shares its columns.
    Stored(Option<RestoredMaterializedSnapshot>),
}

/// The records a page selects from, with the memory their views hold.
struct PageSource {
    generation: MaterializedGeneration,
    views: Reservation,
    stored: StoredSource,
    /// The digest of the captured states' identities.
    sources: [u8; 32],
}

/// One page's selection from its captured records.
struct PageSelection {
    runtime: Runtime,
    routing: nervix_primitives::sync::StdArc<super::DomainRoutingSnapshot>,
    placement: super::RuntimeStatePlacement,
    schema: nervix_primitives::sync::StdArc<arrow_schema::Schema>,
    query: MaterializedPageQuery,
    memory: SelectionMemory,
}

/// What a page another node reads has set aside so far for its container, and the plan of what
/// encoding its selected rows takes.
struct ReplySetAside {
    plan: ResidentEncodingPlan,
    container: Reservation,
}

impl ReplySetAside {
    /// Sets aside what encoding `record` into the container adds. The page's `first` row is
    /// required: its part grows the container's charge, and a share without room for it runs the
    /// capture again within a larger one. Every later row is optional: it joins only while the
    /// share has room for its part now, and `Ok(false)` ends the page before it. A record whose
    /// columns no container section can hold is too large.
    fn admit(
        &mut self,
        record: &MaterializedGenerationRecord,
        first: bool,
    ) -> Result<bool, Report<StatePageReadError>> {
        let next = self
            .plan
            .with(record)
            .change_context(StatePageReadError::RowTooLarge)?;
        let need = next
            .need()
            .change_context(StatePageReadError::RowTooLarge)?;
        if first {
            self.container
                .grow_to(need)
                .change_context(StatePageReadError::Admission)?;
        } else if self.container.try_grow_to(need).is_err() {
            return Ok(false);
        }
        self.plan = next;
        Ok(true)
    }
}

impl PageSelection {
    /// Selects the page from `generation`, the unit's captured records in canonical order, whose
    /// views `views` holds until the selection ends.
    fn select(
        self,
        generation: MaterializedGeneration,
        stored: StoredSource,
        sources: [u8; 32],
        views: Reservation,
        cancellation: &nervix_execution::Cancellation,
    ) -> Result<MaterializedStateCapture, Report<StatePageReadError>> {
        let PageSelection {
            runtime,
            routing,
            placement,
            schema,
            query,
            memory,
        } = self;
        let SelectionMemory {
            selected: mut metadata,
            catalog: mut catalog_charge,
            descriptor,
        } = memory;
        // A unit that grew since it was counted, or a stored unit, selects from more records than
        // were admitted: the rest of its selection takes its room from the pool at once.
        let need = SelectionNeed::of(
            generation.records().len(),
            query.bounds.rows,
            query.bounds.bytes,
            query.catalog_requested,
        )?;
        metadata
            .grow_to(need.selected)
            .change_context(StatePageReadError::Admission)?;
        catalog_charge
            .grow_to(need.catalog)
            .change_context(StatePageReadError::Admission)?;
        let (descriptor, reply) = match descriptor {
            Some(descriptor) => {
                let (container, descriptor) = descriptor
                    .split(0)
                    .change_context(StatePageReadError::Admission)?;
                let reply = ReplySetAside {
                    plan: ResidentEncodingPlan::new(runtime.inspection_executor(), &schema),
                    container,
                };
                (Some(descriptor), Some(reply))
            }
            None => (None, None),
        };
        let mut walk = PageWalk::new(
            query,
            &placement,
            sources,
            generation.records().len(),
            reply,
        );
        for record in generation.records() {
            cancellation
                .check()
                .change_context(StatePageReadError::Admission)?;
            if !runtime.materialized_stream_key_is_visible(
                Some(&routing),
                &placement,
                &record.branch,
            ) {
                continue;
            }
            walk.visit(record)?;
        }
        drop(views);
        walk.finish(PageEnd {
            generation,
            schema,
            descriptor,
            stored,
            metadata,
            catalog_charge,
        })
    }
}

/// What a finished walk turns into its page beside what it selected.
struct PageEnd {
    generation: MaterializedGeneration,
    schema: nervix_primitives::sync::StdArc<arrow_schema::Schema>,
    descriptor: Option<Reservation>,
    stored: StoredSource,
    metadata: Reservation,
    catalog_charge: Reservation,
}

/// One page's walk over its unit's records in canonical order: what it counts, fingerprints and
/// selects.
struct PageWalk {
    selection: StateInspectionBranchSelection,
    after: Option<(Option<BranchKeyFingerprint>, u64)>,
    filter: Option<nervix_interconnect::MaterializedPartitionFilter>,
    max_rows: usize,
    max_bytes: u64,
    /// The offset the page starts at.
    start: u64,
    catalog_requested: bool,
    catalog: nervix_models::StateInspectionKeyCatalog,
    fingerprint: nervix_crypto::digest::Sha256,
    counts: StateInspectionCounts,
    selected: Vec<MaterializedGenerationRecord>,
    selected_bytes: u64,
    previous_branch: Option<BranchKeyFingerprint>,
    /// The offset after the last selected record.
    next: u64,
    /// Whether the page took its last record.
    full: bool,
    /// What a page another node reads set aside for its container so far.
    reply: Option<ReplySetAside>,
}

impl PageWalk {
    fn new(
        query: MaterializedPageQuery,
        placement: &super::RuntimeStatePlacement,
        sources: [u8; 32],
        records: usize,
        reply: Option<ReplySetAside>,
    ) -> Self {
        let MaterializedPageQuery {
            selection,
            after,
            bounds,
            catalog_requested,
            filter,
            ..
        } = query;
        let catalog = if catalog_requested {
            nervix_models::StateInspectionKeyCatalog::requested()
        } else {
            nervix_models::StateInspectionKeyCatalog::Unrequested
        };
        let mut fingerprint = nervix_crypto::digest::Sha256::new();
        fingerprint.update(b"nervix/state-inspection/materialized-metadata");
        fingerprint.update(&sources);
        if let super::RuntimeState::MaterializedRelay { schema } = &placement.state {
            fingerprint.update(schema.as_digest());
        }
        let start = match after {
            Some((_, offset)) => offset,
            None => 0,
        };
        Self {
            selection,
            after,
            filter,
            max_rows: bounds.rows,
            max_bytes: bounds.bytes,
            start,
            catalog_requested,
            catalog,
            fingerprint,
            counts: StateInspectionCounts::default(),
            selected: Vec::with_capacity(bounds.rows.min(records)),
            selected_bytes: 0,
            previous_branch: None,
            next: start,
            full: false,
            reply,
        }
    }

    /// Visits one record of the unit that this unit's placement sees, in canonical order.
    fn visit(
        &mut self,
        record: &MaterializedGenerationRecord,
    ) -> Result<(), Report<StatePageReadError>> {
        let branch = record.branch.as_ref().map(|branch| branch.fingerprint());
        if !self.selects(branch) {
            return Ok(());
        }
        if self.catalog_requested
            && let Some(key) = &record.branch
        {
            self.catalog
                .insert(key.fingerprint(), key.inherited_origin());
        }
        if self.excludes(branch) {
            return Ok(());
        }
        self.fingerprint_record(record, branch);
        let bytes =
            batch_payload_bytes(&record.row.batch().batch().slice(record.row.row_index(), 1));
        let index = self.count(branch, bytes)?;
        if index < self.start {
            self.previous_branch = branch;
            return Ok(());
        }
        if index == self.start
            && self
                .after
                .is_some_and(|(expected, _)| expected != self.previous_branch)
        {
            return Err(Report::new(StatePageReadError::Position));
        }
        self.offer(record, branch, bytes)
    }

    /// Whether the read's branch selection covers a record of `branch`.
    fn selects(&self, branch: Option<BranchKeyFingerprint>) -> bool {
        match self.selection {
            StateInspectionBranchSelection::All => true,
            StateInspectionBranchSelection::Unbranched => branch.is_none(),
            StateInspectionBranchSelection::Branch(wanted) => branch == Some(wanted),
        }
    }

    /// Whether a partition filter excludes a record of `branch` from the page.
    fn excludes(&self, branch: Option<BranchKeyFingerprint>) -> bool {
        let Some(filter) = &self.filter else {
            return false;
        };
        let Some(branch) = branch else {
            return false;
        };
        filter.excluded.contains(&branch)
    }

    /// Folds `record`'s branch and watermarks into the page's fingerprint.
    fn fingerprint_record(
        &mut self,
        record: &MaterializedGenerationRecord,
        branch: Option<BranchKeyFingerprint>,
    ) {
        match branch {
            Some(branch) => {
                self.fingerprint.update(&[1]);
                self.fingerprint.update(branch.fingerprint());
            }
            None => self.fingerprint.update(&[0]),
        }
        let metadata = record.row.metadata();
        self.fingerprint.update(
            &metadata
                .ingested_at_low_watermark()
                .unix_nanos()
                .to_be_bytes(),
        );
        self.fingerprint.update(
            &metadata
                .ingested_at_high_watermark()
                .unix_nanos()
                .to_be_bytes(),
        );
    }

    /// Counts a record of `branch` taking `bytes`, and its index among the counted records.
    fn count(
        &mut self,
        branch: Option<BranchKeyFingerprint>,
        bytes: u64,
    ) -> Result<u64, Report<StatePageReadError>> {
        let index = self.counts.rows;
        self.counts.rows = self
            .counts
            .rows
            .checked_add(1)
            .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
        self.counts.branches += u64::from(branch.is_some());
        self.counts.bytes = self
            .counts
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| Report::new(StatePageReadError::RowTooLarge))?;
        Ok(index)
    }

    /// Returns `record` in the page when the page has room for it: within its row and byte bounds
    /// and, for a page another node reads, with its share of the reply set aside.
    fn offer(
        &mut self,
        record: &MaterializedGenerationRecord,
        branch: Option<BranchKeyFingerprint>,
        bytes: u64,
    ) -> Result<(), Report<StatePageReadError>> {
        if self.full || self.selected.len() == self.max_rows {
            self.full = true;
            return Ok(());
        }
        let Some(total) = self.selected_bytes.checked_add(bytes) else {
            return Err(Report::new(StatePageReadError::RowTooLarge));
        };
        if total > self.max_bytes {
            self.full = true;
            return Ok(());
        }
        if let Some(reply) = &mut self.reply
            && !reply.admit(record, self.selected.is_empty())?
        {
            // The share has no room for the row's part of the reply now: the page ends before it.
            self.full = true;
            return Ok(());
        }
        self.selected.push(record.clone());
        self.selected_bytes = total;
        self.previous_branch = branch;
        self.next += 1;
        Ok(())
    }

    /// The page the walk selected, once its position and catalog check out.
    fn finish(self, end: PageEnd) -> Result<MaterializedStateCapture, Report<StatePageReadError>> {
        let PageWalk {
            after,
            filter,
            start,
            catalog_requested,
            catalog,
            fingerprint,
            counts,
            selected,
            previous_branch,
            next,
            reply,
            ..
        } = self;
        let PageEnd {
            generation,
            schema,
            descriptor,
            stored,
            metadata,
            catalog_charge,
        } = end;
        if start > counts.rows
            || (start == counts.rows
                && after.is_some_and(|(expected, _)| expected != previous_branch))
        {
            return Err(Report::new(StatePageReadError::Position));
        }
        let selected = MaterializedGeneration::new(
            generation.revision(),
            generation.fence(),
            generation.branch_generation(),
            schema,
            selected,
        );
        if filter
            .as_ref()
            .is_some_and(|filter| filter.catalog != catalog)
        {
            return Err(Report::new(StatePageReadError::CatalogChanged));
        }
        let reply = match (descriptor, reply) {
            (Some(descriptor), Some(reply)) => Some(MaterializedReplyMemory {
                descriptor,
                container: reply.container,
            }),
            _ => None,
        };
        let (stage, stored) = match stored {
            StoredSource::Live => (StateInspectionStage::Live, None),
            StoredSource::Stored(stored) => (StateInspectionStage::Stored, stored),
        };
        Ok(MaterializedStateCapture {
            page: MaterializedInspectionPage {
                fingerprint: fingerprint.finish(),
                counts,
                generation: selected,
                offset: next,
                last_branch: previous_branch,
                more: next < counts.rows,
                stage,
                reply,
                _metadata: metadata,
                _stored: stored,
            },
            catalog,
            _catalog_charge: catalog_requested.then_some(catalog_charge),
        })
    }
}

/// Room for `additional` more of `values` in the memory `charge` admitted for them. The census
/// admitted every counted view, so this grows only for records added since, geometrically, as a
/// required charge: a share without room for it runs the capture again within a larger one.
fn reserve_views<T>(
    values: &mut Vec<T>,
    additional: usize,
    charge: &mut Reservation,
) -> Result<(), Report<StatePageReadError>> {
    let length = values
        .len()
        .checked_add(additional)
        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
    if length <= values.capacity() {
        return Ok(());
    }
    let capacity = length
        .checked_next_power_of_two()
        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
    let bytes = u64::try_from(bytes).map_err(|_| Report::new(StatePageReadError::Admission))?;
    charge
        .grow_to(bytes.max(1))
        .change_context(StatePageReadError::Admission)?;
    values.reserve_exact(capacity - values.len());
    Ok(())
}
