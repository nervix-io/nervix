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
use meticulous::OptionExt as _;
use nervix_execution::{CpuClass, GrowingCharge, MemoryClass, Reservation};
use nervix_models::{
    BranchKeyFingerprint, DomainName, ModelKind, RelayName, StateInspectionBranchSelection,
    StateInspectionCounts, StateInspectionStage, Unit,
};
use nervix_primitives::sync::StdArc;

use super::{
    MaterializedGeneration, MaterializedGenerationRecord, RestoredMaterializedSnapshot, Runtime,
    RuntimeStateKind,
};
use crate::runtime_schema::batch_payload_bytes;

#[cfg(test)]
#[path = "state_inspection_read_publication_tests.rs"]
mod publication_tests;

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
}

pub(crate) struct MaterializedInspectionPage {
    pub(crate) fingerprint: [u8; 32],
    pub(crate) counts: StateInspectionCounts,
    pub(crate) stage: StateInspectionStage,
    pub(crate) generation: MaterializedGeneration,
    pub(crate) offset: u64,
    pub(crate) last_branch: Option<BranchKeyFingerprint>,
    pub(crate) more: bool,
    _metadata: Reservation,
    // Stored columns retain their decode admission while the page shares their Arrow arrays.
    _stored: Option<RestoredMaterializedSnapshot>,
}

/// One captured page and the key catalog captured with it, whose charge the page holds.
pub(crate) struct MaterializedStateCapture {
    pub(crate) page: MaterializedInspectionPage,
    pub(crate) catalog: nervix_models::StateInspectionKeyCatalog,
}

/// The live generation of a unit, as the first step of a page captures it.
struct LiveGeneration {
    generation: MaterializedGeneration,
    sources: [u8; 32],
    /// What the step counted for the generation's row views.
    views: u64,
}

/// What the first step of a page hands on: the unit's live generation, when it has one, and the
/// page's charge.
struct LiveCapture {
    live: Option<LiveGeneration>,
    memory: GrowingCharge,
}

/// The generation a page selects from, with everything that keeps it charged.
struct SelectedSource {
    generation: MaterializedGeneration,
    /// What the page's charge counted for the generation's row views.
    views: u64,
    memory: GrowingCharge,
    stored: Option<RestoredMaterializedSnapshot>,
    stage: StateInspectionStage,
    sources: [u8; 32],
}

impl Runtime {
    pub(crate) fn inspection_executor(&self) -> &nervix_execution::Executor {
        &self.inner.executor
    }

    pub(crate) fn state_inspection_settings(&self) -> super::StateInspectionSettings {
        self.inner.inspection_settings
    }

    /// Capture one page of a materialized relay's state for an inspection, taking every charge
    /// through `executor`.
    ///
    /// The page holds what it captured while it captures more, so its caller runs it within
    /// allowances of the state-read and bulk classes (`Executor::within_allowance`): every charge
    /// is then drawn from a share the caller waited for while holding nothing, and a page that
    /// runs short is captured again within the shares it needed.
    pub(crate) async fn capture_materialized_state_page(
        &self,
        executor: &nervix_execution::Executor,
        domain: &DomainName,
        relay: &RelayName,
        query: MaterializedPageQuery,
    ) -> Result<MaterializedStateCapture, Report<StatePageReadError>> {
        let query = StdArc::new(query);
        let query = &query;
        let unit = query.unit;
        let placement = self
            .state_placement_for_unit(
                domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                unit,
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
        let memory = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(StatePageReadError::Admission)?;
        let captured_routing = routing.clone();
        let captured_relay = relay.clone();
        let captured_schema = schema.clone();
        let live = executor
            .run_cpu(
                CpuClass::Bulk,
                executor.charge_nothing(MemoryClass::Bulk),
                move |_charge, cancellation| {
                    capture_live_generation(
                        &captured_routing,
                        &captured_relay,
                        captured_schema,
                        unit,
                        GrowingCharge::new(memory),
                        cancellation,
                    )
                },
            )
            .await
            .change_context(StatePageReadError::Admission)??;
        let source = match live.live {
            Some(live_generation) => SelectedSource {
                generation: live_generation.generation,
                views: live_generation.views,
                memory: live.memory,
                stored: None,
                stage: StateInspectionStage::Live,
                sources: live_generation.sources,
            },
            None => {
                // The stored generation draws its own working charge from the page's shares.
                drop(live.memory);
                let stored = self
                    .open_stored_materialized_snapshot(&executor, None, &placement)
                    .await
                    .change_context(StatePageReadError::Stored)?;
                let count = match &stored {
                    Some(stored) => stored.records.len(),
                    None => 0,
                };
                let bytes = count
                    .checked_mul(std::mem::size_of::<MaterializedGenerationRecord>())
                    .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
                let views =
                    u64::try_from(bytes).map_err(|_| Report::new(StatePageReadError::Admission))?;
                let memory = executor
                    .reserve(MemoryClass::Bulk, views)
                    .await
                    .change_context(StatePageReadError::Admission)?;
                let mut memory = GrowingCharge::new(memory);
                memory
                    .grow_by(views)
                    .change_context(StatePageReadError::Admission)?;
                let (revision, fence, branch_generation, records) = match &stored {
                    Some(stored) => (
                        stored.revision,
                        stored.fence,
                        stored.branch_generation,
                        stored
                            .records
                            .iter()
                            .map(|record| MaterializedGenerationRecord {
                                branch: record.branch.clone(),
                                row: record.row.clone(),
                            })
                            .collect(),
                    ),
                    None => (0, 0, 0, Vec::new()),
                };
                SelectedSource {
                    generation: MaterializedGeneration::new(
                        revision,
                        fence,
                        branch_generation,
                        schema.clone(),
                        records,
                    ),
                    views,
                    memory,
                    stored,
                    stage: StateInspectionStage::Stored,
                    sources: [0; 32],
                }
            }
        };
        let runtime = self.clone();
        let query = StdArc::clone(query);
        executor
            .run_cpu(
                CpuClass::Bulk,
                executor.charge_nothing(MemoryClass::Bulk),
                move |_charge, cancellation| {
                    select_page(
                        &runtime,
                        &routing,
                        &placement,
                        schema,
                        &query,
                        source,
                        cancellation,
                    )
                },
            )
            .await
            .change_context(StatePageReadError::Admission)?
    }
}

/// The first step of a page: the unit's live generation, with its row views counted in `memory`.
fn capture_live_generation(
    routing: &super::DomainRoutingSnapshot,
    relay: &RelayName,
    schema: StdArc<arrow_schema::Schema>,
    unit: Unit,
    mut memory: GrowingCharge,
    cancellation: &nervix_execution::Cancellation,
) -> Result<LiveCapture, Report<StatePageReadError>> {
    let reads = routing
        .materialized_stream_reads
        .get(relay)
        .ok_or_else(|| Report::new(StatePageReadError::NotReady))?;
    let mut records = Vec::new();
    let mut views = 0_u64;
    #[cfg_attr(
        nervix_lint,
        expect(
            nervix::branch_inversion,
            reason = "materialized or emitter state groups its data by branch; move it to the branch in https://app.clickup.com/t/86bd1z3bd"
        )
    )]
    let mut sources = Vec::new();
    let mut source_views = 0_u64;
    reads.visit_unit_states(unit, |branch, state| {
        cancellation
            .check()
            .change_context(StatePageReadError::Admission)?;
        let captured = state
            .capture_for_inspection(&mut memory)
            .change_context(StatePageReadError::Admission)?;
        let generation = &captured.generation;
        let grown = reserve_views(&mut records, generation.records().len(), &mut memory)?;
        views = views
            .checked_add(grown)
            .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
        records.extend_from_slice(generation.records());
        let grown = reserve_views(&mut sources, 1, &mut memory)?;
        source_views = source_views
            .checked_add(grown)
            .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
        sources.push((
            branch.map(|branch| branch.fingerprint()),
            state.inspection_instance(),
            generation.revision(),
            generation.fence(),
            generation.branch_generation(),
        ));
        // The captured generation's row views go with it, and so does what they were counted.
        let counted = captured.counted;
        drop(captured);
        memory.release(counted);
        Ok::<_, Report<StatePageReadError>>(())
    })?;
    if sources.is_empty() {
        return Ok(LiveCapture { live: None, memory });
    }
    records.sort_unstable_by(|a, b| super::BranchKey::canonical_order(&a.branch, &b.branch));
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
    memory.release(source_views);
    Ok(LiveCapture {
        live: Some(LiveGeneration {
            generation: MaterializedGeneration::new(revision, fence, generation, schema, records),
            sources: digest.finish(),
            views,
        }),
        memory,
    })
}

/// The last step of a page: select the page's rows from `source`, under its charge.
fn select_page(
    runtime: &Runtime,
    routing: &super::DomainRoutingSnapshot,
    placement: &super::RuntimeStatePlacement,
    schema: StdArc<arrow_schema::Schema>,
    query: &MaterializedPageQuery,
    source: SelectedSource,
    cancellation: &nervix_execution::Cancellation,
) -> Result<MaterializedStateCapture, Report<StatePageReadError>> {
    let MaterializedPageQuery {
        unit: _,
        selection,
        after,
        bounds,
        catalog_requested,
        filter,
    } = query;
    let selection = *selection;
    let after = *after;
    let catalog_requested = *catalog_requested;
    let max_rows = bounds.rows;
    let max_bytes = bounds.bytes;
    let SelectedSource {
        generation,
        views,
        mut memory,
        stored,
        stage,
        sources,
    } = source;
    let mut catalog = if catalog_requested {
        nervix_models::StateInspectionKeyCatalog::requested()
    } else {
        nervix_models::StateInspectionKeyCatalog::Unrequested
    };
    if catalog_requested {
        let bytes = generation
            .records()
            .len()
            .checked_mul(
                64 * (std::mem::size_of::<BranchKeyFingerprint>()
                    + std::mem::size_of::<Option<nervix_models::InheritedPartitionOrigin>>()),
            )
            .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
        let bytes = u64::try_from(bytes).map_err(|_| Report::new(StatePageReadError::Admission))?;
        if bytes > max_bytes {
            return Err(Report::new(StatePageReadError::RowTooLarge));
        }
        memory
            .grow_by(bytes)
            .change_context(StatePageReadError::Admission)?;
    }
    let capacity = max_rows.min(generation.records().len());
    let metadata_bytes = capacity
        .checked_mul(
            std::mem::size_of::<MaterializedGenerationRecord>()
                + std::mem::size_of::<std::ops::Range<usize>>(),
        )
        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
    memory
        .grow_by(
            u64::try_from(metadata_bytes)
                .map_err(|_| Report::new(StatePageReadError::Admission))?,
        )
        .change_context(StatePageReadError::Admission)?;
    let mut fingerprint = nervix_crypto::digest::Sha256::new();
    fingerprint.update(b"nervix/state-inspection/materialized-metadata");
    fingerprint.update(&sources);
    if let super::RuntimeState::MaterializedRelay { schema } = &placement.state {
        fingerprint.update(schema.as_digest());
    }

    let mut counts = StateInspectionCounts::default();
    let mut selected = Vec::with_capacity(capacity);
    let start = match after {
        Some((_, offset)) => offset,
        None => 0,
    };
    let mut previous_branch = None;
    let mut next = start;
    let mut selected_bytes = 0_u64;
    let mut full = false;
    for record in generation.records() {
        cancellation
            .check()
            .change_context(StatePageReadError::Admission)?;
        if !runtime.materialized_stream_key_is_visible(Some(routing), placement, &record.branch) {
            continue;
        }
        let branch = record.branch.as_ref().map(|branch| branch.fingerprint());
        if !match selection {
            StateInspectionBranchSelection::All => true,
            StateInspectionBranchSelection::Unbranched => branch.is_none(),
            StateInspectionBranchSelection::Branch(wanted) => branch == Some(wanted),
        } {
            continue;
        }
        if catalog_requested && let Some(key) = &record.branch {
            catalog.insert(key.fingerprint(), key.inherited_origin());
        }
        if filter
            .as_ref()
            .is_some_and(|filter| branch.is_some_and(|branch| filter.excluded.contains(&branch)))
        {
            continue;
        }
        match branch {
            Some(branch) => {
                fingerprint.update(&[1]);
                fingerprint.update(branch.fingerprint());
            }
            None => fingerprint.update(&[0]),
        }
        fingerprint.update(
            &record
                .row
                .metadata()
                .ingested_at_low_watermark()
                .unix_nanos()
                .to_be_bytes(),
        );
        fingerprint.update(
            &record
                .row
                .metadata()
                .ingested_at_high_watermark()
                .unix_nanos()
                .to_be_bytes(),
        );
        let index = counts.rows;
        let bytes =
            batch_payload_bytes(&record.row.batch().batch().slice(record.row.row_index(), 1));
        counts.rows = counts
            .rows
            .checked_add(1)
            .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
        counts.branches += u64::from(branch.is_some());
        counts.bytes = counts
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| Report::new(StatePageReadError::RowTooLarge))?;
        if index < start {
            previous_branch = branch;
            continue;
        }
        if index == start && after.is_some_and(|(expected, _)| expected != previous_branch) {
            return Err(Report::new(StatePageReadError::Position));
        }
        if full || selected.len() == max_rows {
            full = true;
            continue;
        }
        let Some(total) = selected_bytes.checked_add(bytes) else {
            return Err(Report::new(StatePageReadError::RowTooLarge));
        };
        if total > max_bytes {
            full = true;
            continue;
        }
        selected.push(record.clone());
        selected_bytes = total;
        previous_branch = branch;
        next += 1;
    }
    if start > counts.rows
        || (start == counts.rows && after.is_some_and(|(expected, _)| expected != previous_branch))
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
    // The page keeps only its selected rows; the source's row views go with it.
    drop(generation);
    memory.release(views);
    Ok(MaterializedStateCapture {
        page: MaterializedInspectionPage {
            fingerprint: fingerprint.finish(),
            counts,
            generation: selected,
            offset: next,
            last_branch: previous_branch,
            more: next < counts.rows,
            stage,
            _metadata: memory.into_reservation(),
            _stored: stored,
        },
        catalog,
    })
}

/// Geometric metadata growth is counted before allocation, in the page's one charge, and the
/// bytes counted are returned. The finite Bulk memory class bounds captures across branch-local
/// states independently of the selected page length.
fn reserve_views<T>(
    values: &mut Vec<T>,
    additional: usize,
    memory: &mut GrowingCharge,
) -> Result<u64, Report<StatePageReadError>> {
    let length = values
        .len()
        .checked_add(additional)
        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
    if length <= values.capacity() {
        return Ok(0);
    }
    let capacity = length
        .checked_next_power_of_two()
        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
    let grown = capacity
        .checked_sub(values.capacity())
        .verified("the next capacity holds more than the current one, which is below the length");
    let bytes = grown
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
    let bytes = u64::try_from(bytes).map_err(|_| Report::new(StatePageReadError::Admission))?;
    memory
        .grow_by(bytes)
        .change_context(StatePageReadError::Admission)?;
    values.reserve_exact(capacity - values.len());
    Ok(bytes)
}
