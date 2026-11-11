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
use nervix_execution::{CpuClass, MemoryClass, Reservation};
use nervix_models::{
    BranchKeyFingerprint, DomainName, ModelKind, RelayName, StateInspectionBranchSelection,
    StateInspectionCounts, StateInspectionStage, Unit,
};

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
}

/// Effective owner-side bounds after the coordinator and local capability checks.
pub(crate) struct MaterializedPageBounds {
    pub(crate) rows: usize,
    pub(crate) bytes: u64,
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

impl Runtime {
    pub(crate) fn inspection_executor(&self) -> &nervix_execution::Executor {
        &self.inner.executor
    }

    pub(crate) fn state_inspection_settings(&self) -> super::StateInspectionSettings {
        self.inner.inspection_settings
    }

    pub(crate) async fn local_materialized_state_page(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
        selection: StateInspectionBranchSelection,
        after: Option<(Option<BranchKeyFingerprint>, u64)>,
        bounds: MaterializedPageBounds,
    ) -> Result<MaterializedInspectionPage, Report<StatePageReadError>> {
        let MaterializedPageBounds {
            rows: max_rows,
            bytes: max_bytes,
        } = bounds;
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
        let executor = self.inner.executor.clone();
        let captured_routing = routing.clone();
        let captured_relay = relay.clone();
        let captured_schema = schema.clone();
        let captured_executor = executor.clone();
        let admission = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .change_context(StatePageReadError::Admission)?;
        let captured = executor
            .run_cpu(
                CpuClass::Bulk,
                admission,
                move |mut charge, cancellation| {
                    let reads = captured_routing
                        .materialized_stream_reads
                        .get(&captured_relay)
                        .ok_or_else(|| Report::new(StatePageReadError::NotReady))?;
                    let mut records = Vec::new();
                    let mut sources = Vec::new();
                    let mut source_charge = captured_executor
                        .try_reserve(MemoryClass::Bulk, 1)
                        .change_context(StatePageReadError::Admission)?;
                    reads.visit_unit_states(unit, |branch, state| {
                        cancellation
                            .check()
                            .change_context(StatePageReadError::Admission)?;
                        // Keep the generation and its admission in one owner: tuple fields drop
                        // in order, so row-view metadata is freed before its charge is released.
                        let captured = state
                            .capture_for_inspection(&captured_executor)
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
                    Ok::<_, Report<StatePageReadError>>(Some((
                        MaterializedGeneration::new(
                            revision,
                            fence,
                            generation,
                            captured_schema,
                            records,
                        ),
                        charge,
                        digest.finish(),
                    )))
                },
            )
            .await
            .change_context(StatePageReadError::Admission)??;
        let (generation, metadata, stored, stage, sources) =
            if let Some((generation, metadata, sources)) = captured {
                (
                    generation,
                    metadata,
                    None,
                    StateInspectionStage::Live,
                    sources,
                )
            } else {
                let stored = self
                    .open_stored_materialized_snapshot(None, &placement)
                    .await
                    .change_context(StatePageReadError::Stored)?;
                let count = match &stored {
                    Some(stored) => stored.records.len(),
                    None => 0,
                };
                let bytes = count
                    .checked_mul(std::mem::size_of::<MaterializedGenerationRecord>())
                    .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
                let charge = executor
                    .try_reserve(
                        MemoryClass::Bulk,
                        u64::try_from(bytes)
                            .map_err(|_| Report::new(StatePageReadError::Admission))?
                            .max(1),
                    )
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
                (
                    MaterializedGeneration::new(
                        revision,
                        fence,
                        branch_generation,
                        schema.clone(),
                        records,
                    ),
                    charge,
                    stored,
                    StateInspectionStage::Stored,
                    [0; 32],
                )
            };
        // The captured metadata charge also backs the smaller selected vector. Move that charge
        // through the worker and the returned page; no row-view allocation outlives admission.
        let runtime = self.clone();
        let scan_executor = executor.clone();
        executor
            .run_cpu(
                CpuClass::Bulk,
                metadata,
                move |_captured_metadata, cancellation| {
                    let capacity = max_rows.min(generation.records().len());
                    let metadata_bytes = capacity
                        .checked_mul(
                            std::mem::size_of::<MaterializedGenerationRecord>()
                                + std::mem::size_of::<std::ops::Range<usize>>(),
                        )
                        .ok_or_else(|| Report::new(StatePageReadError::Admission))?;
                    let metadata = scan_executor
                        .try_reserve(
                            MemoryClass::Bulk,
                            u64::try_from(metadata_bytes)
                                .map_err(|_| Report::new(StatePageReadError::Admission))?
                                .max(1),
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
                        if !runtime.materialized_stream_key_is_visible(
                            Some(&routing),
                            &placement,
                            &record.branch,
                        ) {
                            continue;
                        }
                        let branch = record.branch.as_ref().map(|branch| branch.fingerprint());
                        if !match selection {
                            StateInspectionBranchSelection::All => true,
                            StateInspectionBranchSelection::Unbranched => branch.is_none(),
                            StateInspectionBranchSelection::Branch(wanted) => {
                                branch == Some(wanted)
                            }
                        } {
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
                        let bytes = batch_payload_bytes(
                            &record.row.batch().batch().slice(record.row.row_index(), 1),
                        );
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
                        if index == start
                            && after.is_some_and(|(expected, _)| expected != previous_branch)
                        {
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
                    Ok(MaterializedInspectionPage {
                        fingerprint: fingerprint.finish(),
                        counts,
                        generation: selected,
                        offset: next,
                        last_branch: previous_branch,
                        more: next < counts.rows,
                        stage,
                        _metadata: metadata,
                        _stored: stored,
                    })
                },
            )
            .await
            .change_context(StatePageReadError::Admission)?
    }
}

/// Geometric metadata growth is admitted before allocation. The finite Bulk memory class bounds
/// captures across branch-local states independently of the selected page length.
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
