//! Window deadlines, accumulator facts and the production state charge.
//!
//! Layer: data plane.
//! - **Owns.** Window deadlines, accumulator facts and the production state charge.
//! - **Depends on.** Published processor snapshots, typed execution plans and bulk admission.
//! - **Must not know.** Models, Plexus, sessions or presentation formats.

use super::*;

impl WindowBranch<'_> {
    pub(super) fn append_metadata(
        self,
        page: &mut PublishedPage,
        bytes: u64,
        cancellation: &Cancellation,
    ) -> ReadResult<()> {
        let Self {
            key: branch,
            revision,
            snapshot,
            plan,
            width,
        } = self;
        let rows = u64::try_from(snapshot.entries.len()).assured("a retained row count fits u64");
        let mut due = match (snapshot.entries.first(), width) {
            (Some(first), Some(width)) => {
                Some(checked_add_duration_to_timestamp(first.timestamp, width))
            }
            _ => None,
        };
        let mut slot = 0_usize;
        for (index, (demand, state)) in plan
            .demands()
            .iter()
            .zip(&snapshot.accumulators)
            .enumerate()
        {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            let arity = if demand.arguments.second().is_some() {
                2
            } else {
                1
            };
            let mut count = 0_u64;
            for entry in &snapshot.entries {
                cancellation
                    .check()
                    .change_context(ProcessorStatePageError::Admission)?;
                let batch = entry.arguments.batch().batch();
                let row = entry.arguments.row_index();
                let mut present = true;
                for offset in 0..arity {
                    let position = slot
                        .checked_add(offset)
                        .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
                    let column = batch
                        .columns()
                        .get(position)
                        .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
                    present &= column.is_valid(row);
                }
                if present || demand.storage == super::WindowAggregateStorageKind::Counter {
                    count = count
                        .checked_add(1)
                        .assured("contributing rows do not exceed retained rows");
                }
            }
            if let WindowAccumulatorSnapshot::LinearHistogram { delayed_removals } = state {
                count = count
                    .checked_add(
                        u64::try_from(delayed_removals.len()).assured("retained removals fit u64"),
                    )
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
                for removal in delayed_removals {
                    cancellation
                        .check()
                        .change_context(ProcessorStatePageError::Admission)?;
                    due = Some(match due {
                        Some(current) => current.min(removal.expires_at),
                        None => removal.expires_at,
                    });
                }
            }
            reserve_item::<StructureRow>(&mut page.memory)?;
            reserve_bytes(&mut page.memory, 256)?;
            reserve_item::<ProcessorStateRun>(&mut page.memory)?;
            page.structures.push(StructureRow {
                index: u32::try_from(index)
                    .map_err(|_| Report::new(ProcessorStatePageError::Columns))?,
                kind: demand.storage.as_ref().to_string(),
                count,
            });
            extend_runs(
                &mut page.metadata_runs,
                ProcessorStateSection::Accumulators,
                branch,
            );
            slot = slot
                .checked_add(arity)
                .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
        }
        reserve_item::<HeaderRow>(&mut page.memory)?;
        reserve_bytes(&mut page.memory, 256)?;
        reserve_item::<ProcessorStateRun>(&mut page.memory)?;
        page.headers.push(HeaderRow {
            revision,
            rows,
            first: snapshot.entries.first().map(|entry| entry.sequence),
            next: snapshot.next_sequence,
            incarnation: snapshot.incarnation,
            due,
            bytes,
        });
        extend_runs(
            &mut page.metadata_runs,
            ProcessorStateSection::Header,
            branch,
        );
        Ok(())
    }
}

pub(super) fn snapshot_charge(
    snapshot: &WindowProcessorStateSnapshot,
    plan: &WindowAccumulatorPlan,
    memory: &mut Reservation,
    cancellation: &Cancellation,
) -> ReadResult<u64> {
    use std::collections::hash_map::Entry;
    let mut arguments = ahash::HashMap::default();
    let mut charge = WindowStateCharge::new(plan);
    for entry in &snapshot.entries {
        cancellation
            .check()
            .change_context(ProcessorStatePageError::Admission)?;
        reserve_bytes(memory, 160)?;
        let key = Arc::as_ptr(entry.arguments.batch());
        let columns = match arguments.entry(key) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(slot) => {
                reserve_item::<WindowArgumentColumns>(memory)?;
                let columns = u64::try_from(entry.arguments.batch().batch().num_columns())
                    .assured("an argument column count fits u64");
                let bytes = columns
                    .checked_mul(256)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
                reserve_bytes(memory, bytes)?;
                let decoded =
                    WindowArgumentColumns::from_snapshot_batch(plan, entry.arguments.batch())
                        .change_context(ProcessorStatePageError::Columns)?;
                let decoded = Arc::new(decoded);
                slot.insert(decoded.clone());
                decoded
            }
        };
        let message = RelayMessage {
            key: entry.key.clone(),
            record: entry.record.clone(),
            acks: AckSet::empty(),
        };
        charge.admit(&message, &columns);
    }
    let bytes = charge
        .finish()
        .change_context(ProcessorStatePageError::Columns)?;
    u64::try_from(bytes)
        .map_err(|error| Report::new(error).change_context(ProcessorStatePageError::Admission))
}
