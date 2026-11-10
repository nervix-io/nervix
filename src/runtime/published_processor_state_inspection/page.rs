//! Retained record ordering, bounded paging and complete selected-state counts.
//!
//! Layer: data plane.
//! - **Owns.** Retained record ordering, bounded paging and complete selected-state counts.
//! - **Depends on.** Published processor snapshots, typed execution plans and bulk admission.
//! - **Must not know.** Models, Plexus, sessions or presentation formats.

use super::*;

impl PublishedPage {
    pub(super) fn build(
        branches: Vec<OpenBranch>,
        plan: ReadPlan,
        now: Timestamp,
        after: Option<(Option<nervix_models::BranchKeyFingerprint>, u64)>,
        bounds: ProcessorStatePageBounds,
        memory: Reservation,
        cancellation: &Cancellation,
    ) -> ReadResult<ProcessorStatePage> {
        let start = match after {
            Some((_, offset)) => offset,
            None => 0,
        };
        let metadata = bounds.rows != 0;
        let mut page = Self {
            skip: start,
            rows_left: bounds.rows,
            bytes_left: bounds.bytes,
            returned: 0,
            previous_branch: None,
            last_branch: None,
            counts: StateInspectionCounts::default(),
            generation: 0,
            digest: nervix_crypto::digest::Sha256::new(),
            keys: Vec::new(),
            windows: Vec::new(),
            headers: Vec::new(),
            structures: Vec::new(),
            runs: Vec::new(),
            metadata_runs: Vec::new(),
            memory,
        };
        page.digest
            .update(b"nervix/state-inspection/published-processors");
        for branch in &branches {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            let fingerprint = branch.key.as_ref().map(BranchKey::fingerprint);
            match fingerprint {
                Some(value) => {
                    page.digest.update(&[1]);
                    page.digest.update(value.fingerprint());
                }
                None => page.digest.update(&[0]),
            }
            page.digest.update(&branch.revision.to_be_bytes());
            page.digest.update(&branch.registration);
            page.generation = page.generation.max(branch.revision);
            page.counts.branches = page
                .counts
                .branches
                .checked_add(u64::from(fingerprint.is_some()))
                .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
            match (&branch.state, &plan) {
                (OpenState::Keys(generation), ReadPlan::Keys { max_time, .. }) => {
                    page.read_keys(&branch.key, generation, *max_time, now, cancellation)?;
                }
                (OpenState::Window(source), ReadPlan::Window { plan, width, .. }) => {
                    let snapshot = source
                        .snapshot()?
                        .ok_or_else(|| Report::new(ProcessorStatePageError::Unrestored))?;
                    let window = WindowBranch {
                        key: &branch.key,
                        revision: branch.revision,
                        snapshot,
                        plan,
                        width: *width,
                    };
                    page.read_window(window, metadata, cancellation)?;
                }
                _ => return Err(Report::new(ProcessorStatePageError::Columns)),
            }
        }
        if start > page.counts.rows
            || after.is_some_and(|(branch, _)| branch != page.previous_branch)
        {
            return Err(Report::new(ProcessorStatePageError::Position));
        }
        let offset = start
            .checked_add(page.returned)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Position))?;
        let sections = page.sections(&plan, cancellation)?;
        page.metadata_runs.append(&mut page.runs);
        let mut windows = Vec::new();
        for branch in branches {
            if let OpenState::Window(WindowRead::Decoded(decoded)) = branch.state {
                windows.push(*decoded);
            }
        }
        Ok(ProcessorStatePage {
            partition_catalog: None,
            fingerprint: page.digest.finish(),
            generation: page.generation,
            counts: page.counts,
            offset,
            last_branch: page.last_branch,
            more: offset < page.counts.rows,
            runs: page.metadata_runs,
            sections,
            memory: Some(ProcessorStatePageMemory {
                _reservation: page.memory,
                _windows: windows,
            }),
        })
    }

    fn skip_branch(&mut self, key: &Option<BranchKey>, rows: usize) -> ReadResult<usize> {
        let rows = u64::try_from(rows).assured("resident rows fit u64");
        self.counts.rows = self
            .counts
            .rows
            .checked_add(rows)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        if self.skip > 0 && self.skip <= rows {
            self.previous_branch = key.as_ref().map(BranchKey::fingerprint);
        }
        let skipped = self.skip.min(rows);
        self.skip = self
            .skip
            .checked_sub(skipped)
            .verified("skipped does not exceed the remaining offset");
        usize::try_from(skipped).map_err(|_| Report::new(ProcessorStatePageError::Position))
    }

    fn count_bytes(&mut self, bytes: u64) -> ReadResult<()> {
        self.counts.bytes = self
            .counts
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        Ok(())
    }

    fn admit(
        &mut self,
        key: &Option<BranchKey>,
        section: ProcessorStateSection,
        bytes: u64,
    ) -> ReadResult<bool> {
        if self.rows_left == 0 || bytes > self.bytes_left {
            self.rows_left = 0;
            return Ok(false);
        }
        self.rows_left = self
            .rows_left
            .checked_sub(1)
            .verified("a full page returned above");
        self.bytes_left = self
            .bytes_left
            .checked_sub(bytes)
            .verified("a row beyond the byte allowance returned above");
        self.returned = self
            .returned
            .checked_add(1)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        self.last_branch = key.as_ref().map(BranchKey::fingerprint);
        // Column gathering can retain both the selected views and their contiguous output buffers.
        let column_bytes = bytes
            .checked_mul(2)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        reserve_bytes(&mut self.memory, column_bytes)?;
        reserve_item::<ProcessorStateRun>(&mut self.memory)?;
        extend_runs(&mut self.runs, section, key);
        Ok(true)
    }

    fn read_keys(
        &mut self,
        branch: &Option<BranchKey>,
        generation: &Generation<Vec<PublishedDeduplicatorKey>>,
        max_time: std::time::Duration,
        now: Timestamp,
        cancellation: &Cancellation,
    ) -> ReadResult<()> {
        // The publication is oldest first. Expiry changes what can suppress a duplicate even when
        // no new input caused the owning task to prune its expired prefix yet.
        let mut expired = 0_usize;
        for key in &generation.value {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            if checked_add_duration_to_timestamp(key.seen_at, max_time) > now {
                break;
            }
            expired = expired
                .checked_add(1)
                .assured("the prefix is within an addressable generation");
        }
        self.digest.update(
            &u64::try_from(expired)
                .assured("an addressable prefix fits u64")
                .to_be_bytes(),
        );
        let active = &generation.value[expired..];
        let skipped = self.skip_branch(branch, active.len())?;
        for (index, key) in active.iter().enumerate() {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            let bytes = key.key.estimated_bytes();
            self.count_bytes(bytes)?;
            if index >= skipped && self.admit(branch, ProcessorStateSection::Keys, bytes)? {
                reserve_item::<KeyRow>(&mut self.memory)?;
                self.keys.push(KeyRow {
                    key: key.key.clone(),
                    seen_at: key.seen_at,
                });
            }
        }
        Ok(())
    }

    fn read_window(
        &mut self,
        window: WindowBranch<'_>,
        metadata: bool,
        cancellation: &Cancellation,
    ) -> ReadResult<()> {
        let WindowBranch {
            key: branch,
            revision: _,
            snapshot,
            plan,
            width: _,
        } = window;
        if snapshot.accumulators.len() != plan.demands().len() {
            return Err(Report::new(ProcessorStatePageError::Columns));
        }
        match snapshot.incarnation {
            Some(value) => {
                self.digest.update(&[1]);
                self.digest.update(&value.to_be_bytes());
            }
            None => self.digest.update(&[0]),
        }
        let skipped = self.skip_branch(branch, snapshot.entries.len())?;
        for (index, entry) in snapshot.entries.iter().enumerate() {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            if &entry.key != branch {
                return Err(Report::new(ProcessorStatePageError::Columns));
            }
            let input = held_row_bytes(&entry.record);
            let arguments = held_row_bytes(&entry.arguments);
            let bytes = input
                .checked_add(arguments)
                .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
            let bytes = bytes
                .checked_add(16)
                .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
            if index >= skipped && self.admit(branch, ProcessorStateSection::Rows, bytes)? {
                reserve_item::<WindowRow>(&mut self.memory)?;
                self.windows.push(WindowRow {
                    entry: entry.clone(),
                });
            }
        }
        let charge = window::snapshot_charge(snapshot, plan, &mut self.memory, cancellation)?;
        self.count_bytes(charge)?;
        if metadata {
            window.append_metadata(self, charge, cancellation)?;
        }
        Ok(())
    }
}
