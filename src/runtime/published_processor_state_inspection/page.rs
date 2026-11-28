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
            column_bytes: 0,
            memory,
            reply: bounds.reply,
            bodies: 0,
        };
        if let PageReply::Remote { descriptor, .. } = page.reply {
            reserve_bytes(&mut page.memory, descriptor)?;
        }
        let decoded_count = branches
            .iter()
            .filter(|branch| matches!(&branch.state, OpenState::Window(WindowRead::Decoded(_))))
            .count();
        reserve_bytes(
            &mut page.memory,
            page_memory::slots::<AdmittedWindowSnapshot>(decoded_count)?,
        )?;
        page.digest
            .update(b"nervix/state-inspection/published-processors");
        // Every selected branch is counted, and its metadata charged, before any retained row is
        // admitted. A page ends before a row the bulk class cannot hold, so rows never take the
        // memory the complete counts and every branch's metadata need.
        reserve_bytes(
            &mut page.memory,
            page_memory::slots::<CountedRows<'_>>(branches.len())?,
        )?;
        let mut counted = Vec::with_capacity(branches.len());
        for branch in &branches {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            let rows = page.count_branch(branch, &plan, now, metadata, cancellation)?;
            counted.push(rows);
        }
        if start > page.counts.rows
            || after.is_some_and(|(branch, _)| branch != page.previous_branch)
        {
            return Err(Report::new(ProcessorStatePageError::Position));
        }
        for rows in counted {
            page.admit_rows(rows, &plan, cancellation)?;
        }
        let offset = start
            .checked_add(page.returned)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Position))?;
        let sections = page.sections(&plan, cancellation)?;
        page_memory::assert_buffers(&sections, page.column_bytes);
        let mut windows = Vec::with_capacity(decoded_count);
        for branch in branches {
            if let OpenState::Window(WindowRead::Decoded(decoded)) = branch.state {
                windows.push(*decoded);
            }
        }
        page.finish(offset, sections, windows)
    }

    /// Counts one selected `branch` into the page's identity, generation and complete counts,
    /// charging its metadata, and returns the rows it holds for admission.
    fn count_branch<'a>(
        &mut self,
        branch: &'a OpenBranch,
        plan: &'a ReadPlan,
        now: Timestamp,
        metadata: bool,
        cancellation: &Cancellation,
    ) -> ReadResult<CountedRows<'a>> {
        let fingerprint = branch.key.as_ref().map(BranchKey::fingerprint);
        match fingerprint {
            Some(value) => {
                self.digest.update(&[1]);
                self.digest.update(value.fingerprint());
            }
            None => self.digest.update(&[0]),
        }
        self.digest.update(&branch.revision.to_be_bytes());
        self.digest.update(&branch.registration);
        self.generation = self.generation.max(branch.revision);
        self.counts.branches = self
            .counts
            .branches
            .checked_add(u64::from(fingerprint.is_some()))
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        match (&branch.state, plan) {
            (OpenState::Keys(generation), ReadPlan::Keys { max_time, .. }) => {
                self.count_keys(&branch.key, generation, *max_time, now, cancellation)
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
                self.count_window(window, metadata, cancellation)
            }
            _ => Err(Report::new(ProcessorStatePageError::Columns)),
        }
    }

    /// The finished page at `offset`, with its `sections` and the decoded `windows` its rows
    /// borrow. For a reply to another node its memory splits into what the rows hold and what
    /// the page set aside for encoding the reply.
    fn finish(
        self,
        offset: u64,
        sections: Vec<ProcessorStateSectionColumns>,
        windows: Vec<AdmittedWindowSnapshot>,
    ) -> ReadResult<ProcessorStatePage> {
        let (reservation, reply) = match self.reply {
            PageReply::Local => (self.memory, None),
            PageReply::Remote {
                descriptor,
                framing,
                ..
            } => {
                let (descriptor, rest) = self
                    .memory
                    .split(descriptor)
                    .change_context(ProcessorStatePageError::Admission)?;
                let (bodies, rest) = rest
                    .split(self.bodies)
                    .change_context(ProcessorStatePageError::Admission)?;
                let reply = ReplyMemory {
                    descriptor,
                    bodies,
                    framing,
                };
                (rest, Some(reply))
            }
        };
        Ok(ProcessorStatePage {
            partition_catalog: nervix_models::StateInspectionKeyCatalog::Unrequested,
            fingerprint: self.digest.finish(),
            generation: self.generation,
            counts: self.counts,
            offset,
            last_branch: self.last_branch,
            more: offset < self.counts.rows,
            runs: self.runs,
            sections,
            memory: Some(ProcessorStatePageMemory {
                _reservation: reservation,
                _windows: windows,
                reply,
            }),
        })
    }

    /// Sets `bytes` aside for the bodies of the page's reply to another node.
    fn set_aside_bodies(&mut self, bytes: u64) -> ReadResult<()> {
        reserve_bytes(&mut self.memory, bytes)?;
        self.bodies = add_bytes(self.bodies, bytes)?;
        Ok(())
    }

    /// Charges one metadata row's buffers and its remote body independently. Metadata is required
    /// for every selected branch even on a page whose retained rows stop early.
    pub(super) fn reserve_metadata(
        &mut self,
        columns: u64,
        body: u64,
        first_in_section: bool,
    ) -> ReadResult<()> {
        reserve_bytes(&mut self.memory, columns)?;
        self.column_bytes = add_bytes(self.column_bytes, columns)?;
        if let PageReply::Remote { framing, .. } = self.reply {
            let body = if first_in_section {
                add_bytes(body, framing)?
            } else {
                body
            };
            self.set_aside_bodies(body)?;
        }
        Ok(())
    }

    pub(super) fn reserve_run(
        &mut self,
        section: ProcessorStateSection,
        key: &Option<BranchKey>,
    ) -> ReadResult<()> {
        let growth = self.run_growth(section, key)?;
        reserve_bytes(&mut self.memory, growth.bytes)?;
        growth.allocate(&mut self.runs);
        extend_runs(&mut self.runs, section, key);
        Ok(())
    }

    fn run_growth(
        &self,
        section: ProcessorStateSection,
        key: &Option<BranchKey>,
    ) -> ReadResult<page_memory::VecGrowth> {
        let continues = self
            .runs
            .last()
            .is_some_and(|run| run.continues(section, key));
        page_memory::VecGrowth::next(&self.runs, usize::from(!continues))
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

    /// Whether the next row of `section` joins the page. The page ends at its row or byte
    /// allowance, and where its share has no room now for everything the row needs to finish: its
    /// columns and, when another node reads the page, its body. That leaves the read a
    /// continuation. A page's first row is required instead, since without it the read makes no
    /// progress: a share without room for it runs the read again within a larger one.
    fn admit(
        &mut self,
        key: &Option<BranchKey>,
        section: ProcessorStateSection,
        row: PublishedRowCharge,
    ) -> ReadResult<bool> {
        if self.rows_left == 0 || row.logical > self.bytes_left {
            self.rows_left = 0;
            return Ok(false);
        }
        let growth = self.run_growth(section, key)?;
        let charge = add_bytes(add_bytes(row.columns, growth.bytes)?, row.slot)?;
        // The peer stream also needs the row's body.
        let body = match self.reply {
            PageReply::Local => 0,
            PageReply::Remote { framing, .. } if self.returned == 0 => {
                add_bytes(row.body, framing)?
            }
            PageReply::Remote { .. } => row.body,
        };
        let bodies = add_bytes(self.bodies, body)?;
        if self.returned != 0 && !self.reply.optional_bodies_fit(bodies) {
            self.rows_left = 0;
            return Ok(false);
        }
        let charge = add_bytes(charge, body)?;
        let target = add_bytes(self.memory.bytes(), charge)?;
        if self.returned == 0 {
            self.memory
                .grow_to(target)
                .change_context(ProcessorStatePageError::Admission)?;
        } else if self.memory.try_grow_to(target).is_err() {
            self.rows_left = 0;
            return Ok(false);
        }
        self.rows_left = self
            .rows_left
            .checked_sub(1)
            .verified("a full page returned above");
        self.bytes_left = self
            .bytes_left
            .checked_sub(row.logical)
            .verified("a row beyond the byte allowance returned above");
        self.returned = self
            .returned
            .checked_add(1)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        self.last_branch = key.as_ref().map(BranchKey::fingerprint);
        self.bodies = bodies;
        self.column_bytes = add_bytes(self.column_bytes, row.columns)?;
        growth.allocate(&mut self.runs);
        extend_runs(&mut self.runs, section, key);
        Ok(true)
    }

    /// Counts one branch's live keys and their bytes, and how many of them a resumed page skips.
    fn count_keys<'a>(
        &mut self,
        branch: &'a Option<BranchKey>,
        generation: &'a Generation<Vec<PublishedDeduplicatorKey>>,
        max_time: std::time::Duration,
        now: Timestamp,
        cancellation: &Cancellation,
    ) -> ReadResult<CountedRows<'a>> {
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
        for key in active {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            self.count_bytes(key.key.estimated_bytes())?;
        }
        Ok(CountedRows::Keys {
            branch,
            keys: active,
            skipped,
        })
    }

    /// Counts one branch's retained rows and its state charge, and admits its metadata.
    fn count_window<'a>(
        &mut self,
        window: WindowBranch<'a>,
        metadata: bool,
        cancellation: &Cancellation,
    ) -> ReadResult<CountedRows<'a>> {
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
        for entry in &snapshot.entries {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            if &entry.key != branch {
                return Err(Report::new(ProcessorStatePageError::Columns));
            }
        }
        let charge = window::snapshot_charge(snapshot, plan, &mut self.memory, cancellation)?;
        self.count_bytes(charge)?;
        if metadata {
            window.append_metadata(self, charge, cancellation)?;
        }
        Ok(CountedRows::Window {
            branch,
            snapshot,
            skipped,
        })
    }

    /// Admits one counted branch's rows, past those a resumed page skips, until a bound ends the
    /// page.
    fn admit_rows(
        &mut self,
        rows: CountedRows<'_>,
        plan: &ReadPlan,
        cancellation: &Cancellation,
    ) -> ReadResult<()> {
        if self.rows_left == 0 {
            return Ok(());
        }
        match rows {
            CountedRows::Keys {
                branch,
                keys,
                skipped,
            } => {
                let ReadPlan::Keys { fields, .. } = plan else {
                    return Err(Report::new(ProcessorStatePageError::Columns));
                };
                for key in keys.iter().skip(skipped) {
                    cancellation
                        .check()
                        .change_context(ProcessorStatePageError::Admission)?;
                    let bytes = key.key.estimated_bytes();
                    let first = self.keys.is_empty();
                    let mut columns =
                        page_memory::fixed_columns(page_memory::slots::<i64>(1)?, 1, first)?;
                    if key.key.parts().len() != fields.len() {
                        return Err(Report::new(ProcessorStatePageError::Columns));
                    }
                    for (field, part) in fields.iter().zip(key.key.parts()) {
                        columns = add_bytes(
                            columns,
                            page_memory::key_part(part, field.data_type(), first)?,
                        )?;
                    }
                    let growth = page_memory::VecGrowth::next(&self.keys, 1)?;
                    let row = PublishedRowCharge {
                        logical: bytes,
                        columns,
                        slot: growth.bytes,
                        body: columns,
                    };
                    if !self.admit(branch, ProcessorStateSection::Keys, row)? {
                        break;
                    }
                    growth.allocate(&mut self.keys);
                    self.keys.push(KeyRow {
                        key: key.key.clone(),
                        seen_at: key.seen_at,
                    });
                }
            }
            CountedRows::Window {
                branch,
                snapshot,
                skipped,
            } => {
                for entry in snapshot.entries.iter().skip(skipped) {
                    cancellation
                        .check()
                        .change_context(ProcessorStatePageError::Admission)?;
                    let input = held_row_bytes(&entry.record);
                    let arguments = held_row_bytes(&entry.arguments);
                    let bytes = add_bytes(input, arguments)?;
                    let fixed = page_memory::slots::<u64>(2)?;
                    let bytes = add_bytes(bytes, fixed)?;
                    let first = self.windows.is_empty();
                    let columns = page_memory::payload_row(&entry.record, first)?;
                    let columns =
                        add_bytes(columns, page_memory::payload_row(&entry.arguments, first)?)?;
                    let columns = add_bytes(columns, page_memory::fixed_columns(fixed, 2, first)?)?;
                    let growth = page_memory::VecGrowth::next(&self.windows, 1)?;
                    let row = PublishedRowCharge {
                        logical: bytes,
                        columns,
                        slot: growth.bytes,
                        body: bytes,
                    };
                    if !self.admit(branch, ProcessorStateSection::Rows, row)? {
                        break;
                    }
                    growth.allocate(&mut self.windows);
                    self.windows.push(WindowRow {
                        entry: entry.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// One counted branch's rows, waiting for every selected branch to be counted before any is
/// admitted into the page.
enum CountedRows<'a> {
    Keys {
        branch: &'a Option<BranchKey>,
        keys: &'a [PublishedDeduplicatorKey],
        skipped: usize,
    },
    Window {
        branch: &'a Option<BranchKey>,
        snapshot: &'a WindowProcessorStateSnapshot,
        skipped: usize,
    },
}
