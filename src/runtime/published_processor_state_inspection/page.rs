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
            reply: bounds.reply,
            bodies: 0,
        };
        page.set_aside_reply(&plan, metadata)?;
        page.digest
            .update(b"nervix/state-inspection/published-processors");
        // Every selected branch is counted, and its metadata charged, before any retained row is
        // admitted. A page ends before a row the bulk class cannot hold, so rows never take the
        // memory the complete counts and every branch's metadata need.
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
            page.admit_rows(rows, cancellation)?;
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
        page.finish(offset, sections, windows)
    }

    /// Sets aside, for a page another node reads and before anything else, what encoding its
    /// descriptor takes and, when it can return rows (`metadata`), the framing of each section's
    /// body. A page its owner presents sets nothing aside.
    fn set_aside_reply(&mut self, plan: &ReadPlan, metadata: bool) -> ReadResult<()> {
        let PageReply::Remote {
            descriptor,
            framing,
        } = self.reply
        else {
            return Ok(());
        };
        reserve_bytes(&mut self.memory, descriptor)?;
        if metadata {
            let framing = framing
                .checked_mul(plan.sections())
                .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
            self.set_aside_bodies(framing)?;
        }
        Ok(())
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
            runs: self.metadata_runs,
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

    /// Reserves one metadata row's `bytes`, and as much again for the body that carries it when
    /// another node reads the page.
    pub(super) fn reserve_metadata(&mut self, bytes: u64) -> ReadResult<()> {
        reserve_bytes(&mut self.memory, bytes)?;
        if let PageReply::Remote { .. } = self.reply {
            self.set_aside_bodies(bytes)?;
        }
        Ok(())
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

    /// Whether the next row of `section`, of `bytes` payload bytes and a resident item charging
    /// `item` bytes, joins the page. The page ends before the row at its row limit, at its byte
    /// allowance, and where the bulk class cannot hold everything the row needs to finish: its
    /// columns and, when another node reads the page, its body. That leaves the read a
    /// continuation. A page that has no row yet refuses instead, since it has nothing to return.
    fn admit(
        &mut self,
        key: &Option<BranchKey>,
        section: ProcessorStateSection,
        bytes: u64,
        item: u64,
    ) -> ReadResult<bool> {
        if self.rows_left == 0 || bytes > self.bytes_left {
            self.rows_left = 0;
            return Ok(false);
        }
        // Column gathering can retain both the selected views and their contiguous output buffers.
        let columns = bytes
            .checked_mul(2)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let charge = add_bytes(columns, item_bytes::<ProcessorStateRun>()?)?;
        let charge = add_bytes(charge, item)?;
        // A row another node reads also sets its bytes aside for the body that carries it.
        let body = match self.reply {
            PageReply::Local => 0,
            PageReply::Remote { .. } => bytes,
        };
        let charge = add_bytes(charge, body)?;
        let target = add_bytes(self.memory.bytes(), charge)?;
        if let Err(refusal) = self.memory.grow_to(target) {
            if self.returned == 0 {
                return Err(refusal.change_context(ProcessorStatePageError::Admission));
            }
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
        self.bodies = add_bytes(self.bodies, body)?;
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
    fn admit_rows(&mut self, rows: CountedRows<'_>, cancellation: &Cancellation) -> ReadResult<()> {
        match rows {
            CountedRows::Keys {
                branch,
                keys,
                skipped,
            } => {
                for key in keys.iter().skip(skipped) {
                    cancellation
                        .check()
                        .change_context(ProcessorStatePageError::Admission)?;
                    let bytes = key.key.estimated_bytes();
                    let item = item_bytes::<KeyRow>()?;
                    if !self.admit(branch, ProcessorStateSection::Keys, bytes, item)? {
                        break;
                    }
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
                    let bytes = add_bytes(bytes, 16)?;
                    let item = item_bytes::<WindowRow>()?;
                    if !self.admit(branch, ProcessorStateSection::Rows, bytes, item)? {
                        break;
                    }
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
