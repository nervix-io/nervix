//! Layer: data plane.
//! Owns: tensor-row paging, complete selected counts and the exact immutable-cut fingerprint.
//! Depends on: admitted native publications, validated state plans and Arrow column builders.
//! Must not know: Models, Plexus, transport, Row frames or rendering.

use columns::{HeaderCapacity, TensorCapacity};

use super::{super::processor_state_inspection::memory::slots, *};

/// What every selected branch read of its state before the page retains any publication.
pub(super) struct CapturedBranches {
    /// Every selected branch with a publication, in canonical key order.
    pub(super) branches: Vec<CapturedBranch>,
    /// The keys a repartitioning read asked for, with the inherited origin of each.
    pub(super) catalog: nervix_models::StateInspectionKeyCatalog,
}

struct Page {
    memory: Reservation,
    start: u64,
    skip: u64,
    rows_left: usize,
    bytes_left: u64,
    returned: u64,
    previous: Option<BranchKeyFingerprint>,
    counts: StateInspectionCounts,
    /// The bindings each selected branch returns, in the branches' order.
    planned: Vec<std::ops::Range<usize>>,
    tensor_rows: Vec<usize>,
    headers: HeaderCapacity,
    /// What retaining one branch's publication takes while the page copies its tensors.
    retained_bytes: u64,
    /// The charge reused for the one branch publication held while its tensors are copied.
    retained: u64,
    reply: PageReply,
    /// The bytes set aside so far for the bodies of a reply to another node.
    bodies: u64,
}

/// One page of the selected branches' tensors. It counts every branch in `captured` and admits
/// tensor rows, past those a resumed page skips, while its bounds and the bulk memory left can hold
/// each; every row after its first is optional. `retain` then reads again, with its publication,
/// each branch whose tensors the page returns, in order, or `None` for one whose publication went
/// away, and the page copies the branch's tensors from that publication before it retains the
/// next.
pub(super) fn build(
    captured: CapturedBranches,
    retain: impl FnMut(&CapturedBranch) -> ReadResult<Option<RetainedBranch>>,
    plan: &InferencerStatePlan,
    after: Option<(Option<BranchKeyFingerprint>, u64)>,
    bounds: ProcessorStatePageBounds,
    memory: Reservation,
    cancellation: &Cancellation,
) -> ReadResult<ProcessorStatePage> {
    let mut page = Page::new(plan, captured.branches.len(), after, bounds, memory)?;
    for branch in &captured.branches {
        cancellation
            .check()
            .change_context(ProcessorStatePageError::Admission)?;
        page.count(branch, plan, cancellation)?;
    }
    page.check_position(after)?;
    page.finish(captured, retain, plan, cancellation)
}

impl Page {
    fn new(
        plan: &InferencerStatePlan,
        branches: usize,
        after: Option<(Option<BranchKeyFingerprint>, u64)>,
        bounds: ProcessorStatePageBounds,
        mut memory: Reservation,
    ) -> ReadResult<Self> {
        let bindings = plan.bindings.len();
        let sections = bindings
            .checked_add(1)
            .assured("a resident declaration and its header fit usize");
        let headers = HeaderCapacity::default();
        let fixed_tensor = checked_add(TensorCapacity::objects()?, slots::<i32>(4)?)?;
        let tensors = u64::try_from(bindings).assured("a resident state declaration fits u64");
        let columns = tensors
            .checked_mul(fixed_tensor)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let mut fixed = 0;
        for bytes in [
            slots::<std::ops::Range<usize>>(branches)?,
            slots::<usize>(bindings)?,
            slots::<(columns::Tensors, TensorCapacity)>(bindings)?,
            slots::<ProcessorStateSectionColumns>(sections)?,
            headers.allocation()?,
            columns,
        ] {
            fixed = checked_add(fixed, bytes)?;
        }
        reserve(&mut memory, fixed)?;
        // A retained publication can outlive the owning task's next replacement. A page charges
        // its complete native payload and shape footprint before retaining that handle.
        let retained_bytes = plan
            .snapshot_bytes()
            .change_context(ProcessorStatePageError::Admission)?;
        let start = match after {
            Some((_, offset)) => offset,
            None => 0,
        };
        let mut page = Self {
            memory,
            start,
            skip: start,
            rows_left: bounds.rows,
            bytes_left: bounds.bytes,
            returned: 0,
            previous: None,
            counts: StateInspectionCounts::default(),
            planned: Vec::with_capacity(branches),
            tensor_rows: vec![0; bindings],
            headers,
            retained_bytes,
            retained: 0,
            reply: bounds.reply,
            bodies: 0,
        };
        if let PageReply::Remote { descriptor, .. } = bounds.reply {
            reserve(&mut page.memory, descriptor)?;
        }
        Ok(page)
    }

    /// Counts one selected `branch` into the complete counts and plans which of its tensors the
    /// page returns.
    fn count(
        &mut self,
        branch: &CapturedBranch,
        plan: &InferencerStatePlan,
        cancellation: &Cancellation,
    ) -> ReadResult<()> {
        let fingerprint = branch.key.as_ref().map(BranchKey::fingerprint);
        self.counts.branches = checked_add(self.counts.branches, u64::from(fingerprint.is_some()))?;
        let entries = u64::try_from(plan.bindings.len()).assured("a state declaration fits u64");
        self.counts.rows = checked_add(self.counts.rows, entries)?;
        self.counts.bytes = checked_add(self.counts.bytes, plan.bytes_per_branch)?;
        if self.skip > 0 && self.skip <= entries {
            self.previous = fingerprint;
        }
        let skipped = self.skip.min(entries);
        self.skip = self
            .skip
            .checked_sub(skipped)
            .verified("skipped is bounded by the remaining offset");
        let first = usize::try_from(skipped).assured("a skip within a declaration fits usize");
        let mut end = first;
        for (index, binding) in plan.bindings.iter().enumerate().skip(first) {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            if !self.admit(index, binding, branch, end == first)? {
                break;
            }
            end = end
                .checked_add(1)
                .assured("a branch returns no more tensors than it declares");
        }
        assert!(self.planned.len() < self.planned.capacity());
        self.planned.push(first..end);
        Ok(())
    }

    /// Admits one of `branch`'s tensor rows, if the page's bounds and the bulk memory left can hold
    /// it now. The branch's `first` returned row also retains the branch's publication while the
    /// page copies its tensors and, for another node, sets aside the body of its header row.
    fn admit(
        &mut self,
        index: usize,
        binding: &InferencerStatePortPlan,
        branch: &CapturedBranch,
        first: bool,
    ) -> ReadResult<bool> {
        if self.rows_left == 0 {
            return Ok(false);
        }
        let rows = self.tensor_rows[index];
        let next = rows
            .checked_add(1)
            .assured("the page's row limit bounds its tensors");
        let before = TensorCapacity::new(binding, rows)?;
        let after = TensorCapacity::new(binding, next)?;
        let bytes = after
            .body()?
            .checked_sub(before.body()?)
            .assured("one more row grows the body");
        if bytes > self.bytes_left {
            self.rows_left = 0;
            return Ok(false);
        }
        let tensors = after
            .buffers()?
            .checked_sub(before.buffers()?)
            .assured("one more row grows its buffers");
        let headers = if first {
            self.headers.with_branch(branch)?
        } else {
            self.headers
        };
        let header = headers
            .buffers()?
            .checked_sub(self.headers.buffers()?)
            .assured("an added header grows its buffers");
        let runs = slots::<ProcessorStateRun>(if first { 2 } else { 1 })?;
        let working = checked_add(checked_add(tensors, header)?, runs)?;
        // The copy loop releases each publication before borrowing the next one.
        let retained = if self.returned == 0 {
            self.retained_bytes
        } else {
            0
        };
        let body = self.row_body_bytes(bytes, branch, first, rows == 0)?;
        let bodies = checked_add(self.bodies, body)?;
        if self.returned != 0 && !self.reply.optional_bodies_fit(bodies) {
            self.rows_left = 0;
            return Ok(false);
        }
        let charge = checked_add(checked_add(working, retained)?, body)?;
        let target = checked_add(self.memory.bytes(), charge)?;
        // The page's first row is required: a share without room for it runs the read again
        // within a larger one. Every later row joins only while the share has room for it now.
        if self.returned == 0 {
            self.memory
                .grow_to(target)
                .change_context(ProcessorStatePageError::Admission)?;
        } else if self.memory.try_grow_to(target).is_err() {
            self.rows_left = 0;
            return Ok(false);
        }
        self.bytes_left = self
            .bytes_left
            .checked_sub(bytes)
            .verified("the row fits the page budget");
        self.rows_left = self
            .rows_left
            .checked_sub(1)
            .verified("a full page returned above");
        self.returned = checked_add(self.returned, 1)?;
        self.tensor_rows[index] = next;
        self.headers = headers;
        self.retained = checked_add(self.retained, retained)?;
        self.bodies = bodies;
        Ok(true)
    }

    fn row_body_bytes(
        &self,
        tensor_bytes: u64,
        branch: &CapturedBranch,
        first_for_branch: bool,
        first_for_tensor: bool,
    ) -> ReadResult<u64> {
        let PageReply::Remote { framing, .. } = self.reply else {
            return Ok(0);
        };
        let mut body = tensor_bytes;
        if first_for_tensor {
            body = checked_add(body, framing)?;
        }
        if first_for_branch {
            body = checked_add(body, columns::Headers::row_bytes(branch))?;
            if self.returned == 0 {
                body = checked_add(body, framing)?;
            }
        }
        Ok(body)
    }

    /// Refuses a resumed page whose position does not belong to the counted branches.
    fn check_position(&self, after: Option<(Option<BranchKeyFingerprint>, u64)>) -> ReadResult<()> {
        if self.start > self.counts.rows || after.is_some_and(|(key, _)| key != self.previous) {
            return Err(Report::new(ProcessorStatePageError::Position));
        }
        Ok(())
    }

    /// The page, in the branches' order: every branch's identity in its fingerprint and, for each
    /// branch it returns tensors from, those tensors and the branch's header, copied from the
    /// publication `retain` reads again for it. A branch whose publication went away since it was
    /// counted ends the page before it.
    fn finish(
        self,
        captured: CapturedBranches,
        mut retain: impl FnMut(&CapturedBranch) -> ReadResult<Option<RetainedBranch>>,
        plan: &InferencerStatePlan,
        cancellation: &Cancellation,
    ) -> ReadResult<ProcessorStatePage> {
        let CapturedBranches { branches, catalog } = captured;
        let mut digest = nervix_crypto::digest::Sha256::new();
        digest.update(b"nervix/state-inspection/inference-publications");
        let mut generation = 0_u64;
        let mut headers = columns::Headers::new(self.headers);
        let mut tensors = Vec::with_capacity(plan.bindings.len());
        for (index, binding) in plan.bindings.iter().enumerate() {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            let capacity = TensorCapacity::new(binding, self.tensor_rows[index])?;
            tensors.push((columns::Tensors::new(index, &capacity)?, capacity));
        }
        let returned = usize::try_from(self.returned).assured("a page's row limit fits usize");
        let run_capacity = returned
            .checked_add(self.headers.rows)
            .assured("the admitted runs fit usize");
        let mut runs = Vec::with_capacity(run_capacity);
        let mut filled = 0_u64;
        let mut last = None;
        let mut ended = false;
        for (branch, planned) in branches.iter().zip(&self.planned) {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            let fingerprint = branch.key.as_ref().map(BranchKey::fingerprint);
            let mut publication = None;
            if !planned.is_empty() && !ended {
                match retain(branch)? {
                    Some(retained) => publication = Some(retained),
                    None => ended = true,
                }
            }
            // A branch the page returns tensors from is named by the cut it copies them from.
            let identity = match &publication {
                Some(retained) => &retained.branch,
                None => branch,
            };
            match fingerprint {
                Some(key) => {
                    digest.update(&[1]);
                    digest.update(key.fingerprint());
                }
                None => digest.update(&[0]),
            }
            digest.update(&identity.registration);
            digest.update(&identity.revision.to_be_bytes());
            generation = generation.max(identity.revision);
            let Some(retained) = publication else {
                continue;
            };
            for index in planned.clone() {
                cancellation
                    .check()
                    .change_context(ProcessorStatePageError::Admission)?;
                tensors[index]
                    .0
                    .append(retained.value()?, &plan.bindings[index], cancellation)?;
                let section =
                    u32::try_from(index).change_context(ProcessorStatePageError::Columns)?;
                extend_runs(
                    &mut runs,
                    ProcessorStateSection::Tensor(section),
                    &branch.key,
                );
                filled = checked_add(filled, 1)?;
            }
            headers.append(&retained, plan.bytes_per_branch)?;
            last = fingerprint;
        }
        // Header runs precede all tensor runs. Append the headers for the copied prefix and move
        // them to the front in place, using the one admitted run allocation.
        let tensor_runs = runs.len();
        let mut tensors_left = filled;
        for (branch, planned) in branches.iter().zip(&self.planned) {
            if tensors_left == 0 {
                break;
            }
            if !planned.is_empty() {
                extend_runs(&mut runs, ProcessorStateSection::Header, &branch.key);
                let copied =
                    u64::try_from(planned.len()).assured("a branch's tensor count fits u64");
                tensors_left = tensors_left
                    .checked_sub(copied)
                    .assured("each copied branch supplied its whole planned range");
            }
        }
        assert_eq!(
            runs.capacity(),
            run_capacity,
            "the declared run allocation did not grow"
        );
        runs.rotate_left(tensor_runs);
        // Every returned branch's tensors are copied, so the page retains no publication any more.
        let (publications_charge, memory) = self
            .memory
            .split(self.retained)
            .change_context(ProcessorStatePageError::Admission)?;
        drop(publications_charge);
        let total = u64::try_from(branches.len()).assured("an admitted branch count fits u64");
        let capacity = tensors
            .len()
            .checked_add(1)
            .assured("a resident state declaration and its header fit usize");
        let mut sections = Vec::with_capacity(capacity);
        sections.push(headers.finish(total, self.headers)?);
        for (tensor, capacity) in tensors {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            sections.push(tensor.finish(total, &capacity)?);
        }
        let offset = checked_add(self.start, filled)?;
        let (reservation, reply) = match self.reply {
            PageReply::Local => (memory, None),
            PageReply::Remote {
                descriptor,
                framing,
                ..
            } => {
                let (descriptor, rest) = memory
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
            partition_catalog: catalog,
            fingerprint: digest.finish(),
            generation,
            counts: self.counts,
            offset,
            last_branch: last,
            more: offset < self.counts.rows,
            runs,
            sections,
            memory: Some(ProcessorStatePageMemory {
                _reservation: reservation,
                _windows: Vec::new(),
                reply,
            }),
        })
    }
}
