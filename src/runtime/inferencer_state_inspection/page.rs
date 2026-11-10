//! Layer: data plane.
//! Owns: tensor-row paging, complete selected counts and the exact immutable-cut fingerprint.
//! Depends on: admitted native publications, validated state plans and Arrow column builders.
//! Must not know: Models, Plexus, transport, Row frames or rendering.

use super::*;

struct Page {
    memory: Reservation,
    start: u64,
    skip: u64,
    rows_left: usize,
    bytes_left: u64,
    returned: u64,
    previous: Option<BranchKeyFingerprint>,
    last: Option<BranchKeyFingerprint>,
    counts: StateInspectionCounts,
    generation: u64,
    digest: nervix_crypto::digest::Sha256,
    headers: columns::Headers,
    tensors: Vec<columns::Tensors>,
    runs: Vec<ProcessorStateRun>,
    metadata: Vec<ProcessorStateRun>,
}

pub(super) fn build(
    branches: Vec<CapturedBranch>,
    plan: &InferencerStatePlan,
    catalog: Option<BTreeSet<BranchKeyFingerprint>>,
    after: Option<(Option<BranchKeyFingerprint>, u64)>,
    bounds: ProcessorStatePageBounds,
    memory: Reservation,
    cancellation: &Cancellation,
) -> ReadResult<ProcessorStatePage> {
    let mut page = Page::new(plan, after, bounds, memory, cancellation)?;
    for branch in &branches {
        cancellation
            .check()
            .change_context(ProcessorStatePageError::Admission)?;
        page.read(branch, plan, cancellation)?;
    }
    page.finish(branches.len(), after, catalog, cancellation)
}

impl Page {
    fn new(
        plan: &InferencerStatePlan,
        after: Option<(Option<BranchKeyFingerprint>, u64)>,
        bounds: ProcessorStatePageBounds,
        mut memory: Reservation,
        cancellation: &Cancellation,
    ) -> ReadResult<Self> {
        let sections =
            u64::try_from(plan.bindings.len()).assured("a resident state declaration fits u64");
        let builders = u64::try_from(std::mem::size_of::<columns::Tensors>())
            .assured("a builder size fits u64");
        let builder_bytes = sections
            .checked_mul(builders)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let builder_bytes = builder_bytes
            .checked_mul(4)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let header_bytes = u64::try_from(std::mem::size_of::<columns::Headers>())
            .assured("a header builder size fits u64")
            .checked_mul(4)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        reserve(&mut memory, checked_add(builder_bytes, header_bytes)?)?;
        let mut tensors = Vec::with_capacity(plan.bindings.len());
        for index in 0..plan.bindings.len() {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            tensors.push(columns::Tensors::new(index)?);
        }
        let start = match after {
            Some((_, offset)) => offset,
            None => 0,
        };
        let mut digest = nervix_crypto::digest::Sha256::new();
        digest.update(b"nervix/state-inspection/inference-publications");
        Ok(Self {
            memory,
            start,
            skip: start,
            rows_left: bounds.rows,
            bytes_left: bounds.bytes,
            returned: 0,
            previous: None,
            last: None,
            counts: StateInspectionCounts::default(),
            generation: 0,
            digest,
            headers: columns::Headers::default(),
            tensors,
            runs: Vec::new(),
            metadata: Vec::new(),
        })
    }

    fn read(
        &mut self,
        branch: &CapturedBranch,
        plan: &InferencerStatePlan,
        cancellation: &Cancellation,
    ) -> ReadResult<()> {
        let fingerprint = branch.key.as_ref().map(BranchKey::fingerprint);
        match fingerprint {
            Some(key) => {
                self.digest.update(&[1]);
                self.digest.update(key.fingerprint());
            }
            None => self.digest.update(&[0]),
        }
        self.digest.update(&branch.registration);
        self.digest
            .update(&branch.generation.revision.to_be_bytes());
        self.generation = self.generation.max(branch.generation.revision);
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
        let before = self.returned;
        for (index, binding) in plan.bindings.iter().enumerate() {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            if u64::try_from(index).assured("a declaration index fits u64") < skipped {
                continue;
            }
            if !self.admit(binding)? {
                break;
            }
            self.tensors[index].append(branch.value()?, binding, cancellation)?;
            let index = u32::try_from(index).change_context(ProcessorStatePageError::Columns)?;
            extend_runs(
                &mut self.runs,
                ProcessorStateSection::Tensor(index),
                &branch.key,
            );
            self.last = fingerprint;
        }
        if self.returned != before {
            self.headers.append(branch, plan.bytes_per_branch)?;
            extend_runs(
                &mut self.metadata,
                ProcessorStateSection::Header,
                &branch.key,
            );
        }
        Ok(())
    }

    fn admit(&mut self, binding: &InferencerStatePortPlan) -> ReadResult<bool> {
        if self.rows_left == 0 {
            return Ok(false);
        }
        let values = u64::try_from(binding.elements)
            .assured("a tensor element count fits u64")
            .checked_mul(u64::try_from(std::mem::size_of::<f32>()).assured("f32 size fits u64"))
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let axes = u64::try_from(binding.schema.dimensions.len())
            .assured("tensor rank fits u64")
            .checked_mul(u64::try_from(std::mem::size_of::<u64>()).assured("u64 size fits u64"))
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let names = u64::try_from(binding.input.len()).assured("a port name fits u64");
        let names = checked_add(
            names,
            u64::try_from(binding.output.len()).assured("a port name fits u64"),
        )?;
        // Cover both column builders and their output, Row encoding scratch and branch metadata.
        let bytes = checked_add(checked_add(values, axes)?, names)?;
        let metadata = std::mem::size_of::<columns::Headers>()
            .checked_add(std::mem::size_of::<CapturedBranch>())
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let metadata = metadata
            .checked_add(std::mem::size_of::<ProcessorStateRun>())
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let metadata = u64::try_from(metadata).assured("resident metadata sizes fit u64");
        let working = checked_add(bytes, metadata)?
            .checked_mul(4)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        if working > self.bytes_left {
            self.rows_left = 0;
            return Ok(false);
        }
        reserve(&mut self.memory, working)?;
        self.bytes_left = self
            .bytes_left
            .checked_sub(working)
            .verified("the row fits the page budget");
        self.rows_left = self
            .rows_left
            .checked_sub(1)
            .verified("a full page returned above");
        self.returned = checked_add(self.returned, 1)?;
        Ok(true)
    }

    fn finish(
        mut self,
        branches: usize,
        after: Option<(Option<BranchKeyFingerprint>, u64)>,
        catalog: Option<BTreeSet<BranchKeyFingerprint>>,
        cancellation: &Cancellation,
    ) -> ReadResult<ProcessorStatePage> {
        if self.start > self.counts.rows || after.is_some_and(|(key, _)| key != self.previous) {
            return Err(Report::new(ProcessorStatePageError::Position));
        }
        let total = u64::try_from(branches).assured("an admitted branch count fits u64");
        let capacity = self
            .tensors
            .len()
            .checked_add(1)
            .assured("a resident state declaration and its header fit usize");
        let mut sections = Vec::with_capacity(capacity);
        sections.push(self.headers.finish(total));
        for tensor in self.tensors {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            sections.push(tensor.finish(total));
        }
        self.metadata.append(&mut self.runs);
        let offset = checked_add(self.start, self.returned)?;
        Ok(ProcessorStatePage {
            partition_catalog: catalog,
            fingerprint: self.digest.finish(),
            generation: self.generation,
            counts: self.counts,
            offset,
            last_branch: self.last,
            more: offset < self.counts.rows,
            runs: self.metadata,
            sections,
            memory: Some(ProcessorStatePageMemory {
                _reservation: self.memory,
                _windows: Vec::new(),
            }),
        })
    }
}
