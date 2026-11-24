//! Layer: data plane.
//! Owns: admission and selection of registered inference publications from one installed route cut.
//! Depends on: typed placements, immutable routes, native generations and replication progress.
//! Must not know: Models, Plexus, tensor execution or external presentation.

use super::*;

impl Runtime {
    /// Read one page of an inferencer unit's state, drawing its charge through `executor`.
    pub(crate) async fn local_inferencer_state_page(
        &self,
        executor: &Executor,
        read: ProcessorStatePageRead<'_>,
    ) -> ReadResult<ProcessorStatePage> {
        let ProcessorStatePageRead {
            domain,
            processor,
            unit,
            selection,
            after,
            bounds,
        } = read;
        let routing = self
            .domain_routing(domain)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?
            .load_full();
        let expected = self
            .state_placement_for_unit(
                domain,
                RuntimeStateKind::Inferencer,
                ModelKind::Inferencer,
                &processor.identifier,
                unit,
                None,
            )
            .change_context(ProcessorStatePageError::Unavailable)?;
        let slot = self
            .inner
            .state_replication_routing
            .assignment(&expected)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
        let assignment = slot
            .load_full()
            .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
        let executor = executor.clone();
        let capture = InferencerPageCapture {
            runtime: self.clone(),
            routing,
            expected,
            assignment,
            processor: processor.clone(),
            unit,
            selection,
            after,
            bounds,
        };
        // A page whose capture outgrows the room its class has now gives back everything it holds
        // and is captured again under the charge it needs, never waiting while it holds any.
        executor
            .run_cpu_growing(
                CpuClass::Bulk,
                MemoryClass::Bulk,
                1,
                capture,
                |capture, memory, cancellation| {
                    let page = capture.page(memory, cancellation);
                    Attempt::of(page, capture)
                },
            )
            .await
            .change_context(ProcessorStatePageError::Admission)?
    }
}

/// Everything one inferencer page is captured from, kept whole so a capture short of room can run
/// again from the same cut.
struct InferencerPageCapture {
    runtime: Runtime,
    routing: StdArc<crate::runtime::domain_execution::DomainRoutingSnapshot>,
    expected: crate::runtime::state_store::RuntimeStatePlacement,
    assignment: StdArc<crate::runtime::state_assignment::ScheduledStateAssignment>,
    processor: NodeRef,
    unit: Unit,
    selection: ProcessorStateSelection,
    after: Option<(Option<BranchKeyFingerprint>, u64)>,
    bounds: ProcessorStatePageBounds,
}

impl InferencerPageCapture {
    /// One attempt at the page under `memory`, which it grows as it captures. A growth its class
    /// has no room for now ends the attempt with the [`nervix_execution::Shortfall`] it needed.
    fn page(
        &self,
        memory: Reservation,
        cancellation: &Cancellation,
    ) -> ReadResult<ProcessorStatePage> {
        let mut memory = GrowingCharge::new(memory);
        let installed = self
            .routing
            .processor_plans
            .get(&NodeUnitRef::new(self.processor.clone(), self.unit))
            .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
        let template = installed
            .template
            .processors
            .get(&self.processor.identifier)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
        let RelayProcessorOperationTemplate::Inferencer {
            state: Some(plan), ..
        } = &template.operation
        else {
            return Err(Report::new(ProcessorStatePageError::Unrestored));
        };
        let owners = self
            .assignment
            .checkpoint_owners
            .as_ref()
            .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
        // A retained publication can outlive the owning task's next replacement. Charge
        // its complete native payload and shape footprint before retaining that handle.
        let retained_bytes = plan
            .snapshot_bytes()
            .change_context(ProcessorStatePageError::Admission)?;
        let mut captured = Vec::new();
        // A read during repartitioning asks for every key this unit holds, with the
        // inherited origin of each, so the reader can place keys the transition moves.
        let mut catalog = match &self.selection.partition_exclusions {
            Some(_) => nervix_models::StateInspectionKeyCatalog::requested(),
            None => nervix_models::StateInspectionKeyCatalog::Unrequested,
        };
        self.runtime
            .inner
            .state_replication_routing
            .visit_inferencers(|state| {
                cancellation
                    .check()
                    .change_context(ProcessorStatePageError::Admission)?;
                let Some(state) = state else {
                    return Ok(());
                };
                let placement = &state.placement;
                if placement.domain != self.expected.domain
                    || placement.kind != self.expected.kind
                    || placement.identifier != self.expected.identifier
                    || placement.unit != self.unit
                    || placement.state != self.expected.state
                    || !selected(self.selection.branch, &placement.branch_key)
                {
                    return Ok(());
                }
                let generation = state.generations.load();
                if generation.value.is_none() {
                    return Ok(());
                }
                if let Some(key) = &placement.branch_key {
                    if catalog.is_requested() {
                        // Charge a sparse BTree node before inserting a fingerprint.
                        let bytes = u64::try_from(std::mem::size_of::<BranchKeyFingerprint>())
                            .assured("a fingerprint's size fits u64")
                            .checked_mul(16)
                            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
                        reserve(&mut memory, bytes)?;
                        catalog.insert(key.fingerprint(), key.inherited_origin());
                    }
                    if let Some(excluded) = &self.selection.partition_exclusions
                        && excluded.contains(&key.fingerprint())
                    {
                        return Ok(());
                    }
                }
                let key_bytes = match &placement.branch_key {
                    Some(key) => u64::try_from(key.as_str().len())
                        .assured("a resident branch key fits u64")
                        .checked_mul(4)
                        .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?,
                    None => 0,
                };
                reserve(
                    &mut memory,
                    checked_add(
                        checked_add(retained_bytes, key_bytes)?,
                        u64::try_from(std::mem::size_of::<CapturedBranch>())
                            .assured("resident type sizes fit u64")
                            .checked_mul(4)
                            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?,
                    )?,
                )?;
                captured.push(CapturedBranch::capture(
                    state,
                    generation,
                    &owners.replicas,
                )?);
                Ok(())
            })?;
        captured.sort_by(|left, right| BranchKey::canonical_order(&left.key, &right.key));
        page::build(
            captured,
            plan,
            catalog,
            self.after,
            self.bounds,
            memory,
            cancellation,
        )
    }
}

fn selected(selection: StateInspectionBranchSelection, key: &Option<BranchKey>) -> bool {
    match selection {
        StateInspectionBranchSelection::All => true,
        StateInspectionBranchSelection::Unbranched => key.is_none(),
        StateInspectionBranchSelection::Branch(wanted) => {
            key.as_ref().is_some_and(|key| key.fingerprint() == wanted)
        }
    }
}

impl CapturedBranch {
    fn capture(
        state: &ReplicatedInferencerState,
        generation: StdArc<Generation<Option<InferencerGeneration>>>,
        replicas: &BTreeSet<ClusterNodeName>,
    ) -> ReadResult<Self> {
        let confirmed = state
            .replication()
            .with_progress(|progress| progress.holding(replicas, generation.revision));
        Ok(Self {
            key: state.placement.branch_key.clone(),
            registration: state
                .inspection_identity()
                .ok_or_else(|| Report::new(ProcessorStatePageError::Unrestored))?,
            persisted: state
                .generations
                .last_persisted_lsm()
                .min(generation.revision),
            confirmed: u64::try_from(confirmed).assured("a resident replica count fits u64"),
            replicas: u64::try_from(replicas.len()).assured("a resident replica set fits u64"),
            generation,
        })
    }
}
