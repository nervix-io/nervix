//! Layer: data plane.
//! Owns: admission and selection of registered inference publications from one installed route cut.
//! Depends on: typed placements, immutable routes, native generations and replication progress.
//! Must not know: Models, Plexus, tensor execution or external presentation.

use super::*;

impl Runtime {
    pub(crate) async fn local_inferencer_state_page(
        &self,
        domain: &DomainName,
        processor: &NodeRef,
        unit: Unit,
        selection: ProcessorStateSelection,
        after: Option<(Option<BranchKeyFingerprint>, u64)>,
        bounds: ProcessorStatePageBounds,
    ) -> ReadResult<ProcessorStatePage> {
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
        let executor = self.inner.executor.clone();
        let memory = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .change_context(ProcessorStatePageError::Admission)?;
        let runtime = self.clone();
        let processor = processor.clone();
        executor
            .run_cpu(CpuClass::Bulk, memory, move |mut memory, cancellation| {
                let installed = routing
                    .processor_plans
                    .get(&NodeUnitRef::new(processor.clone(), unit))
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
                let template = installed
                    .template
                    .processors
                    .get(&processor.identifier)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
                let RelayProcessorOperationTemplate::StatefulInferencer { state: plan, .. } =
                    &template.operation
                else {
                    return Err(Report::new(ProcessorStatePageError::Unrestored));
                };
                let owners = assignment
                    .checkpoint_owners
                    .as_ref()
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
                // A retained publication can outlive the owning task's next replacement. Charge
                // its complete native payload and shape footprint before retaining that handle.
                let retained_bytes = plan
                    .snapshot_bytes()
                    .change_context(ProcessorStatePageError::Admission)?;
                let mut captured = Vec::new();
                let mut catalog = selection
                    .partition_exclusions
                    .as_ref()
                    .map(|_| BTreeSet::new());
                runtime
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
                        if placement.domain != expected.domain
                            || placement.kind != expected.kind
                            || placement.identifier != expected.identifier
                            || placement.unit != unit
                            || placement.state != expected.state
                            || !selected(selection.branch, &placement.branch_key)
                        {
                            return Ok(());
                        }
                        let generation = state.generations.load();
                        if generation.value.is_none() {
                            return Ok(());
                        }
                        if let Some(key) = &placement.branch_key {
                            if let Some(catalog) = &mut catalog {
                                // Charge a sparse BTree node before inserting a fingerprint.
                                let bytes =
                                    u64::try_from(std::mem::size_of::<BranchKeyFingerprint>())
                                        .assured("a fingerprint's size fits u64")
                                        .checked_mul(16)
                                        .ok_or_else(|| {
                                            Report::new(ProcessorStatePageError::Admission)
                                        })?;
                                reserve(&mut memory, bytes)?;
                                catalog.insert(key.fingerprint());
                            }
                            if let Some(excluded) = &selection.partition_exclusions
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
                                    .ok_or_else(|| {
                                        Report::new(ProcessorStatePageError::Admission)
                                    })?,
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
                page::build(captured, plan, catalog, after, bounds, memory, cancellation)
            })
            .await
            .change_context(ProcessorStatePageError::Admission)?
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
