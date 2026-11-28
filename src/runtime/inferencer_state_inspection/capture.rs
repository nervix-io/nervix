//! Layer: data plane.
//! Owns: admission and selection of registered inference publications from one installed route cut.
//! Depends on: typed placements, immutable routes, native generations and replication progress.
//! Must not know: Models, Plexus, tensor execution or external presentation.

use super::*;

impl Runtime {
    /// Reads one page of an inferencer unit's state, drawing its charges through `executor`, the
    /// allowance its caller runs the read within.
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
        let memory = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
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
                let RelayProcessorOperationTemplate::Inferencer {
                    state: Some(plan), ..
                } = &template.operation
                else {
                    return Err(Report::new(ProcessorStatePageError::Unrestored));
                };
                let owners = assignment
                    .checkpoint_owners
                    .as_ref()
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
                let mut captured = Vec::new();
                // A read during repartitioning asks for every key this unit holds, with the
                // inherited origin of each, so the reader can place keys the transition moves.
                let mut catalog = match &selection.partition_exclusions {
                    Some(_) => nervix_models::StateInspectionKeyCatalog::requested(),
                    None => nervix_models::StateInspectionKeyCatalog::Unrequested,
                };
                // The first pass counts and identifies every selected branch and retains none of
                // their publications, which only the branches the page returns tensors from need.
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
                        if !inspected(placement, &expected, unit, selection.branch) {
                            return Ok(());
                        }
                        let generation = state.generations.load();
                        if generation.value.is_none() {
                            return Ok(());
                        }
                        if let Some(key) = &placement.branch_key {
                            if catalog.is_requested() {
                                // Charge a sparse BTree node before inserting a fingerprint.
                                let bytes =
                                    u64::try_from(std::mem::size_of::<BranchKeyFingerprint>())
                                        .assured("a fingerprint's size fits u64")
                                        .checked_mul(16)
                                        .ok_or_else(|| {
                                            Report::new(ProcessorStatePageError::Admission)
                                        })?;
                                reserve(&mut memory, bytes)?;
                                catalog.insert(key.fingerprint(), key.inherited_origin());
                            }
                            if let Some(excluded) = &selection.partition_exclusions
                                && excluded.contains(&key.fingerprint())
                            {
                                return Ok(());
                            }
                        }
                        super::super::processor_state_inspection::memory::VecGrowth::reserve(
                            &mut memory,
                            &mut captured,
                        )?;
                        captured.push(CapturedBranch::capture(
                            state,
                            &generation,
                            &owners.replicas,
                        )?);
                        Ok(())
                    })?;
                // The selected unit registers at most one publication per typed branch key.
                captured.sort_unstable_by(|left, right| {
                    BranchKey::canonical_order(&left.key, &right.key)
                });
                let captured = page::CapturedBranches {
                    branches: captured,
                    catalog,
                };
                // The second pass reads again, with its publication, each branch the page returns
                // tensors from, once the page has charged for retaining it, through the route its
                // placement names.
                let retain = |branch: &CapturedBranch| -> ReadResult<Option<RetainedBranch>> {
                    let placement = RuntimeStatePlacement {
                        branch_key: branch.key.clone(),
                        ..expected.clone()
                    };
                    let Some(state) = runtime
                        .inner
                        .state_replication_routing
                        .inferencer(&placement)
                    else {
                        return Ok(None);
                    };
                    let generation = state.generations.load();
                    if generation.value.is_none() {
                        return Ok(None);
                    }
                    let retained = RetainedBranch::retain(&state, generation, &owners.replicas)?;
                    Ok(Some(retained))
                };
                page::build(captured, retain, plan, after, bounds, memory, cancellation)
            })
            .await
            .change_context(ProcessorStatePageError::Admission)?
    }
}

/// Whether `placement` is a branch of the inspected unit's state, `expected`, that `selection`
/// selects.
fn inspected(
    placement: &RuntimeStatePlacement,
    expected: &RuntimeStatePlacement,
    unit: Unit,
    selection: StateInspectionBranchSelection,
) -> bool {
    placement.domain == expected.domain
        && placement.kind == expected.kind
        && placement.identifier == expected.identifier
        && placement.unit == unit
        && placement.state == expected.state
        && selected(selection, &placement.branch_key)
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
    /// `state`'s identity at `generation`, its current publication, without retaining it.
    fn capture(
        state: &ReplicatedInferencerState,
        generation: &Generation<Option<InferencerGeneration>>,
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
            revision: generation.revision,
            persisted: state
                .generations
                .last_persisted_lsm()
                .min(generation.revision),
            confirmed: u64::try_from(confirmed).assured("a resident replica count fits u64"),
            replicas: u64::try_from(replicas.len()).assured("a resident replica set fits u64"),
        })
    }
}

impl RetainedBranch {
    /// `state`'s identity at `generation`, with that publication retained.
    fn retain(
        state: &ReplicatedInferencerState,
        generation: StdArc<Generation<Option<InferencerGeneration>>>,
        replicas: &BTreeSet<ClusterNodeName>,
    ) -> ReadResult<Self> {
        let branch = CapturedBranch::capture(state, &generation, replicas)?;
        Ok(Self { branch, generation })
    }
}
