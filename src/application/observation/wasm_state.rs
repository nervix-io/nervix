//! Typed WASM processor guest-state inspections assembled from their unit owners' bounded pages.
//!
//! Layer: control plane.
//! - **Owns.** WASM state read planning, per-unit owner routing, continuation fencing, the owner's
//!   bounded page of committed lifetimes and sampled checkpoints, and the unit summaries a
//!   description prints.
//! - **Depends on.** The committed schedule, the runtime's checkpoint samples, the WASM state page
//!   operation and the shared inspection envelope.
//! - **Must not know.** Guest bytes, checkpoint storage or replication, or how a guest executes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns the boundary declared above")
)]

use std::{cmp::Ordering, collections::BTreeMap};

use meticulous::ResultExt as _;
use nervix_client_wire::{
    MAX_ENCODED_WASM_BRANCH_ENTRY_BYTES, SessionLimits, StateInspection, StateInspectionPage,
    StateInspectionResult, StateInspectionUnit,
};
use nervix_consensus::ConsensusRuntimeState;
use nervix_interconnect::{ReadWasmStatePage, ReadWasmStatePageResponse, WasmStatePage};
use nervix_models::{
    ClusterNodeIdentity, CreateWasmProcessor, DomainName, DomainNodeRef, ModelKind,
    ResolvedBranching, ResolvedStateBranchSelection, ScheduledNode, ShowWasmProcessorState,
    StateInspectionBranchSelection, StateInspectionCounts, StateInspectionOrigin,
    StateInspectionRejection, StateInspectionStage, Unit, WasmCheckpointInspection,
    WasmProcessorName, WasmStateBinding, WasmStateEntryKey, WasmStateInspectionError,
    WasmUnitStateInspection, WasmUnitStateSelection,
};
use nervix_primitives::time::timeout;

use super::inspection_pages::{
    add_counts, inspection_owner, inspection_refusal, rendered_inspection,
};
use crate::{
    application::{command_result::CommandResult, session_service::SessionServiceImpl},
    state_inspection_cursor::WasmStateInspectionCursor,
};

/// One read's committed processor schedule, its resolved selection and its entry budget.
struct WasmInspectionPlan {
    domain: DomainName,
    processor: WasmProcessorName,
    node: ScheduledNode,
    binding: WasmStateBinding,
    branched: bool,
    control_revision: u64,
    selection: StateInspectionBranchSelection,
    entries: usize,
    cursor: Option<WasmStateInspectionCursor>,
    limits: SessionLimits,
}

/// Where one unit's entries start in this read.
struct UnitResume {
    /// Every entry of the unit was returned by an earlier page.
    skip: bool,
    after: Option<WasmStateEntryKey>,
}

impl UnitResume {
    fn of(cursor: Option<&WasmStateInspectionCursor>, unit: Unit) -> Self {
        let Some(cursor) = cursor else {
            return Self {
                skip: false,
                after: None,
            };
        };
        match unit.cmp(&cursor.unit) {
            Ordering::Less => Self {
                skip: true,
                after: None,
            },
            Ordering::Equal => Self {
                skip: false,
                after: Some(cursor.after),
            },
            Ordering::Greater => Self {
                skip: false,
                after: None,
            },
        }
    }
}

/// The last entry a page returned, which a continuation resumes after.
#[derive(Clone, Copy)]
struct WasmStatePosition {
    unit: Unit,
    after: WasmStateEntryKey,
}

/// The read origin of one unit's complete selection on its owner: an exact fingerprint of every
/// selected fact, the newest lifetime and the newest committed revision among them.
fn wasm_origin(
    node: ClusterNodeIdentity,
    selection: &WasmUnitStateSelection,
) -> StateInspectionOrigin {
    let mut fingerprint = nervix_crypto::digest::Sha256::new();
    fingerprint.update(b"nervix/state-inspection/wasm");
    let header = serde_json::to_vec(&(selection.default_generation(), selection.reset()))
        .assured("lifetime facts are JSON values");
    fingerprint.update(
        &u64::try_from(header.len())
            .assured("a bounded header fits u64")
            .to_be_bytes(),
    );
    fingerprint.update(&header);
    let mut generation = u64::from(selection.default_generation());
    let mut revision = 0;
    for entry in selection.entries() {
        let bytes = serde_json::to_vec(entry).assured("branch entries are JSON values");
        fingerprint.update(
            &u64::try_from(bytes.len())
                .assured("a bounded entry fits u64")
                .to_be_bytes(),
        );
        fingerprint.update(&bytes);
        let Some(checkpoint) = &entry.checkpoint else {
            continue;
        };
        generation = generation.max(u64::from(checkpoint.generation));
        if let Some(committed) = checkpoint.committed_revision {
            revision = revision.max(committed.get());
        }
    }
    StateInspectionOrigin {
        node,
        fingerprint: fingerprint.finish(),
        generation,
        revision,
        stage: StateInspectionStage::Live,
    }
}

/// The summary of every unit of a scheduled WASM processor, from the checkpoints its owners
/// sampled, without branch entries.
pub(in crate::application) fn wasm_unit_summaries(
    node: &ScheduledNode,
    checkpoints: Vec<WasmCheckpointInspection>,
) -> Result<Vec<(Unit, WasmUnitStateInspection)>, WasmStateInspectionError> {
    let mut by_unit = BTreeMap::<Unit, Vec<WasmCheckpointInspection>>::new();
    for checkpoint in checkpoints {
        let Some(unit) = node.unit_of_branch(checkpoint.branch.as_ref()) else {
            continue;
        };
        by_unit.entry(unit).or_default().push(checkpoint);
    }
    let mut summaries = Vec::with_capacity(node.units.len());
    for (unit, scheduled) in &node.units {
        let sampled = by_unit.remove(unit).unwrap_or_default();
        let selection = WasmUnitStateSelection::of_unit(
            scheduled,
            sampled,
            StateInspectionBranchSelection::All,
        )?;
        summaries.push((*unit, selection.page(None, 0).state));
    }
    Ok(summaries)
}

impl SessionServiceImpl {
    pub(in crate::application) async fn show_wasm_processor_state(
        &self,
        domain: &DomainName,
        show: ShowWasmProcessorState,
    ) -> CommandResult {
        let entity =
            DomainNodeRef::node_in(domain.clone(), ModelKind::WasmProcessor, &show.processor);
        let settings = self.inner.runtime.state_inspection_settings();
        let result = timeout(
            *settings.read_timeout.value(),
            self.inspect_wasm_state(domain, &show),
        )
        .await;
        let state = StateInspection {
            entity,
            result: match result {
                Ok(Ok(page)) => StateInspectionResult::Page(page),
                Ok(Err(reason)) => StateInspectionResult::Rejected(reason),
                Err(_) => StateInspectionResult::Rejected(StateInspectionRejection::Deadline),
            },
        };
        rendered_inspection(state, show.format)
    }

    async fn wasm_inspection_plan(
        &self,
        domain: &DomainName,
        show: &ShowWasmProcessorState,
    ) -> Result<WasmInspectionPlan, StateInspectionRejection> {
        let state = self.inner.consensus.current_runtime_state().await;
        let domain_schedule = state
            .schedule
            .domain(domain)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        let processor = domain_schedule
            .scheduled::<CreateWasmProcessor>(&show.processor)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        let binding = WasmStateBinding::of_scheduled(processor.node)
            .ok_or(StateInspectionRejection::NotReady)?;
        let branching = processor
            .node
            .resolved_branching
            .as_ref()
            .ok_or(StateInspectionRejection::NotReady)?;
        let selection = match &show.selection {
            None => StateInspectionBranchSelection::All,
            Some(selection) => match selection.resolve(branching).map_err(|error| {
                inspection_refusal(error, StateInspectionRejection::InvalidSelection)
            })? {
                ResolvedStateBranchSelection::AllBranches => StateInspectionBranchSelection::All,
                ResolvedStateBranchSelection::Unbranched => {
                    StateInspectionBranchSelection::Unbranched
                }
                ResolvedStateBranchSelection::Branch { fingerprint, .. } => {
                    StateInspectionBranchSelection::Branch(fingerprint)
                }
            },
        };
        let limits = SessionLimits::DEFAULT;
        let settings = self.inner.runtime.state_inspection_settings();
        // Every returned entry fits its encoded bound, so a reply of this many entries stays
        // within the session's transfer bound by construction.
        let maximum = settings
            .max_rows
            .value()
            .get()
            .min(limits.collection_entries())
            .min(limits.transfer_bytes() / MAX_ENCODED_WASM_BRANCH_ENTRY_BYTES);
        let entries = match &show.page {
            Some(page) => usize::try_from(page.limit.get()).map_err(|error| {
                inspection_refusal(error, StateInspectionRejection::InvalidLimit)
            })?,
            None => maximum,
        };
        if entries > maximum {
            return Err(StateInspectionRejection::InvalidLimit);
        }
        let token = show.page.as_ref().and_then(|page| page.after.as_deref());
        let cursor = match token {
            Some(token) => Some(
                WasmStateInspectionCursor::decode(token, limits.string_bytes()).map_err(
                    |error| inspection_refusal(error, StateInspectionRejection::StaleContinuation),
                )?,
            ),
            None => None,
        };
        if let Some(cursor) = &cursor
            && (cursor.domain != *domain
                || cursor.processor != show.processor
                || cursor.control_revision != state.revision
                || cursor.selection != selection
                || !processor.node.units.contains_key(&cursor.unit))
        {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        Ok(WasmInspectionPlan {
            domain: domain.clone(),
            processor: show.processor.clone(),
            node: processor.node.clone(),
            binding,
            branched: matches!(branching, ResolvedBranching::Branched { .. }),
            control_revision: state.revision,
            selection,
            entries,
            cursor,
            limits,
        })
    }

    async fn inspect_wasm_state(
        &self,
        domain: &DomainName,
        show: &ShowWasmProcessorState,
    ) -> Result<StateInspectionPage, StateInspectionRejection> {
        let plan = self.wasm_inspection_plan(domain, show).await?;
        let incarnations = self.live_node_incarnations().await;
        let local = self.inner.consensus.local_node_id();
        let mut units = plan.node.units.iter().collect::<Vec<_>>();
        units.sort_by_key(|(unit, _)| **unit);
        let mut output = Vec::with_capacity(units.len());
        let mut counts = StateInspectionCounts::default();
        let mut budget = plan.entries;
        let mut more = false;
        let mut position = None;
        for (unit, placement) in &units {
            let owner = inspection_owner(&plan.node, **unit, local)?;
            let incarnation = incarnations
                .get(&owner)
                .copied()
                .ok_or(StateInspectionRejection::OwnerUnavailable)?;
            let resume = UnitResume::of(plan.cursor.as_ref(), **unit);
            let allowed = if resume.skip { 0 } else { budget };
            let request = ReadWasmStatePage {
                domain: plan.domain.clone(),
                processor: plan.processor.clone(),
                unit: **unit,
                control_revision: plan.control_revision,
                selection: plan.selection,
                after: resume.after,
                max_entries: u64::try_from(allowed).assured("an entry budget fits u64"),
                target: ClusterNodeIdentity::new(owner.clone(), incarnation),
            };
            let WasmStatePage { origin, page } = self.wasm_state_unit_page(request).await?;
            let replicas = placement
                .replica_nodes()
                .into_iter()
                .filter(|node| **node != owner)
                .cloned()
                .collect::<Vec<_>>();
            let unit_counts = StateInspectionCounts {
                branches: if plan.branched {
                    u64::try_from(page.state.entries).assured("a bounded entry count fits u64")
                } else {
                    0
                },
                rows: 0,
                bytes: 0,
            };
            add_counts(&mut counts, unit_counts)?;
            let mut state = page.state;
            if resume.skip {
                state.branches.clear();
            } else {
                budget = budget
                    .checked_sub(state.branches.len())
                    .ok_or(StateInspectionRejection::ReplyTooLarge)?;
                if let Some(last) = state.branches.last() {
                    position = Some(WasmStatePosition {
                        unit: **unit,
                        after: last.key(),
                    });
                }
                if page.more {
                    // An owner that could return none of the entries it was allowed cannot make
                    // progress, so a continuation from here would repeat this page forever.
                    if allowed > 0 && state.branches.is_empty() {
                        return Err(StateInspectionRejection::ReplyTooLarge);
                    }
                    more = true;
                    budget = 0;
                }
            }
            output.push(StateInspectionUnit {
                unit: **unit,
                owner,
                replicas,
                origin,
                counts: unit_counts,
                sections: Vec::new(),
                wasm: Some(state),
            });
        }
        let continuation = if more {
            let Some(position) = position else {
                return Err(StateInspectionRejection::ReplyTooLarge);
            };
            Some(
                WasmStateInspectionCursor {
                    domain: plan.domain,
                    processor: plan.processor,
                    control_revision: plan.control_revision,
                    selection: plan.selection,
                    unit: position.unit,
                    after: position.after,
                }
                .encode(plan.limits.string_bytes())
                .map_err(|error| {
                    inspection_refusal(error, StateInspectionRejection::ReplyTooLarge)
                })?,
            )
        } else {
            None
        };
        Ok(StateInspectionPage {
            counts,
            unit_count: u64::try_from(units.len()).assured("a bounded unit count fits u64"),
            units: output,
            continuation,
            wasm_binding: Some(plan.binding),
        })
    }

    async fn wasm_state_unit_page(
        &self,
        request: ReadWasmStatePage,
    ) -> Result<WasmStatePage, StateInspectionRejection> {
        if request.target.node_id() == self.inner.consensus.local_node_id() {
            return self.native_wasm_state_page(request).await;
        }
        let deadline = *self
            .inner
            .runtime
            .state_inspection_settings()
            .read_timeout
            .value();
        let target = request.target.clone();
        let response = match self
            .inner
            .interconnect
            .request_with_timeout(target.node_id(), request, deadline)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                return Err(inspection_refusal(
                    error,
                    StateInspectionRejection::OwnerUnavailable,
                ));
            }
        };
        let page = response.result?;
        if page.origin.node != target {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        Ok(page)
    }

    /// The runtime state at exactly `revision`, waiting while this node has not applied it yet.
    /// A node already past it cannot serve a read the coordinator planned at that revision.
    async fn runtime_state_at(
        &self,
        revision: u64,
    ) -> Result<ConsensusRuntimeState, StateInspectionRejection> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let state = self.inner.consensus.current_runtime_state().await;
            match state.revision.cmp(&revision) {
                Ordering::Equal => return Ok(state),
                Ordering::Greater => return Err(StateInspectionRejection::StaleContinuation),
                Ordering::Less => {
                    if self
                        .inner
                        .consensus
                        .wait_for_runtime_revision_after(state.revision)
                        .await
                        .is_none()
                    {
                        return Err(StateInspectionRejection::OwnerUnavailable);
                    }
                }
            }
        }
    }

    /// One unit's page read at its owner from the committed lifetimes at the request's revision
    /// and the checkpoints this node samples for the unit.
    pub(in crate::application) async fn native_wasm_state_page(
        &self,
        request: ReadWasmStatePage,
    ) -> Result<WasmStatePage, StateInspectionRejection> {
        let local = ClusterNodeIdentity::new(
            self.inner.consensus.local_node_id().clone(),
            self.inner.cluster.local_incarnation(),
        );
        if request.target != local {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        let state = self.runtime_state_at(request.control_revision).await?;
        let domain_schedule = state
            .schedule
            .domain(&request.domain)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        let processor = domain_schedule
            .scheduled::<CreateWasmProcessor>(&request.processor)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        let node = processor.node;
        if !node.unit_executes_on(request.unit, local.node_id()) {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        let scheduled = node
            .units
            .get(&request.unit)
            .ok_or(StateInspectionRejection::StaleContinuation)?;
        let mut checkpoints = Vec::new();
        for checkpoint in self
            .inner
            .runtime
            .inspect_wasm_processor_state_for(&request.domain, request.processor.clone())
        {
            if node.unit_of_branch(checkpoint.branch.as_ref()) == Some(request.unit) {
                checkpoints.push(checkpoint);
            }
        }
        let selection = WasmUnitStateSelection::of_unit(scheduled, checkpoints, request.selection)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        let origin = wasm_origin(local, &selection);
        let capacity = ReadWasmStatePage::entry_capacity(self.inner.runtime.inspection_executor());
        let limit = usize::try_from(request.max_entries.min(capacity))
            .assured("an entry capacity bounded by one reply fits usize");
        Ok(WasmStatePage {
            origin,
            page: selection.page(request.after, limit),
        })
    }

    pub(in crate::application) async fn handle_read_wasm_state_page(
        &self,
        request: ReadWasmStatePage,
    ) -> ReadWasmStatePageResponse {
        let deadline = *self
            .inner
            .runtime
            .state_inspection_settings()
            .read_timeout
            .value();
        let result = match timeout(deadline, self.native_wasm_state_page(request)).await {
            Ok(result) => result,
            Err(_) => Err(StateInspectionRejection::Deadline),
        };
        ReadWasmStatePageResponse { result }
    }
}
