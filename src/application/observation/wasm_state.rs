//! Typed WASM processor guest-state inspections assembled from their unit owners' bounded pages.
//!
//! Layer: control plane.
//! - **Owns.** WASM state read planning, per-unit owner routing, continuation fencing, the owner's
//!   bounded page of committed lifetimes, sampled checkpoints and guest-state descriptions, the
//!   entry, section, byte and render-time budgets that bound a reply, and the unit summaries a
//!   description prints.
//! - **Depends on.** The committed schedule, the runtime's checkpoint samples and description
//!   renders, the WASM state page operation and the shared inspection envelope.
//! - **Must not know.** Guest bytes, checkpoint storage or replication, or how a guest executes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns the boundary declared above")
)]

use std::{cmp::Ordering, collections::BTreeMap, time::Duration};

use error_stack::ResultExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    MAX_ENCODED_WASM_BRANCH_ENTRY_BYTES, SessionLimits, StateInspection, StateInspectionPage,
    StateInspectionResult, StateInspectionUnit, encoded_wasm_branch_entry_bytes,
};
use nervix_consensus::ConsensusRuntimeState;
use nervix_interconnect::{ReadWasmStatePage, ReadWasmStatePageResponse, WasmStatePage};
use nervix_models::{
    ClusterNodeIdentity, CreateWasmProcessor, DomainName, DomainNodeRef, ModelKind, ModelName,
    ResolvedBranching, ResolvedStateBranchSelection, ScheduledNode, ShowWasmProcessorState,
    StateInspectionBranchSelection, StateInspectionCounts, StateInspectionOrigin,
    StateInspectionRejection, StateInspectionStage, Unit, WasmBranchStateInspection,
    WasmCheckpointInspection, WasmProcessorName, WasmStateBinding, WasmStateEntryKey,
    WasmStateInspectionError, WasmUnitStateInspection, WasmUnitStatePage, WasmUnitStateSelection,
};
use nervix_primitives::time::{Instant, timeout};

use super::inspection_pages::{
    add_counts, inspection_owner, inspection_refusal, rendered_inspection,
};
use crate::{
    application::{AppError, command_result::CommandResult, session_service::SessionServiceImpl},
    runtime::{WasmDescriptionSize, WasmDescriptionSource},
    state_inspection_cursor::WasmStateInspectionCursor,
};

/// One read's committed processor schedule, its resolved selection and its budgets.
struct WasmInspectionPlan {
    domain: DomainName,
    processor: WasmProcessorName,
    node: ScheduledNode,
    binding: WasmStateBinding,
    branched: bool,
    control_revision: u64,
    selection: StateInspectionBranchSelection,
    entries: usize,
    /// The most guest-state sections the reply carries.
    sections: usize,
    cursor: Option<WasmStateInspectionCursor>,
    limits: SessionLimits,
    /// The read's complete deadline, which the time each owner may render in derives from.
    read_timeout: Duration,
}

/// What a reply still has room for as it visits the units in order.
struct WasmReplyBudget {
    entries: usize,
    sections: usize,
    bytes: usize,
}

impl WasmReplyBudget {
    /// Charges the entries one unit returned, measured as their owner measured them. An owner
    /// that returned more than the reply had room for broke the read, which is refused.
    fn spend(
        &mut self,
        entries: &[WasmBranchStateInspection],
        limits: &SessionLimits,
    ) -> Result<(), StateInspectionRejection> {
        let mut sections = 0_usize;
        let mut bytes = 0_usize;
        for entry in entries {
            let encoded = encoded_wasm_branch_entry_bytes(entry, limits).map_err(|error| {
                inspection_refusal(error, StateInspectionRejection::ReplyTooLarge)
            })?;
            bytes = bytes
                .checked_add(encoded)
                .ok_or(StateInspectionRejection::ReplyTooLarge)?;
            if let Some(description) = &entry.description {
                sections = sections
                    .checked_add(description.sections().len())
                    .assured("every counted section is held in memory, so their count fits usize");
            }
        }
        self.entries = self
            .entries
            .checked_sub(entries.len())
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        self.sections = self
            .sections
            .checked_sub(sections)
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        Ok(())
    }

    /// Leaves no room for another entry, once a unit reported entries beyond its page.
    fn close(&mut self) {
        self.entries = 0;
    }
}

/// What an owner's page still has room for as it renders its entries in order.
struct WasmPageRoom {
    sections: usize,
    session_bytes: u64,
    archived_bytes: u64,
}

/// What one entry takes of a page.
#[derive(Clone, Copy)]
struct WasmEntrySize {
    sections: usize,
    /// The entry's session-encoded bytes, or `None` when the session cannot carry it at all, such
    /// as a guest field type nested deeper than its nesting bound.
    session_bytes: Option<u64>,
    archived_bytes: u64,
}

impl WasmEntrySize {
    fn of(entry: &WasmBranchStateInspection, limits: &SessionLimits) -> Self {
        let mut sections = 0;
        if let Some(description) = &entry.description {
            sections = description.sections().len();
        }
        let mut session_bytes = None;
        match encoded_wasm_branch_entry_bytes(entry, limits) {
            Ok(encoded) => {
                session_bytes = Some(u64::try_from(encoded).assured("an encoded size fits u64"));
            }
            Err(error) => {
                tracing::debug!(
                    error = %format_args!("{error:#}"),
                    "a WASM branch entry does not encode within the session's limits"
                );
            }
        }
        Self {
            sections,
            session_bytes,
            archived_bytes: WasmStatePage::archived_entry_bytes(entry),
        }
    }

    /// Whether the entry fits room of this size.
    fn fits(self, room: &WasmPageRoom) -> bool {
        let Some(session_bytes) = self.session_bytes else {
            return false;
        };
        self.sections <= room.sections
            && session_bytes <= room.session_bytes
            && self.archived_bytes <= room.archived_bytes
    }

    /// The larger of the entry's encoded sizes, which a diagnostic reports.
    fn bytes(self) -> u64 {
        match self.session_bytes {
            Some(session_bytes) => session_bytes.max(self.archived_bytes),
            None => self.archived_bytes,
        }
    }
}

impl WasmPageRoom {
    fn spend(&mut self, size: WasmEntrySize) {
        let session_bytes = size
            .session_bytes
            .verified("an entry is spent only after it was found to fit, which needs its size");
        self.sections = self
            .sections
            .checked_sub(size.sections)
            .verified("an entry is spent only after it was found to fit");
        self.session_bytes = self
            .session_bytes
            .checked_sub(session_bytes)
            .verified("an entry is spent only after it was found to fit");
        self.archived_bytes = self
            .archived_bytes
            .checked_sub(size.archived_bytes)
            .verified("an entry is spent only after it was found to fit");
    }
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

/// The render source of the entry `key`, among sources in entry order.
fn wasm_description_source(
    sources: &[WasmDescriptionSource],
    key: WasmStateEntryKey,
) -> Option<&WasmDescriptionSource> {
    let index = sources
        .binary_search_by_key(&key, WasmDescriptionSource::key)
        .ok()?;
    sources.get(index)
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
    mut by_unit: BTreeMap<Unit, Vec<WasmCheckpointInspection>>,
) -> Result<Vec<(Unit, WasmUnitStateInspection)>, WasmStateInspectionError> {
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
    pub(in crate::application) async fn handle_describe_inherited_wasm_request(
        &self,
        request: nervix_interconnect::DescribeInheritedWasmStateRequest,
    ) -> Result<
        Vec<(Unit, Vec<nervix_models::WasmCheckpointInspection>)>,
        nervix_interconnect::RemoteOperationFailure,
    > {
        self.prepare_owner_control_request(
            &request.domain,
            ModelKind::WasmProcessor,
            &request.processor,
        )
        .await?;
        Ok(self
            .inner
            .runtime
            .inspect_inherited_wasm_processor_state_for(&request.domain, &request.processor)
            .into_iter()
            .collect())
    }

    pub(super) async fn inherited_wasm_checkpoints(
        &self,
        domain: &DomainName,
        processor: &nervix_models::ModelName,
        node: &ScheduledNode,
    ) -> error_stack::Result<
        std::collections::BTreeMap<Unit, Vec<nervix_models::WasmCheckpointInspection>>,
        super::ObservationError,
    > {
        let mut checkpoints =
            std::collections::BTreeMap::<Unit, Vec<nervix_models::WasmCheckpointInspection>>::new();
        for owner in node.execution_nodes() {
            let reported = if &owner == self.inner.consensus.local_node_id() {
                self.inner
                    .runtime
                    .inspect_inherited_wasm_processor_state_for(domain, processor)
                    .into_iter()
                    .collect::<Vec<_>>()
            } else {
                self.inner
                    .interconnect
                    .request(
                        &owner,
                        nervix_interconnect::DescribeInheritedWasmStateRequest {
                            domain: domain.clone(),
                            processor: processor.clone(),
                        },
                    )
                    .await
                    .change_context(super::ObservationError::RemoteDescribeRequest {
                        domain: domain.clone(),
                        entity: nervix_models::NodeRef::new(
                            ModelKind::WasmProcessor,
                            processor.clone(),
                        ),
                        node: owner.clone(),
                    })?
                    .result
                    .map_err(|failure| {
                        error_stack::Report::new(super::ObservationError::RemoteDescribeFailure {
                            failure,
                        })
                    })?
            };
            for (unit, values) in reported {
                checkpoints.entry(unit).or_default().extend(values);
            }
        }
        Ok(checkpoints)
    }

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
            entity: entity.into(),
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
        // Every returned entry's own facts fit their encoded bound, so a reply of this many entries
        // stays within the session's transfer bound by construction before descriptions, which the
        // byte budget charges as measured.
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
        let sections = settings
            .max_sections
            .value()
            .get()
            .min(limits.collection_entries());
        Ok(WasmInspectionPlan {
            domain: domain.clone(),
            processor: show.processor.clone(),
            node: processor.node.clone(),
            binding,
            branched: matches!(branching, ResolvedBranching::Branched { .. }),
            control_revision: state.revision,
            selection,
            entries,
            sections,
            cursor,
            limits,
            read_timeout: *settings.read_timeout.value(),
        })
    }

    async fn inspect_wasm_state(
        &self,
        domain: &DomainName,
        show: &ShowWasmProcessorState,
    ) -> Result<StateInspectionPage, StateInspectionRejection> {
        let started = Instant::now();
        let plan = self.wasm_inspection_plan(domain, show).await?;
        let incarnations = self.live_node_incarnations().await;
        let local = self.inner.consensus.local_node_id();
        let mut units = plan.node.units.iter().collect::<Vec<_>>();
        units.sort_by_key(|(unit, _)| **unit);
        let mut output = Vec::with_capacity(units.len());
        let mut counts = StateInspectionCounts::default();
        let mut budget = WasmReplyBudget {
            entries: plan.entries,
            sections: plan.sections,
            bytes: plan.limits.transfer_bytes(),
        };
        let mut more = false;
        let mut position = None;
        for (unit, placement) in &units {
            let owner = inspection_owner(&plan.node, **unit, local)?;
            let incarnation = incarnations
                .get(&owner)
                .copied()
                .ok_or(StateInspectionRejection::OwnerUnavailable)?;
            let resume = UnitResume::of(plan.cursor.as_ref(), **unit);
            let allowed = if resume.skip { 0 } else { budget.entries };
            // Half of what remains of the read bounds the renders of one unit's page, so the owner
            // answers, and later units still have time, before the read's deadline.
            let remaining = match plan.read_timeout.checked_sub(started.elapsed()) {
                Some(remaining) => remaining,
                None => Duration::ZERO,
            };
            let request = ReadWasmStatePage {
                domain: plan.domain.clone(),
                processor: plan.processor.clone(),
                unit: **unit,
                control_revision: plan.control_revision,
                selection: plan.selection,
                after: resume.after,
                max_entries: u64::try_from(allowed).assured("an entry budget fits u64"),
                max_sections: u64::try_from(budget.sections).assured("a section budget fits u64"),
                max_bytes: u64::try_from(budget.bytes).assured("a byte budget fits u64"),
                render_budget: remaining / 2,
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
                budget.spend(&state.branches, &plan.limits)?;
                if let Some(last) = state.branches.last() {
                    position = Some(WasmStatePosition {
                        unit: **unit,
                        after: last.key(),
                    });
                }
                if page.more {
                    // An owner that returned none of the entries it was allowed before this reply
                    // returned any cannot make progress, so a continuation from here would repeat
                    // this page forever. Once the reply holds entries, one the remaining budget
                    // cannot hold waits for the continuation.
                    if allowed > 0 && state.branches.is_empty() && position.is_none() {
                        return Err(StateInspectionRejection::ReplyTooLarge);
                    }
                    more = true;
                    budget.close();
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
        let scheduled_processor = domain_schedule
            .scheduled::<CreateWasmProcessor>(&request.processor)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        let node = scheduled_processor.node;
        if !node.unit_executes_on(request.unit, local.node_id()) {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        let scheduled = node
            .units
            .get(&request.unit)
            .ok_or(StateInspectionRejection::StaleContinuation)?;
        let epoch = node
            .partition_parametrizer()
            .and_then(|branch| domain_schedule.partition_epochs.get(branch));
        let inherited = node.has_inherited_partition_state(epoch);
        let processor = ModelName::from(&request.processor);
        let mut checkpoints = Vec::new();
        let mut sources = Vec::new();
        for sample in self
            .inner
            .runtime
            .sample_wasm_processor_checkpoints(&request.domain, &processor)
        {
            // An inherited key's lifetimes are the unit's whose placement holds them; otherwise
            // a branch belongs to the unit the schedule assigns its key to.
            let belongs = if inherited {
                sample.unit == request.unit
            } else {
                node.unit_of_branch(sample.checkpoint.branch.as_ref()) == Some(request.unit)
            };
            if belongs {
                checkpoints.push(sample.checkpoint);
                sources.push(sample.source);
            }
        }
        let selection = WasmUnitStateSelection::of_unit(scheduled, checkpoints, request.selection)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        let origin = wasm_origin(local, &selection);
        let executor = self.inner.runtime.inspection_executor();
        let capacity = ReadWasmStatePage::entry_capacity(executor);
        let limit = usize::try_from(request.max_entries.min(capacity))
            .assured("an entry capacity bounded by one reply fits usize");
        let mut page = selection.page(request.after, limit);
        self.describe_wasm_page_entries(&request, &processor, &sources, &mut page)
            .await?;
        self.fit_wasm_page_to_reply(&request, &processor, &sources, &origin, &mut page)?;
        Ok(WasmStatePage { origin, page })
    }

    /// Renders the guest-state description of each of the page's entries with a checkpoint, in
    /// order, and stops the page before the first entry its section, byte or render-time budget
    /// no longer holds. A description no read could carry is replaced by its failure, so the first
    /// entry of a page always fits a read's complete budgets.
    async fn describe_wasm_page_entries(
        &self,
        request: &ReadWasmStatePage,
        processor: &ModelName,
        sources: &[WasmDescriptionSource],
        page: &mut WasmUnitStatePage,
    ) -> Result<(), StateInspectionRejection> {
        let entries = std::mem::take(&mut page.state.branches);
        if entries.iter().all(|entry| entry.checkpoint.is_none()) {
            page.state.branches = entries;
            return Ok(());
        }
        let runtime = &self.inner.runtime;
        let plan = runtime
            .wasm_description_plan(&request.domain, processor)
            .ok_or(StateInspectionRejection::OwnerUnavailable)?;
        let limits = SessionLimits::DEFAULT;
        let read = self.wasm_description_read_size(&limits);
        let entry_room = ReadWasmStatePage::entry_room(runtime.inspection_executor());
        // The room of a read with nothing else in it, which any description must fit.
        let whole_read = WasmPageRoom {
            sections: read.sections,
            session_bytes: read.bytes,
            archived_bytes: entry_room,
        };
        let read_sections = u64::try_from(read.sections).assured("a section bound fits u64");
        let mut room = WasmPageRoom {
            sections: usize::try_from(request.max_sections.min(read_sections))
                .assured("a section budget bounded by the node's section bound fits usize"),
            session_bytes: request
                .max_bytes
                .min(u64::try_from(limits.transfer_bytes()).assured("a session limit fits u64")),
            archived_bytes: entry_room,
        };
        let deadline = Instant::now().checked_add(request.render_budget);
        let mut kept = Vec::with_capacity(entries.len());
        let mut stopped = false;
        for mut entry in entries {
            nervix_primitives::task::consume_budget().await;
            let mut source = None;
            if entry.checkpoint.is_some() {
                let found = wasm_description_source(sources, entry.key()).verified(
                    "every checkpoint of the selection came from a sample that carried its source",
                );
                let left = match deadline {
                    Some(deadline) => deadline.checked_duration_since(Instant::now()),
                    None => Some(request.render_budget),
                };
                let budget = match left {
                    Some(left) if !left.is_zero() => left,
                    // Out of render time: the entries rendered so far are the page. The first
                    // entry still renders, with no time, so its deadline ends it at its first
                    // wait unless it finishes before one.
                    _ if !kept.is_empty() => {
                        stopped = true;
                        break;
                    }
                    _ => Duration::ZERO,
                };
                entry.description = Some(
                    runtime
                        .render_wasm_state_description(&plan, found, budget)
                        .await,
                );
                source = Some(found);
            }
            let mut size = WasmEntrySize::of(&entry, &limits);
            if let Some(source) = source
                && !size.fits(&whole_read)
            {
                let measured = WasmDescriptionSize {
                    sections: size.sections,
                    bytes: size.bytes(),
                };
                entry.description = Some(source.exceeding_read(&plan, measured, read));
                size = WasmEntrySize::of(&entry, &limits);
            }
            if size.session_bytes.is_none() {
                // Only an entry's own facts and a bounded failure remain, which always encode.
                return Err(StateInspectionRejection::ReplyTooLarge);
            }
            if !size.fits(&room) {
                stopped = true;
                break;
            }
            room.spend(size);
            kept.push(entry);
        }
        if stopped {
            page.more = true;
        }
        page.state.branches = kept;
        Ok(())
    }

    /// The most guest-state sections and bytes one entry's description may take in any read this
    /// node serves: its section bound, and the smaller of the session's transfer bound and the
    /// room one owner reply leaves for entries.
    fn wasm_description_read_size(&self, limits: &SessionLimits) -> WasmDescriptionSize {
        let runtime = &self.inner.runtime;
        let session_bytes =
            u64::try_from(limits.transfer_bytes()).assured("a session limit fits u64");
        let entry_room = ReadWasmStatePage::entry_room(runtime.inspection_executor());
        WasmDescriptionSize {
            sections: runtime
                .state_inspection_settings()
                .max_sections
                .value()
                .get(),
            bytes: session_bytes.min(entry_room),
        }
    }

    /// Drops the page's trailing entries until its reply fits the page operation's payload
    /// limit, which the entries' own measures approach but alignment inside the reply can exceed
    /// by a few bytes. A page reduced to one entry that still does not fit carries that entry's
    /// description as its failure instead, so the page makes progress.
    fn fit_wasm_page_to_reply(
        &self,
        request: &ReadWasmStatePage,
        processor: &ModelName,
        sources: &[WasmDescriptionSource],
        origin: &StateInspectionOrigin,
        page: &mut WasmUnitStatePage,
    ) -> Result<(), StateInspectionRejection> {
        let executor = self.inner.runtime.inspection_executor();
        let mut replaced = false;
        loop {
            let reply = ReadWasmStatePageResponse {
                result: Ok(WasmStatePage {
                    origin: origin.clone(),
                    page: page.clone(),
                }),
            };
            if reply.fits(executor) {
                return Ok(());
            }
            if page.state.branches.len() > 1 {
                page.state.branches.pop();
                page.more = true;
                continue;
            }
            let Some(entry) = page.state.branches.first_mut() else {
                return Err(StateInspectionRejection::ReplyTooLarge);
            };
            if replaced || entry.description.is_none() {
                return Err(StateInspectionRejection::ReplyTooLarge);
            }
            let plan = self
                .inner
                .runtime
                .wasm_description_plan(&request.domain, processor)
                .ok_or(StateInspectionRejection::OwnerUnavailable)?;
            let source = wasm_description_source(sources, entry.key()).verified(
                "an entry with a description has a checkpoint, whose sample carried its source",
            );
            let limits = SessionLimits::DEFAULT;
            let size = WasmEntrySize::of(entry, &limits);
            let measured = WasmDescriptionSize {
                sections: size.sections,
                bytes: size.bytes(),
            };
            entry.description = Some(source.exceeding_read(
                &plan,
                measured,
                self.wasm_description_read_size(&limits),
            ));
            replaced = true;
        }
    }

    /// Answer the WASM state reads other nodes send this one: state pages, and the checkpoints
    /// of an inherited key's lifetimes.
    pub(in crate::application) fn register_wasm_state_inspection_interconnect_handlers(
        &self,
    ) -> error_stack::Result<(), AppError> {
        let wasm_state_page_service = self.clone();
        self.inner
            .interconnect
            .register_handler::<nervix_interconnect::ReadWasmStatePage, _, _>(
                move |_context, request| {
                    let service = wasm_state_page_service.clone();
                    async move { service.handle_read_wasm_state_page(request).await }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let inherited_wasm_service = self.clone();
        self.inner
            .interconnect
            .register_handler::<nervix_interconnect::DescribeInheritedWasmStateRequest, _, _>(
                move |_context, request| {
                    let service = inherited_wasm_service.clone();
                    async move {
                        nervix_interconnect::DescribeInheritedWasmStateResponse {
                            result: service
                                .handle_describe_inherited_wasm_request(request)
                                .await,
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        Ok(())
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::num::NonZeroU64;

    use nervix_models::{
        FieldName, ParseAsType, WasmCheckpointStage, WasmCheckpointState,
        WasmGuestStateDescription, WasmGuestStateField, WasmGuestStateRender,
        WasmGuestStateSection, WasmStateGeneration,
    };

    use super::*;

    /// An unbranched entry whose checkpoint committed revision 2, with a description of `sections`
    /// one-field sections when `sections` is `Some`.
    fn entry(sections: Option<usize>) -> WasmBranchStateInspection {
        let revision = NonZeroU64::new(2);
        let mut description = None;
        if let Some(count) = sections {
            let mut described = Vec::with_capacity(count);
            for index in 0..count {
                described.push(WasmGuestStateSection {
                    name: FieldName::parse(&format!("section_{index}"))
                        .assured("a generated section name follows the name rules"),
                    fields: vec![WasmGuestStateField {
                        name: FieldName::parse("count")
                            .assured("a literal field name follows the name rules"),
                        ty: ParseAsType::U32,
                        optional: false,
                    }],
                    rows: 1,
                });
            }
            description = Some(WasmGuestStateDescription {
                generation: WasmStateGeneration::FIRST,
                revision,
                render: WasmGuestStateRender::Described {
                    sections: described,
                },
            });
        }
        WasmBranchStateInspection {
            branch: None,
            checkpoint: Some(WasmCheckpointState {
                generation: WasmStateGeneration::FIRST,
                committed_revision: revision,
                latest_revision: revision,
                stage: WasmCheckpointStage::LocallyDurable,
                required_replicas: None,
                confirmed_replicas: None,
            }),
            recovery: None,
            description,
        }
    }

    #[test]
    fn a_reply_budget_charges_each_returned_entry_and_refuses_an_owner_beyond_it() {
        let limits = SessionLimits::DEFAULT;
        let entries = vec![entry(Some(2)), entry(None)];
        let described_bytes = encoded_wasm_branch_entry_bytes(&entries[0], &limits)
            .assured("a described fixture entry encodes");
        let plain_bytes = encoded_wasm_branch_entry_bytes(&entries[1], &limits)
            .assured("a plain fixture entry encodes");
        let bytes = described_bytes
            .checked_add(plain_bytes)
            .assured("two fixture entries are a few hundred bytes");

        let mut budget = WasmReplyBudget {
            entries: 3,
            sections: 5,
            bytes: bytes.checked_add(10).assured("a fixture budget is small"),
        };
        assert_eq!(budget.spend(&entries, &limits), Ok(()));
        assert_eq!(budget.entries, 1);
        assert_eq!(budget.sections, 3);
        assert_eq!(budget.bytes, 10);

        let mut few_entries = WasmReplyBudget {
            entries: 1,
            sections: 2,
            bytes,
        };
        assert_eq!(
            few_entries.spend(&entries, &limits),
            Err(StateInspectionRejection::ReplyTooLarge)
        );
        let mut few_sections = WasmReplyBudget {
            entries: 2,
            sections: 1,
            bytes,
        };
        assert_eq!(
            few_sections.spend(&entries, &limits),
            Err(StateInspectionRejection::ReplyTooLarge)
        );
        let mut few_bytes = WasmReplyBudget {
            entries: 2,
            sections: 2,
            bytes: bytes
                .checked_sub(1)
                .assured("an encoded entry is not empty"),
        };
        assert_eq!(
            few_bytes.spend(&entries, &limits),
            Err(StateInspectionRejection::ReplyTooLarge)
        );
    }

    #[test]
    fn an_entry_takes_its_sections_and_both_encoded_sizes_of_a_page() {
        let limits = SessionLimits::DEFAULT;
        let described = entry(Some(2));
        let size = WasmEntrySize::of(&described, &limits);
        assert_eq!(size.sections, 2);
        assert_eq!(
            size.session_bytes,
            Some(
                u64::try_from(
                    encoded_wasm_branch_entry_bytes(&described, &limits)
                        .assured("a described fixture entry encodes")
                )
                .assured("an encoded size fits u64")
            )
        );
        assert_eq!(
            size.archived_bytes,
            WasmStatePage::archived_entry_bytes(&described)
        );
        assert_eq!(WasmEntrySize::of(&entry(None), &limits).sections, 0);

        let exact = WasmEntrySize {
            sections: 2,
            session_bytes: Some(100),
            archived_bytes: 120,
        };
        let room = WasmPageRoom {
            sections: 2,
            session_bytes: 100,
            archived_bytes: 120,
        };
        assert!(exact.fits(&room));
        assert!(!exact.fits(&WasmPageRoom {
            sections: 1,
            ..room
        }));
        assert!(!exact.fits(&WasmPageRoom {
            session_bytes: 99,
            ..room
        }));
        assert!(!exact.fits(&WasmPageRoom {
            archived_bytes: 119,
            ..room
        }));
        let unencodable = WasmEntrySize {
            session_bytes: None,
            ..exact
        };
        assert!(!unencodable.fits(&room));
        assert_eq!(exact.bytes(), 120);
        assert_eq!(unencodable.bytes(), 120);
        let session_larger = WasmEntrySize {
            session_bytes: Some(150),
            ..exact
        };
        assert_eq!(session_larger.bytes(), 150);

        let mut left = WasmPageRoom {
            sections: 3,
            session_bytes: 250,
            archived_bytes: 300,
        };
        left.spend(exact);
        assert_eq!(left.sections, 1);
        assert_eq!(left.session_bytes, 150);
        assert_eq!(left.archived_bytes, 180);
    }
}
