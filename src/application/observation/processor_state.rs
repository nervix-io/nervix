//! Typed stateful processor inspections coordinated against captured unit owners.
//!
//! Layer: control plane.
//! - **Owns.** Processor schedule validation, section layouts, per-unit routing, cursor fencing,
//!   the owner page's projection into section columns, bounded inspection assembly and the
//!   `DESCRIBE` state counts.
//! - **Depends on.** Committed vocabulary, the data plane's owner pages, the native Arrow section
//!   carriage, the shared Arrow-to-Row writer and typed outcomes.
//! - **Must not know.** How branch tasks hold, release or match the rows they keep.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns the boundary declared above")
)]

use arrow_array::RecordBatch;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    RowBranch, RowSchema, SessionLimits, StateInspection, StateInspectionPage,
    StateInspectionResult, StateInspectionSection, StateInspectionUnit, StateRowsEncoder,
};
use nervix_interconnect::{
    ProcessorStatePageDescriptor, ProcessorStateRunTransfer, ProcessorStateSectionTransfer,
    ReadProcessorStatePage, StreamHandlerError, StreamingResponse,
};
use nervix_models::{
    BranchKeyFingerprint, ClusterNodeIdentity, ClusterSchedule, CreateCorrelator,
    CreateDeduplicator, CreateRelay, CreateReorderer, CreateSchema, CreateWindowProcessor,
    DomainName, DomainNodeRef, DomainSchedule, Expression, FieldScope, ModelKind, NodeRef,
    ParseAsType, ProcessorStateSection, ResolvedBranching, ResolvedStateBranchSelection,
    ScheduledNode, SchemaField, ShowProcessorState, StateInspectedProcessor,
    StateInspectionBranchSelection, StateInspectionCounts, StateInspectionOrigin,
    StateInspectionPosition, StateInspectionRejection, StateInspectionStage, Unit,
};
use nervix_primitives::time::timeout;

use super::inspection_pages::{
    add_counts, distinct_field, inspection_owner, inspection_refusal, rendered_inspection,
    resume_position,
};
use crate::{
    application::{command_result::CommandResult, session_service::SessionServiceImpl},
    client_row::{ArrowRowBatch, write_runtime_value},
    runtime::{
        BranchKey, ProcessorStatePageBounds, ProcessorStatePageError, ProcessorStateSelection,
        state_inspection_exchange::{
            ProcessorPageSection, ReceivedProcessorPage, receive_processor_page,
            stream_processor_page,
        },
    },
    state_inspection_cursor::StateInspectionCursor,
};

mod inferencer;
mod partition;

/// One section of a processor's state before the owner appends its own columns: which section it
/// is, the payload fields its rows carry, and, for a reorderer, whether each order key carries
/// sensitive input.
#[derive(Debug, Clone)]
struct SectionLayout {
    section: ProcessorStateSection,
    payload: Vec<SchemaField>,
    sensitive_keys: Vec<bool>,
}

/// The configuration an inspection of one processor reads at one committed control cut.
struct ProcessorLayout {
    node: ScheduledNode,
    sections: Vec<SectionLayout>,
    moving_partitions: Option<nervix_models::BranchPartitions>,
    _metadata: Option<nervix_execution::Reservation>,
}

/// A section's complete fields, as the owner names and types the columns it appends.
fn section_fields(
    layout: &SectionLayout,
    key_types: &[(ParseAsType, bool)],
    cancellation: &nervix_execution::Cancellation,
) -> Result<Vec<SchemaField>, StateInspectionRejection> {
    let mut fields = layout.payload.clone();
    let mut appended = Vec::new();
    match layout.section {
        ProcessorStateSection::Pending => {
            appended.push(("output".to_string(), ParseAsType::U32, false, false));
            if key_types.len() != layout.sensitive_keys.len() {
                return Err(StateInspectionRejection::NotReady);
            }
            for (index, ((ty, optional), sensitive)) in
                key_types.iter().zip(&layout.sensitive_keys).enumerate()
            {
                // Keys are numbered from one in `BY` order.
                let position = index
                    .checked_add(1)
                    .assured("a reorderer's keys are far fewer than usize::MAX");
                appended.push((
                    format!("order_key_{position}"),
                    ty.clone(),
                    *optional,
                    *sensitive,
                ));
            }
            appended.push((
                "received_at".to_string(),
                ParseAsType::Datetime,
                false,
                false,
            ));
            appended.push((
                "release_at".to_string(),
                ParseAsType::Datetime,
                false,
                false,
            ));
        }
        ProcessorStateSection::Keys => {
            if key_types.len() != layout.sensitive_keys.len() {
                return Err(StateInspectionRejection::NotReady);
            }
            for (index, ((ty, _optional), sensitive)) in
                key_types.iter().zip(&layout.sensitive_keys).enumerate()
            {
                let position = index
                    .checked_add(1)
                    .assured("the compiled key list fits usize");
                appended.push((format!("key_{position}"), ty.clone(), true, *sensitive));
            }
            appended.push(("seen_at".to_string(), ParseAsType::Datetime, false, false));
        }
        ProcessorStateSection::Rows => {
            if key_types.len() != layout.sensitive_keys.len() {
                return Err(StateInspectionRejection::NotReady);
            }
            appended.push(("sequence".to_string(), ParseAsType::U64, false, false));
            appended.push((
                "received_at".to_string(),
                ParseAsType::Datetime,
                false,
                false,
            ));
            for (index, ((ty, optional), sensitive)) in
                key_types.iter().zip(&layout.sensitive_keys).enumerate()
            {
                appended.push((
                    nervix_vm::window::argument_snapshot_column(index),
                    ty.clone(),
                    *optional,
                    *sensitive,
                ));
            }
        }
        ProcessorStateSection::Header
        | ProcessorStateSection::Accumulators
        | ProcessorStateSection::Tensor(_) => {}
        ProcessorStateSection::Left | ProcessorStateSection::Right => {
            appended.push((
                "received_at".to_string(),
                ParseAsType::Datetime,
                false,
                false,
            ));
            appended.push((
                "timeout_at".to_string(),
                ParseAsType::Datetime,
                false,
                false,
            ));
        }
    }
    for (base, ty, optional, sensitive) in appended {
        let field = distinct_field(&fields, &base, ty, optional, sensitive, cancellation)?;
        fields.push(field);
    }
    Ok(fields)
}

/// Whether evaluating `expression` reads a field that `payload` or the branch declares sensitive.
/// A key derived from sensitive input is redacted exactly as that input is.
fn reads_sensitive_field(
    expression: &Expression,
    payload: &[SchemaField],
    branch: &[SchemaField],
) -> bool {
    let mut sensitive = false;
    expression.visit_fields(&mut |reference| {
        let declared = match reference.scope {
            FieldScope::Branch => branch,
            _ => payload,
        };
        for field in declared {
            if field.name == reference.field && field.sensitive {
                sensitive = true;
            }
        }
    });
    sensitive
}

fn relay_schema_fields(
    schedule: &DomainSchedule,
    relays: &[nervix_models::RelayName],
) -> Result<Vec<SchemaField>, StateInspectionRejection> {
    let relay = relays.first().ok_or(StateInspectionRejection::NotReady)?;
    let relay = schedule
        .configured::<CreateRelay>(relay)
        .ok_or(StateInspectionRejection::NotReady)?;
    let schema = schedule
        .configured::<CreateSchema>(&relay.schema)
        .ok_or(StateInspectionRejection::NotReady)?;
    Ok(schema.fields.clone())
}

fn branch_fields(node: &ScheduledNode) -> Vec<SchemaField> {
    match &node.resolved_branching {
        Some(ResolvedBranching::Branched { schema, .. }) => schema.fields.clone(),
        Some(ResolvedBranching::Unbranched) | None => Vec::new(),
    }
}

/// The scheduled processor an inspection names, with the sections its state is read as.
fn processor_layout(
    schedule: &DomainSchedule,
    processor: &NodeRef,
) -> Result<ProcessorLayout, StateInspectionRejection> {
    let mut layout = match processor.kind {
        ModelKind::Reorderer => {
            let reorderer = schedule
                .scheduled::<CreateReorderer>(processor.identifier.clone())
                .ok_or(StateInspectionRejection::UnknownEntity)?;
            let payload = relay_schema_fields(schedule, reorderer.config.from.relays())?;
            let branch = branch_fields(reorderer.node);
            let sensitive_keys = reorderer
                .config
                .order_by
                .iter()
                .map(|expression| reads_sensitive_field(expression, &payload, &branch))
                .collect();
            Ok(ProcessorLayout {
                node: reorderer.node.clone(),
                moving_partitions: None,
                _metadata: None,
                sections: vec![SectionLayout {
                    section: ProcessorStateSection::Pending,
                    payload,
                    sensitive_keys,
                }],
            })
        }
        ModelKind::Correlator => {
            let correlator = schedule
                .scheduled::<CreateCorrelator>(processor.identifier.clone())
                .ok_or(StateInspectionRejection::UnknownEntity)?;
            let left = relay_schema_fields(schedule, correlator.config.left.relays())?;
            let right = relay_schema_fields(schedule, correlator.config.right.relays())?;
            Ok(ProcessorLayout {
                node: correlator.node.clone(),
                moving_partitions: None,
                _metadata: None,
                sections: vec![
                    SectionLayout {
                        section: ProcessorStateSection::Left,
                        payload: left,
                        sensitive_keys: Vec::new(),
                    },
                    SectionLayout {
                        section: ProcessorStateSection::Right,
                        payload: right,
                        sensitive_keys: Vec::new(),
                    },
                ],
            })
        }
        ModelKind::Deduplicator => {
            let deduplicator = schedule
                .scheduled::<CreateDeduplicator>(processor.identifier.clone())
                .ok_or(StateInspectionRejection::UnknownEntity)?;
            let payload = relay_schema_fields(schedule, deduplicator.config.from.relays())?;
            let branch = branch_fields(deduplicator.node);
            let sensitive_keys = deduplicator
                .config
                .deduplicate_on
                .iter()
                .map(|expression| reads_sensitive_field(expression, &payload, &branch))
                .collect();
            Ok(ProcessorLayout {
                node: deduplicator.node.clone(),
                moving_partitions: None,
                _metadata: None,
                sections: vec![SectionLayout {
                    section: ProcessorStateSection::Keys,
                    payload: Vec::new(),
                    sensitive_keys,
                }],
            })
        }
        ModelKind::WindowProcessor => window_layout(schedule, processor),
        _ => Err(StateInspectionRejection::UnknownEntity),
    }?;
    if let ModelKind::Deduplicator | ModelKind::WindowProcessor = processor.kind
        && let Some(ResolvedBranching::Branched {
            branch, partitions, ..
        }) = &layout.node.resolved_branching
        && schedule.partition_transition_of(branch).is_some()
    {
        layout.moving_partitions = Some(*partitions);
    }
    Ok(layout)
}

fn window_layout(
    schedule: &DomainSchedule,
    processor: &NodeRef,
) -> Result<ProcessorLayout, StateInspectionRejection> {
    let window = schedule
        .scheduled::<CreateWindowProcessor>(processor.identifier.clone())
        .ok_or(StateInspectionRejection::UnknownEntity)?;
    let payload = relay_schema_fields(schedule, window.config.from.relays())?;
    let mut secrets = Vec::new();
    for field in &payload {
        if field.sensitive {
            secrets.push(field.name.as_str());
        }
    }
    let sensitivity = nervix_vm::SchemaSensitivity::from_sensitive_fields(secrets);
    let input = nervix_primitives::sync::StdArc::new(SchemaField::arrow_schema(&payload));
    let mut programs = Vec::new();
    for (_, node) in schedule.nodes.iter() {
        if let nervix_models::Model::Udf(udf) = node.config.as_ref() {
            programs.push(crate::registry::udf_program(udf));
        }
    }
    let options = nervix_vm::CompileOptions {
        udf_signatures: nervix_roto::signatures_for(programs.iter()),
        ..nervix_vm::CompileOptions::default()
    };
    let mut sensitive_keys = Vec::new();
    for output in window.config.output_routes.outputs() {
        let program = nervix_vm::window::lower_window_assignments(&output.construction)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        let inferred = nervix_vm::window::CompiledWindowRoute::argument_inspection_fields(
            &program.inner,
            &input,
            &sensitivity,
            &options,
        )
        .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        for field in inferred {
            sensitive_keys.push(field.sensitive);
        }
    }
    let mut sections = vec![SectionLayout {
        section: ProcessorStateSection::Rows,
        payload,
        sensitive_keys,
    }];
    for section in [
        ProcessorStateSection::Header,
        ProcessorStateSection::Accumulators,
    ] {
        sections.push(SectionLayout {
            section,
            payload: section.metadata_fields(),
            sensitive_keys: Vec::new(),
        });
    }
    Ok(ProcessorLayout {
        node: window.node.clone(),
        moving_partitions: None,
        _metadata: None,
        sections,
    })
}

/// The node reference a `SHOW <kind> <name> STATE` names.
fn inspected_node(processor: &StateInspectedProcessor) -> NodeRef {
    match processor {
        StateInspectedProcessor::Inferencer(name) => {
            NodeRef::new(ModelKind::Inferencer, name.clone())
        }
        StateInspectedProcessor::Reorderer(name) => {
            NodeRef::new(ModelKind::Reorderer, name.clone())
        }
        StateInspectedProcessor::Correlator(name) => {
            NodeRef::new(ModelKind::Correlator, name.clone())
        }
        StateInspectedProcessor::Deduplicator(name) => {
            NodeRef::new(ModelKind::Deduplicator, name.clone())
        }
        StateInspectedProcessor::WindowProcessor(name) => {
            NodeRef::new(ModelKind::WindowProcessor, name.clone())
        }
    }
}

struct ProcessorInspectionPlan {
    domain: DomainName,
    processor: NodeRef,
    layout: ProcessorLayout,
    control_revision: u64,
    branch: Option<RowBranch>,
    selection: StateInspectionBranchSelection,
    rows: usize,
    sections: usize,
    cursor: Option<StateInspectionCursor>,
    limits: SessionLimits,
}

/// The remaining capacity belongs to the entire reply, across all execution units.
struct InspectionBudget {
    rows: usize,
    sections: usize,
    bytes: usize,
}

impl InspectionBudget {
    /// The rows the next unit may return: none once the reply is closed, the continuation already
    /// read the unit, or no section or byte is left.
    fn allowed_rows(&self, closed: bool, skip: bool) -> usize {
        if closed || skip || self.sections == 0 || self.bytes == 0 {
            0
        } else {
            self.rows
        }
    }

    /// Charges what one unit's encoded sections took of the reply.
    fn spend(&mut self, encoded: &EncodedOwnerPage) -> Result<(), StateInspectionRejection> {
        let sections = encoded.sections.len();
        if sections > self.sections {
            return Err(StateInspectionRejection::ReplyTooLarge);
        }
        self.sections = self
            .sections
            .checked_sub(sections)
            .verified("a page with more sections than the budget was refused above");
        self.bytes = self
            .bytes
            .checked_sub(encoded.bytes)
            .ok_or(StateInspectionRejection::NotReady)?;
        self.rows = self
            .rows
            .checked_sub(encoded.rows)
            .ok_or(StateInspectionRejection::NotReady)?;
        Ok(())
    }
}

/// Where a reply stands as it visits the units in order: whether it closed, whether rows remain,
/// and the position a continuation resumes from.
struct ReplyProgress {
    closed: bool,
    more: bool,
    position: Option<StateInspectionPosition>,
}

impl ReplyProgress {
    /// After a unit that returned no row: a unit the reply read in full moves the position past it,
    /// and one that still holds rows closes the reply with more to read.
    fn passed_without_rows(&mut self, unit: Unit, skip: bool, unit_more: bool) {
        if self.closed || skip {
            return;
        }
        if unit_more {
            self.more = true;
            self.closed = true;
        } else {
            self.position = Some(StateInspectionPosition::AfterUnit(unit));
        }
    }

    /// After a unit whose rows the reply encoded: one with rows left closes the reply, resuming
    /// after its last returned row, and one returned in full moves the position past it.
    fn passed_with_rows(
        &mut self,
        unit: Unit,
        after: Option<(Option<BranchKeyFingerprint>, u64)>,
        encoded: &EncodedOwnerPage,
        unit_more: bool,
    ) -> Result<(), StateInspectionRejection> {
        if !unit_more {
            self.position = Some(StateInspectionPosition::AfterUnit(unit));
            return Ok(());
        }
        self.more = true;
        self.closed = true;
        if encoded.rows != 0 {
            let start = match after {
                Some((_, offset)) => offset,
                None => 0,
            };
            let returned = u64::try_from(encoded.rows).assured("bounded row counts fit u64");
            self.position = Some(StateInspectionPosition::Rows {
                unit,
                branch: encoded.last_branch,
                offset: start
                    .checked_add(returned)
                    .ok_or(StateInspectionRejection::ReplyTooLarge)?,
            });
        }
        Ok(())
    }
}

/// The replicas a unit's envelope names: none for a processor that runs on every node, and
/// otherwise the unit's replica nodes other than its owner.
fn unit_replicas(
    node: &ScheduledNode,
    placement: &nervix_models::ScheduledUnit,
    owner: &nervix_models::ClusterNodeName,
) -> Vec<nervix_models::ClusterNodeName> {
    if node.config.executes_on_every_cluster_node() {
        return Vec::new();
    }
    placement
        .replica_nodes()
        .into_iter()
        .filter(|node| *node != owner)
        .cloned()
        .collect()
}

/// Adds one unit's captured identity to the reply's source fingerprint, length first.
fn digest_unit_identity(
    fingerprint: &mut nervix_crypto::digest::Sha256,
    unit: Unit,
    owner: &nervix_models::ClusterNodeName,
    replicas: &[nervix_models::ClusterNodeName],
    origin: &StateInspectionOrigin,
) {
    let identity = serde_json::to_vec(&(unit, owner, replicas, origin))
        .assured("captured identities are JSON values");
    fingerprint.update(
        &u64::try_from(identity.len())
            .assured("a bounded identity fits u64")
            .to_be_bytes(),
    );
    fingerprint.update(&identity);
}

/// Whether a continuation was issued for this exact read: the same domain, processor, schema,
/// control revision and selector, at a unit the processor still has.
fn cursor_reads_this_state(
    cursor: &StateInspectionCursor,
    domain: &DomainName,
    processor: &NodeRef,
    node: &ScheduledNode,
    revision: u64,
    selection: StateInspectionBranchSelection,
) -> bool {
    let unit = match &cursor.position {
        StateInspectionPosition::AfterUnit(unit) | StateInspectionPosition::Rows { unit, .. } => {
            unit
        }
    };
    cursor.domain == *domain
        && cursor.entity == *processor
        && cursor.schema == node.schema_fingerprint
        && cursor.control_revision == revision
        && cursor.selection == selection
        && node.units.contains_key(unit)
}

/// The branch selector a `SHOW ... STATE` names, resolved against the processor's branching.
fn resolve_selection(
    show: &ShowProcessorState,
    branching: &ResolvedBranching,
) -> Result<StateInspectionBranchSelection, StateInspectionRejection> {
    let Some(selection) = &show.selection else {
        return Ok(StateInspectionBranchSelection::All);
    };
    let resolved = selection
        .resolve(branching)
        .map_err(|error| inspection_refusal(error, StateInspectionRejection::InvalidSelection))?;
    Ok(match resolved {
        ResolvedStateBranchSelection::AllBranches => StateInspectionBranchSelection::All,
        ResolvedStateBranchSelection::Unbranched => StateInspectionBranchSelection::Unbranched,
        ResolvedStateBranchSelection::Branch { fingerprint, .. } => {
            StateInspectionBranchSelection::Branch(fingerprint)
        }
    })
}

/// One section of an owner's page projected into its complete fields: the columns of its returned
/// rows in that field order.
pub(in crate::application) struct OwnerSection {
    section: ProcessorStateSection,
    fields: Vec<SchemaField>,
    rows: u64,
    batch: Option<RecordBatch>,
}

/// A run of consecutive returned rows of one section and one branch, in the unit's row order.
pub(in crate::application) struct OwnerRun {
    section: ProcessorStateSection,
    branch: Option<BranchKey>,
    rows: usize,
}

/// One owner page as the coordinator encodes it, whether it was read here or received.
pub(in crate::application) struct OwnerPage {
    descriptor: ProcessorStatePageDescriptor,
    runs: Vec<OwnerRun>,
    sections: Vec<OwnerSection>,
    /// Retains the published reader's allocation admission through projection and encoding.
    _memory: Option<crate::runtime::ProcessorStatePageMemory>,
}

impl SessionServiceImpl {
    async fn prepared_processor_layout(
        &self,
        schedule: ClusterSchedule,
        domain: &DomainName,
        processor: &NodeRef,
    ) -> Result<ProcessorLayout, StateInspectionRejection> {
        match processor.kind {
            ModelKind::Inferencer => {
                inferencer::prepare(
                    self.inner.runtime.inspection_executor(),
                    schedule,
                    domain,
                    processor,
                )
                .await
            }
            ModelKind::Deduplicator | ModelKind::WindowProcessor => {
                let executor = self.inner.runtime.inspection_executor();
                let charge = executor
                    .try_reserve(nervix_execution::MemoryClass::Bulk, 1)
                    .map_err(|error| {
                        inspection_refusal(error, StateInspectionRejection::NotReady)
                    })?;
                let domain = domain.clone();
                let processor = processor.clone();
                executor
                    .run_cpu(
                        nervix_execution::CpuClass::Bulk,
                        charge,
                        move |_charge, cancellation| {
                            cancellation.check().map_err(|error| {
                                inspection_refusal(error, StateInspectionRejection::Deadline)
                            })?;
                            let domain = schedule
                                .domain(&domain)
                                .ok_or(StateInspectionRejection::UnknownEntity)?;
                            processor_layout(domain, &processor)
                        },
                    )
                    .await
                    .map_err(|error| {
                        inspection_refusal(error, StateInspectionRejection::NotReady)
                    })?
            }
            _ => {
                let domain = schedule
                    .domain(domain)
                    .ok_or(StateInspectionRejection::UnknownEntity)?;
                processor_layout(domain, processor)
            }
        }
    }
    pub(in crate::application) async fn show_processor_state(
        &self,
        domain: &DomainName,
        show: ShowProcessorState,
    ) -> CommandResult {
        let node = inspected_node(&show.processor);
        let entity = DomainNodeRef::node_in(domain.clone(), node.kind, node.identifier);
        let settings = self.inner.runtime.state_inspection_settings();
        let result = timeout(
            *settings.read_timeout.value(),
            self.inspect_processor_state(domain, &show),
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

    async fn processor_inspection_plan(
        &self,
        domain: &DomainName,
        show: &ShowProcessorState,
    ) -> Result<ProcessorInspectionPlan, StateInspectionRejection> {
        let processor = inspected_node(&show.processor);
        let state = self.inner.consensus.current_runtime_state().await;
        let layout = self
            .prepared_processor_layout(state.schedule, domain, &processor)
            .await?;
        let branching = layout
            .node
            .resolved_branching
            .as_ref()
            .ok_or(StateInspectionRejection::NotReady)?;
        let selection = resolve_selection(show, branching)?;
        let branch = match branching {
            ResolvedBranching::Unbranched => None,
            ResolvedBranching::Branched { branch, schema, .. } => Some(
                RowBranch::new(branch.clone(), schema.fields.clone()).map_err(|error| {
                    inspection_refusal(error, StateInspectionRejection::NotReady)
                })?,
            ),
        };
        let limits = SessionLimits::DEFAULT;
        let settings = self.inner.runtime.state_inspection_settings();
        let rows = match &show.page {
            Some(page) => usize::try_from(page.limit.get()).map_err(|error| {
                inspection_refusal(error, StateInspectionRejection::InvalidLimit)
            })?,
            None => settings
                .max_rows
                .value()
                .get()
                .min(limits.collection_entries()),
        };
        if rows > settings.max_rows.value().get() || rows > limits.collection_entries() {
            return Err(StateInspectionRejection::InvalidLimit);
        }
        let token = match &show.page {
            Some(page) => page.after.as_deref(),
            None => None,
        };
        let cursor = match token {
            Some(token) => Some(
                StateInspectionCursor::decode(token, limits.string_bytes()).map_err(|error| {
                    inspection_refusal(error, StateInspectionRejection::StaleContinuation)
                })?,
            ),
            None => None,
        };
        if let Some(cursor) = &cursor
            && !cursor_reads_this_state(
                cursor,
                domain,
                &processor,
                &layout.node,
                state.revision,
                selection,
            )
        {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        let sections = settings
            .max_sections
            .value()
            .get()
            .min(limits.collection_entries());
        Ok(ProcessorInspectionPlan {
            domain: domain.clone(),
            processor,
            layout,
            control_revision: state.revision,
            branch,
            selection,
            rows,
            sections,
            cursor,
            limits,
        })
    }

    async fn inspect_processor_state(
        &self,
        domain: &DomainName,
        show: &ShowProcessorState,
    ) -> Result<StateInspectionPage, StateInspectionRejection> {
        let plan = self.processor_inspection_plan(domain, show).await?;
        let incarnations = self.live_node_incarnations().await;
        let partitions = self
            .capture_partition_inspection(&plan, &incarnations)
            .await?;
        let local = self.inner.consensus.local_node_id();
        let mut units = plan.layout.node.units.iter().collect::<Vec<_>>();
        units.sort_by_key(|(unit, _)| **unit);
        let mut output = Vec::new();
        let mut counts = StateInspectionCounts::default();
        let mut fingerprint = nervix_crypto::digest::Sha256::new();
        fingerprint.update(b"nervix/state-inspection/processor-owners");
        fingerprint.update(plan.layout.node.schema_fingerprint.as_digest());
        let mut budget = InspectionBudget {
            rows: plan.rows,
            sections: plan.sections,
            bytes: plan.limits.transfer_bytes(),
        };
        let mut progress = ReplyProgress {
            closed: false,
            more: false,
            position: plan.cursor.as_ref().map(|cursor| cursor.position.clone()),
        };
        for (unit, placement) in &units {
            let owner = inspection_owner(&plan.layout.node, **unit, local)?;
            let incarnation = incarnations
                .get(&owner)
                .copied()
                .ok_or(StateInspectionRejection::OwnerUnavailable)?;
            let (skip, after) = resume_position(plan.cursor.as_ref(), **unit);
            let allowed = budget.allowed_rows(progress.closed, skip);
            let partition_exclusions = match &partitions {
                Some(partitions) => Some(partitions.exclusions(**unit)?),
                None => None,
            };
            let request = ReadProcessorStatePage {
                domain: domain.clone(),
                processor: plan.processor.clone(),
                unit: **unit,
                control_revision: plan.control_revision,
                schema: plan.layout.node.schema_fingerprint,
                selection: plan.selection,
                partition_exclusions,
                after,
                max_rows: u64::try_from(allowed).assured("a row limit fits u64"),
                max_bytes: u64::try_from(budget.bytes).assured("the session byte limit fits u64"),
                target: ClusterNodeIdentity::new(owner.clone(), incarnation),
            };
            let page = self.processor_unit_page(request, &plan).await?;
            if let Some(partitions) = &partitions {
                partitions.validate(**unit, page.descriptor.partition_catalog.as_ref())?;
            }
            let replicas = unit_replicas(&plan.layout.node, placement, &owner);
            let descriptor = &page.descriptor;
            digest_unit_identity(
                &mut fingerprint,
                **unit,
                &owner,
                &replicas,
                &descriptor.origin,
            );
            add_counts(&mut counts, descriptor.counts)?;
            let mut supplied = 0_usize;
            for run in &page.runs {
                if run.section.is_metadata() {
                    continue;
                }
                supplied = supplied
                    .checked_add(run.rows)
                    .assured("a unit page holds at most the rows it was asked for");
            }
            let metadata = page
                .runs
                .iter()
                .any(|run| run.section.is_metadata() && run.rows != 0);
            if progress.closed || skip || (supplied == 0 && !metadata) {
                let unit_more = descriptor.more;
                output.push(StateInspectionUnit {
                    unit: **unit,
                    owner,
                    replicas,
                    origin: descriptor.origin.clone(),
                    counts: descriptor.counts,
                    sections: Vec::new(),
                    wasm: None,
                });
                progress.passed_without_rows(**unit, skip, unit_more);
                continue;
            }
            let encoded = self.encode_owner_page(&plan, page, budget.bytes).await?;
            budget.spend(&encoded)?;
            let unit_more = encoded.more || encoded.rows < supplied;
            progress.passed_with_rows(**unit, after, &encoded, unit_more)?;
            output.push(StateInspectionUnit {
                unit: **unit,
                owner,
                replicas,
                origin: encoded.origin,
                counts: encoded.counts,
                sections: encoded.sections,
                wasm: None,
            });
        }
        let more = progress.more;
        let position = progress.position;
        if more && budget.rows == plan.rows {
            return Err(StateInspectionRejection::ReplyTooLarge);
        }
        let generations = fingerprint.finish();
        if let Some(cursor) = &plan.cursor
            && cursor.generations != generations
        {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        let continuation = if more {
            Some(
                StateInspectionCursor {
                    domain: plan.domain,
                    entity: plan.processor,
                    schema: plan.layout.node.schema_fingerprint,
                    control_revision: plan.control_revision,
                    generations,
                    selection: plan.selection,
                    position: position.ok_or(StateInspectionRejection::ReplyTooLarge)?,
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
            wasm_binding: None,
        })
    }

    /// One unit's owner page: read here when this node owns the unit, otherwise received from the
    /// owner through the bounded page operation.
    async fn processor_unit_page(
        &self,
        request: ReadProcessorStatePage,
        plan: &ProcessorInspectionPlan,
    ) -> Result<OwnerPage, StateInspectionRejection> {
        if request.target.node_id() == self.inner.consensus.local_node_id() {
            return self.native_processor_state_page(request).await;
        }
        let deadline = *self
            .inner
            .runtime
            .state_inspection_settings()
            .read_timeout
            .value();
        let target = request.target.clone();
        let stream = match self
            .inner
            .interconnect
            .request_stream_with_timeout(request.target.node_id(), request.clone(), deadline)
            .await
        {
            Ok(stream) => stream,
            Err(error) => {
                return Err(inspection_refusal(
                    error,
                    StateInspectionRejection::OwnerUnavailable,
                ));
            }
        };
        let payload = plan
            .layout
            .sections
            .iter()
            .map(|section| (section.section, section.payload.clone()))
            .collect::<Vec<_>>();
        let received: ReceivedProcessorPage = match receive_processor_page(
            self.inner.runtime.inspection_executor(),
            stream,
            &request,
            &payload,
        )
        .await
        {
            Ok(page) => page?,
            Err(error) => {
                return Err(inspection_refusal(
                    error,
                    StateInspectionRejection::OwnerUnavailable,
                ));
            }
        };
        if received.descriptor.origin.node != target {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        let mut runs = Vec::with_capacity(received.descriptor.runs.len());
        for run in &received.descriptor.runs {
            let branch = BranchKey::from_remote_key(run.branch.clone()).map_err(|error| {
                inspection_refusal(error, StateInspectionRejection::OwnerUnavailable)
            })?;
            let rows = usize::try_from(run.rows).map_err(|error| {
                inspection_refusal(error, StateInspectionRejection::OwnerUnavailable)
            })?;
            runs.push(OwnerRun {
                section: run.section,
                branch,
                rows,
            });
        }
        let sections = received
            .sections
            .into_iter()
            .map(|section| OwnerSection {
                section: section.section,
                fields: section.fields,
                rows: section.rows,
                batch: section.batch,
            })
            .collect();
        Ok(OwnerPage {
            descriptor: received.descriptor,
            runs,
            sections,
            _memory: None,
        })
    }

    /// Writes an owner page's returned rows into Row frames, in the unit's row order, within the
    /// reply's remaining bytes.
    async fn encode_owner_page(
        &self,
        plan: &ProcessorInspectionPlan,
        page: OwnerPage,
        byte_limit: usize,
    ) -> Result<EncodedOwnerPage, StateInspectionRejection> {
        let branch = plan.branch.clone();
        let limits = plan.limits;
        let executor = self.inner.runtime.inspection_executor();
        let admission = executor
            .try_reserve(nervix_execution::MemoryClass::Bulk, 1)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        executor
            .run_cpu(
                nervix_execution::CpuClass::Bulk,
                admission,
                move |_charge, cancellation| {
                    encode_page(page, branch.as_ref(), &limits, byte_limit, cancellation)
                },
            )
            .await
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?
    }

    /// Reads one page of a local unit's reorderer or correlator state and projects its columns
    /// into the sections' complete fields.
    pub(in crate::application) async fn native_processor_state_page(
        &self,
        request: ReadProcessorStatePage,
    ) -> Result<OwnerPage, StateInspectionRejection> {
        let local = ClusterNodeIdentity::new(
            self.inner.consensus.local_node_id().clone(),
            self.inner.cluster.local_incarnation(),
        );
        if request.target != local {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        let state = self.inner.consensus.current_runtime_state().await;
        if state.revision != request.control_revision {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        let layout = self
            .prepared_processor_layout(state.schedule, &request.domain, &request.processor)
            .await?;
        if layout.node.schema_fingerprint != request.schema {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        if !layout.node.unit_executes_on(request.unit, local.node_id()) {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        let settings = self.inner.runtime.state_inspection_settings();
        let rows = usize::try_from(request.max_rows)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::InvalidLimit))?
            .min(settings.max_rows.value().get());
        let page = match self
            .inner
            .runtime
            .local_processor_state_page(
                &request.domain,
                &request.processor,
                request.unit,
                ProcessorStateSelection {
                    branch: request.selection,
                    partition_exclusions: request.partition_exclusions,
                },
                request.after,
                ProcessorStatePageBounds {
                    rows,
                    bytes: request.max_bytes.min(
                        u64::try_from(SessionLimits::DEFAULT.transfer_bytes())
                            .assured("session limits fit u64"),
                    ),
                },
            )
            .await
        {
            Ok(page) => page,
            Err(error) => {
                let reason = StateInspectionRejection::from(error.current_context());
                return Err(inspection_refusal(error, reason));
            }
        };
        let executor = self.inner.runtime.inspection_executor();
        let admission = executor
            .try_reserve(nervix_execution::MemoryClass::Bulk, 1)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        let sections_layout = layout.sections.clone();
        let page_sections = page.sections;
        let sections = executor
            .run_cpu(
                nervix_execution::CpuClass::Bulk,
                admission,
                move |_charge, cancellation| {
                    project_sections(&sections_layout, page_sections, cancellation)
                },
            )
            .await
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))??;
        let mut runs = Vec::with_capacity(page.runs.len());
        let mut transfer_runs = Vec::with_capacity(page.runs.len());
        for run in page.runs {
            transfer_runs.push(ProcessorStateRunTransfer {
                section: run.section,
                branch: BranchKey::to_remote_key(&run.branch),
                rows: u64::try_from(run.rows).assured("a bounded row count fits u64"),
            });
            runs.push(OwnerRun {
                section: run.section,
                branch: run.branch,
                rows: run.rows,
            });
        }
        let descriptor = ProcessorStatePageDescriptor {
            partition_catalog: page.partition_catalog,
            origin: StateInspectionOrigin {
                node: local,
                fingerprint: page.fingerprint,
                generation: page.generation,
                revision: state.revision,
                stage: StateInspectionStage::Live,
            },
            counts: page.counts,
            offset: page.offset,
            last_branch: page.last_branch,
            more: page.more,
            runs: transfer_runs,
            sections: sections
                .iter()
                .map(|section| ProcessorStateSectionTransfer {
                    section: section.section,
                    rows: section.rows,
                    fields: section.fields.clone(),
                    body: None,
                })
                .collect(),
        };
        Ok(OwnerPage {
            descriptor,
            runs,
            sections,
            _memory: page.memory,
        })
    }

    pub(in crate::application) async fn stream_processor_inspection_page(
        &self,
        request: ReadProcessorStatePage,
    ) -> Result<StreamingResponse, Report<StreamHandlerError>> {
        let max_bytes = request.max_bytes.min(
            u64::try_from(SessionLimits::DEFAULT.transfer_bytes())
                .assured("session limits fit u64"),
        );
        let deadline = *self
            .inner
            .runtime
            .state_inspection_settings()
            .read_timeout
            .value();
        let executor = self.inner.runtime.inspection_executor();
        let mut memory = None;
        let result = match timeout(deadline, self.native_processor_state_page(request)).await {
            Ok(Ok(page)) => {
                memory = page._memory;
                Ok((
                    page.descriptor,
                    page.sections
                        .into_iter()
                        .map(|section| ProcessorPageSection {
                            batch: section.batch,
                        })
                        .collect::<Vec<_>>(),
                ))
            }
            Ok(Err(reason)) => Err(reason),
            Err(_) => Err(StateInspectionRejection::Deadline),
        };
        let response = stream_processor_page(executor, result, max_bytes).await;
        drop(memory);
        response
    }

    /// The `branches:`, `rows:` and `state bytes:` lines `DESCRIBE` reports for each unit of a
    /// reorderer or correlator, read from its owners without returning any row.
    pub(in crate::application) async fn processor_state_lines(
        &self,
        domain: &DomainName,
        processor: NodeRef,
        node: &ScheduledNode,
    ) -> Vec<String> {
        let state = self.inner.consensus.current_runtime_state().await;
        let incarnations = self.live_node_incarnations().await;
        let local = self.inner.consensus.local_node_id();
        let settings = self.inner.runtime.state_inspection_settings();
        let partitioned = node.is_partitioned();
        let mut units = node.units.keys().copied().collect::<Vec<_>>();
        units.sort();
        let mut lines = Vec::new();
        for unit in units {
            nervix_primitives::task::consume_budget().await;
            let prefix = if partitioned {
                format!("{} ", unit.label_suffix().trim_start())
            } else {
                String::new()
            };
            let counts = match inspection_owner(node, unit, local) {
                Ok(owner) => match incarnations.get(&owner).copied() {
                    Some(incarnation) => {
                        let request = ReadProcessorStatePage {
                            domain: domain.clone(),
                            processor: processor.clone(),
                            unit,
                            control_revision: state.revision,
                            schema: node.schema_fingerprint,
                            selection: StateInspectionBranchSelection::All,
                            partition_exclusions: None,
                            after: None,
                            max_rows: 0,
                            max_bytes: 0,
                            target: ClusterNodeIdentity::new(owner, incarnation),
                        };
                        match timeout(
                            *settings.read_timeout.value(),
                            self.processor_unit_counts(request),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(_) => Err(StateInspectionRejection::Deadline),
                        }
                    }
                    None => Err(StateInspectionRejection::OwnerUnavailable),
                },
                Err(reason) => Err(reason),
            };
            let label = if processor.kind == ModelKind::Deduplicator {
                "keys"
            } else {
                "rows"
            };
            match counts {
                Ok(counts) => {
                    lines.push(format!("{prefix}branches: {}", counts.branches));
                    lines.push(format!("{prefix}{label}: {}", counts.rows));
                    lines.push(format!("{prefix}state bytes: {}", counts.bytes));
                }
                Err(reason) => {
                    for line in ["branches", label, "state bytes"] {
                        lines.push(format!("{prefix}{line}: unavailable ({})", reason.as_ref()));
                    }
                }
            }
        }
        lines
    }

    async fn processor_unit_counts(
        &self,
        request: ReadProcessorStatePage,
    ) -> Result<StateInspectionCounts, StateInspectionRejection> {
        if request.target.node_id() == self.inner.consensus.local_node_id() {
            let page = self.native_processor_state_page(request).await?;
            return Ok(page.descriptor.counts);
        }
        let deadline = *self
            .inner
            .runtime
            .state_inspection_settings()
            .read_timeout
            .value();
        let stream = self
            .inner
            .interconnect
            .request_stream_with_timeout(request.target.node_id(), request.clone(), deadline)
            .await
            .map_err(|error| {
                inspection_refusal(error, StateInspectionRejection::OwnerUnavailable)
            })?;
        let received = receive_processor_page(
            self.inner.runtime.inspection_executor(),
            stream,
            &request,
            &[],
        )
        .await
        .map_err(|error| inspection_refusal(error, StateInspectionRejection::OwnerUnavailable))??;
        Ok(received.descriptor.counts)
    }
}

/// The typed refusal an inspection reports when a unit's owner could not read its page. A
/// continuation position the unit's state no longer holds is stale. A unit task this node no
/// longer runs, or a branch task that could not answer, leaves the owner unavailable. A processor
/// still restoring its branches, or a page the node could not admit or gather, is not ready yet.
impl From<&ProcessorStatePageError> for StateInspectionRejection {
    fn from(error: &ProcessorStatePageError) -> Self {
        match error {
            ProcessorStatePageError::Position => Self::StaleContinuation,
            ProcessorStatePageError::Unavailable | ProcessorStatePageError::Branch => {
                Self::OwnerUnavailable
            }
            ProcessorStatePageError::Unrestored
            | ProcessorStatePageError::Admission
            | ProcessorStatePageError::Columns => Self::NotReady,
        }
    }
}

/// Projects a data-plane page's columns into each section's complete fields, appending the
/// owner's columns with the types its programs produce.
fn project_sections(
    layout: &[SectionLayout],
    sections: Vec<crate::runtime::ProcessorStateSectionColumns>,
    cancellation: &nervix_execution::Cancellation,
) -> Result<Vec<OwnerSection>, StateInspectionRejection> {
    if layout.len() != sections.len() {
        return Err(StateInspectionRejection::NotReady);
    }
    let mut projected = Vec::with_capacity(sections.len());
    for (layout, section) in layout.iter().zip(sections) {
        cancellation
            .check()
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
        if layout.section != section.section {
            return Err(StateInspectionRejection::NotReady);
        }
        let returned = match section.payload.first() {
            Some(column) => column.len(),
            None => match section.appended.first() {
                Some(column) => column.len(),
                None => 0,
            },
        };
        if returned == 0 {
            // Nothing of this section was returned: the page carries only its complete count.
            projected.push(OwnerSection {
                section: section.section,
                fields: Vec::new(),
                rows: section.rows,
                batch: None,
            });
            continue;
        }
        let mut key_types = Vec::with_capacity(section.key_fields.len());
        for field in &section.key_fields {
            let ty = crate::runtime_schema::parse_as_type_from_arrow(field.data_type())
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
            key_types.push((ty, field.is_nullable()));
        }
        let fields = section_fields(layout, &key_types, cancellation)?;
        let mut columns = section.payload;
        columns.extend(section.appended);
        let schema = nervix_primitives::sync::StdArc::new(SchemaField::arrow_schema(&fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        projected.push(OwnerSection {
            section: section.section,
            fields,
            rows: section.rows,
            batch: Some(batch),
        });
    }
    Ok(projected)
}

/// One unit's encoded sections and what they consumed of the reply.
struct EncodedOwnerPage {
    origin: StateInspectionOrigin,
    counts: StateInspectionCounts,
    sections: Vec<StateInspectionSection>,
    rows: usize,
    bytes: usize,
    more: bool,
    last_branch: Option<BranchKeyFingerprint>,
}

/// Writes the page's returned rows into one Row frame each, in the unit's row order, stopping at
/// the first row whose frame no longer fits the reply.
fn encode_page(
    page: OwnerPage,
    branch: Option<&RowBranch>,
    limits: &SessionLimits,
    byte_limit: usize,
    cancellation: &nervix_execution::Cancellation,
) -> Result<EncodedOwnerPage, StateInspectionRejection> {
    let OwnerPage {
        descriptor,
        runs,
        sections,
        _memory,
    } = page;
    let mut schemas = Vec::with_capacity(sections.len());
    for section in &sections {
        schemas.push(RowSchema {
            fields: section.fields.clone(),
            branch: branch.cloned(),
        });
    }
    let mut columns = Vec::with_capacity(sections.len());
    for (section, schema) in sections.iter().zip(&schemas) {
        columns.push(match &section.batch {
            Some(batch) => {
                Some(ArrowRowBatch::new(schema, batch).map_err(|error| {
                    inspection_refusal(error, StateInspectionRejection::NotReady)
                })?)
            }
            None => None,
        });
    }
    let mut frames = vec![Vec::new(); sections.len()];
    let mut next_row = vec![0_usize; sections.len()];
    let mut frame_bytes = 0_usize;
    let mut rows = 0_usize;
    let mut last_branch = None;
    let mut full = false;
    'runs: for run in &runs {
        let Some(index) = run.section.index_in(&sections, |section| section.section) else {
            return Err(StateInspectionRejection::NotReady);
        };
        let key = &run.branch;
        let columns = columns[index]
            .as_ref()
            .ok_or(StateInspectionRejection::NotReady)?;
        for _ in 0..run.rows {
            cancellation
                .check()
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
            let row = next_row[index];
            let mut encoder = match (branch, key) {
                (None, None) => StateRowsEncoder::unbranched(limits),
                (Some(branch), Some(key)) => {
                    key.conform_to(branch.fields()).map_err(|error| {
                        inspection_refusal(error, StateInspectionRejection::NotReady)
                    })?;
                    StateRowsEncoder::branched(limits, |writer| {
                        for field in branch.fields() {
                            if field.sensitive {
                                writer.push_redacted()?;
                            } else {
                                let value = key
                                    .field_value(field.name.as_str())
                                    .verified("the branch conforms to its declared fields");
                                write_runtime_value(value, writer)?;
                            }
                        }
                        Ok(())
                    })
                    .map_err(|error| {
                        inspection_refusal(error, StateInspectionRejection::ReplyTooLarge)
                    })?
                }
                _ => return Err(StateInspectionRejection::NotReady),
            };
            encoder
                .push_row(|writer| columns.write_row(row, writer))
                .map_err(|error| {
                    inspection_refusal(error, StateInspectionRejection::ReplyTooLarge)
                })?;
            let frame = encoder.finish(limits).map_err(|error| {
                inspection_refusal(error, StateInspectionRejection::ReplyTooLarge)
            })?;
            let Some(total) = frame_bytes
                .checked_add(frame.bytes().len())
                .filter(|&bytes| bytes <= byte_limit)
            else {
                if run.section.is_metadata() {
                    return Err(StateInspectionRejection::ReplyTooLarge);
                }
                full = true;
                break 'runs;
            };
            frame_bytes = total;
            frames[index].push(frame);
            next_row[index] = row
                .checked_add(1)
                .assured("a section's rows are counted by an addressable batch");
            if !run.section.is_metadata() {
                rows = rows
                    .checked_add(1)
                    .assured("a page's rows are counted by addressable batches");
                last_branch = key.as_ref().map(BranchKey::fingerprint);
            }
        }
    }
    let mut encoded = Vec::new();
    for ((section, schema), frames) in sections.into_iter().zip(schemas).zip(frames) {
        if frames.is_empty() {
            continue;
        }
        let returned = u64::try_from(frames.len()).assured("bounded row counts fit u64");
        encoded.push(StateInspectionSection {
            name: section.section.name(),
            schema,
            frames,
            omitted_rows: section
                .rows
                .checked_sub(returned)
                .ok_or(StateInspectionRejection::NotReady)?,
            watermarks: None,
        });
    }
    Ok(EncodedOwnerPage {
        origin: descriptor.origin,
        counts: descriptor.counts,
        sections: encoded,
        rows,
        bytes: frame_bytes,
        more: descriptor.more || full,
        last_branch,
    })
}

#[cfg(test)]
#[path = "processor_state_tests.rs"]
mod tests;
