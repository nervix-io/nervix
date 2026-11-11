//! Typed materialized relay inspections coordinated against captured unit owners.
//!
//! Layer: control plane.
//! - **Owns.** Schedule validation, per-unit routing, cursor fencing and bounded inspection assembly.
//! - **Depends on.** Committed vocabulary, native owner pages, shared Row codecs and typed outcomes.
//! - **Must not know.** How materialized rows execute, persist, replicate or are installed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns the boundary declared above")
)]

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    RowBranch, RowSchema, SessionLimits, StateInspection, StateInspectionPage,
    StateInspectionResult, StateInspectionSection, StateInspectionUnit, StateRowsEncoder,
    StateWatermarkColumns,
};
use nervix_interconnect::{
    MaterializedStatePageDescriptor, ReadMaterializedStatePage, StreamHandlerError,
    StreamingResponse,
};
use nervix_models::{
    ClusterNodeIdentity, CreateRelay, CreateSchema, DomainName, DomainNodeRef, ModelKind, NodeRef,
    ParseAsType, RelayName, ResolvedBranching, ResolvedStateBranchSelection, ScheduledNode,
    ShowRelayMaterializedState, StateInspectionBranchSelection, StateInspectionCounts,
    StateInspectionOrigin, StateInspectionPosition, StateInspectionRejection,
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
        MaterializedGenerationRecord, MaterializedInspectionPage, MaterializedStateCapture,
        StatePageReadError,
        state_inspection_exchange::{receive_materialized_page, stream_materialized_page},
    },
    state_inspection_cursor::StateInspectionCursor,
};

fn resume_offset(after: Option<(Option<nervix_models::BranchKeyFingerprint>, u64)>) -> u64 {
    match after {
        Some((_, offset)) => offset,
        None => 0,
    }
}

struct InspectionPlan {
    domain: DomainName,
    relay: RelayName,
    node: ScheduledNode,
    control_revision: u64,
    schema: RowSchema,
    selection: StateInspectionBranchSelection,
    rows: usize,
    sections: usize,
    cursor: Option<StateInspectionCursor>,
    limits: SessionLimits,
    moving_partitions: Option<nervix_models::ResolvedBranchPartitions>,
}

struct NativeMaterializedCapture {
    descriptor: MaterializedStatePageDescriptor,
    state: MaterializedStateCapture,
}

/// The remaining capacity belongs to the entire reply, across all execution units.
struct InspectionBudget {
    rows: usize,
    sections: usize,
    bytes: usize,
}

impl InspectionBudget {
    fn allowed_rows(&self, closed: bool, skip: bool) -> usize {
        if closed || skip || self.sections == 0 || self.bytes == 0 {
            0
        } else {
            self.rows
        }
    }
}

enum InspectionUnitRows {
    Local(MaterializedInspectionPage),
    Remote(crate::runtime::state_inspection_exchange::ReceivedMaterializedPage),
}

impl InspectionUnitRows {
    fn records(&self) -> impl Iterator<Item = MaterializedGenerationRecord> + '_ {
        let mut next = 0;
        std::iter::from_fn(move || {
            let record = match self {
                Self::Local(page) => page.generation.records().get(next).cloned(),
                Self::Remote(page) => page.record(next),
            };
            next += usize::from(record.is_some());
            record
        })
    }
    fn len(&self) -> usize {
        match self {
            Self::Local(page) => page.generation.records().len(),
            Self::Remote(page) => page.len(),
        }
    }
}

impl SessionServiceImpl {
    async fn capture_materialized_partition_inspection(
        &self,
        plan: &InspectionPlan,
        incarnations: &std::collections::BTreeMap<
            nervix_models::ClusterNodeName,
            nervix_models::ClusterNodeIncarnation,
        >,
    ) -> Result<
        Option<super::processor_state::partition::PartitionInspection>,
        StateInspectionRejection,
    > {
        let Some(partitions) = plan.moving_partitions else {
            return Ok(None);
        };
        let executor = self.inner.runtime.inspection_executor();
        let maximum =
            u64::try_from(plan.limits.transfer_bytes()).assured("session byte limits fit u64");
        let mut inspection = super::processor_state::partition::PartitionInspection::new(
            partitions, executor, maximum,
        )?;
        for unit in plan.node.units.keys() {
            let owner = inspection_owner(&plan.node, *unit, self.inner.consensus.local_node_id())?;
            let incarnation = incarnations
                .get(&owner)
                .copied()
                .ok_or(StateInspectionRejection::OwnerUnavailable)?;
            let request = nervix_interconnect::ReadMaterializedKeyCatalog {
                read: ReadMaterializedStatePage {
                    domain: plan.domain.clone(),
                    relay: plan.relay.clone(),
                    unit: *unit,
                    control_revision: plan.control_revision,
                    schema: plan.node.schema_fingerprint,
                    selection: plan.selection,
                    after: None,
                    max_rows: 0,
                    max_bytes: maximum,
                    target: ClusterNodeIdentity::new(owner.clone(), incarnation),
                },
            };
            let response = if &owner == self.inner.consensus.local_node_id() {
                self.handle_materialized_key_catalog(request).await
            } else {
                self.inner
                    .interconnect
                    .request(&owner, request)
                    .await
                    .map_err(|error| {
                        inspection_refusal(error, StateInspectionRejection::OwnerUnavailable)
                    })?
            };
            let catalog = response.result?;
            let admission = executor
                .try_reserve(nervix_execution::MemoryClass::Bulk, 1)
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
            let unit = *unit;
            inspection = executor
                .run_cpu(
                    nervix_execution::CpuClass::Bulk,
                    admission,
                    move |_charge, cancellation| {
                        inspection.record(unit, catalog, cancellation)?;
                        Ok::<_, StateInspectionRejection>(inspection)
                    },
                )
                .await
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))??;
        }
        Ok(Some(inspection))
    }

    pub(in crate::application) async fn show_stream_materialized_state(
        &self,
        domain: &DomainName,
        show: ShowRelayMaterializedState,
    ) -> CommandResult {
        let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, &show.relay);
        let settings = self.inner.runtime.state_inspection_settings();
        let result = timeout(
            *settings.read_timeout.value(),
            self.inspect_materialized_state(domain, &show),
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

    async fn inspection_plan(
        &self,
        domain: &DomainName,
        show: &ShowRelayMaterializedState,
    ) -> Result<InspectionPlan, StateInspectionRejection> {
        let state = self.inner.consensus.current_runtime_state().await;
        let domain_schedule = state
            .schedule
            .domain(domain)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        let relay = domain_schedule
            .scheduled::<CreateRelay>(&show.relay)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        if relay.config.materialized_state.is_none() {
            return Err(StateInspectionRejection::NotMaterialized);
        }
        let schema = domain_schedule
            .configured::<CreateSchema>(&relay.config.schema)
            .ok_or(StateInspectionRejection::NotReady)?;
        let branching = relay
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
        let branch = match branching {
            ResolvedBranching::Unbranched => None,
            ResolvedBranching::Branched { branch, schema, .. }
            | ResolvedBranching::AwaitingTopology { branch, schema } => Some(
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
        let token = show.page.as_ref().and_then(|page| page.after.as_deref());
        let cursor = match token {
            Some(token) => Some(
                StateInspectionCursor::decode(token, limits.string_bytes()).map_err(|error| {
                    inspection_refusal(error, StateInspectionRejection::StaleContinuation)
                })?,
            ),
            None => None,
        };
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.domain != *domain
                || cursor.entity != NodeRef::new(ModelKind::Relay, show.relay.clone())
                || cursor.schema != relay.node.schema_fingerprint
                || cursor.control_revision != state.revision
                || cursor.selection != selection
        }) {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        if cursor.as_ref().is_some_and(|cursor| {
            !relay.node.units.contains_key(match &cursor.position {
                StateInspectionPosition::AfterUnit(unit)
                | StateInspectionPosition::Rows { unit, .. } => unit,
            })
        }) {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        let moving_partitions = if let Some(branch) = relay.node.partition_branch()
            && let Some(epoch) = domain_schedule.partition_epochs.get(branch)
            && epoch.transition.is_some()
        {
            Some(epoch.partitions)
        } else {
            None
        };
        Ok(InspectionPlan {
            domain: domain.clone(),
            relay: show.relay.clone(),
            node: relay.node.clone(),
            control_revision: state.revision,
            schema: RowSchema {
                fields: schema.fields.clone(),
                branch,
            },
            selection,
            rows,
            sections: settings
                .max_sections
                .value()
                .get()
                .min(limits.collection_entries()),
            cursor,
            limits,
            moving_partitions,
        })
    }

    async fn inspect_materialized_state(
        &self,
        domain: &DomainName,
        show: &ShowRelayMaterializedState,
    ) -> Result<StateInspectionPage, StateInspectionRejection> {
        let plan = self.inspection_plan(domain, show).await?;
        let incarnations = self.live_node_incarnations().await;
        let partition_inspection = self
            .capture_materialized_partition_inspection(&plan, &incarnations)
            .await?;
        let local = self.inner.consensus.local_node_id();
        let mut units = plan.node.units.iter().collect::<Vec<_>>();
        units.sort_by_key(|(unit, _)| **unit);
        let mut output = Vec::new();
        let mut counts = StateInspectionCounts::default();
        let mut fingerprint = nervix_crypto::digest::Sha256::new();
        fingerprint.update(b"nervix/state-inspection/owners");
        fingerprint.update(plan.node.schema_fingerprint.as_digest());
        let mut budget = InspectionBudget {
            rows: plan.rows,
            sections: plan.sections,
            bytes: plan.limits.transfer_bytes(),
        };
        let mut closed = false;
        let mut more = false;
        let mut position = plan.cursor.as_ref().map(|cursor| cursor.position.clone());
        for (unit, placement) in &units {
            let owner = inspection_owner(&plan.node, **unit, local)?;
            let incarnation = incarnations
                .get(&owner)
                .copied()
                .ok_or(StateInspectionRejection::OwnerUnavailable)?;
            let target = ClusterNodeIdentity::new(owner.clone(), incarnation);
            let (skip, after) = resume_position(plan.cursor.as_ref(), **unit);
            let allowed = budget.allowed_rows(closed, skip);
            let request = ReadMaterializedStatePage {
                domain: domain.clone(),
                relay: show.relay.clone(),
                unit: **unit,
                schema: plan.node.schema_fingerprint,
                control_revision: plan.control_revision,
                selection: plan.selection,
                after,
                max_rows: u64::try_from(allowed).assured("a row limit fits u64"),
                max_bytes: u64::try_from(budget.bytes).assured("the session byte limit fits u64"),
                target,
            };
            let filter = partition_inspection
                .as_ref()
                .map(|inspection| inspection.filter(**unit))
                .transpose()?;
            let (descriptor, records) = self.inspection_unit_page(&request, &plan, filter).await?;
            let replicas = if plan.node.config.executes_on_every_cluster_node() {
                Vec::new()
            } else {
                placement
                    .replica_nodes()
                    .into_iter()
                    .filter(|node| **node != owner)
                    .cloned()
                    .collect::<Vec<_>>()
            };
            let identity = serde_json::to_vec(&(
                **unit,
                &owner,
                &replicas,
                &descriptor.origin,
                descriptor.fence,
            ))
            .assured("captured identities are JSON values");
            fingerprint.update(
                &u64::try_from(identity.len())
                    .assured("a bounded identity fits u64")
                    .to_be_bytes(),
            );
            fingerprint.update(&identity);
            add_counts(&mut counts, descriptor.counts)?;
            if closed || skip {
                output.push(StateInspectionUnit {
                    unit: **unit,
                    owner,
                    replicas,
                    origin: descriptor.origin,
                    counts: descriptor.counts,
                    sections: Vec::new(),
                    wasm: None,
                });
                continue;
            }
            let supplied = records.len();
            let mut returned = 0;
            let mut last_branch = None;
            let mut sections = Vec::new();
            if supplied != 0 {
                if budget.sections == 0 {
                    return Err(StateInspectionRejection::NotReady);
                }
                let schema = plan.schema.clone();
                let limits = plan.limits;
                let total = descriptor.counts.rows;
                let executor = self.inner.runtime.inspection_executor();
                let admission = executor
                    .try_reserve(nervix_execution::MemoryClass::Bulk, 1)
                    .map_err(|error| {
                        inspection_refusal(error, StateInspectionRejection::NotReady)
                    })?;
                let encoded = executor
                    .run_cpu(
                        nervix_execution::CpuClass::Bulk,
                        admission,
                        move |_charge, cancellation| {
                            encode_records(
                                &schema,
                                &records,
                                total,
                                &limits,
                                budget.bytes,
                                cancellation,
                            )
                        },
                    )
                    .await
                    .map_err(|error| {
                        inspection_refusal(error, StateInspectionRejection::NotReady)
                    })??;
                returned = encoded.rows;
                last_branch = encoded.last_branch;
                if let Some(section) = encoded.section {
                    budget.bytes = budget
                        .bytes
                        .checked_sub(encoded.bytes)
                        .ok_or(StateInspectionRejection::NotReady)?;
                    sections.push(section);
                    budget.sections -= 1;
                }
            }
            budget.rows = budget
                .rows
                .checked_sub(returned)
                .ok_or(StateInspectionRejection::NotReady)?;
            output.push(StateInspectionUnit {
                unit: **unit,
                owner,
                replicas,
                origin: descriptor.origin,
                counts: descriptor.counts,
                sections,
                wasm: None,
            });
            if descriptor.more || returned < supplied {
                more = true;
                closed = true;
                position = if returned == 0 {
                    position
                } else {
                    Some(StateInspectionPosition::Rows {
                        unit: **unit,
                        branch: last_branch,
                        offset: resume_offset(after)
                            .checked_add(
                                u64::try_from(returned).assured("bounded row counts fit u64"),
                            )
                            .ok_or(StateInspectionRejection::ReplyTooLarge)?,
                    })
                };
            } else {
                position = Some(StateInspectionPosition::AfterUnit(**unit));
            }
        }
        if more && budget.rows == plan.rows {
            return Err(StateInspectionRejection::ReplyTooLarge);
        }
        let generations = fingerprint.finish();
        if plan
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.generations != generations)
        {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        let continuation = if more {
            Some(
                StateInspectionCursor {
                    domain: plan.domain,
                    entity: NodeRef::new(ModelKind::Relay, plan.relay),
                    schema: plan.node.schema_fingerprint,
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

    async fn inspection_unit_page(
        &self,
        request: &ReadMaterializedStatePage,
        plan: &InspectionPlan,
        filter: Option<nervix_interconnect::MaterializedPartitionFilter>,
    ) -> Result<(MaterializedStatePageDescriptor, InspectionUnitRows), StateInspectionRejection>
    {
        if request.target.node_id() == self.inner.consensus.local_node_id() {
            let (descriptor, page) = match filter {
                Some(filter) => {
                    let capture = self
                        .native_materialized_capture(request.clone(), true, Some(filter))
                        .await?;
                    (capture.descriptor, capture.state.page)
                }
                None => {
                    self.native_materialized_inspection_page(request.clone())
                        .await?
                }
            };
            return Ok((descriptor, InspectionUnitRows::Local(page)));
        }
        let deadline = *self
            .inner
            .runtime
            .state_inspection_settings()
            .read_timeout
            .value();
        let stream = match match filter {
            Some(filter) => {
                self.inner
                    .interconnect
                    .request_stream_with_timeout(
                        request.target.node_id(),
                        nervix_interconnect::ReadPartitionedMaterializedStatePage {
                            read: request.clone(),
                            filter,
                        },
                        deadline,
                    )
                    .await
            }
            None => {
                self.inner
                    .interconnect
                    .request_stream_with_timeout(
                        request.target.node_id(),
                        request.clone(),
                        deadline,
                    )
                    .await
            }
        } {
            Ok(stream) => stream,
            Err(error) => {
                return Err(inspection_refusal(
                    error,
                    StateInspectionRejection::OwnerUnavailable,
                ));
            }
        };
        let schema = crate::runtime_schema::compile_schema(&CreateSchema {
            name: nervix_models::SchemaName::parse("inspection")
                .assured("the fixed schema name is valid"),
            fields: plan.schema.fields.clone(),
        })
        .arrow_schema();
        let page = match receive_materialized_page(
            self.inner.runtime.inspection_executor(),
            stream,
            &schema,
            request,
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
        if page.descriptor.origin.node != request.target {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        Ok((page.descriptor.clone(), InspectionUnitRows::Remote(page)))
    }

    pub(in crate::application) async fn native_materialized_inspection_page(
        &self,
        request: ReadMaterializedStatePage,
    ) -> Result<
        (MaterializedStatePageDescriptor, MaterializedInspectionPage),
        StateInspectionRejection,
    > {
        self.native_materialized_capture(request, false, None)
            .await
            .map(|capture| (capture.descriptor, capture.state.page))
    }

    async fn native_materialized_capture(
        &self,
        request: ReadMaterializedStatePage,
        catalog: bool,
        filter: Option<nervix_interconnect::MaterializedPartitionFilter>,
    ) -> Result<NativeMaterializedCapture, StateInspectionRejection> {
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
        let schedule = state.schedule;
        let domain = schedule
            .domain(&request.domain)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        let relay = domain
            .scheduled::<CreateRelay>(&request.relay)
            .ok_or(StateInspectionRejection::UnknownEntity)?;
        if relay.config.materialized_state.is_none() {
            return Err(StateInspectionRejection::NotMaterialized);
        }
        if relay.node.schema_fingerprint != request.schema {
            return Err(StateInspectionRejection::StaleContinuation);
        }
        if !relay.node.unit_executes_on(request.unit, local.node_id()) {
            return Err(StateInspectionRejection::OwnerUnavailable);
        }
        let settings = self.inner.runtime.state_inspection_settings();
        let rows = usize::try_from(request.max_rows)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::InvalidLimit))?
            .min(settings.max_rows.value().get());
        let captured = match self
            .inner
            .runtime
            .capture_materialized_state_page(
                &request.domain,
                &request.relay,
                request.unit,
                request.selection,
                request.after,
                crate::runtime::MaterializedPageBounds {
                    rows,
                    bytes: request.max_bytes.min(
                        u64::try_from(SessionLimits::DEFAULT.transfer_bytes())
                            .assured("session limits fit u64"),
                    ),
                },
                catalog,
                filter,
            )
            .await
        {
            Ok(page) => page,
            Err(error) => {
                let reason = match error.current_context() {
                    StatePageReadError::Position => StateInspectionRejection::StaleContinuation,
                    StatePageReadError::RowTooLarge => StateInspectionRejection::ReplyTooLarge,
                    _ => StateInspectionRejection::NotReady,
                };
                return Err(inspection_refusal(error, reason));
            }
        };
        let page = &captured.page;
        let descriptor = MaterializedStatePageDescriptor {
            origin: StateInspectionOrigin {
                node: local,
                fingerprint: page.fingerprint,
                generation: page.generation.branch_generation(),
                revision: page.generation.revision(),
                stage: page.stage,
            },
            fence: page.generation.fence(),
            counts: page.counts,
            offset: page.offset,
            last_branch: page.last_branch,
            more: page.more,
        };
        Ok(NativeMaterializedCapture {
            descriptor,
            state: captured,
        })
    }

    pub(in crate::application) async fn handle_materialized_key_catalog(
        &self,
        request: nervix_interconnect::ReadMaterializedKeyCatalog,
    ) -> nervix_interconnect::MaterializedKeyCatalogResponse {
        let result = self
            .native_materialized_capture(request.read, true, None)
            .await
            .map(|capture| capture.state.catalog);
        nervix_interconnect::MaterializedKeyCatalogResponse { result }
    }

    pub(in crate::application) async fn stream_partitioned_materialized_inspection_page(
        &self,
        request: nervix_interconnect::ReadPartitionedMaterializedStatePage,
    ) -> Result<StreamingResponse, Report<StreamHandlerError>> {
        self.stream_materialized_inspection_capture(request.read, Some(request.filter))
            .await
    }

    pub(in crate::application) async fn stream_materialized_inspection_page(
        &self,
        request: ReadMaterializedStatePage,
    ) -> Result<StreamingResponse, Report<StreamHandlerError>> {
        self.stream_materialized_inspection_capture(request, None)
            .await
    }

    async fn stream_materialized_inspection_capture(
        &self,
        request: ReadMaterializedStatePage,
        filter: Option<nervix_interconnect::MaterializedPartitionFilter>,
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
        match timeout(deadline, async {
            let result = match filter {
                Some(filter) => self
                    .native_materialized_capture(request, true, Some(filter))
                    .await
                    .map(|capture| (capture.descriptor, capture.state.page)),
                None => self.native_materialized_inspection_page(request).await,
            };
            stream_materialized_page(executor, result, max_bytes).await
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                stream_materialized_page(
                    executor,
                    Err(StateInspectionRejection::Deadline),
                    max_bytes,
                )
                .await
            }
        }
    }
}

struct EncodedInspectionRows {
    section: Option<StateInspectionSection>,
    rows: usize,
    bytes: usize,
    last_branch: Option<nervix_models::BranchKeyFingerprint>,
}

fn encode_records(
    payload_schema: &RowSchema,
    records: &InspectionUnitRows,
    total_rows: u64,
    limits: &SessionLimits,
    byte_limit: usize,
    cancellation: &nervix_execution::Cancellation,
) -> Result<EncodedInspectionRows, StateInspectionRejection> {
    let mut schema = payload_schema.clone();
    let low = distinct_field(
        &schema.fields,
        "ingested_at_low_watermark",
        ParseAsType::Datetime,
        false,
        false,
        cancellation,
    )?;
    let low_index = schema.fields.len();
    schema.fields.push(low);
    let high = distinct_field(
        &schema.fields,
        "ingested_at_high_watermark",
        ParseAsType::Datetime,
        false,
        false,
        cancellation,
    )?;
    let high_index = schema.fields.len();
    schema.fields.push(high);
    let mut frames = Vec::with_capacity(records.len());
    let mut frame_bytes = 0_usize;
    let mut last_branch = None;
    for record in records.records() {
        cancellation
            .check()
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
        let columns = ArrowRowBatch::new(payload_schema, record.row.batch().batch())
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        let mut encoder = match (&payload_schema.branch, &record.branch) {
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
            .push_row(|writer| {
                columns.write_row(record.row.row_index(), writer)?;
                writer.push_datetime(record.row.metadata().ingested_at_low_watermark())?;
                writer.push_datetime(record.row.metadata().ingested_at_high_watermark())
            })
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::ReplyTooLarge))?;
        let frame = encoder
            .finish(limits)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::ReplyTooLarge))?;
        let Some(total) = frame_bytes
            .checked_add(frame.bytes().len())
            .filter(|&bytes| bytes <= byte_limit)
        else {
            break;
        };
        frame_bytes = total;
        last_branch = record.branch.as_ref().map(|branch| branch.fingerprint());
        frames.push(frame);
    }
    let rows = frames.len();
    if rows == 0 {
        return Ok(EncodedInspectionRows {
            section: None,
            rows: 0,
            bytes: 0,
            last_branch: None,
        });
    }
    let returned = u64::try_from(rows).assured("bounded row counts fit u64");
    Ok(EncodedInspectionRows {
        rows,
        bytes: frame_bytes,
        last_branch,
        section: Some(StateInspectionSection {
            name: "records".to_string(),
            schema,
            frames,
            omitted_rows: total_rows
                .checked_sub(returned)
                .ok_or(StateInspectionRejection::NotReady)?,
            watermarks: Some(StateWatermarkColumns {
                low: u32::try_from(low_index).map_err(|error| {
                    inspection_refusal(error, StateInspectionRejection::ReplyTooLarge)
                })?,
                high: u32::try_from(high_index).map_err(|error| {
                    inspection_refusal(error, StateInspectionRejection::ReplyTooLarge)
                })?,
            }),
        }),
    })
}
