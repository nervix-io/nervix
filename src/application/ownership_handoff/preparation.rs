//! Capturing and preparing the moves of a planned ownership handoff.
//!
//! Layer: control plane.
//!
//! - **Owns.** The capture of every moved unit's state on its source node, the frozen cut of every
//!   old unit a regrouped reader reads, and the preparation of every move on its destination and
//!   replicas, all at the same time within one preparation deadline.
//! - **Depends on.** The interconnect to drive each capture and preparation, and the planned
//!   handoff's request identity and attempt bookkeeping in the parent module.
//! - **Must not know.** Which schedule change asked for the move, or how the parent confirms,
//!   commits, activates and finishes the handoff.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::btree_map;

use nervix_interconnect::CaptureOwnershipHandoffStateRequest as RemoteCaptureOwnershipHandoffStateRequest;

use super::*;

/// The frozen cut of each old unit a regrouped reader's moves read, by unit.
type FrozenIngestorUnitSources =
    BTreeMap<NodeUnitRef, Vec<nervix_interconnect::OwnershipHandoffCheckpoint>>;

/// How the capture of one planned move ended: with its input, its failure, or the deadline.
struct PlannedMoveCapture<'a> {
    moved: &'a PlannedOwnershipMove,
    capture: Result<
        OwnershipHandoffResult<nervix_interconnect::OwnershipHandoffInput>,
        nervix_primitives::time::error::Elapsed,
    >,
}

/// A planned move whose input was captured and is ready to prepare.
struct CapturedPlannedMove<'a> {
    moved: &'a PlannedOwnershipMove,
    input: nervix_interconnect::OwnershipHandoffInput,
}

/// How the preparation of one planned move ended on its destination and replicas.
struct PlannedMovePreparation<'a> {
    moved: &'a PlannedOwnershipMove,
    preparation: Result<OwnershipHandoffResult<()>, nervix_primitives::time::error::Elapsed>,
}

/// How the capture of one old unit's frozen cut ended.
struct FrozenSourceCapture<'a> {
    key: &'a NodeUnitRef,
    source: &'a PlannedOwnershipMove,
    capture: Result<
        OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>>,
        nervix_primitives::time::error::Elapsed,
    >,
}

impl SessionServiceImpl {
    /// Captures every move of a planned handoff and prepares it on its destination and replicas.
    ///
    /// The moves proceed at the same time, each within the preparation deadline, so preparing a
    /// handoff takes about as long as its slowest move rather than the sum of its moves. Doing them
    /// one after another made a reader regrouped from twelve units into sixteen wait for every one
    /// of its sixteen moves, each reading all twelve old units, in turn.
    ///
    /// A capture only reads, so the first failed capture ends the step at once. A preparation
    /// persists on its recipients, so every attempt is recorded before any preparation starts, and
    /// after a failure the preparations already started finish, or reach the deadline, before the
    /// caller discards the attempts.
    pub(super) async fn prepare_planned_ownership_moves(
        &self,
        scope: &PlannedHandoffRequestScope<'_>,
        preparations: &mut ClusterOwnershipHandoffPreparations,
        moves: &[PlannedOwnershipMove],
    ) -> Result<(), Report<DomainAlterError>> {
        let frozen_sources = self
            .capture_frozen_ingestor_unit_sources(scope, moves)
            .await?;
        let mut captures = FuturesUnordered::new();
        for moved in moves {
            let frozen_sources = &frozen_sources;
            captures.push(async move {
                let capture = nervix_primitives::time::timeout_at(
                    scope.preparation_deadline,
                    self.capture_planned_ownership_input(scope, moved, frozen_sources),
                )
                .await;
                PlannedMoveCapture { moved, capture }
            });
        }
        let mut captured = Vec::with_capacity(moves.len());
        while let Some(PlannedMoveCapture { moved, capture }) = captures.next().await {
            nervix_primitives::task::consume_budget().await;
            match capture {
                Ok(Ok(input)) => captured.push(CapturedPlannedMove { moved, input }),
                Ok(Err(reason)) => {
                    return Err(scope.failure(format!(
                        "failed to capture {} '{}' on source node '{}': {reason}",
                        moved.entity.kind.as_str(),
                        moved.entity.identifier.as_str(),
                        moved.former_owner
                    )));
                }
                Err(_) => {
                    return Err(scope.failure(format!(
                        "timed out capturing {} '{}' on source node '{}'",
                        moved.entity.kind.as_str(),
                        moved.entity.identifier.as_str(),
                        moved.former_owner
                    )));
                }
            }
        }
        for captured_move in &captured {
            let input_checkpoint_count = match &captured_move.input {
                nervix_interconnect::OwnershipHandoffInput::Unit(checkpoints) => checkpoints.len(),
                nervix_interconnect::OwnershipHandoffInput::IngestorUnits { sources, .. } => {
                    sources
                        .iter()
                        .flat_map(|source| &source.checkpoints)
                        .count()
                }
            };
            preparations.record_attempt(captured_move.moved, input_checkpoint_count);
        }
        let mut preparing = FuturesUnordered::new();
        for CapturedPlannedMove { moved, input } in captured {
            let request = scope.preparation_request(moved, input);
            preparing.push(async move {
                let preparation = nervix_primitives::time::timeout_at(
                    scope.preparation_deadline,
                    self.prepare_planned_ownership_input(moved, scope.node_incarnations, request),
                )
                .await;
                PlannedMovePreparation { moved, preparation }
            });
        }
        let mut failure = None;
        while let Some(PlannedMovePreparation { moved, preparation }) = preparing.next().await {
            nervix_primitives::task::consume_budget().await;
            let error = match preparation {
                Ok(Ok(())) => continue,
                Ok(Err(reason)) => scope.failure(format!(
                    "failed to prepare {} '{}' from node '{}' on node '{}': {reason}",
                    moved.entity.kind.as_str(),
                    moved.entity.identifier.as_str(),
                    moved.former_owner,
                    moved.destination
                )),
                Err(_) => scope.failure(format!(
                    "timed out preparing {} '{}' from node '{}' on node '{}'",
                    moved.entity.kind.as_str(),
                    moved.entity.identifier.as_str(),
                    moved.former_owner,
                    moved.destination
                )),
            };
            if failure.is_none() {
                failure = Some(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Captures, once each and all at the same time, the frozen cut of every old unit a regrouped
    /// reader's moves read.
    ///
    /// Every move of such a reader projects every one of these cuts for its own new unit, so the
    /// cuts are captured before the moves rather than by whichever move reaches a unit first.
    async fn capture_frozen_ingestor_unit_sources(
        &self,
        scope: &PlannedHandoffRequestScope<'_>,
        moves: &[PlannedOwnershipMove],
    ) -> Result<FrozenIngestorUnitSources, Report<DomainAlterError>> {
        let mut sources = BTreeMap::<NodeUnitRef, PlannedOwnershipMove>::new();
        for moved in moves {
            let PlannedOwnershipInput::IngestorUnits(layout) = &moved.input else {
                continue;
            };
            for (unit, node) in &layout.sources {
                let key = NodeUnitRef::new(moved.entity.clone(), *unit);
                if let btree_map::Entry::Vacant(entry) = sources.entry(key) {
                    let mut source = moved.clone();
                    source.unit = *unit;
                    source.former_owner = node.clone();
                    entry.insert(source);
                }
            }
        }
        let mut captures = FuturesUnordered::new();
        for (key, source) in &sources {
            captures.push(async move {
                let capture = nervix_primitives::time::timeout_at(
                    scope.preparation_deadline,
                    self.capture_ownership_handoff_state(
                        scope,
                        source,
                        scope.incarnation(&source.former_owner),
                        nervix_interconnect::OwnershipHandoffSelection::Unit,
                    ),
                )
                .await;
                FrozenSourceCapture {
                    key,
                    source,
                    capture,
                }
            });
        }
        let mut frozen = FrozenIngestorUnitSources::new();
        while let Some(FrozenSourceCapture {
            key,
            source,
            capture,
        }) = captures.next().await
        {
            nervix_primitives::task::consume_budget().await;
            match capture {
                Ok(Ok(checkpoints)) => {
                    debug!(
                        domain = scope.domain.as_str(),
                        ingestor = source.entity.identifier.as_str(),
                        unit = ?source.unit,
                        node = %source.former_owner,
                        coordination = %scope.coordination,
                        checkpoint_count = checkpoints.len(),
                        "captured frozen checkpoints for regrouped ingestor source"
                    );
                    frozen.insert(key.clone(), checkpoints);
                }
                Ok(Err(reason)) => {
                    return Err(scope.failure(format!(
                        "failed to capture {} '{}' on source node '{}': {reason}",
                        source.entity.kind.as_str(),
                        source.entity.identifier.as_str(),
                        source.former_owner
                    )));
                }
                Err(_) => {
                    return Err(scope.failure(format!(
                        "timed out capturing {} '{}' on source node '{}'",
                        source.entity.kind.as_str(),
                        source.entity.identifier.as_str(),
                        source.former_owner
                    )));
                }
            }
        }
        Ok(frozen)
    }

    async fn capture_planned_ownership_input(
        &self,
        scope: &PlannedHandoffRequestScope<'_>,
        moved: &PlannedOwnershipMove,
        frozen_sources: &FrozenIngestorUnitSources,
    ) -> OwnershipHandoffResult<nervix_interconnect::OwnershipHandoffInput> {
        use nervix_interconnect::{
            IngestorUnitSelection, OwnershipHandoffInput, OwnershipHandoffSelection,
            OwnershipHandoffSource,
        };
        match &moved.input {
            PlannedOwnershipInput::Unit => Ok(OwnershipHandoffInput::Unit(
                self.capture_ownership_handoff_state(
                    scope,
                    moved,
                    scope.incarnation(&moved.former_owner),
                    OwnershipHandoffSelection::Unit,
                )
                .await?,
            )),
            PlannedOwnershipInput::IngestorUnits(layout) => {
                let selection = IngestorUnitSelection {
                    unit: moved.unit,
                    count: layout.count,
                };
                let mut sources = Vec::with_capacity(layout.sources.len());
                for (unit, node) in &layout.sources {
                    nervix_primitives::task::consume_budget().await;
                    let mut source = moved.clone();
                    source.unit = *unit;
                    source.former_owner = node.clone();
                    let captured = frozen_sources
                        .get(&NodeUnitRef::new(moved.entity.clone(), *unit))
                        .verified("every source unit's frozen cut is captured before the moves")
                        .clone();
                    let checkpoints = self
                        .capture_ownership_handoff_state(
                            scope,
                            &source,
                            scope.incarnation(node),
                            OwnershipHandoffSelection::IngestorUnits {
                                selection,
                                checkpoints: captured,
                            },
                        )
                        .await?;
                    sources.push(OwnershipHandoffSource {
                        node: node.clone(),
                        incarnation: scope.incarnation(node),
                        unit: *unit,
                        checkpoints,
                    });
                }
                Ok(OwnershipHandoffInput::IngestorUnits {
                    selection,
                    schema_fingerprint: layout.schema_fingerprint,
                    kafka_offsets: layout.kafka_offsets,
                    recipient: moved.destination.clone(),
                    recipient_incarnation: scope.incarnation(&moved.destination),
                    sources,
                })
            }
        }
    }

    async fn prepare_planned_ownership_input(
        &self,
        moved: &PlannedOwnershipMove,
        incarnations: &BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
        request: RemotePrepareOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        for node in moved.preparation_nodes() {
            let mut request = request.clone();
            if let nervix_interconnect::OwnershipHandoffInput::IngestorUnits {
                recipient,
                recipient_incarnation,
                ..
            } = &mut request.input
            {
                *recipient = node.clone();
                *recipient_incarnation = *incarnations
                    .get(node)
                    .verified("every preparation recipient has a bound incarnation");
            }
            self.prepare_ownership_handoff_state(request).await?;
        }
        Ok(())
    }

    async fn capture_ownership_handoff_state(
        &self,
        scope: &PlannedHandoffRequestScope<'_>,
        moved: &PlannedOwnershipMove,
        source_incarnation: ClusterNodeIncarnation,
        selection: nervix_interconnect::OwnershipHandoffSelection,
    ) -> OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>> {
        let coordination = scope.coordination;
        let operation_id = scope.operation_id;
        let domain = scope.domain;
        let base_schedule_fingerprint = scope.base_schedule_fingerprint;
        Self::verify_ownership_handoff_node_incarnation(
            &self.live_node_incarnations().await,
            &moved.former_owner,
            source_incarnation,
            "source",
        )?;
        if moved.former_owner == *self.inner.consensus.local_node_id() {
            let scheduled = self
                .prepare_owner_control_request(
                    domain,
                    moved.entity.kind,
                    moved.entity.identifier.clone(),
                )
                .await
                .map_err(|failure| OwnershipHandoffError::participant(failure.to_string()))?;
            if !scheduled
                .units
                .get(&moved.unit)
                .is_some_and(|unit| unit.is_primary_on(self.inner.consensus.local_node_id()))
            {
                return Err(OwnershipHandoffError::participant(format!(
                    "{} '{}' is not owned by source node '{}'",
                    moved.entity.kind.as_str(),
                    moved.entity.identifier.as_str(),
                    moved.former_owner
                )));
            }
            return self
                .inner
                .runtime
                .capture_selected_ownership_handoff_state(
                    coordination,
                    domain,
                    &moved.entity,
                    moved.unit,
                    base_schedule_fingerprint,
                    selection,
                )
                .await;
        }
        let response = self
            .inner
            .interconnect
            .request(
                &moved.former_owner,
                RemoteCaptureOwnershipHandoffStateRequest {
                    coordination: coordination.clone(),
                    operation_id: operation_id.to_string(),
                    source: moved.former_owner.clone(),
                    source_incarnation,
                    domain: domain.clone(),
                    unit: moved.unit,
                    entity: moved.entity.clone(),
                    base_schedule_fingerprint,
                    selection,
                },
            )
            .await
            .map_err(|error| OwnershipHandoffError::transport(error.to_string()))?;
        response.map_err(|failure| OwnershipHandoffError::participant(failure.to_string()))
    }
}
