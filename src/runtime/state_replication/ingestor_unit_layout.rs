//! Frozen intake-state projection and preparation for a new instance layout.
//!
//! Layer: data plane.
//! - **Owns.** Disjoint position and metric projections, branch-lifecycle joins and prepared cuts.
//! - **Depends on.** Typed execution identities, native checkpoint owners and bounded bulk workers.
//! - **Must not know.** Models, connector drivers, placement policy or schedule publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns frozen intake-state transformations")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "an entity gate freezes the source units until their prepared cut is installed"
    )
)]

use nervix_execution::{Cancellation, CpuClass, MemoryClass, Reservation};
use nervix_interconnect::{
    IngestorUnitSelection, OwnershipHandoffCheckpoint, OwnershipHandoffInput, StateCheckpointRead,
};
use nervix_models::{NodeUnitRef, SchemaFingerprint, Unit};

use super::*;
use crate::{
    metrics::RuntimeMetricsSnapshot,
    runtime::{
        branch_aggregated_state::{
            BranchAggregatedRuntimeStateSnapshot, encode_branch_aggregated_snapshot,
        },
        kafka_offset_state::{
            IngestorOffsetCheckpoint, merge_offset_snapshots, project_offset_snapshot,
        },
    },
};

#[derive(Debug, thiserror::Error)]
enum IngestorUnitLayoutError {
    #[error("the target is not an instance within the declared layout")]
    Selection,
    #[error("the source checkpoint does not belong to the declared source unit")]
    Scope,
    #[error("the source unit inventory does not match the frozen execution revision")]
    SourceInventory,
    #[error("the source unit checkpoint inventory is incomplete or duplicated")]
    CheckpointInventory,
    #[error("a non-ingestor state component cannot be redistributed as intake state")]
    Component,
    #[error("the prepared state revision or allocation bound exceeds its integer range")]
    Bounds,
}

fn refuse(issue: IngestorUnitLayoutError) -> Report<OwnershipHandoffError> {
    Report::new(issue).change_context(OwnershipHandoffError::IngestorUnitLayout)
}

fn instance_index(selection: IngestorUnitSelection) -> OwnershipHandoffResult<u64> {
    match selection.unit.kind() {
        nervix_models::UnitKind::Instance(index) if index < selection.count.get() => Ok(index),
        _ => Err(refuse(IngestorUnitLayoutError::Selection)),
    }
}

fn project_checkpoint(
    placement: &RuntimeStatePlacement,
    mut snapshot: PersistedRuntimeStateEntry,
    selection: IngestorUnitSelection,
    cancellation: &Cancellation,
) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
    let index = instance_index(selection)?;
    let nervix_models::UnitKind::Instance(source_index) = placement.unit.kind() else {
        return Err(refuse(IngestorUnitLayoutError::Selection));
    };
    snapshot.payload = match placement.state.kind() {
        RuntimeStateKind::KafkaOffset => {
            project_offset_snapshot(&snapshot.payload, selection, cancellation)
                .change_context(OwnershipHandoffError::IngestorUnitLayout)?
        }
        RuntimeStateKind::BranchAggregated => {
            let snapshot = decode_branch_aggregated_snapshot(&snapshot.payload)
                .change_context(OwnershipHandoffError::IngestorUnitLayout)?;
            encode_branch_aggregated_snapshot(&BranchAggregatedRuntimeStateSnapshot {
                metrics: snapshot.metrics.select_instances(index, selection.count),
            })
            .change_context(OwnershipHandoffError::IngestorUnitLayout)?
        }
        RuntimeStateKind::BranchLru if source_index % selection.count.get() == index => {
            snapshot.payload
        }
        RuntimeStateKind::BranchLru => encode_branch_lru_snapshot(&[])
            .change_context(OwnershipHandoffError::IngestorUnitLayout)?,
        _ => return Err(refuse(IngestorUnitLayoutError::Component)),
    };
    Ok(snapshot)
}

impl Runtime {
    pub(super) async fn project_captured_ingestor_unit_state(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        unit: Unit,
        selection: IngestorUnitSelection,
        checkpoints: Vec<OwnershipHandoffCheckpoint>,
    ) -> OwnershipHandoffResult<Vec<OwnershipHandoffCheckpoint>> {
        instance_index(selection)?;
        if entity.kind != ModelKind::Ingestor
            || !matches!(unit.kind(), nervix_models::UnitKind::Instance(_))
        {
            return Err(refuse(IngestorUnitLayoutError::Scope));
        }
        let scheduled = self.ownership_handoff_scheduled_node(domain, entity)?;
        let source_unit = scheduled
            .units
            .get(&unit)
            .ok_or_else(|| refuse(IngestorUnitLayoutError::SourceInventory))?;
        let source = source_unit
            .primary_node
            .as_ref()
            .ok_or_else(|| refuse(IngestorUnitLayoutError::SourceInventory))?;
        let (snapshots, _charges) = self
            .read_handoff_unit(domain, entity, unit, source, checkpoints, None)
            .await?;
        let mut projected = Vec::with_capacity(snapshots.len());
        for (placement, snapshot) in snapshots {
            let snapshot = self
                .project_ingestor_unit_checkpoint(&placement, snapshot, selection)
                .await?;
            let snapshot = self
                .describe_state_checkpoint(&placement, snapshot)
                .await
                .change_context(OwnershipHandoffError::IngestorUnitLayout)?;
            projected.push(OwnershipHandoffCheckpoint {
                placement: placement.to_remote(),
                snapshot,
            });
        }
        Ok(projected)
    }

    pub(in crate::runtime) async fn project_ingestor_unit_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
        selection: IngestorUnitSelection,
    ) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
        let length = u64::try_from(snapshot.payload.len())
            .map_err(|_| refuse(IngestorUnitLayoutError::Bounds))?;
        let reservation = self
            .inner
            .executor
            .reserve(
                MemoryClass::Bulk,
                length
                    .checked_mul(3)
                    .ok_or_else(|| refuse(IngestorUnitLayoutError::Bounds))?
                    .max(1),
            )
            .await
            .change_context(OwnershipHandoffError::IngestorUnitLayout)?;
        let placement = placement.clone();
        self.inner
            .executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                project_checkpoint(&placement, snapshot, selection, cancellation)
            })
            .await
            .change_context(OwnershipHandoffError::IngestorUnitLayout)?
    }

    async fn read_handoff_unit(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        unit: Unit,
        source: &ClusterNodeName,
        checkpoints: Vec<OwnershipHandoffCheckpoint>,
        selection: Option<IngestorUnitSelection>,
    ) -> OwnershipHandoffResult<(
        Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
        Vec<Reservation>,
    )> {
        let mut decoded = Vec::with_capacity(checkpoints.len());
        let mut charges = Vec::new();
        let mut placements = HashSet::default();
        for checkpoint in checkpoints {
            let placement = RuntimeStatePlacement::from_remote(checkpoint.placement)
                .change_context(OwnershipHandoffError::IngestorUnitLayout)?;
            if placement.domain != *domain
                || placement.kind != entity.kind
                || placement.identifier != entity.identifier
                || placement.unit != unit
                || !self.runtime_state_placement_is_current(&placement)
            {
                return Err(refuse(IngestorUnitLayoutError::Scope));
            }
            if !placements.insert(placement.clone()) {
                return Err(refuse(IngestorUnitLayoutError::CheckpointInventory));
            }
            let read = match selection {
                Some(selection) => StateCheckpointRead::IngestorUnits(selection),
                None => StateCheckpointRead::HandoffCapture,
            };
            let (snapshot, charge) = self
                .fetch_state_checkpoint_charged(source, &placement, &checkpoint.snapshot, read)
                .await
                .change_context(OwnershipHandoffError::IngestorUnitLayout)?;
            if selection.is_none() {
                self.validate_ownership_handoff_snapshot(&placement, &snapshot)?;
            }
            charges.push(charge);
            decoded.push((placement, snapshot));
        }
        let scheduled = self.ownership_handoff_scheduled_node(domain, entity)?;
        let expected =
            self.expected_ownership_handoff_placements(domain, &scheduled, unit, &decoded)?;
        if expected != placements {
            return Err(refuse(IngestorUnitLayoutError::CheckpointInventory));
        }
        Ok((decoded, charges))
    }

    pub(super) async fn decode_ownership_handoff_input(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        unit: Unit,
        source: &ClusterNodeName,
        input: OwnershipHandoffInput,
    ) -> OwnershipHandoffResult<Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>> {
        match input {
            OwnershipHandoffInput::Unit(checkpoints) => {
                let (decoded, _charges) = self
                    .read_handoff_unit(domain, entity, unit, source, checkpoints, None)
                    .await?;
                Ok(decoded)
            }
            OwnershipHandoffInput::IngestorUnits {
                selection,
                schema_fingerprint,
                kafka_offsets,
                sources,
                ..
            } => {
                instance_index(selection)?;
                let scheduled = self.ownership_handoff_scheduled_node(domain, entity)?;
                let source_units = sources
                    .iter()
                    .map(|source| source.unit)
                    .collect::<BTreeSet<_>>();
                if entity.kind != ModelKind::Ingestor
                    || selection.unit != unit
                    || sources.len() != source_units.len()
                    || source_units != scheduled.units.keys().copied().collect()
                {
                    return Err(refuse(IngestorUnitLayoutError::SourceInventory));
                }
                let source_count = std::num::NonZeroU64::new(
                    u64::try_from(source_units.len())
                        .map_err(|_| refuse(IngestorUnitLayoutError::Bounds))?,
                )
                .ok_or_else(|| refuse(IngestorUnitLayoutError::SourceInventory))?;
                let mut decoded = Vec::new();
                let mut charges = Vec::new();
                for source in sources {
                    let (snapshots, admitted) = self
                        .read_handoff_unit(
                            domain,
                            entity,
                            source.unit,
                            &source.node,
                            source.checkpoints,
                            Some(selection),
                        )
                        .await?;
                    decoded.extend(snapshots);
                    charges.extend(admitted);
                }
                let length = decoded
                    .iter()
                    .try_fold(0_u64, |total, (_, snapshot)| {
                        total.checked_add(u64::try_from(snapshot.payload.len()).ok()?)
                    })
                    .ok_or_else(|| refuse(IngestorUnitLayoutError::Bounds))?;
                let reservation = self
                    .inner
                    .executor
                    .reserve(
                        MemoryClass::Bulk,
                        length
                            .checked_mul(3)
                            .ok_or_else(|| refuse(IngestorUnitLayoutError::Bounds))?
                            .max(1),
                    )
                    .await
                    .change_context(OwnershipHandoffError::IngestorUnitLayout)?;
                let domain = domain.clone();
                let target = NodeUnitRef::new(entity.clone(), unit);
                self.inner
                    .executor
                    .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                        let _inputs = charges;
                        merge_checkpoints(
                            domain,
                            target,
                            schema_fingerprint,
                            kafka_offsets,
                            source_count,
                            decoded,
                            cancellation,
                        )
                    })
                    .await
                    .change_context(OwnershipHandoffError::IngestorUnitLayout)?
            }
        }
    }
}

fn merge_checkpoints(
    domain: DomainName,
    target: NodeUnitRef,
    schema: SchemaFingerprint,
    kafka_offsets: bool,
    source_count: std::num::NonZeroU64,
    inputs: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
    cancellation: &Cancellation,
) -> OwnershipHandoffResult<Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>> {
    let lsm = inputs
        .iter()
        .map(|(_, snapshot)| snapshot.lsm)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| refuse(IngestorUnitLayoutError::Bounds))?;
    let mut offsets = Vec::new();
    let mut metrics = Vec::new();
    let mut branches = HashMap::default();
    for (placement, snapshot) in inputs {
        cancellation
            .check()
            .change_context(OwnershipHandoffError::IngestorUnitLayout)?;
        match placement.state {
            RuntimeState::KafkaOffset => offsets.push(IngestorOffsetCheckpoint {
                unit: placement.unit,
                payload: snapshot.payload,
            }),
            RuntimeState::BranchAggregated => metrics.push(
                decode_branch_aggregated_snapshot(&snapshot.payload)
                    .change_context(OwnershipHandoffError::IngestorUnitLayout)?
                    .metrics,
            ),
            RuntimeState::BranchLru {
                schema: source_schema,
            } if source_schema == schema => {
                for entry in decode_branch_lru_snapshot(&snapshot.payload)
                    .change_context(OwnershipHandoffError::IngestorUnitLayout)?
                {
                    branches
                        .entry(entry.key.clone())
                        .and_modify(
                            |current: &mut BranchInstanceSnapshotEntry<Option<BranchKey>>| {
                                current.last_ingestion =
                                    current.last_ingestion.max(entry.last_ingestion);
                                current.incarnation = current.incarnation.max(entry.incarnation);
                            },
                        )
                        .or_insert(entry);
                }
            }
            RuntimeState::BranchLru { .. } => {}
            _ => return Err(refuse(IngestorUnitLayoutError::Component)),
        }
    }
    let metrics = RuntimeMetricsSnapshot::merge_disjoint(metrics)
        .change_context(OwnershipHandoffError::IngestorUnitLayout)?;
    let mut branches = branches.into_values().collect::<Vec<_>>();
    branches.sort_by(|left, right| {
        left.last_ingestion
            .cmp(&right.last_ingestion)
            .then_with(|| {
                left.key
                    .as_ref()
                    .map(BranchKey::as_str)
                    .cmp(&right.key.as_ref().map(BranchKey::as_str))
            })
    });
    let mut states = vec![
        (
            RuntimeState::BranchAggregated,
            encode_branch_aggregated_snapshot(&BranchAggregatedRuntimeStateSnapshot { metrics })
                .change_context(OwnershipHandoffError::IngestorUnitLayout)?,
        ),
        (
            RuntimeState::BranchLru { schema },
            encode_branch_lru_snapshot(&branches)
                .change_context(OwnershipHandoffError::IngestorUnitLayout)?,
        ),
    ];
    if kafka_offsets {
        states.push((
            RuntimeState::KafkaOffset,
            merge_offset_snapshots(&offsets, source_count, cancellation)
                .change_context(OwnershipHandoffError::IngestorUnitLayout)?,
        ));
    }
    Ok(states
        .into_iter()
        .map(|(state, payload)| {
            (
                RuntimeStatePlacement {
                    domain: domain.clone(),
                    state,
                    kind: target.node.kind,
                    identifier: target.node.identifier.clone(),
                    unit: target.unit,
                    branch_key: None,
                },
                PersistedRuntimeStateEntry { lsm, payload },
            )
        })
        .collect())
}
