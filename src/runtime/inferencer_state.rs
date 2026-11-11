//! Layer: data plane.
//! Owns: branch-local native inference publications, restore, sealing and checkpoint persistence.
//! Depends on: the shared branch-state owner, bounded execution, typed plans and the snapshot engine.
//! Must not know: Models, language parsing, scheduling decisions or external transports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns published inference tensor generations")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "consumes validated state plans and placements")
)]

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{CpuClass, Executor, MemoryClass, QueueAdmission, Reservation};
use nervix_primitives::sync::Arc;

use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStateOperationError,
    RuntimeStatePlacement, RuntimeStateResult, RuntimeStateStore,
    inferencer::{NativeGeneration, PreparedStatefulInference, StatefulInferenceSession},
    published_generation::{Generation, PublishedGenerations},
    replicated_branch_state::{BranchSnapshotCodec, ReplicatedBranchState},
    snapshot_staging::SnapshotStaging,
};
use crate::{
    inferencer_state_snapshot::{BranchSnapshot, Snapshot},
    registry::InferencerStatePlan,
};

#[derive(Debug)]
pub(super) struct InferencerSnapshotCodec;
pub(super) type ReplicatedInferencerState = ReplicatedBranchState<InferencerSnapshotCodec>;

#[derive(Debug, Clone)]
pub(super) struct InferencerGeneration {
    pub(in crate::runtime) incarnation: u64,
    pub(in crate::runtime) generation: Arc<NativeGeneration>,
    snapshot_bytes: u64,
}

impl ReplicatedInferencerState {
    pub(super) async fn new(
        placement: RuntimeStatePlacement,
        initial: Option<(PersistedRuntimeStateEntry, Reservation)>,
        plan: &InferencerStatePlan,
        executor: &Executor,
    ) -> Result<Self, Report<RuntimePersistenceError>> {
        let Some((initial, reservation)) = initial else {
            return Ok(Self::from_generations(
                placement,
                PublishedGenerations::restored(0, None),
            ));
        };
        let limit = executor.limits().snapshot_section_bytes.as_u64();
        let snapshot_bytes = plan
            .snapshot_bytes()
            .change_context(RuntimePersistenceError::InferencerSnapshot)?;
        crate::inferencer_state_snapshot::check_bound(
            initial.payload.len().arch_into(),
            snapshot_bytes,
        )
        .change_context(RuntimePersistenceError::InferencerSnapshot)?;
        let plan = plan.clone();
        let revision = initial.lsm;
        let value = executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                let snapshot =
                    Snapshot::decode(initial.payload, limit.min(snapshot_bytes), cancellation)
                        .change_context(RuntimePersistenceError::InferencerSnapshot)?;
                if snapshot.revision != revision {
                    return Err(Report::new(RuntimePersistenceError::InferencerSnapshot));
                }
                let Some(branch) = snapshot.branch else {
                    return Ok(None);
                };
                let generation = NativeGeneration::restore(branch.tensors, &plan, cancellation)
                    .change_context(RuntimePersistenceError::InferencerSnapshot)?;
                Ok::<_, Report<RuntimePersistenceError>>(Some(InferencerGeneration {
                    incarnation: branch.incarnation,
                    generation: Arc::new(generation),
                    snapshot_bytes,
                }))
            })
            .await
            .change_context(RuntimePersistenceError::StorageExecution)??;
        Ok(Self::from_generations(
            placement,
            PublishedGenerations::restored(revision, value),
        ))
    }

    pub(super) fn restore_session(
        &self,
        prepared: &PreparedStatefulInference,
        incarnation: u64,
        metrics: crate::metrics::InferencerStateMetrics,
    ) -> StatefulInferenceSession {
        let mut session =
            prepared.instantiate(metrics.track_branch(prepared.state.bytes_per_branch));
        let published = self.generations.load();
        let restored = published
            .value
            .as_ref()
            .is_some_and(|value| value.incarnation == incarnation);
        metrics.initialized(restored);
        if let Some(value) = &published.value {
            if value.incarnation == incarnation {
                session.restore_generation(value.generation.clone());
            } else {
                self.publish(None);
            }
        }
        session
    }

    pub(super) fn publish_session(
        &self,
        session: &StatefulInferenceSession,
        incarnation: u64,
        snapshot_bytes: u64,
    ) {
        self.publish(Some(InferencerGeneration {
            incarnation,
            generation: session.generation(),
            snapshot_bytes,
        }));
    }

    pub(super) async fn snapshot_after(
        &self,
        after_lsm: Option<u64>,
        executor: &Executor,
    ) -> Result<Option<PersistedRuntimeStateEntry>, Report<RuntimePersistenceError>> {
        let Some(published) = self.generations.load_after(after_lsm) else {
            return Ok(None);
        };
        let (snapshot, _charge) =
            Self::snapshot_of(published, executor, QueueAdmission::RefuseWhenFull).await?;
        Ok(Some(snapshot))
    }

    async fn snapshot_of(
        published: nervix_primitives::sync::StdArc<Generation<Option<InferencerGeneration>>>,
        executor: &Executor,
        admission: QueueAdmission,
    ) -> Result<(PersistedRuntimeStateEntry, Reservation), Report<RuntimePersistenceError>> {
        let bytes = match &published.value {
            Some(value) => value.snapshot_bytes,
            None => crate::inferencer_state_snapshot::footprint([])
                .change_context(RuntimePersistenceError::InferencerSnapshot)?,
        };
        let limit = executor.limits().snapshot_section_bytes.as_u64();
        let reservation = executor
            .reserve(
                MemoryClass::Bulk,
                RuntimeStateStore::bounded_snapshot_working_bytes(bytes)?,
            )
            .await
            .change_context(RuntimePersistenceError::StorageAdmission)?;
        executor
            .run_cpu_with(
                CpuClass::Bulk,
                admission,
                reservation,
                move |charge, cancellation| {
                    let branch = if let Some(value) = &published.value {
                        let tensors = value
                            .generation
                            .snapshot(cancellation)
                            .change_context(RuntimePersistenceError::InferencerSnapshot)?;
                        Some(BranchSnapshot {
                            incarnation: value.incarnation,
                            tensors,
                        })
                    } else {
                        None
                    };
                    let payload = Snapshot {
                        revision: published.revision,
                        branch,
                    }
                    .encode(limit, cancellation)
                    .change_context(RuntimePersistenceError::InferencerSnapshot)?;
                    Ok((
                        PersistedRuntimeStateEntry {
                            lsm: published.revision,
                            payload,
                        },
                        charge,
                    ))
                },
            )
            .await
            .change_context(RuntimePersistenceError::StorageExecution)?
    }
}

impl BranchSnapshotCodec for InferencerSnapshotCodec {
    type Published = Option<InferencerGeneration>;
    async fn persist_after(
        state: &ReplicatedInferencerState,
        after_lsm: u64,
        store: &RuntimeStateStore,
        executor: &Executor,
        _staging: &SnapshotStaging,
    ) -> RuntimeStateResult<Option<u64>> {
        let Some(published) = state.generations.load_after(Some(after_lsm)) else {
            return Ok(None);
        };
        let (snapshot, charge) = ReplicatedInferencerState::snapshot_of(
            published,
            executor,
            QueueAdmission::WaitForPlace,
        )
        .await
        .change_context(RuntimeStateOperationError::Persistence)?;
        let revision = snapshot.lsm;
        let written = store
            .persist_owned_snapshot(&state.placement, snapshot, charge)
            .await
            .change_context(RuntimeStateOperationError::Persistence)?;
        Ok(written.then_some(revision))
    }
}
