//! A materialized relay unit's part in moving its branch's keys between units while the branch's
//! partition count changes.
//!
//! Layer: data plane.
//!
//! - **Owns.** A holder's release and forget of one key's row; a claimant unit's catalog reads,
//!   parked batches and row claims; the tasks that read catalogs, claim and forget rows; when a
//!   claimed row's holder may forget it; the form a released row travels in; and the unit's
//!   published holdings.
//! - **Depends on.** The unit's replicated materialized state and branch registry, the Arrow body
//!   codec, the interconnect claim exchange and the unit's partition transition.
//! - **Must not know.** Plexus text, transaction planning, consensus, or when a transition retires.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a row moves between units once per claim, and a unit's holdings change only \
                  when a row it holds outside its partition is forgotten, expires or is evicted"
    )
)]

use error_stack::{Report, ResultExt as _};
use nervix_execution::MemoryClass;
use nervix_interconnect::{
    PartitionClaimOutcome, PartitionClaimStep, PartitionClaimSubject, ReleasedKey, ReleasedRow,
    RemoteOperationFailure, RemoteOperationSubject,
};
use nervix_models::{
    DomainName, DomainNodeRef, DomainNodeUnitRef, NodeRef, PartitionEpochNumber, Timestamp, Unit,
};
use nervix_primitives::{
    sync::{Arc, StdArc, mpsc, oneshot},
    time::Instant,
};
use nervix_recovery::NoReceiver as _;
use tracing::{debug, warn};

use super::{
    BranchInstanceRegistry, BranchKey, BranchScope, MaterializedRelayStateOriginator,
    MaterializedRelayStateRead, NodeQuiesceWorkGuard, RelayInteractionCommand, RelayRecordBatch,
    Runtime,
    partition_claims::{
        CatalogResolution, CatalogedKey, ClaimAdmission, ClaimRetries, ClaimStart,
        PartitionClaimError, PartitionClaimant, ReadCatalog, RetryStep, UnitPartitionTransition,
    },
    partition_holdings::HoldingsTracker,
};
use crate::runtime_schema::{RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow};

/// The keys a materialized relay unit holds rows for, with the time each was last written.
pub(super) type MaterializedBranches = BranchInstanceRegistry<Option<BranchKey>, ()>;

/// What a materialized relay unit's state task is asked while its branch moves keys.
pub(super) enum RelayStateCommand {
    /// A claimant asks this unit, which may hold a key's row outside its partition, to release or
    /// forget it.
    PartitionKey(RowHolderRequest),
    /// A claim task this unit started reports its progress.
    PartitionClaim(RowClaimProgress),
    /// The partition transition of the relay's branch retired: every row lies in its partition, so
    /// the unit stops claiming rows and applies the batches it parked.
    TransitionRetired,
}

impl RelayInteractionCommand for RelayStateCommand {}

/// A holder's step for one key's row, as a claimant asked for it.
pub(super) struct RowHolderRequest {
    /// The epoch the claimant claims in; a holder in another epoch is not ready for it.
    pub(super) epoch: PartitionEpochNumber,
    pub(super) key: Option<BranchKey>,
    pub(super) step: PartitionClaimStep,
    pub(super) response: oneshot::Sender<Result<PartitionClaimOutcome, RemoteOperationFailure>>,
}

/// What a row claim task reports back to its claimant unit's state task.
pub(super) enum RowClaimProgress {
    /// The catalog every peer unit answered, or why a peer did not answer.
    CatalogRead(error_stack::Result<ReadCatalog, PartitionClaimError>),
    /// The claim of `key` from `holder` found its row, found it held nowhere, or failed.
    Claimed {
        key: BranchKey,
        holder: Unit,
        result: error_stack::Result<Option<RuntimeRow>, PartitionClaimError>,
    },
    /// The holder of `key` forgot its row, or why it did not.
    Forgotten {
        key: BranchKey,
        holder: Unit,
        result: error_stack::Result<(), PartitionClaimError>,
    },
}

/// A batch that waits for its key's row to be claimed, with the quiesce work it holds, so a drain
/// waits for the claim too.
pub(super) struct ParkedRowBatch {
    pub(super) batch: RelayRecordBatch,
    pub(super) work: Option<NodeQuiesceWorkGuard>,
}

/// What a batch does next once its unit's moves saw it.
pub(super) enum RowAdmission {
    /// Apply the batch now.
    Apply(ParkedRowBatch),
    /// The batch waits for its key's row.
    Parked,
}

/// A row claimed from its holder, to install before anything else of its key.
pub(super) struct ClaimedRow {
    pub(super) key: BranchKey,
    pub(super) holder: Unit,
    pub(super) row: RuntimeRow,
}

/// What one claim task's progress settled: the row to install first, when one was claimed, and the
/// batches it released, in arrival order.
#[derive(Default)]
pub(super) struct SettledRows {
    pub(super) claimed: Option<ClaimedRow>,
    pub(super) batches: Vec<ParkedRowBatch>,
}

/// A claimed row whose holder keeps it until the revision of this unit's state that holds it is
/// persisted here, so neither side's failure loses the row.
struct UnpersistedClaim {
    key: BranchKey,
    holder: Unit,
    revision: u64,
}

/// The materialized relay unit whose keys move, as its state task knows it.
pub(super) struct MovingRelayUnit {
    pub(super) domain: DomainName,
    /// The relay, as the entity whose units the keys move between.
    pub(super) entity: NodeRef,
    /// The relay's schema, which every row claimed into the unit must have.
    pub(super) schema: StdArc<arrow_schema::Schema>,
    pub(super) transition: UnitPartitionTransition,
}

/// A materialized relay unit's moves while its branch is in a partition transition.
///
/// A unit of the current count claims every row its catalog names, without waiting for a batch of
/// the row's key: a materialized row is read far more often than it is written, so a row moved
/// only by its next write could hold its transition open until its TTL. At most as many claims as
/// the unit parks batches are in flight at once.
pub(super) struct MaterializedPartitionMoves {
    transition: UnitPartitionTransition,
    domain: DomainName,
    entity: NodeRef,
    /// The relay's schema, which every row claimed into this unit must have.
    schema: StdArc<arrow_schema::Schema>,
    claimant: PartitionClaimant<ParkedRowBatch>,
    /// Whether this unit is one of the current count's, and so claims the rows its catalog names.
    claims_catalog: bool,
    /// The cataloged rows waiting for room among the claims in flight.
    queued: std::collections::VecDeque<CatalogedKey>,
    /// How many claims may be in flight at once.
    claims_bound: usize,
    holdings: HoldingsTracker,
    retries: ClaimRetries,
    unpersisted: Vec<UnpersistedClaim>,
    /// When the claimed rows not yet persisted are checked again.
    persistence_check: Option<Instant>,
    /// The state task's own command channel, which claim tasks report their progress through.
    commands: mpsc::Sender<RelayStateCommand>,
}

impl MaterializedPartitionMoves {
    /// Begins the moves of `moving`, which holds rows for `held`, publishing the keys it holds
    /// outside their partition. `capacity` bounds the batches it parks.
    pub(super) fn begin<'a>(
        runtime: &Runtime,
        moving: MovingRelayUnit,
        held: impl IntoIterator<Item = &'a Option<BranchKey>>,
        commands: mpsc::Sender<RelayStateCommand>,
        capacity: usize,
    ) -> Self {
        let MovingRelayUnit {
            domain,
            entity,
            schema,
            transition,
        } = moving;
        let unit = DomainNodeUnitRef::new(
            DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone()),
            transition.unit,
        );
        let holdings = HoldingsTracker::begin(runtime, unit, &transition, held);
        let claims_catalog = transition.is_current_unit();
        let mut moves = Self {
            transition,
            domain,
            entity,
            schema,
            claimant: PartitionClaimant::new(capacity),
            claims_catalog,
            queued: std::collections::VecDeque::new(),
            claims_bound: capacity.max(1),
            holdings,
            retries: ClaimRetries::new(),
            unpersisted: Vec::new(),
            persistence_check: None,
            commands,
        };
        if moves.claims_catalog && moves.claimant.start_catalog_read() {
            moves.spawn_catalog_read(runtime);
        }
        moves
    }

    /// Starts the claims of queued cataloged rows while the claims in flight leave room.
    fn claim_queued(&mut self, runtime: &Runtime) {
        while self.claimant.claiming() < self.claims_bound {
            let Some(next) = self.queued.pop_front() else {
                return;
            };
            // A row a batch already claims, or whose claim settled, is not claimed again.
            if self.claimant.begin_claim(&next.key, next.holder) {
                self.spawn_claim(runtime, next.key, next.holder);
            }
        }
    }

    /// Whether the unit holds as many parked batches as its bound allows, so it takes no more.
    pub(super) fn is_full(&self) -> bool {
        self.claimant.is_full()
    }

    /// When the unit next has a failed step to try again or claimed rows to check for persistence.
    pub(super) fn next_wake(&self) -> Option<Instant> {
        match (self.retries.next_due(), self.persistence_check) {
            (Some(retry), Some(check)) => Some(retry.min(check)),
            (Some(retry), None) => Some(retry),
            (None, check) => check,
        }
    }

    fn publish(&self) {
        self.holdings.publish(self.claimant.claiming());
    }

    fn subject(&self, holder: Unit) -> PartitionClaimSubject {
        PartitionClaimSubject {
            domain: self.domain.clone(),
            entity: self.entity.clone(),
            epoch: self.transition.epoch,
            holder,
            claimant: self.transition.unit,
        }
    }

    /// Takes `batch` for a key this unit holds no row for, unless the key's row may lie in another
    /// unit: then the batch waits for the key's claim, which this starts when it is the first to
    /// wait. `held` says whether this unit holds the key's row.
    pub(super) fn admit(
        &mut self,
        runtime: &Runtime,
        held: bool,
        batch: ParkedRowBatch,
    ) -> RowAdmission {
        if held {
            return RowAdmission::Apply(batch);
        }
        let key = batch.batch.key.clone();
        let Some(concrete) = key.clone() else {
            return RowAdmission::Apply(batch);
        };
        if !self.claimant.is_parked(&concrete) && !self.transition.may_lie_elsewhere(&key) {
            return RowAdmission::Apply(batch);
        }
        match self.claimant.admit(concrete.clone(), batch) {
            ClaimAdmission::Begin(batch) => RowAdmission::Apply(batch),
            ClaimAdmission::Parked {
                start: ClaimStart::Nothing,
            } => RowAdmission::Parked,
            ClaimAdmission::Parked {
                start: ClaimStart::CatalogRead,
            } => {
                self.spawn_catalog_read(runtime);
                RowAdmission::Parked
            }
            ClaimAdmission::Parked {
                start: ClaimStart::Claim { holder },
            } => {
                self.spawn_claim(runtime, concrete, holder);
                self.publish();
                RowAdmission::Parked
            }
        }
    }

    fn spawn_catalog_read(&self, runtime: &Runtime) {
        let runtime = runtime.clone();
        let domain = self.domain.clone();
        let entity = self.entity.clone();
        let transition = self.transition.clone();
        let commands = self.commands.clone();
        nervix_primitives::task::spawn(async move {
            let result = runtime
                .read_partition_catalog(&domain, &entity, &transition)
                .await;
            commands
                .send(RelayStateCommand::PartitionClaim(
                    RowClaimProgress::CatalogRead(result),
                ))
                .await
                .means_shutdown("materialized relay unit awaiting its partition catalog");
        });
    }

    fn spawn_claim(&self, runtime: &Runtime, key: BranchKey, holder: Unit) {
        let runtime = runtime.clone();
        let subject = self.subject(holder);
        let schema = self.schema.clone();
        let commands = self.commands.clone();
        nervix_primitives::task::spawn(async move {
            let result = runtime.claim_materialized_row(&subject, schema, &key).await;
            commands
                .send(RelayStateCommand::PartitionClaim(
                    RowClaimProgress::Claimed {
                        key,
                        holder,
                        result,
                    },
                ))
                .await
                .means_shutdown("materialized relay unit awaiting a row claim");
        });
    }

    fn spawn_forget(&self, runtime: &Runtime, key: BranchKey, holder: Unit) {
        let runtime = runtime.clone();
        let subject = self.subject(holder);
        let commands = self.commands.clone();
        nervix_primitives::task::spawn(async move {
            let result = runtime
                .request_partition_step(&subject, &key, PartitionClaimStep::Forget)
                .await
                .map(|_| ());
            commands
                .send(RelayStateCommand::PartitionClaim(
                    RowClaimProgress::Forgotten {
                        key,
                        holder,
                        result,
                    },
                ))
                .await
                .means_shutdown("materialized relay unit awaiting a row forget");
        });
    }

    /// Tries every failed step whose time came again, and asks the holder of every claimed row
    /// this unit persisted since to forget it.
    pub(super) fn wake(&mut self, runtime: &Runtime, state: &MaterializedRelayStateRead) {
        let now = Instant::now();
        for step in self.retries.take_due(now) {
            match step {
                RetryStep::CatalogRead => {
                    if self.claimant.start_catalog_read() {
                        self.spawn_catalog_read(runtime);
                    }
                }
                RetryStep::Claim { key, holder } => self.spawn_claim(runtime, key, holder),
                RetryStep::Forget { key, holder } => self.spawn_forget(runtime, key, holder),
            }
        }
        if self.persistence_check.is_some_and(|check| check <= now) {
            self.forget_persisted(runtime, state);
        }
    }

    /// Asks the holder of every claimed row whose revision is persisted here to forget it. A node
    /// without a state store persists nothing, so its claims are forgotten at once.
    fn forget_persisted(&mut self, runtime: &Runtime, state: &MaterializedRelayStateRead) {
        let persists = runtime.inner.state_store.is_some();
        let persisted = state.persisted_lsm();
        let mut waiting = Vec::new();
        for claim in std::mem::take(&mut self.unpersisted) {
            if !persists || claim.revision <= persisted {
                self.spawn_forget(runtime, claim.key, claim.holder);
            } else {
                waiting.push(claim);
            }
        }
        self.unpersisted = waiting;
        if self.unpersisted.is_empty() {
            self.persistence_check = None;
            return;
        }
        // The materialized checkpoint writer persists on this cadence, so a check sooner finds
        // nothing new.
        let cadence = runtime
            .inner
            .state_replication_poll_interval
            .min(runtime.state_snapshot_interval());
        self.persistence_check = Some(Instant::now() + cadence);
    }

    /// Records that the row claimed for `key` from `holder` is installed here at `revision`; its
    /// holder forgets it once that revision is persisted.
    pub(super) fn installed(
        &mut self,
        runtime: &Runtime,
        state: &MaterializedRelayStateRead,
        claimed: ClaimedRow,
        revision: u64,
    ) {
        self.holdings.claimed_one();
        self.publish();
        self.unpersisted.push(UnpersistedClaim {
            key: claimed.key,
            holder: claimed.holder,
            revision,
        });
        self.forget_persisted(runtime, state);
    }

    /// Applies one claim task's progress, returning what it settled for the state task to apply.
    pub(super) fn progress(
        &mut self,
        runtime: &Runtime,
        progress: RowClaimProgress,
    ) -> SettledRows {
        let mut settled = SettledRows::default();
        match progress {
            RowClaimProgress::CatalogRead(Ok(catalog)) => {
                for resolution in self.claimant.catalog_read(catalog.holders()) {
                    match resolution {
                        CatalogResolution::Begin { inputs, .. } => settled.batches.extend(inputs),
                        CatalogResolution::Claim { key, holder } => {
                            self.spawn_claim(runtime, key, holder);
                        }
                    }
                }
                if self.claims_catalog {
                    self.queued.extend(catalog.into_keys());
                    self.claim_queued(runtime);
                }
                self.publish();
            }
            RowClaimProgress::CatalogRead(Err(error)) => {
                warn!(
                    domain = self.domain.as_str(),
                    relay = self.entity.identifier.as_str(),
                    error = %format_args!("{error:#}"),
                    "partition catalog read failed; parked batches wait for the next attempt"
                );
                self.claimant.catalog_failed();
                self.retries.later(RetryStep::CatalogRead);
            }
            RowClaimProgress::Claimed {
                key,
                holder,
                result: Ok(row),
            } => {
                settled.batches.extend(self.claimant.claim_settled(&key));
                self.claim_queued(runtime);
                self.publish();
                if let Some(row) = row {
                    settled.claimed = Some(ClaimedRow { key, holder, row });
                }
            }
            RowClaimProgress::Claimed {
                key,
                holder,
                result: Err(error),
            } => {
                warn!(
                    domain = self.domain.as_str(),
                    relay = self.entity.identifier.as_str(),
                    error = %format_args!("{error:#}"),
                    "row claim failed; the key's batches wait for the next attempt"
                );
                // A claim that already settled has nothing waiting for it.
                if self.claimant.claim_holder(&key).is_some() {
                    self.retries.later(RetryStep::Claim { key, holder });
                }
            }
            RowClaimProgress::Forgotten { result: Ok(()), .. } => {}
            RowClaimProgress::Forgotten {
                key,
                holder,
                result: Err(error),
            } => {
                debug!(
                    domain = self.domain.as_str(),
                    relay = self.entity.identifier.as_str(),
                    error = %format_args!("{error:#}"),
                    "row forget failed; it is tried again"
                );
                self.retries.later(RetryStep::Forget { key, holder });
            }
        }
        settled
    }

    /// Answers a claimant's step for one key's row at this holding unit.
    ///
    /// A release answers with the key's row, which this unit keeps, its expiry counting from now,
    /// until the claimant installed and persisted it and asks this unit to forget it. A forget
    /// drops the row. Both are idempotent.
    pub(super) async fn answer_holder(
        &mut self,
        runtime: &Runtime,
        state: &mut MaterializedRelayStateOriginator,
        branches: &mut MaterializedBranches,
        request: RowHolderRequest,
        now: Timestamp,
    ) {
        let outcome = self
            .holder_step(runtime, state, branches, &request, now)
            .await;
        request
            .response
            .send(outcome)
            .means_peer_left("partition claimant");
    }

    async fn holder_step(
        &mut self,
        runtime: &Runtime,
        state: &mut MaterializedRelayStateOriginator,
        branches: &mut MaterializedBranches,
        request: &RowHolderRequest,
        now: Timestamp,
    ) -> Result<PartitionClaimOutcome, RemoteOperationFailure> {
        let subject = || RemoteOperationSubject::Entity {
            domain: self.domain.clone(),
            entity: self.entity.clone(),
        };
        if request.epoch != self.transition.epoch {
            return Err(RemoteOperationFailure::not_ready(subject()));
        }
        if !self.transition.misplaces(&request.key) {
            // The current count places the key in this unit, so no claimant can own its row.
            return Err(RemoteOperationFailure::failed(
                subject(),
                "the key belongs to this unit under the current partition count",
            ));
        }
        match request.step {
            PartitionClaimStep::Release => {
                let Some(record) = state.read().record(&request.key) else {
                    return Ok(PartitionClaimOutcome::NotHeld);
                };
                let released = runtime.released_row(&record.row).await.map_err(|error| {
                    RemoteOperationFailure::failed(subject(), format!("{error:#}"))
                })?;
                // A released row expires by its branch's TTL counted from its release.
                branches.touch(&request.key, now);
                Ok(PartitionClaimOutcome::Released(ReleasedKey::Row(released)))
            }
            PartitionClaimStep::Forget => {
                runtime
                    .delete_materialized_stream_key(state, &request.key)
                    .map_err(|error| {
                        RemoteOperationFailure::failed(subject(), format!("{error:#}"))
                    })?;
                branches.remove(&request.key);
                self.retain_held(branches);
                Ok(PartitionClaimOutcome::Forgotten)
            }
        }
    }

    /// Drops from the published holdings every key this unit no longer holds a row for because it
    /// forgot, expired or evicted it.
    pub(super) fn retain_held(&mut self, branches: &MaterializedBranches) {
        if self
            .holdings
            .retain(|key| branches.contains_key(&Some(key.clone())))
        {
            self.publish();
        }
    }

    /// Every batch the unit parked, for a state task that ends its claims before they settle: one
    /// that stops refuses them, and one whose transition retired applies them.
    pub(super) fn drain_parked(&mut self) -> Vec<ParkedRowBatch> {
        let mut batches = Vec::new();
        for (_, parked) in self.claimant.drain_parked() {
            batches.extend(parked);
        }
        batches
    }
}

/// Why a released row could not be produced or taken.
#[derive(Debug, thiserror::Error)]
pub(super) enum ReleasedRowError {
    #[error("the released row could not be encoded")]
    Encode,
    #[error("the released row could not be admitted for decoding")]
    Admission,
    #[error("the released row is not one row of the relay's schema")]
    Decode,
}

impl Runtime {
    /// The form `row`, a materialized relay's row its holder releases, travels in: one Arrow
    /// section of the relay's schema, carried as a snapshot section is, with the row's metadata.
    async fn released_row(
        &self,
        row: &RuntimeRow,
    ) -> error_stack::Result<ReleasedRow, ReleasedRowError> {
        let section = row
            .one_row_batch()
            .encode_arrow_snapshot_section(&self.inner.executor)
            .await
            .change_context(ReleasedRowError::Encode)?;
        Ok(ReleasedRow {
            batch: section.as_ref().to_vec(),
            metadata: row.metadata().to_remote(),
        })
    }

    /// Takes `released`, a row its holder released, as one row of `schema`.
    async fn take_released_row(
        &self,
        schema: StdArc<arrow_schema::Schema>,
        released: ReleasedRow,
    ) -> error_stack::Result<RuntimeRow, ReleasedRowError> {
        let executor = &self.inner.executor;
        let section = executor
            .charge_owned(MemoryClass::Bulk, released.batch)
            .await
            .change_context(ReleasedRowError::Admission)?;
        let batch = RuntimeRecordBatch::decode_arrow_snapshot_section(executor, schema, section)
            .await
            .change_context(ReleasedRowError::Decode)?;
        if batch.batch().num_rows() != 1 {
            return Err(Report::new(ReleasedRowError::Decode));
        }
        RuntimeRow::new(
            Arc::new(batch),
            0,
            RuntimeRecordMetadata::from_remote(released.metadata),
        )
        .change_context(ReleasedRowError::Decode)
    }

    /// Claims the row of `key` for the claimant `subject` names: asks its holder to release it and
    /// takes the row it answers as a row of the relay's schema, or finds it held nowhere.
    async fn claim_materialized_row(
        &self,
        subject: &PartitionClaimSubject,
        schema: StdArc<arrow_schema::Schema>,
        key: &BranchKey,
    ) -> error_stack::Result<Option<RuntimeRow>, PartitionClaimError> {
        let outcome = self
            .request_partition_step(subject, key, PartitionClaimStep::Release)
            .await?;
        let released = match outcome {
            PartitionClaimOutcome::NotHeld => return Ok(None),
            PartitionClaimOutcome::Released(ReleasedKey::Row(released)) => released,
            PartitionClaimOutcome::Released(ReleasedKey::Branch(_))
            | PartitionClaimOutcome::Forgotten => {
                return Err(Report::new(PartitionClaimError::UnexpectedAnswer {
                    entity: subject.entity.clone(),
                    unit: subject.holder,
                }));
            }
        };
        let branch = Some(key.clone());
        let row = self
            .take_released_row(schema, released)
            .await
            .change_context_lazy(|| PartitionClaimError::ReleasedRow {
                branch: BranchScope::from(&branch),
            })?;
        Ok(Some(row))
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_interconnect::RuntimeState;
    use nervix_models::{BranchName, BranchPartitions, ModelKind, ModelName, SchemaFingerprint};

    use super::{super::RuntimeStatePlacement, *};
    use crate::{
        runtime::{RelayPartitioning, test_fixtures::string_branch_key},
        runtime_schema::{RuntimeValue, test_runtime_row},
    };

    fn partitions(count: u16) -> BranchPartitions {
        BranchPartitions::Direct(
            std::num::NonZeroU16::new(count).assured("the test counts are positive"),
        )
    }

    /// Unit 1 of a branch growing from four partitions to eight, which keeps `beta` outside its
    /// partition: eight partitions place it in partition 5.
    fn transition() -> UnitPartitionTransition {
        UnitPartitionTransition {
            branch: BranchName::parse("by_tenant").assured("the test branch name is valid"),
            epoch: PartitionEpochNumber::FIRST.next(),
            current: RelayPartitioning::of_partitions(partitions(8)),
            previous: RelayPartitioning::of_partitions(partitions(4)),
            unit: Unit::partition(1),
        }
    }

    fn row(status: &str, watermark: i64) -> RuntimeRow {
        test_runtime_row([(
            "status".to_string(),
            RuntimeValue::String(status.to_string()),
        )])
        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(watermark))
    }

    fn entity() -> NodeRef {
        NodeRef::new(
            ModelKind::Relay,
            ModelName::parse("profiles").assured("the test relay name is valid"),
        )
    }

    struct Holder {
        runtime: Runtime,
        state: MaterializedRelayStateOriginator,
        branches: MaterializedBranches,
        moves: MaterializedPartitionMoves,
        released: RuntimeRow,
    }

    impl Holder {
        async fn new() -> Self {
            let runtime = Runtime::default();
            let released = row("ready", 7);
            let domain = DomainName::parse("payments").assured("the test domain is valid");
            let placement = RuntimeStatePlacement {
                domain: domain.clone(),
                state: RuntimeState::MaterializedRelay {
                    schema: SchemaFingerprint::from_digest([9; 32]),
                },
                kind: ModelKind::Relay,
                identifier: entity().identifier,
                unit: Unit::partition(1),
                branch_key: None,
            };
            let mut assignment = runtime.replicated_materialized_stream_state(
                placement,
                released.arrow_schema(),
                None,
                Vec::new(),
                None,
            );
            let mut state = assignment
                .originator
                .take()
                .assured("an unplaced test relay holds its authoritative assignment");
            let beta = string_branch_key("tenant", "beta");
            runtime
                .apply_materialized_stream_records(&mut state, &beta, [released.clone()])
                .await
                .assured("the test relay's assignment stays authoritative");
            let branches = state
                .read()
                .restored_branch_instances(Timestamp::from_unix_nanos(1));
            let (commands, _command_rx) = mpsc::channel(1);
            let held = branches.snapshot_entries();
            let moves = MaterializedPartitionMoves::begin(
                &runtime,
                MovingRelayUnit {
                    domain,
                    entity: entity(),
                    schema: state.read().schema().clone(),
                    transition: transition(),
                },
                held.iter().map(|entry| &entry.key),
                commands,
                4,
            );
            Self {
                runtime,
                state,
                branches,
                moves,
                released,
            }
        }

        async fn step(
            &mut self,
            epoch: PartitionEpochNumber,
            key: Option<BranchKey>,
            step: PartitionClaimStep,
        ) -> Result<PartitionClaimOutcome, RemoteOperationFailure> {
            let (response, answer) = oneshot::channel();
            self.moves
                .answer_holder(
                    &self.runtime,
                    &mut self.state,
                    &mut self.branches,
                    RowHolderRequest {
                        epoch,
                        key,
                        step,
                        response,
                    },
                    Timestamp::from_unix_nanos(2),
                )
                .await;
            answer.await.assured("the holder answers every step")
        }

        fn pending(&self) -> u64 {
            let holdings = self.runtime.partition_transition_holdings(
                &DomainName::parse("payments").assured("the test domain is valid"),
                &transition().branch,
                transition().epoch,
            );
            holdings.iter().map(|unit| unit.pending).sum()
        }
    }

    #[nervix_primitives::test]
    async fn a_released_row_stays_with_its_holder_until_forgotten_and_travels_whole() {
        let mut holder = Holder::new().await;
        let beta = string_branch_key("tenant", "beta");
        assert_eq!(
            holder.pending(),
            1,
            "the holder publishes the key it holds outside its unit"
        );
        let epoch = transition().epoch;
        let released = holder
            .step(epoch, beta.clone(), PartitionClaimStep::Release)
            .await
            .assured("the holder releases the row it holds");
        let PartitionClaimOutcome::Released(ReleasedKey::Row(released)) = released else {
            panic!("the holder released something other than the key's row");
        };
        let again = holder
            .step(epoch, beta.clone(), PartitionClaimStep::Release)
            .await
            .assured("a release asked again is answered again");
        assert_eq!(
            again,
            PartitionClaimOutcome::Released(ReleasedKey::Row(released.clone())),
            "a release is idempotent until the claimant forgets the row"
        );
        assert_eq!(
            holder.pending(),
            1,
            "a released row stays held until it is forgotten"
        );
        let taken = holder
            .runtime
            .take_released_row(holder.state.read().schema().clone(), released)
            .await
            .assured("the claimant takes the released row as a row of the relay's schema");
        assert_eq!(
            taken.one_row_batch().batch(),
            holder.released.one_row_batch().batch(),
            "the released row's columns travel whole"
        );
        assert_eq!(
            taken.metadata().to_remote(),
            holder.released.metadata().to_remote(),
            "the released row keeps its ingestion watermarks"
        );

        let forgotten = holder
            .step(epoch, beta.clone(), PartitionClaimStep::Forget)
            .await
            .assured("the holder forgets the row its claimant installed");
        assert_eq!(forgotten, PartitionClaimOutcome::Forgotten);
        assert!(holder.state.read().record(&beta).is_none());
        assert!(!holder.branches.contains_key(&beta));
        assert_eq!(
            holder.pending(),
            0,
            "a forgotten key leaves the holder's holdings"
        );
        let after = holder
            .step(epoch, beta, PartitionClaimStep::Release)
            .await
            .assured("a forgotten key is answered");
        assert_eq!(after, PartitionClaimOutcome::NotHeld);
    }

    #[nervix_primitives::test]
    async fn a_holder_refuses_a_step_of_another_epoch_or_for_a_key_of_its_own_unit() {
        let mut holder = Holder::new().await;
        let beta = string_branch_key("tenant", "beta");
        let next = transition().epoch.next();
        assert!(
            holder
                .step(next, beta.clone(), PartitionClaimStep::Forget)
                .await
                .is_err(),
            "a holder in another epoch is not ready for the step"
        );
        // Eight partitions place `gamma` in partition 1, this holder's own unit.
        let gamma = string_branch_key("tenant", "gamma");
        assert!(
            holder
                .step(transition().epoch, gamma, PartitionClaimStep::Forget)
                .await
                .is_err(),
            "no claimant may forget a key of the holder's own unit"
        );
        assert!(holder.state.read().record(&beta).is_some());
    }
}
