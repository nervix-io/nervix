//! The keys each unit of a branch-keyed entity holds outside their partition while its branch's
//! partition count changes, as the unit's owner publishes them for claimants and observers.
//!
//! Layer: data plane.
//!
//! - **Owns.** Each local unit's published holdings in its branch's current epoch (every key it
//!   holds that the current count places in another unit, with that unit; the keys it claimed; the
//!   claims it has in flight), the registry claimants and observers read them through, and the
//!   catalog pages and holdings answers built from them.
//! - **Depends on.** Branch-key fingerprints, units, epochs and the interconnect answer shapes.
//! - **Must not know.** How a unit parks input, moves state, or when a transition retires.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a unit publishes its holdings when it starts and whenever a key it holds \
                  outside its partition is released, forgotten, expires or is claimed; no record \
                  path reaches this registry"
    )
)]

use imbl::{GenericOrdMap, shared_ptr::DefaultSharedPtr};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::{
    PartitionCatalogKey, PartitionCatalogShard, PartitionClaimOutcome, PartitionClaimStep,
    PartitionClaimSubject, PartitionTransitionAnswer, PartitionTransitionRequest,
    RemoteOperationFailure, RemoteOperationSubject, UnitPartitionHoldings,
};
use nervix_models::{
    BranchKeyFingerprint, BranchName, DomainName, DomainNodeRef, DomainNodeUnitRef, ModelKind,
    NodeUnitRef, PartitionEpochNumber, RelayName, Unit,
};
use nervix_primitives::{
    collections::DashMap,
    publication::ArcSwap,
    sync::{Arc, StdArc},
};

use super::{
    BranchKey, ProcessorNodeCommand, RelayUnitRef, Runtime,
    materialized_partition_moves::{RelayStateCommand, RowHolderRequest},
    partition_claims::{KeyPartitionTarget, UnitPartitionTransition},
    processor_partition_moves::HolderKeyRequest,
};

#[cfg(all(test, feature = "shuttle"))]
#[path = "partition_holdings_shuttle_tests.rs"]
mod shuttle_tests;

/// The keys one unit holds outside their partition, in fingerprint order. Persistent, so a
/// publication shares it rather than copying it.
pub(super) type MisplacedKeys = GenericOrdMap<BranchKeyFingerprint, MisplacedKey, DefaultSharedPtr>;

/// One key a unit holds outside its partition.
#[derive(Debug, Clone)]
pub(super) struct MisplacedKey {
    pub(super) key: BranchKey,
    /// The unit the current count places the key in.
    pub(super) target: KeyPartitionTarget,
}

/// What one unit of a branch-keyed entity holds in its branch's current epoch.
#[derive(Debug, Clone)]
pub(super) struct UnitHoldings {
    pub(super) branch: BranchName,
    pub(super) epoch: PartitionEpochNumber,
    /// Every key the unit holds, live or released and not yet forgotten, that the current count
    /// places in another unit.
    pub(super) misplaced: MisplacedKeys,
    /// The keys the unit claimed from another unit in this epoch.
    pub(super) claimed: u64,
    /// The keys whose claim the unit has in flight: each still has its state in another unit.
    pub(super) claiming: u64,
}

impl UnitHoldings {
    /// The keys this unit's part of the transition still has to move: those it holds outside
    /// their partition and those it is claiming.
    fn pending(&self) -> u64 {
        let held = u64::try_from(self.misplaced.len())
            .assured("a unit holds fewer keys than its address space has bytes");
        held.checked_add(self.claiming)
            .assured("a unit holds and claims fewer than 2^64 keys at once")
    }
}

/// Every local unit's published holdings, by unit. Each unit's owner registers once, republishes
/// through the handle it keeps, and withdraws only its own registration, so a successor owner of
/// the same unit keeps the one it made.
#[derive(Default)]
pub(in crate::runtime) struct PartitionHoldingsRegistry {
    units: DashMap<DomainNodeUnitRef, Arc<ArcSwap<UnitHoldings>>, ahash::RandomState>,
}

impl PartitionHoldingsRegistry {
    /// Publishes `holdings` as `unit`'s, in place of whatever an earlier owner of the unit
    /// published, and returns the handle the owner republishes through.
    fn register(
        &self,
        unit: DomainNodeUnitRef,
        holdings: UnitHoldings,
    ) -> Arc<ArcSwap<UnitHoldings>> {
        let published = Arc::new(ArcSwap::from_pointee(holdings));
        self.units.insert(unit, published.clone());
        published
    }

    /// Withdraws `published` from `unit`, unless a successor owner of the unit replaced it.
    fn withdraw(&self, unit: &DomainNodeUnitRef, published: &Arc<ArcSwap<UnitHoldings>>) {
        self.units
            .remove_if(unit, |_, registered| Arc::ptr_eq(registered, published));
    }

    /// What `unit` publishes, when an owner of it does on this node.
    fn get(&self, unit: &DomainNodeUnitRef) -> Option<StdArc<UnitHoldings>> {
        let published = self.units.get(unit)?;
        Some(published.value().load_full())
    }

    /// The holdings every unit of `branch`'s entities in `domain` publishes in `epoch`.
    fn holdings_in(
        &self,
        domain: &DomainName,
        branch: &BranchName,
        epoch: PartitionEpochNumber,
    ) -> Vec<UnitPartitionHoldings> {
        let mut answered = Vec::new();
        for entry in self.units.iter() {
            let unit = entry.key();
            if &unit.entity.domain != domain {
                continue;
            }
            let holdings = entry.value().load();
            if &holdings.branch != branch || holdings.epoch != epoch {
                continue;
            }
            answered.push(UnitPartitionHoldings {
                entity: unit.entity.node.clone(),
                unit: unit.unit,
                pending: holdings.pending(),
                claimed: holdings.claimed,
            });
        }
        answered
    }
}

/// The holdings one unit's owner publishes, withdrawn when the owner ends. What it publishes is
/// also counted in the node's keys-pending gauge of the unit's branch, and withdrawn from it with
/// the publication.
pub(super) struct UnitHoldingsPublication {
    runtime: Runtime,
    unit: DomainNodeUnitRef,
    published: Arc<ArcSwap<UnitHoldings>>,
}

impl UnitHoldingsPublication {
    /// Publishes `holdings` as the holdings of `unit` on this node, replacing whatever an owner
    /// of the same unit published before.
    pub(super) fn register(
        runtime: &Runtime,
        unit: DomainNodeUnitRef,
        holdings: UnitHoldings,
    ) -> Self {
        runtime.observe_partition_keys_pending(
            &unit.entity.domain,
            &holdings.branch,
            0,
            holdings.pending(),
        );
        let published = runtime
            .inner
            .partition_holdings
            .register(unit.clone(), holdings);
        Self {
            runtime: runtime.clone(),
            unit,
            published,
        }
    }

    /// Replaces the published holdings with `holdings`.
    pub(super) fn publish(&self, holdings: UnitHoldings) {
        let before = self.published.load().pending();
        self.runtime.observe_partition_keys_pending(
            &self.unit.entity.domain,
            &holdings.branch,
            before,
            holdings.pending(),
        );
        self.published.store(StdArc::new(holdings));
    }

    /// Counts one key claimed into the unit in the node's claimed counter of its branch.
    fn observe_claimed(&self, branch: &BranchName) {
        let dispatcher = self.runtime.inner.remote_dispatcher.load();
        self.runtime
            .inner
            .metrics
            .observe_branch_partition_key_claimed(
                &self.unit.entity.domain,
                branch,
                dispatcher
                    .as_deref()
                    .map(super::RemoteDispatcher::local_node_id),
            );
    }
}

impl Drop for UnitHoldingsPublication {
    fn drop(&mut self) {
        // A successor owner of the same unit may have registered already; only this owner's own
        // publication is withdrawn.
        let published = &self.published;
        self.runtime
            .inner
            .partition_holdings
            .withdraw(&self.unit, published);
        let withdrawn = published.load();
        self.runtime.observe_partition_keys_pending(
            &self.unit.entity.domain,
            &withdrawn.branch,
            withdrawn.pending(),
            0,
        );
    }
}

impl Runtime {
    /// Moves this node's keys-pending gauge of `branch` from a unit's `before` to its `after`.
    fn observe_partition_keys_pending(
        &self,
        domain: &DomainName,
        branch: &BranchName,
        before: u64,
        after: u64,
    ) {
        if before == after {
            return;
        }
        let before = i64::try_from(before).assured("a unit holds fewer than 2^63 keys");
        let after = i64::try_from(after).assured("a unit holds fewer than 2^63 keys");
        let delta = after
            .checked_sub(before)
            .assured("two counts below 2^63 differ by less than 2^63");
        let dispatcher = self.inner.remote_dispatcher.load();
        self.inner.metrics.add_branch_partition_keys_pending(
            domain,
            branch,
            dispatcher
                .as_deref()
                .map(super::RemoteDispatcher::local_node_id),
            delta,
        );
    }

    /// Records the partition epoch every branch of `revision` executes in on this node.
    pub(super) fn observe_partition_epochs(
        &self,
        domain: &DomainName,
        revision: &super::ExecutionRevision,
    ) {
        let dispatcher = self.inner.remote_dispatcher.load();
        let node = dispatcher
            .as_deref()
            .map(super::RemoteDispatcher::local_node_id);
        let mut branches = std::collections::BTreeSet::new();
        for relay in revision.activation.relays.values() {
            if let Some(branch) = relay.branching.branch() {
                branches.insert(branch);
            }
        }
        for branch in branches {
            let epoch = match revision.partition_epochs.get(branch) {
                Some(epoch) => epoch.number,
                None => PartitionEpochNumber::FIRST,
            };
            self.inner
                .metrics
                .set_branch_partition_epoch(domain, branch, node, epoch);
        }
    }
}

/// The holdings one unit keeps and publishes while its branch moves keys between units: the keys
/// it holds outside their partition and the keys it claimed in the epoch.
pub(super) struct HoldingsTracker {
    branch: BranchName,
    epoch: PartitionEpochNumber,
    misplaced: MisplacedKeys,
    claimed: u64,
    publication: UnitHoldingsPublication,
}

impl HoldingsTracker {
    /// Begins tracking `unit`, which holds `held` under `transition`, and publishes what it holds
    /// outside its partition.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the owner supplies an iterator over the keys of its own branch registry"
        )
    )]
    pub(super) fn begin<'a>(
        runtime: &Runtime,
        unit: DomainNodeUnitRef,
        transition: &UnitPartitionTransition,
        held: impl IntoIterator<Item = &'a Option<BranchKey>>,
    ) -> Self {
        let mut misplaced = MisplacedKeys::new();
        for key in held {
            if !transition.misplaces(key) {
                continue;
            }
            if let (Some(concrete), Some(target)) = (key, transition.target_of(key)) {
                misplaced.insert(
                    concrete.fingerprint(),
                    MisplacedKey {
                        key: concrete.clone(),
                        target,
                    },
                );
            }
        }
        let publication = UnitHoldingsPublication::register(
            runtime,
            unit,
            UnitHoldings {
                branch: transition.branch.clone(),
                epoch: transition.epoch,
                misplaced: misplaced.clone(),
                claimed: 0,
                claiming: 0,
            },
        );
        Self {
            branch: transition.branch.clone(),
            epoch: transition.epoch,
            misplaced,
            claimed: 0,
            publication,
        }
    }

    pub(super) fn needs_confirmation(&self, key: &Option<BranchKey>) -> bool {
        match key {
            Some(key) => self.misplaced.contains_key(&key.fingerprint()),
            None => false,
        }
    }

    pub(super) fn confirmed_here(&mut self, key: &Option<BranchKey>) {
        if let Some(key) = key {
            self.misplaced.remove(&key.fingerprint());
        }
    }

    /// Counts one key claimed into this unit.
    pub(super) fn claimed_one(&mut self) {
        self.claimed = self
            .claimed
            .checked_add(1)
            .assured("a unit claims fewer than 2^64 keys in one epoch");
        self.publication.observe_claimed(&self.branch);
    }

    /// Drops every key `holds` says this unit no longer holds, and reports whether any was.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the owner's predicate reads only its own branch registry, \
                                   which it holds exclusively")
    )]
    pub(super) fn retain(&mut self, holds: impl Fn(&BranchKey) -> bool) -> bool {
        let mut gone = Vec::new();
        for (fingerprint, misplaced) in self.misplaced.iter() {
            if !holds(&misplaced.key) {
                gone.push(*fingerprint);
            }
        }
        for fingerprint in &gone {
            self.misplaced.remove(fingerprint);
        }
        !gone.is_empty()
    }

    /// Publishes the unit's holdings with `claiming` claims in flight.
    pub(super) fn publish(&self, claiming: usize) {
        let claiming =
            u64::try_from(claiming).assured("a unit claims fewer keys at once than it can address");
        self.publication.publish(UnitHoldings {
            branch: self.branch.clone(),
            epoch: self.epoch,
            misplaced: self.misplaced.clone(),
            claimed: self.claimed,
            claiming,
        });
    }
}

impl Runtime {
    /// The holdings every unit of `branch`'s entities this node owns publishes in `epoch`.
    pub(crate) fn partition_transition_holdings(
        &self,
        domain: &DomainName,
        branch: &BranchName,
        epoch: PartitionEpochNumber,
    ) -> Vec<UnitPartitionHoldings> {
        self.inner
            .partition_holdings
            .holdings_in(domain, branch, epoch)
    }

    /// One page of the keys `holder` holds in `epoch` that the current count places in
    /// `claimant`, after `after`, with at most `limit` keys. `None` when this node publishes no
    /// holdings of `holder` in that epoch.
    pub(crate) fn partition_catalog_page(
        &self,
        holder: &DomainNodeUnitRef,
        epoch: PartitionEpochNumber,
        claimant: Unit,
        after: Option<BranchKeyFingerprint>,
        limit: usize,
    ) -> Option<PartitionCatalogShard> {
        let published = self.inner.partition_holdings.get(holder)?;
        if published.epoch != epoch {
            return None;
        }
        let mut keys = Vec::new();
        let mut next = None;
        let lower = match after {
            Some(after) => std::ops::Bound::Excluded(after),
            None => std::ops::Bound::Unbounded,
        };
        let range = published.misplaced.range::<_, BranchKeyFingerprint>((
            lower,
            std::ops::Bound::<BranchKeyFingerprint>::Unbounded,
        ));
        for (fingerprint, misplaced) in range {
            if misplaced.target != KeyPartitionTarget::Known(claimant) {
                continue;
            }
            if keys.len() == limit {
                next = keys.last().map(|key: &PartitionCatalogKey| key.fingerprint);
                break;
            }
            // A held key outside its partition is always concrete, so it always has fields.
            let fields = BranchKey::to_remote_key(&Some(misplaced.key.clone()));
            keys.push(PartitionCatalogKey {
                fingerprint: *fingerprint,
                key: fields,
            });
        }
        Some(PartitionCatalogShard { keys, next })
    }
}

impl Runtime {
    /// Answers one partition-transition request a claimant or an observer sent this node.
    pub(crate) async fn answer_partition_transition(
        &self,
        request: PartitionTransitionRequest,
    ) -> Result<PartitionTransitionAnswer, RemoteOperationFailure> {
        match request {
            PartitionTransitionRequest::Locate {
                subject,
                fingerprint,
            } => {
                let unit = DomainNodeUnitRef::new(
                    subject.entity.clone().in_domain(&subject.domain),
                    subject.holder,
                );
                let Some(holdings) = self.inner.partition_holdings.get(&unit) else {
                    return Err(RemoteOperationFailure::not_ready(
                        RemoteOperationSubject::Entity {
                            domain: subject.domain,
                            entity: subject.entity,
                        },
                    ));
                };
                if holdings.epoch != subject.epoch {
                    return Err(RemoteOperationFailure::not_ready(
                        RemoteOperationSubject::Entity {
                            domain: subject.domain,
                            entity: subject.entity,
                        },
                    ));
                }
                Ok(PartitionTransitionAnswer::Located(
                    holdings.misplaced.get(&fingerprint).map(|held| {
                        nervix_interconnect::PartitionKeyLocation {
                            origin: held.key.inherited_origin(),
                        }
                    }),
                ))
            }
            PartitionTransitionRequest::Holdings {
                domain,
                branch,
                epoch,
            } => Ok(PartitionTransitionAnswer::Holdings(
                self.partition_transition_holdings(&domain, &branch, epoch),
            )),
            PartitionTransitionRequest::CatalogShard { subject, after } => {
                let holder = DomainNodeUnitRef::new(
                    DomainNodeRef::node_in(
                        subject.domain.clone(),
                        subject.entity.kind,
                        subject.entity.identifier.clone(),
                    ),
                    subject.holder,
                );
                // A catalog page carries no more keys than a page of branch checkpoints a peer
                // lists in one control answer.
                let page = super::state_replication::BRANCH_CHECKPOINT_LISTING_PAGE.get();
                match self.partition_catalog_page(
                    &holder,
                    subject.epoch,
                    subject.claimant,
                    after,
                    page,
                ) {
                    Some(shard) => Ok(PartitionTransitionAnswer::CatalogShard(shard)),
                    None => Err(RemoteOperationFailure::not_ready(
                        RemoteOperationSubject::Entity {
                            domain: subject.domain,
                            entity: subject.entity,
                        },
                    )),
                }
            }
            PartitionTransitionRequest::Claim { subject, key, step } => {
                let outcome = self.answer_partition_claim(subject, key, step).await?;
                Ok(PartitionTransitionAnswer::Claim(outcome))
            }
        }
    }

    /// Hands one claim step to the owner of the holding unit it names: the processor unit's
    /// supervisor, or the materialized relay unit's state task.
    async fn answer_partition_claim(
        &self,
        subject: PartitionClaimSubject,
        key: nervix_models::RemoteBranchKey,
        step: PartitionClaimStep,
    ) -> Result<PartitionClaimOutcome, RemoteOperationFailure> {
        let entity_subject = || RemoteOperationSubject::Entity {
            domain: subject.domain.clone(),
            entity: subject.entity.clone(),
        };
        let key = BranchKey::from_remote_key(key).map_err(|error| {
            RemoteOperationFailure::failed(entity_subject(), format!("{error:#}"))
        })?;
        if subject.entity.kind == ModelKind::Relay {
            return self
                .answer_materialized_partition_claim(&subject, key, step)
                .await;
        }
        let commands = {
            let Some(execution) = self.inner.executions.get(&subject.domain) else {
                return Err(RemoteOperationFailure::unavailable(entity_subject()));
            };
            let unit = NodeUnitRef::new(subject.entity.clone(), subject.holder);
            let Some(task) = execution.node_tasks.get(&unit) else {
                return Err(RemoteOperationFailure::not_ready(entity_subject()));
            };
            task.commands.clone()
        };
        let (response, receiver) = nervix_primitives::sync::oneshot::channel();
        let request = HolderKeyRequest {
            epoch: subject.epoch,
            key,
            step,
            response,
        };
        if commands
            .send(ProcessorNodeCommand::PartitionKey(request))
            .await
            .is_err()
        {
            return Err(RemoteOperationFailure::not_ready(entity_subject()));
        }
        match receiver.await {
            Ok(outcome) => outcome,
            Err(_) => Err(RemoteOperationFailure::not_ready(entity_subject())),
        }
    }
}

impl Runtime {
    /// Hands one claim step to the state task of the materialized relay unit it names.
    async fn answer_materialized_partition_claim(
        &self,
        subject: &PartitionClaimSubject,
        key: Option<BranchKey>,
        step: PartitionClaimStep,
    ) -> Result<PartitionClaimOutcome, RemoteOperationFailure> {
        let entity_subject = || RemoteOperationSubject::Entity {
            domain: subject.domain.clone(),
            entity: subject.entity.clone(),
        };
        let commands = {
            let Some(execution) = self.inner.executions.get(&subject.domain) else {
                return Err(RemoteOperationFailure::unavailable(entity_subject()));
            };
            let unit =
                RelayUnitRef::new(RelayName::from(&subject.entity.identifier), subject.holder);
            let Some(task) = execution.relay_state_tasks.get(&unit) else {
                return Err(RemoteOperationFailure::not_ready(entity_subject()));
            };
            task.commands.clone()
        };
        let (response, receiver) = nervix_primitives::sync::oneshot::channel();
        let request = RowHolderRequest {
            epoch: subject.epoch,
            key,
            step,
            response,
        };
        if commands
            .send(RelayStateCommand::PartitionKey(request))
            .await
            .is_err()
        {
            return Err(RemoteOperationFailure::not_ready(entity_subject()));
        }
        match receiver.await {
            Ok(outcome) => outcome,
            Err(_) => Err(RemoteOperationFailure::not_ready(entity_subject())),
        }
    }
}
