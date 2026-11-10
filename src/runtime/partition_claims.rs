//! The claims through which a unit of a branch-keyed entity takes over the keys a previous
//! partition count placed in another unit of the same entity.
//!
//! Layer: data plane.
//!
//! - **Owns.** A claimant unit's catalog of the keys other units hold for it, the input it parks
//!   for a key while that key is unresolved or claimed, the bound on that parked input, which
//!   claims are in flight and when a failed step is tried again; the requests a claimant sends to
//!   the owner of a holding unit (a claim step, a catalog page) and how each fails.
//! - **Depends on.** Branch keys, execution-unit identity, the partition epoch an execution plan
//!   carries, the installed revision's unit owners and the interconnect claim exchange.
//! - **Must not know.** Plexus text, consensus, transaction planning, how a key's state is stored
//!   or transferred, or when a transition retires.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a claimant reads its catalog once per transition and asks a holder for each \
                  claim step of a key once, again only after a failed attempt"
    )
)]

use std::collections::{BTreeMap, VecDeque};

use error_stack::{Report, ResultExt as _};
use indexmap::IndexMap;
use meticulous::OptionExt as _;
use nervix_interconnect::{
    PartitionClaimOutcome, PartitionClaimStep, PartitionClaimSubject, PartitionTransitionAnswer,
    PartitionTransitionRequest, RemoteOperationFailure,
};
use nervix_models::{
    BranchKeyFingerprint, BranchName, ClusterNodeName, DomainName, NodeRef, PartitionEpochNumber,
    Unit,
};
use nervix_primitives::time::Instant;

use super::{BranchKey, BranchScope, RelayPartitioning, Runtime, RuntimeReconnectBackoff};
use crate::registry::ExecutionNode;

#[cfg(all(test, feature = "shuttle"))]
#[path = "partition_claims_shuttle_tests.rs"]
mod shuttle_tests;

/// The partition transition one unit of a branch-keyed processor executes in, as its plan carries
/// it: present only while keys placed under the branch's previous count may lie outside their
/// partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UnitPartitionTransition {
    pub(super) branch: BranchName,
    pub(super) epoch: PartitionEpochNumber,
    /// How the current count places keys.
    pub(super) current: RelayPartitioning,
    /// How the previous count placed them.
    pub(super) previous: RelayPartitioning,
    /// The unit this execution is.
    pub(super) unit: Unit,
}

impl UnitPartitionTransition {
    /// The transition `unit` of `node` executes in, while its branch is in one.
    pub(super) fn of(node: &ExecutionNode, unit: Unit) -> Option<Self> {
        let transition = node.partition_transition.as_ref()?;
        Some(Self {
            branch: transition.branch.clone(),
            epoch: transition.epoch,
            current: RelayPartitioning::of(node.resolved_branching.as_ref()),
            previous: RelayPartitioning::of_partitions(transition.previous),
            unit,
        })
    }

    /// Whether this unit holds `key` outside its partition under the current count.
    pub(super) fn misplaces(&self, key: &Option<BranchKey>) -> bool {
        match self.current.unit_of(key) {
            Ok(unit) => unit != self.unit,
            // A key no unit owns is no unit's to claim.
            Err(_) => false,
        }
    }

    /// The unit the current count places `key` in.
    pub(super) fn target_of(&self, key: &Option<BranchKey>) -> Option<Unit> {
        self.current.unit_of(key).ok()
    }

    /// The units of the previous count other than this one: every unit that may hold a key this
    /// unit owns now, and so every unit this unit reads its catalog from.
    pub(super) fn catalog_peers(&self) -> Vec<Unit> {
        let mut peers = Vec::new();
        for unit in self.previous.units() {
            if unit != self.unit {
                peers.push(unit);
            }
        }
        peers
    }

    /// Whether the current count schedules this unit, rather than keeping it only because the
    /// previous count placed keys in it.
    pub(super) fn is_current_unit(&self) -> bool {
        self.current.units().contains(&self.unit)
    }

    /// Whether the previous count placed `key` in another unit, so it may still lie there.
    pub(super) fn may_lie_elsewhere(&self, key: &Option<BranchKey>) -> bool {
        match self.previous.unit_of(key) {
            Ok(unit) => unit != self.unit,
            Err(_) => false,
        }
    }
}

/// What a claimant knows about where the keys it owns now lie, as far as its catalog was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ClaimCatalog {
    /// No catalog read has begun.
    Unread,
    /// The catalog is being read from every peer unit.
    Reading,
    /// Every peer unit answered: the key fingerprints each holds that land in this unit.
    Read(BTreeMap<BranchKeyFingerprint, Unit>),
}

/// How a claimant proceeds with one input for a key it does not hold.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ClaimAdmission<I> {
    /// No unit holds the key: it begins here, with this input.
    Begin(I),
    /// The input waits for the key to be resolved; the claimant starts the work `start` names.
    Parked { start: ClaimStart },
}

/// The asynchronous work a parked input requires the claimant to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ClaimStart {
    /// Nothing new: the catalog read or the key's claim is already in flight.
    Nothing,
    /// Read the catalog from every peer unit.
    CatalogRead,
    /// Claim the key from the unit that holds it.
    Claim { holder: Unit },
}

/// A key the catalog read resolved, with the input it parked.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CatalogResolution<I> {
    /// The key begins here; its parked input runs in arrival order.
    Begin { key: BranchKey, inputs: VecDeque<I> },
    /// The key is claimed from `holder`; its input stays parked.
    Claim { key: BranchKey, holder: Unit },
}

/// One key another unit holds that the current count places in a claimant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CatalogedKey {
    pub(super) key: BranchKey,
    pub(super) holder: Unit,
}

/// What one claimant's catalog read found: every key another unit holds for it, by fingerprint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ReadCatalog {
    keys: BTreeMap<BranchKeyFingerprint, CatalogedKey>,
}

impl ReadCatalog {
    fn insert(&mut self, fingerprint: BranchKeyFingerprint, key: CatalogedKey) {
        self.keys.insert(fingerprint, key);
    }

    /// The unit holding each key, by fingerprint.
    pub(super) fn holders(&self) -> BTreeMap<BranchKeyFingerprint, Unit> {
        let mut holders = BTreeMap::new();
        for (fingerprint, cataloged) in &self.keys {
            holders.insert(*fingerprint, cataloged.holder);
        }
        holders
    }

    /// Every key the catalog names, in fingerprint order.
    pub(super) fn into_keys(self) -> Vec<CatalogedKey> {
        self.keys.into_values().collect()
    }
}

/// The input one claimant unit parks for the keys it is resolving or claiming, bounded by a count
/// of parked inputs: once the bound is reached the unit stops taking input, so its relays hold the
/// rest back, until a resolution releases what it parked.
pub(super) struct PartitionClaimant<I> {
    catalog: ClaimCatalog,
    parked: IndexMap<BranchKey, VecDeque<I>, ahash::RandomState>,
    parked_inputs: usize,
    capacity: usize,
    claiming: IndexMap<BranchKey, Unit, ahash::RandomState>,
}

impl<I> PartitionClaimant<I> {
    /// A claimant that parks at most `capacity` inputs, which must be at least one.
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            catalog: ClaimCatalog::Unread,
            parked: IndexMap::default(),
            parked_inputs: 0,
            capacity: capacity.max(1),
            claiming: IndexMap::default(),
        }
    }

    /// Whether the claimant holds as much parked input as its bound allows.
    pub(super) fn is_full(&self) -> bool {
        self.parked_inputs >= self.capacity
    }

    /// How many claims this claimant has in flight.
    pub(super) fn claiming(&self) -> usize {
        self.claiming.len()
    }

    /// Whether the claimant parks input for `key`, so a further input for it must wait behind it.
    pub(super) fn is_parked(&self, key: &BranchKey) -> bool {
        self.parked.contains_key(key)
    }

    /// Decides what one input for `key`, a key this unit does not hold, does next.
    pub(super) fn admit(&mut self, key: BranchKey, input: I) -> ClaimAdmission<I> {
        // A key with parked input, or with a claim in flight, settles all of its input at once.
        if self.parked.contains_key(&key) || self.claiming.contains_key(&key) {
            self.park(key, input);
            return ClaimAdmission::Parked {
                start: ClaimStart::Nothing,
            };
        }
        let start = match &self.catalog {
            ClaimCatalog::Unread => {
                self.catalog = ClaimCatalog::Reading;
                ClaimStart::CatalogRead
            }
            ClaimCatalog::Reading => ClaimStart::Nothing,
            ClaimCatalog::Read(holders) => match holders.get(&key.fingerprint()) {
                None => return ClaimAdmission::Begin(input),
                Some(holder) => {
                    let holder = *holder;
                    self.claiming.insert(key.clone(), holder);
                    ClaimStart::Claim { holder }
                }
            },
        };
        self.park(key, input);
        ClaimAdmission::Parked { start }
    }

    fn park(&mut self, key: BranchKey, input: I) {
        self.parked.entry(key).or_default().push_back(input);
        self.parked_inputs = self
            .parked_inputs
            .checked_add(1)
            .assured("every parked input occupies memory, so fewer than usize::MAX are parked");
    }

    /// Records the catalog every peer unit answered, and resolves every key parked while it was
    /// read, in the order their first input arrived.
    pub(super) fn catalog_read(
        &mut self,
        holders: BTreeMap<BranchKeyFingerprint, Unit>,
    ) -> Vec<CatalogResolution<I>> {
        let mut resolutions = Vec::new();
        let mut waiting = Vec::new();
        for key in self.parked.keys() {
            if !self.claiming.contains_key(key) {
                waiting.push(key.clone());
            }
        }
        for key in waiting {
            match holders.get(&key.fingerprint()) {
                Some(holder) => {
                    self.claiming.insert(key.clone(), *holder);
                    resolutions.push(CatalogResolution::Claim {
                        key,
                        holder: *holder,
                    });
                }
                None => {
                    let inputs = self.release(&key);
                    resolutions.push(CatalogResolution::Begin { key, inputs });
                }
            }
        }
        self.catalog = ClaimCatalog::Read(holders);
        resolutions
    }

    /// The catalog read failed: the next input for an unresolved key reads it again.
    pub(super) fn catalog_failed(&mut self) -> bool {
        if let ClaimCatalog::Reading = self.catalog {
            self.catalog = ClaimCatalog::Unread;
            return true;
        }
        false
    }

    /// Begins reading the catalog before any input asks for it, for a claimant that claims every
    /// key its catalog names, and again after a failed read. Returns whether a read is to start.
    pub(super) fn start_catalog_read(&mut self) -> bool {
        if let ClaimCatalog::Unread = self.catalog {
            self.catalog = ClaimCatalog::Reading;
            return true;
        }
        false
    }

    /// Begins the claim of `key` from `holder`, a key the read catalog still names, unless a claim
    /// of it is in flight already. Returns whether the claim began.
    pub(super) fn begin_claim(&mut self, key: &BranchKey, holder: Unit) -> bool {
        let listed = match &self.catalog {
            ClaimCatalog::Read(holders) => holders.contains_key(&key.fingerprint()),
            ClaimCatalog::Unread | ClaimCatalog::Reading => false,
        };
        if !listed || self.claiming.contains_key(key) {
            return false;
        }
        self.claiming.insert(key.clone(), holder);
        true
    }

    /// Begins reading the catalog again after a failed read, when input still waits for it.
    /// Returns whether a read is to start.
    pub(super) fn restart_catalog_read(&mut self) -> bool {
        if let ClaimCatalog::Unread = self.catalog
            && !self.parked.is_empty()
        {
            self.catalog = ClaimCatalog::Reading;
            return true;
        }
        false
    }

    /// The claim of `key` settled, with or without state from its holder: the key begins or
    /// resumes here, and the input it parked runs in arrival order.
    pub(super) fn claim_settled(&mut self, key: &BranchKey) -> VecDeque<I> {
        self.claiming.shift_remove(key);
        if let ClaimCatalog::Read(holders) = &mut self.catalog {
            holders.remove(&key.fingerprint());
        }
        self.release(key)
    }

    /// The holder that a key in flight is claimed from, while its claim runs.
    pub(super) fn claim_holder(&self, key: &BranchKey) -> Option<Unit> {
        self.claiming.get(key).copied()
    }

    /// Every input this claimant parked, for a unit that ends its claims before they settle, as a
    /// teardown or a retired transition does, in the order each key's first input arrived.
    pub(super) fn drain_parked(&mut self) -> Vec<(BranchKey, VecDeque<I>)> {
        self.claiming.clear();
        self.parked_inputs = 0;
        self.parked.drain(..).collect()
    }

    fn release(&mut self, key: &BranchKey) -> VecDeque<I> {
        let inputs = self.parked.shift_remove(key).unwrap_or_default();
        self.parked_inputs = self
            .parked_inputs
            .checked_sub(inputs.len())
            .verified("the parked count includes every input a key parked");
        inputs
    }
}

/// A claim step that failed and is tried again once its time comes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RetryStep {
    /// Read the catalog again for the input still parked.
    CatalogRead,
    /// Claim `key` from `holder` again.
    Claim { key: BranchKey, holder: Unit },
    /// Ask `holder` again to forget `key`, which this unit installed.
    Forget { key: BranchKey, holder: Unit },
}

/// The failed claim steps of one claimant unit, each tried again after the runtime's reconnect
/// backoff, which grows with each failure and so bounds how often a unit asks an owner that does
/// not answer.
pub(super) struct ClaimRetries {
    pending: Vec<PendingRetry>,
    backoff: RuntimeReconnectBackoff,
}

/// One failed step and when it is tried again.
struct PendingRetry {
    at: Instant,
    step: RetryStep,
}

impl ClaimRetries {
    pub(super) fn new() -> Self {
        Self {
            pending: Vec::new(),
            backoff: RuntimeReconnectBackoff::default(),
        }
    }

    /// Tries `step` again after the next backoff delay.
    pub(super) fn later(&mut self, step: RetryStep) {
        let at = Instant::now() + self.backoff.take_next_delay();
        self.pending.push(PendingRetry { at, step });
    }

    /// When the next step is due, while one waits.
    pub(super) fn next_due(&self) -> Option<Instant> {
        let mut next = None::<Instant>;
        for retry in &self.pending {
            next = match next {
                Some(at) if at <= retry.at => Some(at),
                _ => Some(retry.at),
            };
        }
        next
    }

    /// Takes every step whose time came by `now`, in the order they failed.
    pub(super) fn take_due(&mut self, now: Instant) -> Vec<RetryStep> {
        let mut due = Vec::new();
        let mut waiting = Vec::new();
        for retry in std::mem::take(&mut self.pending) {
            if retry.at <= now {
                due.push(retry.step);
            } else {
                waiting.push(retry);
            }
        }
        self.pending = waiting;
        due
    }
}

/// Every way moving one key between units fails. Each leaves the key's state in its holder, so the
/// claimant tries again.
#[derive(Debug, thiserror::Error)]
pub(super) enum PartitionClaimError {
    #[error("unit {unit:?} of {entity:?} has no owner in the installed schedule")]
    UnitUnowned { entity: NodeRef, unit: Unit },
    #[error("the owner of unit {unit:?} of {entity:?} did not answer")]
    Request { entity: NodeRef, unit: Unit },
    #[error("the owner of unit {unit:?} of {entity:?} refused the request")]
    Refused {
        entity: NodeRef,
        unit: Unit,
        failure: RemoteOperationFailure,
    },
    #[error("the owner of unit {unit:?} of {entity:?} answered another request")]
    UnexpectedAnswer { entity: NodeRef, unit: Unit },
    #[error(
        "the catalog unit {unit:?} of {entity:?} answered names a key that is not a branch key"
    )]
    CatalogKey { entity: NodeRef, unit: Unit },
    #[error("a released checkpoint names a placement this node cannot read")]
    ReleasedPlacement,
    #[error("the released state of {branch} could not be fetched from its holder")]
    Fetch { branch: BranchScope },
    #[error("the released state of {branch} could not be installed under this unit")]
    Install { branch: BranchScope },
    #[error("the branch of a released key could not be restored here ({branch})")]
    Restore { branch: BranchScope },
    #[error("the released row of {branch} is not a row of the relay's schema")]
    ReleasedRow { branch: BranchScope },
}

impl Runtime {
    /// The node owning `unit` of `entity` in the installed revision of `domain`.
    pub(super) fn partition_unit_owner(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        unit: Unit,
    ) -> error_stack::Result<ClusterNodeName, PartitionClaimError> {
        let unowned = || {
            Report::new(PartitionClaimError::UnitUnowned {
                entity: entity.clone(),
                unit,
            })
        };
        let execution = self.inner.executions.get(domain).ok_or_else(unowned)?;
        let node = execution.revision.nodes.get(entity).ok_or_else(unowned)?;
        let placement = node.units.get(&unit).ok_or_else(unowned)?;
        placement.primary_node.clone().ok_or_else(unowned)
    }

    /// Sends `request` to the owner of `unit` of `entity`, answering it here when this node owns
    /// the unit.
    async fn send_partition_request(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        unit: Unit,
        request: PartitionTransitionRequest,
    ) -> error_stack::Result<PartitionTransitionAnswer, PartitionClaimError> {
        let owner = self.partition_unit_owner(domain, entity, unit)?;
        let dispatcher = self.inner.remote_dispatcher.load_full().ok_or_else(|| {
            Report::new(PartitionClaimError::Request {
                entity: entity.clone(),
                unit,
            })
        })?;
        let answer = if &owner == dispatcher.local_node_id() {
            self.answer_partition_transition(request).await
        } else {
            dispatcher
                .request_with_timeout(
                    &owner,
                    request,
                    <PartitionTransitionRequest as nervix_interconnect::InterconnectRequest>::TIMEOUT,
                )
                .await
                .change_context_lazy(|| PartitionClaimError::Request {
                    entity: entity.clone(),
                    unit,
                })?
                .result
        };
        answer.map_err(|failure| {
            Report::new(PartitionClaimError::Refused {
                entity: entity.clone(),
                unit,
                failure,
            })
        })
    }

    /// Asks the holder `subject` names to take `step` for `key`.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a claimant asks for each claim step of a key once, again only after a \
                      failed attempt"
        )
    )]
    pub(super) async fn request_partition_step(
        &self,
        subject: &PartitionClaimSubject,
        key: &BranchKey,
        step: PartitionClaimStep,
    ) -> error_stack::Result<PartitionClaimOutcome, PartitionClaimError> {
        let request = PartitionTransitionRequest::Claim {
            subject: subject.clone(),
            key: BranchKey::to_remote_key(&Some(key.clone())).unwrap_or_default(),
            step,
        };
        let answer = self
            .send_partition_request(&subject.domain, &subject.entity, subject.holder, request)
            .await?;
        match answer {
            PartitionTransitionAnswer::Claim(outcome) => Ok(outcome),
            PartitionTransitionAnswer::CatalogShard(_) | PartitionTransitionAnswer::Holdings(_) => {
                Err(Report::new(PartitionClaimError::UnexpectedAnswer {
                    entity: subject.entity.clone(),
                    unit: subject.holder,
                }))
            }
        }
    }

    /// Reads the catalog of `transition`'s unit: every key each other unit of the previous count
    /// holds that the current count places in this unit, with the unit that holds it.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a unit reads its partition catalog once per transition, again only after a \
                      failed read"
        )
    )]
    pub(super) async fn read_partition_catalog(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        transition: &UnitPartitionTransition,
    ) -> error_stack::Result<ReadCatalog, PartitionClaimError> {
        let mut catalog = ReadCatalog::default();
        for peer in transition.catalog_peers() {
            let mut after = None;
            loop {
                nervix_primitives::task::consume_budget().await;
                let request = PartitionTransitionRequest::CatalogShard {
                    subject: PartitionClaimSubject {
                        domain: domain.clone(),
                        entity: entity.clone(),
                        epoch: transition.epoch,
                        holder: peer,
                        claimant: transition.unit,
                    },
                    after,
                };
                let answer = self
                    .send_partition_request(domain, entity, peer, request)
                    .await?;
                let PartitionTransitionAnswer::CatalogShard(shard) = answer else {
                    return Err(Report::new(PartitionClaimError::UnexpectedAnswer {
                        entity: entity.clone(),
                        unit: peer,
                    }));
                };
                for listed in shard.keys {
                    let key =
                        BranchKey::from_remote_key(Some(listed.key)).change_context_lazy(|| {
                            PartitionClaimError::CatalogKey {
                                entity: entity.clone(),
                                unit: peer,
                            }
                        })?;
                    let Some(key) = key else {
                        return Err(Report::new(PartitionClaimError::CatalogKey {
                            entity: entity.clone(),
                            unit: peer,
                        }));
                    };
                    catalog.insert(listed.fingerprint, CatalogedKey { key, holder: peer });
                }
                match shard.next {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
        }
        Ok(catalog)
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::FieldName;

    use super::*;
    use crate::runtime_schema::RuntimeValue;

    fn tenant(name: &str) -> BranchKey {
        BranchKey::from_fields([(
            FieldName::parse("tenant").assured("the test uses a valid field name"),
            RuntimeValue::String(name.to_string()),
        )])
        .assured("one field forms a concrete branch key")
    }

    #[test]
    fn inputs_park_until_the_catalog_is_read_and_a_key_no_peer_holds_begins_with_them_in_order() {
        let mut claimant = PartitionClaimant::new(8);
        let eta = tenant("eta");
        assert_eq!(
            claimant.admit(eta.clone(), 1),
            ClaimAdmission::Parked {
                start: ClaimStart::CatalogRead
            }
        );
        assert_eq!(
            claimant.admit(eta.clone(), 2),
            ClaimAdmission::Parked {
                start: ClaimStart::Nothing
            }
        );
        let resolutions = claimant.catalog_read(BTreeMap::new());
        assert_eq!(
            resolutions,
            vec![CatalogResolution::Begin {
                key: eta.clone(),
                inputs: VecDeque::from([1, 2])
            }]
        );
        assert!(!claimant.is_parked(&eta));
        assert_eq!(claimant.admit(tenant("acme"), 3), ClaimAdmission::Begin(3));
    }

    #[test]
    fn a_key_a_peer_holds_is_claimed_once_and_its_input_runs_after_the_claim_in_arrival_order() {
        let mut claimant = PartitionClaimant::new(8);
        let beta = tenant("beta");
        claimant.catalog_read(BTreeMap::from([(beta.fingerprint(), Unit::partition(1))]));
        assert_eq!(
            claimant.admit(beta.clone(), 1),
            ClaimAdmission::Parked {
                start: ClaimStart::Claim {
                    holder: Unit::partition(1)
                }
            }
        );
        assert_eq!(
            claimant.admit(beta.clone(), 2),
            ClaimAdmission::Parked {
                start: ClaimStart::Nothing
            }
        );
        assert_eq!(claimant.claim_holder(&beta), Some(Unit::partition(1)));
        assert_eq!(claimant.claim_settled(&beta), VecDeque::from([1, 2]));
        assert_eq!(claimant.claiming(), 0);
        assert_eq!(claimant.claim_holder(&beta), None);
        // The settled key no longer names a holder, so its next input begins at once.
        assert_eq!(claimant.admit(beta, 3), ClaimAdmission::Begin(3));
    }

    #[test]
    fn a_catalog_read_claims_every_parked_key_a_peer_holds_and_begins_the_others() {
        let mut claimant = PartitionClaimant::new(8);
        let beta = tenant("beta");
        let eta = tenant("eta");
        claimant.admit(beta.clone(), 1);
        claimant.admit(eta.clone(), 2);
        let resolutions =
            claimant.catalog_read(BTreeMap::from([(beta.fingerprint(), Unit::partition(5))]));
        assert_eq!(
            resolutions,
            vec![
                CatalogResolution::Claim {
                    key: beta.clone(),
                    holder: Unit::partition(5)
                },
                CatalogResolution::Begin {
                    key: eta,
                    inputs: VecDeque::from([2])
                }
            ]
        );
        assert!(claimant.is_parked(&beta));
        assert_eq!(claimant.claiming(), 1);
        assert_eq!(claimant.claim_settled(&beta), VecDeque::from([1]));
    }

    #[test]
    fn the_bound_counts_every_parked_input_and_releases_as_keys_resolve() {
        let mut claimant = PartitionClaimant::new(2);
        let beta = tenant("beta");
        claimant.admit(beta.clone(), 1);
        assert!(!claimant.is_full());
        claimant.admit(tenant("kappa"), 2);
        assert!(claimant.is_full());
        claimant.catalog_read(BTreeMap::new());
        assert!(!claimant.is_full());
    }

    #[test]
    fn a_failed_catalog_read_is_started_again_by_the_next_unresolved_input() {
        let mut claimant = PartitionClaimant::new(4);
        claimant.admit(tenant("beta"), 1);
        assert!(claimant.catalog_failed());
        assert!(!claimant.catalog_failed());
        assert_eq!(
            claimant.admit(tenant("lambda"), 2),
            ClaimAdmission::Parked {
                start: ClaimStart::CatalogRead
            }
        );
    }

    #[test]
    fn a_teardown_drains_every_parked_input_in_arrival_order() {
        let mut claimant = PartitionClaimant::new(4);
        claimant.admit(tenant("beta"), 1);
        claimant.admit(tenant("kappa"), 2);
        claimant.admit(tenant("beta"), 3);
        let drained = claimant.drain_parked();
        assert_eq!(
            drained,
            vec![
                (tenant("beta"), VecDeque::from([1, 3])),
                (tenant("kappa"), VecDeque::from([2]))
            ]
        );
        assert!(!claimant.is_full());
    }

    #[test]
    fn a_cataloged_key_claimed_before_its_input_parks_that_input_until_the_claim_settles() {
        let mut claimant = PartitionClaimant::new(4);
        assert!(claimant.start_catalog_read());
        assert!(
            !claimant.start_catalog_read(),
            "one catalog read runs at a time"
        );
        let beta = tenant("beta");
        claimant.catalog_read(BTreeMap::from([(beta.fingerprint(), Unit::partition(1))]));
        assert!(claimant.begin_claim(&beta, Unit::partition(1)));
        assert!(
            !claimant.begin_claim(&beta, Unit::partition(1)),
            "a key is claimed once at a time"
        );
        assert_eq!(
            claimant.admit(beta.clone(), 1),
            ClaimAdmission::Parked {
                start: ClaimStart::Nothing
            },
            "input of a key being claimed waits for the claim it did not start"
        );
        assert_eq!(claimant.claim_settled(&beta), VecDeque::from([1]));
        assert!(
            !claimant.begin_claim(&beta, Unit::partition(1)),
            "a settled key is not claimed again"
        );
        assert!(
            !claimant.begin_claim(&tenant("eta"), Unit::partition(1)),
            "a key the catalog does not name is not claimed"
        );
    }

    #[test]
    fn a_failed_catalog_read_can_start_again_without_waiting_input() {
        let mut claimant = PartitionClaimant::<u64>::new(4);
        assert!(claimant.start_catalog_read());
        assert!(claimant.catalog_failed());
        assert!(claimant.start_catalog_read());
    }

    #[test]
    fn only_a_unit_the_current_count_schedules_is_a_current_unit() {
        let partitions = |count: u16| {
            nervix_models::BranchPartitions::Direct(
                std::num::NonZeroU16::new(count).assured("the test counts are positive"),
            )
        };
        let shrinking = |unit| UnitPartitionTransition {
            branch: BranchName::parse("by_tenant").assured("the test branch name is valid"),
            epoch: PartitionEpochNumber::FIRST.next(),
            current: RelayPartitioning::of_partitions(partitions(4)),
            previous: RelayPartitioning::of_partitions(partitions(8)),
            unit,
        };
        assert!(shrinking(Unit::partition(3)).is_current_unit());
        assert!(!shrinking(Unit::partition(5)).is_current_unit());
    }

    #[test]
    fn a_unit_misplaces_exactly_the_keys_the_current_count_places_elsewhere() {
        let partitions = |count: u16| {
            nervix_models::BranchPartitions::Direct(
                std::num::NonZeroU16::new(count).assured("the test counts are positive"),
            )
        };
        let transition = UnitPartitionTransition {
            branch: BranchName::parse("by_tenant").assured("the test branch name is valid"),
            epoch: PartitionEpochNumber::FIRST.next(),
            current: RelayPartitioning::of_partitions(partitions(8)),
            previous: RelayPartitioning::of_partitions(partitions(4)),
            unit: Unit::partition(1),
        };
        let beta = Some(tenant("beta"));
        let gamma = Some(tenant("gamma"));
        // Under eight partitions beta lands in partition 5 and gamma in partition 1.
        assert!(transition.misplaces(&beta));
        assert_eq!(transition.target_of(&beta), Some(Unit::partition(5)));
        assert!(!transition.misplaces(&gamma));
        assert!(!transition.misplaces(&None));
        assert_eq!(
            transition.catalog_peers(),
            vec![Unit::partition(0), Unit::partition(2), Unit::partition(3)]
        );
    }
}
