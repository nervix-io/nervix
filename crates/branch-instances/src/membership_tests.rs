use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc as StdArc, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::Timestamp;
use triomphe::Arc;

use super::{BranchAdmission, BranchMembership, BranchPresence, OwnedBranches};

const INFALLIBLE: &str = "the test constructor's error type is Infallible";

fn at(millis: i64) -> Timestamp {
    Timestamp::from_unix_nanos(
        millis
            .checked_mul(1_000_000)
            .assured("the test clock stays within a few seconds"),
    )
}

fn capacity(branches: usize) -> Option<NonZeroUsize> {
    Some(NonZeroUsize::new(branches).assured("the tests name positive capacities"))
}

/// Admit one batch for `key`, creating a branch whose state is `created`.
fn admit(
    owner: &mut OwnedBranches<u8, u64>,
    key: Option<u8>,
    now: Timestamp,
    limit: Option<NonZeroUsize>,
    created: u64,
) -> BranchAdmission<u8, u64> {
    owner
        .admit(key.as_ref(), now, limit, |_, _| {
            Ok::<u64, std::convert::Infallible>(created)
        })
        .assured(INFALLIBLE)
}

/// The concrete branches of `membership`, ordered for comparison.
fn branches(membership: &BranchMembership<u8>) -> BTreeSet<u8> {
    membership.branches().copied().collect()
}

#[test]
fn a_claim_publishes_an_empty_membership_for_its_owner() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let before = presence.load();

    let owner = OwnedBranches::<u8, u64>::claim(presence.clone());

    let claimed = presence.load();
    assert!(!StdArc::ptr_eq(&before, &claimed));
    assert!(branches(&claimed).is_empty());
    assert!(!claimed.contains(None));
    drop(owner);
}

#[test]
fn a_new_branch_publishes_once_and_an_established_branch_publishes_nothing() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let mut owner = OwnedBranches::<u8, u64>::claim(presence.clone());

    let created = admit(&mut owner, Some(7), at(1), None, 70);
    assert_eq!(created.branch.as_deref(), Some(&70));
    let published = presence.load();
    assert_eq!(branches(&published), BTreeSet::from([7]));

    for millis in 2..10 {
        let touched = admit(&mut owner, Some(7), at(millis), None, 71);
        assert_eq!(
            touched.branch.as_deref(),
            Some(&70),
            "an established branch keeps the state it was created with"
        );
        assert!(touched.evicted.is_empty());
        assert!(
            StdArc::ptr_eq(&published, &presence.load()),
            "a batch for an established branch must not publish"
        );
    }
}

#[test]
fn unbranched_work_is_published_once_and_has_no_branch_state() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let mut owner = OwnedBranches::<u8, u64>::claim(presence.clone());

    let first = admit(&mut owner, None, at(1), capacity(1), 0);
    assert!(first.branch.is_none());
    let published = presence.load();
    assert!(published.contains(None));
    assert!(branches(&published).is_empty());

    admit(&mut owner, None, at(2), capacity(1), 0);
    assert!(StdArc::ptr_eq(&published, &presence.load()));
}

#[test]
fn an_admission_past_capacity_publishes_the_new_branch_and_the_eviction_together() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let mut owner = OwnedBranches::<u8, u64>::claim(presence.clone());
    admit(&mut owner, Some(1), at(1), capacity(2), 10);
    admit(&mut owner, Some(2), at(2), capacity(2), 20);
    admit(&mut owner, Some(1), at(3), capacity(2), 11);

    let admitted = admit(&mut owner, Some(3), at(4), capacity(2), 30);

    let evicted = admitted
        .evicted
        .iter()
        .map(|(key, state)| (*key, **state))
        .collect::<Vec<_>>();
    assert_eq!(
        evicted,
        vec![(2, 20)],
        "the least recently used branch goes"
    );
    assert_eq!(branches(&presence.load()), BTreeSet::from([1, 3]));
}

#[test]
fn an_evicted_branch_is_recreated_with_new_state_and_a_new_incarnation() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let mut owner = OwnedBranches::<u8, u64>::claim(presence.clone());
    let mut incarnations = Vec::new();
    owner
        .admit(Some(&1), at(1), capacity(1), |_, incarnation| {
            incarnations.push(incarnation);
            Ok::<u64, std::convert::Infallible>(10)
        })
        .assured(INFALLIBLE);
    admit(&mut owner, Some(2), at(2), capacity(1), 20);
    assert!(!presence.contains(Some(&1)));

    let recreated = owner
        .admit(Some(&1), at(3), capacity(1), |_, incarnation| {
            incarnations.push(incarnation);
            Ok::<u64, std::convert::Infallible>(12)
        })
        .assured(INFALLIBLE);

    assert_eq!(recreated.branch.as_deref(), Some(&12));
    assert!(incarnations[1] > incarnations[0]);
    assert_eq!(branches(&presence.load()), BTreeSet::from([1]));
}

#[test]
fn expiry_publishes_the_release_of_idle_branches_only() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let mut owner = OwnedBranches::<u8, u64>::claim(presence.clone());
    admit(&mut owner, Some(1), at(0), None, 10);
    admit(&mut owner, Some(2), at(20), None, 20);
    let before = presence.load();

    assert!(owner.expire(at(25), Duration::from_millis(30)).is_empty());
    assert!(
        StdArc::ptr_eq(&before, &presence.load()),
        "an expiry scan that releases nothing must not publish"
    );

    let expired = owner.expire(at(35), Duration::from_millis(30));
    let expired = expired.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
    assert_eq!(expired, vec![1]);
    assert_eq!(branches(&presence.load()), BTreeSet::from([2]));
}

#[test]
fn dropping_the_owner_releases_its_membership() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let mut owner = OwnedBranches::<u8, u64>::claim(presence.clone());
    admit(&mut owner, Some(1), at(1), None, 10);
    admit(&mut owner, None, at(1), None, 0);

    drop(owner);

    let released = presence.load();
    assert!(branches(&released).is_empty());
    assert!(!released.contains(None));
}

#[test]
fn a_replaced_owner_never_publishes_over_its_successor() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let mut predecessor = OwnedBranches::<u8, u64>::claim(presence.clone());
    admit(&mut predecessor, Some(1), at(1), None, 10);

    let mut successor = OwnedBranches::<u8, u64>::claim(presence.clone());
    assert!(
        branches(&presence.load()).is_empty(),
        "a claim replaces what the predecessor left"
    );
    admit(&mut successor, Some(2), at(2), None, 20);
    let published = presence.load();

    admit(&mut predecessor, Some(3), at(3), capacity(1), 30);
    predecessor.expire(at(100), Duration::from_millis(1));
    drop(predecessor);

    assert!(StdArc::ptr_eq(&published, &presence.load()));
    assert_eq!(branches(&presence.load()), BTreeSet::from([2]));
    drop(successor);
    assert!(branches(&presence.load()).is_empty());
}

#[test]
fn churn_allocation_does_not_grow_with_the_branches_an_owner_holds() {
    let small = churn_allocations(16);
    let large = churn_allocations(4_096);
    eprintln!(
        "one churn admission allocates {} bytes in {} calls with 16 branches held and {} bytes in \
         {} calls with 4096",
        small.bytes_allocated, small.alloc_calls, large.bytes_allocated, large.alloc_calls,
    );
    // A publication that copied the membership would allocate in proportion to it, 256 times as
    // much at the larger size. A persistent set copies only the path to the changed branches, which
    // randomized hashing can make a few levels deeper at the larger size, never proportionally.
    let bound = small
        .bytes_allocated
        .checked_mul(16)
        .assured("one small admission allocates far less than a sixteenth of the address space");
    assert!(
        large.bytes_allocated <= bound,
        "admitting one branch past capacity allocated {} bytes with 4096 branches held and {} \
         bytes with 16",
        large.bytes_allocated,
        small.bytes_allocated,
    );
}

/// The allocations of admitting one new branch into an owner that is full at `held` branches, so
/// the admission creates one branch, evicts the least recently used one and publishes once.
fn churn_allocations(held: usize) -> alloc_count::AllocStats {
    let presence = Arc::new(BranchPresence::<u32>::new());
    let mut owner = OwnedBranches::<u32, u64>::claim(presence);
    let limit = NonZeroUsize::new(held).assured("the churn sizes are positive");
    let held = u32::try_from(held).assured("the churn sizes fit u32");
    for key in 0..held {
        admit_u32(&mut owner, key, limit);
    }
    // Warm the path the measured admission copies, so both sizes measure one steady admission.
    admit_u32(&mut owner, held, limit);
    let measured = held
        .checked_add(1)
        .assured("the churn sizes stay far below u32::MAX");
    let (stats, ()) = alloc_count::alloc_count!({
        admit_u32(&mut owner, measured, limit);
    });
    stats
}

fn admit_u32(owner: &mut OwnedBranches<u32, u64>, key: u32, limit: NonZeroUsize) {
    owner
        .admit(Some(&key), at(1), Some(limit), |_, _| {
            Ok::<u64, std::convert::Infallible>(u64::from(key))
        })
        .assured(INFALLIBLE);
}

#[test]
fn an_established_branch_admission_allocates_nothing() {
    let presence = Arc::new(BranchPresence::<u8>::new());
    let mut owner = OwnedBranches::<u8, u64>::claim(presence);
    admit(&mut owner, Some(1), at(1), capacity(4), 10);
    admit(&mut owner, Some(2), at(2), capacity(4), 20);

    let (stats, ()) = alloc_count::alloc_count!({
        admit(&mut owner, Some(1), at(3), capacity(4), 11);
    });

    assert_eq!(stats.alloc_calls, 0, "{stats:?}");
}

/// One operation of the membership property, decoded from fuzzer bytes.
#[derive(Debug)]
enum Operation {
    /// Claim the presence for a new owner.
    Claim,
    /// Admit one batch through one live owner.
    Admit {
        owner: u8,
        key: Option<u8>,
        advance_millis: u8,
        capacity: Option<NonZeroUsize>,
    },
    /// Run one expiry scan through one live owner.
    Expire {
        owner: u8,
        advance_millis: u8,
        max_idle_millis: u8,
    },
    /// Drop one live owner.
    Release { owner: u8 },
}

/// The concrete branch keys the property draws from: few enough that sequences recreate branches.
const PROPERTY_KEYS: u8 = 6;

impl Operation {
    /// Decode the operations in `bytes`, four bytes each; a trailing partial operation is ignored.
    fn decode(bytes: &[u8]) -> Vec<Self> {
        let mut operations = Vec::new();
        let (whole_operations, _trailing_partial) = bytes.as_chunks::<4>();
        for chunk in whole_operations {
            let owner = chunk[1];
            let key = match chunk[2] % (PROPERTY_KEYS + 1) {
                0 => None,
                key => Some(key),
            };
            let operation = match chunk[0] % 6 {
                0 => Self::Claim,
                1 => Self::Release { owner },
                2 => Self::Expire {
                    owner,
                    advance_millis: chunk[2] % 8,
                    max_idle_millis: chunk[3] % 16,
                },
                3 => Self::Admit {
                    owner,
                    key,
                    advance_millis: chunk[3] % 8,
                    capacity: NonZeroUsize::new(usize::from(chunk[3] % 4) + 1),
                },
                _ => Self::Admit {
                    owner,
                    key,
                    advance_millis: chunk[3] % 8,
                    capacity: None,
                },
            };
            operations.push(operation);
        }
        operations
    }
}

/// The specified contract of one owner lifetime: its unbranched flag and its branches in activity
/// order, least recently used first, each with its last admission time and state.
///
/// The model is a sequence because activity order is what it specifies. It holds at most
/// `PROPERTY_KEYS` branches, which bounds every scan of it.
#[derive(Debug, Default)]
struct ModelOwner {
    lifetime: u64,
    unbranched: bool,
    branches: Vec<ModelBranch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ModelBranch {
    key: u8,
    last_admitted: i64,
    state: u64,
}

/// The visible set the presence must publish: the current owner lifetime and its membership.
#[derive(Debug, Default, PartialEq, Eq)]
struct ModelMembership {
    unbranched: bool,
    branches: BTreeSet<u8>,
}

impl ModelOwner {
    fn membership(&self) -> ModelMembership {
        ModelMembership {
            unbranched: self.unbranched,
            branches: self.branches.iter().map(|branch| branch.key).collect(),
        }
    }
}

/// Everything the property tracks for one live owner: the production owner and its model.
struct LiveOwner {
    production: OwnedBranches<u8, u64>,
    model: ModelOwner,
}

/// Runs one decoded sequence against the production owners and the model, checking after every
/// operation that the published membership is exactly the current owner's specified membership,
/// and that an operation that changes no visible state publishes nothing.
struct MembershipProperty {
    presence: Arc<BranchPresence<u8>>,
    owners: Vec<LiveOwner>,
    /// The latest claimed lifetime, which alone may change the published membership.
    current: Option<u64>,
    published: ModelMembership,
    now_millis: i64,
    next_state: u64,
}

impl MembershipProperty {
    fn new() -> Self {
        Self {
            presence: Arc::new(BranchPresence::new()),
            owners: Vec::new(),
            current: None,
            published: ModelMembership::default(),
            now_millis: 0,
            next_state: 0,
        }
    }

    fn run(&mut self, operations: Vec<Operation>) {
        for operation in operations {
            let before = self.presence.load();
            let changed = self.apply(operation);
            let after = self.presence.load();
            assert_eq!(
                self.observed(&after),
                self.published,
                "the published membership must be the current owner's membership"
            );
            if !changed {
                assert!(
                    StdArc::ptr_eq(&before, &after),
                    "an operation that changes no visible state must not publish"
                );
            }
        }
    }

    fn observed(&self, membership: &BranchMembership<u8>) -> ModelMembership {
        for key in 1..=PROPERTY_KEYS {
            assert_eq!(
                membership.contains(Some(&key)),
                membership.branches().any(|branch| *branch == key),
            );
        }
        assert_eq!(membership.branch_count(), membership.branches().count());
        ModelMembership {
            unbranched: membership.contains(None),
            branches: membership.branches().copied().collect(),
        }
    }

    /// Apply `operation`, and answer whether the published membership changed.
    fn apply(&mut self, operation: Operation) -> bool {
        match operation {
            Operation::Claim => self.claim(),
            Operation::Admit {
                owner,
                key,
                advance_millis,
                capacity,
            } => {
                self.advance(advance_millis);
                self.admit(owner, key, capacity)
            }
            Operation::Expire {
                owner,
                advance_millis,
                max_idle_millis,
            } => {
                self.advance(advance_millis);
                self.expire(owner, max_idle_millis)
            }
            Operation::Release { owner } => self.release(owner),
        }
    }

    fn advance(&mut self, millis: u8) {
        self.now_millis = self
            .now_millis
            .checked_add(i64::from(millis))
            .assured("a sequence of at most 64 operations advances the clock by at most 448 ms");
    }

    fn claim(&mut self) -> bool {
        let lifetime = match self.current {
            None => 1,
            Some(lifetime) => lifetime
                .checked_add(1)
                .assured("a sequence of at most 64 operations claims at most 64 times"),
        };
        let production = OwnedBranches::claim(self.presence.clone());
        self.owners.push(LiveOwner {
            production,
            model: ModelOwner {
                lifetime,
                ..ModelOwner::default()
            },
        });
        self.current = Some(lifetime);
        self.published = ModelMembership::default();
        true
    }

    fn live_owner(&mut self, owner: u8) -> Option<usize> {
        if self.owners.is_empty() {
            return None;
        }
        Some(usize::from(owner) % self.owners.len())
    }

    fn admit(&mut self, owner: u8, key: Option<u8>, capacity: Option<NonZeroUsize>) -> bool {
        let Some(index) = self.live_owner(owner) else {
            return false;
        };
        let now = self.now_millis;
        let created_state = self.next_state;
        let live = &mut self.owners[index];

        let mut created = false;
        let mut model_state = None;
        let mut membership_changed = false;
        match key {
            None => {
                membership_changed |= !live.model.unbranched;
                live.model.unbranched = true;
            }
            Some(key) => {
                let position = live
                    .model
                    .branches
                    .iter()
                    .position(|branch| branch.key == key);
                let branch = match position {
                    Some(position) => {
                        let mut branch = live.model.branches.remove(position);
                        branch.last_admitted = now;
                        branch
                    }
                    None => {
                        created = true;
                        membership_changed = true;
                        ModelBranch {
                            key,
                            last_admitted: now,
                            state: created_state,
                        }
                    }
                };
                model_state = Some(branch.state);
                live.model.branches.push(branch);
            }
        }
        let mut model_evicted = Vec::new();
        if let Some(capacity) = capacity {
            while live.model.branches.len() > capacity.get() {
                model_evicted.push(live.model.branches.remove(0));
                membership_changed = true;
            }
        }

        let admission = live
            .production
            .admit(key.as_ref(), at(now), capacity, |_, _| {
                Ok::<u64, std::convert::Infallible>(created_state)
            })
            .assured(INFALLIBLE);
        assert_eq!(admission.branch.as_deref().copied(), model_state);
        let evicted = admission
            .evicted
            .iter()
            .map(|(key, state)| (*key, **state))
            .collect::<Vec<_>>();
        let expected = model_evicted
            .iter()
            .map(|branch| (branch.key, branch.state))
            .collect::<Vec<_>>();
        assert_eq!(
            evicted, expected,
            "eviction releases least recently used first"
        );

        if created {
            self.next_state = self
                .next_state
                .checked_add(1)
                .assured("a sequence of at most 64 operations creates at most 64 branches");
        }
        self.publish_if_current(index, membership_changed)
    }

    fn expire(&mut self, owner: u8, max_idle_millis: u8) -> bool {
        let Some(index) = self.live_owner(owner) else {
            return false;
        };
        let now = self.now_millis;
        let live = &mut self.owners[index];

        let mut model_expired = Vec::new();
        while let Some(oldest) = live.model.branches.first() {
            let idle = now
                .checked_sub(oldest.last_admitted)
                .assured("the property clock never moves backwards");
            if idle < i64::from(max_idle_millis) {
                break;
            }
            model_expired.push(live.model.branches.remove(0));
        }
        let expired = live
            .production
            .expire(at(now), Duration::from_millis(u64::from(max_idle_millis)));
        let expired = expired
            .iter()
            .map(|(key, state)| (*key, **state))
            .collect::<Vec<_>>();
        let expected = model_expired
            .iter()
            .map(|branch| (branch.key, branch.state))
            .collect::<Vec<_>>();
        assert_eq!(
            expired, expected,
            "expiry releases idle branches oldest first"
        );

        self.publish_if_current(index, !model_expired.is_empty())
    }

    fn release(&mut self, owner: u8) -> bool {
        let Some(index) = self.live_owner(owner) else {
            return false;
        };
        let released = self.owners.remove(index);
        let was_current = Some(released.model.lifetime) == self.current;
        let was_empty = released.model.membership() == ModelMembership::default();
        drop(released.production);
        if !was_current || was_empty {
            return false;
        }
        self.published = ModelMembership::default();
        true
    }

    /// Record that the owner at `index` changed its membership, which is visible only while its
    /// lifetime is the current one.
    fn publish_if_current(&mut self, index: usize, membership_changed: bool) -> bool {
        let live = &self.owners[index];
        if !membership_changed || Some(live.model.lifetime) != self.current {
            return false;
        }
        self.published = live.model.membership();
        true
    }
}

#[test]
fn bolero_owned_branches_publish_exactly_the_current_owners_membership() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            let mut property = MembershipProperty::new();
            property.run(Operation::decode(bytes));
        });
}
