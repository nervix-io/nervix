//! Layer: test harness.
//! Owns: what a replica task plans from the owner's catalog and announcements, and the property
//! that a replica catches every branch up with its owner however catalog reads, fetches and
//! announcements are lost, repeated or reordered.
//! May depend on: the replica's branch checkpoints, the owner's catalog, typed branch keys and the
//! branches a lifecycle names.
//! Must not know: the interconnect, storage, or how a checkpoint is installed.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

use nervix_models::SchemaFingerprint;

use super::*;
use crate::runtime::{
    branch_checkpoint_catalog::{BranchCheckpointCatalog, CatalogRegistration},
    state_replication::checkpoint_listing::OwnerCheckpointListing,
    test_fixtures::string_branch_key,
};

const PAGE: NonZeroUsize = nonzero_ext::nonzero!(2_usize);

fn state() -> RuntimeState {
    RuntimeState::Deduplicator {
        schema: SchemaFingerprint::from_digest([7; 32]),
    }
}

fn tenant(name: &str) -> Option<BranchKey> {
    string_branch_key("tenant", name)
}

fn named(branches: &[Option<BranchKey>]) -> NamedBranches {
    NamedBranches::from_keys(branches.iter().cloned())
}

/// `listing` as a replica receives it: converted to the interconnect envelope, encoded, decoded and
/// converted back, which leaves every page, branch, state and revision of it as the owner listed it.
fn through_the_wire(listing: CheckpointListing) -> CheckpointListing {
    let owner = OwnerCheckpointListing::Listed(listing);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&owner.to_remote())
        .expect("a branch checkpoint listing encodes");
    let wire =
        rkyv::from_bytes::<nervix_interconnect::BranchCheckpointListing, rkyv::rancor::Error>(
            &bytes,
        )
        .expect("an encoded branch checkpoint listing decodes");
    let received = OwnerCheckpointListing::from_remote(wire).expect("a listed branch key decodes");
    assert_eq!(
        received, owner,
        "a listing reaches the replica exactly as its owner listed it"
    );
    let OwnerCheckpointListing::Listed(listing) = received else {
        panic!("a listed catalog reaches the replica listed");
    };
    listing
}

/// Read every page of `catalog` after the replica's cursor into `replica`.
fn read_catalog(replica: &mut ReplicaBranchCheckpoints, catalog: &BranchCheckpointCatalog) {
    loop {
        let listing = catalog.changes_after(replica.cursor(), PAGE);
        if !replica.apply(listing) {
            return;
        }
    }
}

fn step(branch: &Option<BranchKey>, held: Option<Held>, target: u64) -> BranchStep {
    BranchStep {
        branch: branch.clone(),
        state: state(),
        held,
        target,
        acknowledge: false,
    }
}

#[test]
fn a_round_plans_a_step_only_for_branches_that_changed() {
    let catalog = BranchCheckpointCatalog::with_epoch(1);
    let acme = tenant("acme");
    let beta = tenant("beta");
    let acme_entry = catalog.register(acme.clone(), state(), 1);
    let _beta_entry = catalog.register(beta.clone(), state(), 1);
    let lifecycle = named(&[acme.clone(), beta.clone()]);
    let mut replica = ReplicaBranchCheckpoints::default();
    read_catalog(&mut replica, &catalog);
    replica.follow_lifecycle(None, &lifecycle);

    let first = replica.plan(&lifecycle);
    assert_eq!(first.len(), 2);
    for planned in first {
        replica.settle(planned, StepOutcome::Settled(Held::Revision(1)));
    }

    read_catalog(&mut replica, &catalog);
    assert_eq!(replica.plan(&lifecycle), Vec::new());

    acme_entry.record(2);
    read_catalog(&mut replica, &catalog);
    assert_eq!(
        replica.plan(&lifecycle),
        vec![step(&acme, Some(Held::Revision(1)), 2)]
    );
}

#[test]
fn a_branch_the_lifecycle_does_not_name_waits_for_the_lifecycle_that_names_it() {
    let catalog = BranchCheckpointCatalog::with_epoch(1);
    let acme = tenant("acme");
    let _entry = catalog.register(acme.clone(), state(), 3);
    let without_acme = named(&[]);
    let with_acme = named(std::slice::from_ref(&acme));
    let mut replica = ReplicaBranchCheckpoints::default();
    replica.follow_lifecycle(None, &without_acme);
    read_catalog(&mut replica, &catalog);

    assert_eq!(replica.plan(&without_acme), Vec::new());

    replica.follow_lifecycle(Some(&without_acme), &with_acme);
    assert_eq!(replica.plan(&with_acme), vec![step(&acme, None, 3)]);
}

#[test]
fn unbranched_work_is_caught_up_whatever_the_lifecycle_names() {
    let catalog = BranchCheckpointCatalog::with_epoch(1);
    let _entry = catalog.register(None, state(), 3);
    let lifecycle = named(&[]);
    let mut replica = ReplicaBranchCheckpoints::default();
    replica.follow_lifecycle(None, &lifecycle);
    read_catalog(&mut replica, &catalog);

    let planned = replica.plan(&lifecycle);
    assert_eq!(planned, vec![step(&None, None, 3)]);
    for held in planned {
        replica.settle(held, StepOutcome::Settled(Held::Revision(3)));
    }
    replica.follow_lifecycle(Some(&lifecycle), &lifecycle);
    assert_eq!(
        replica.held.get(&None),
        Some(&Held::Revision(3)),
        "a lifecycle that does not name unbranched work never drops what this node holds of it"
    );
}

#[test]
fn a_failed_step_is_planned_again_with_what_was_learned() {
    let catalog = BranchCheckpointCatalog::with_epoch(1);
    let acme = tenant("acme");
    let _entry = catalog.register(acme.clone(), state(), 4);
    let lifecycle = named(std::slice::from_ref(&acme));
    let mut replica = ReplicaBranchCheckpoints::default();
    read_catalog(&mut replica, &catalog);
    replica.follow_lifecycle(None, &lifecycle);

    let planned = replica.plan(&lifecycle);
    assert_eq!(planned, vec![step(&acme, None, 4)]);
    for failed in planned {
        replica.settle(failed, StepOutcome::Failed(Some(Held::Revision(2))));
    }

    assert_eq!(
        replica.plan(&lifecycle),
        vec![step(&acme, Some(Held::Revision(2)), 4)]
    );
}

#[test]
fn a_stale_step_is_dropped_until_the_owner_lists_the_branch_again() {
    let catalog = BranchCheckpointCatalog::with_epoch(1);
    let acme = tenant("acme");
    let entry = catalog.register(acme.clone(), state(), 2);
    let lifecycle = named(std::slice::from_ref(&acme));
    let mut replica = ReplicaBranchCheckpoints::default();
    read_catalog(&mut replica, &catalog);
    replica.follow_lifecycle(None, &lifecycle);
    for stale in replica.plan(&lifecycle) {
        replica.settle(stale, StepOutcome::Stale);
    }

    read_catalog(&mut replica, &catalog);
    assert_eq!(replica.plan(&lifecycle), Vec::new());

    entry.record(3);
    read_catalog(&mut replica, &catalog);
    assert_eq!(replica.plan(&lifecycle), vec![step(&acme, None, 3)]);
}

#[test]
fn an_announcement_of_another_state_never_hides_the_catalogued_checkpoint() {
    let catalog = BranchCheckpointCatalog::with_epoch(1);
    let acme = tenant("acme");
    let _entry = catalog.register(acme.clone(), state(), 2);
    let lifecycle = named(std::slice::from_ref(&acme));
    let replaced = RuntimeState::WindowProcessor {
        schema: SchemaFingerprint::from_digest([9; 32]),
    };
    let mut replica = ReplicaBranchCheckpoints::default();
    read_catalog(&mut replica, &catalog);
    replica.follow_lifecycle(None, &lifecycle);

    replica.take_announced(HashMap::from_iter([(
        acme.clone(),
        AnnouncedCheckpoint {
            state: replaced,
            lsm: 40,
        },
    )]));

    assert_eq!(
        replica.plan(&lifecycle),
        vec![BranchStep {
            acknowledge: true,
            ..step(&acme, None, 2)
        }]
    );
}

#[test]
fn an_announced_revision_is_acknowledged_even_when_already_held() {
    let acme = tenant("acme");
    let lifecycle = named(std::slice::from_ref(&acme));
    let mut replica = ReplicaBranchCheckpoints::default();
    replica.settle(
        step(&acme, None, 5),
        StepOutcome::Settled(Held::Revision(5)),
    );

    replica.take_announced(HashMap::from_iter([(
        acme.clone(),
        AnnouncedCheckpoint {
            state: state(),
            lsm: 5,
        },
    )]));

    assert_eq!(
        replica.plan(&lifecycle),
        vec![BranchStep {
            acknowledge: true,
            ..step(&acme, Some(Held::Revision(5)), 5)
        }]
    );
}

#[test]
fn a_restarted_listing_replaces_what_the_replica_knew_of_the_owner() {
    let acme = tenant("acme");
    let beta = tenant("beta");
    let lifecycle = named(&[acme.clone(), beta.clone()]);
    let first_owner = BranchCheckpointCatalog::with_epoch(1);
    let _acme_entry = first_owner.register(acme.clone(), state(), 2);
    let mut replica = ReplicaBranchCheckpoints::default();
    replica.follow_lifecycle(None, &lifecycle);
    read_catalog(&mut replica, &first_owner);
    for planned in replica.plan(&lifecycle) {
        replica.settle(planned, StepOutcome::Settled(Held::Revision(2)));
    }

    let second_owner = BranchCheckpointCatalog::with_epoch(2);
    let _beta_entry = second_owner.register(beta.clone(), state(), 1);
    read_catalog(&mut replica, &second_owner);

    assert_eq!(replica.plan(&lifecycle), vec![step(&beta, None, 1)]);
}

#[test]
fn a_dropped_branch_is_read_again_from_this_nodes_copy_once_named_again() {
    let catalog = BranchCheckpointCatalog::with_epoch(1);
    let acme = tenant("acme");
    let _entry = catalog.register(acme.clone(), state(), 2);
    let with_acme = named(std::slice::from_ref(&acme));
    let without_acme = named(&[]);
    let mut replica = ReplicaBranchCheckpoints::default();
    replica.follow_lifecycle(None, &with_acme);
    read_catalog(&mut replica, &catalog);
    for planned in replica.plan(&with_acme) {
        replica.settle(planned, StepOutcome::Settled(Held::Revision(2)));
    }

    replica.follow_lifecycle(Some(&with_acme), &without_acme);
    replica.follow_lifecycle(Some(&without_acme), &with_acme);

    assert_eq!(replica.plan(&with_acme), vec![step(&acme, None, 2)]);
}

/// The branches the property draws from, the absent key for unbranched work among them.
const PROPERTY_BRANCHES: usize = 4;

/// One step of a catch-up sequence, decoded from fuzz input.
#[derive(Debug, Clone, Copy)]
enum Operation {
    /// The owner starts the state of a branch, or replaces it, at the newest revision it had.
    Start(usize),
    /// The owner's state of a branch publishes a new revision.
    Publish(usize),
    /// The owner's state of a branch goes away.
    Stop(usize),
    /// The owner's catalog is replaced, as a new owner or an owner restart replaces it.
    ReplaceCatalog,
    /// The owner's branch lifecycle starts or stops naming a branch.
    Name(usize),
    Evict(usize),
    /// The replica installs the owner's branch lifecycle.
    SyncLifecycle,
    /// The owner announces a branch's revision, or one it announced earlier, to the replica.
    Announce(usize),
    AnnounceStale(usize),
    /// The replica runs a round: it reads the catalog unless the read is lost, takes the
    /// announcements, and each planned step fails when its bit in `failures` is set.
    Round {
        catalog_lost: bool,
        failures: u8,
    },
}

impl Operation {
    fn decode(bytes: &[u8]) -> Vec<Self> {
        let mut operations = Vec::new();
        let mut bytes = bytes.iter().copied();
        while let Some(opcode) = bytes.next() {
            let branch = usize::from(opcode >> 4) % PROPERTY_BRANCHES;
            let operation = match opcode % 10 {
                0 => Self::Start(branch),
                1 => Self::Publish(branch),
                2 => Self::Stop(branch),
                3 => Self::ReplaceCatalog,
                4 => Self::Name(branch),
                5 => Self::Evict(branch),
                6 => Self::SyncLifecycle,
                7 => Self::Announce(branch),
                8 => Self::AnnounceStale(branch),
                _ => {
                    let Some(failures) = bytes.next() else {
                        break;
                    };
                    Self::Round {
                        catalog_lost: opcode & 0x80 != 0,
                        failures,
                    }
                }
            };
            operations.push(operation);
        }
        operations
    }
}

/// The owner's live state of one branch.
struct OwnerBranch {
    lsm: u64,
    registration: CatalogRegistration,
}

/// An owner and a replica of one entity, driven through a catch-up sequence.
struct CatchUpProperty {
    branches: Vec<Option<BranchKey>>,
    catalog: BranchCheckpointCatalog,
    next_epoch: u64,
    /// The newest revision each branch reached on the owner, which a restarted state resumes
    /// from, so the owner's revisions of a branch never fall.
    newest: BTreeMap<usize, u64>,
    live: BTreeMap<usize, OwnerBranch>,
    owner_lifecycle: BTreeSet<usize>,
    replica_lifecycle: BTreeSet<usize>,
    /// What the replica's stable storage holds of each branch.
    replica_store: BTreeMap<usize, u64>,
    /// The announcements the replica received and has not taken yet.
    inbox: BTreeMap<usize, u64>,
    /// Every revision the owner announced for each branch, newest last.
    announced: BTreeMap<usize, Vec<u64>>,
    replica: ReplicaBranchCheckpoints,
}

impl CatchUpProperty {
    fn new() -> Self {
        let mut branches = vec![None];
        for index in 1..PROPERTY_BRANCHES {
            branches.push(tenant(&format!("tenant-{index}")));
        }
        Self {
            branches,
            catalog: BranchCheckpointCatalog::with_epoch(0),
            next_epoch: 1,
            newest: BTreeMap::new(),
            live: BTreeMap::new(),
            owner_lifecycle: BTreeSet::new(),
            replica_lifecycle: BTreeSet::new(),
            replica_store: BTreeMap::new(),
            inbox: BTreeMap::new(),
            announced: BTreeMap::new(),
            replica: ReplicaBranchCheckpoints::default(),
        }
    }

    fn run(&mut self, operations: Vec<Operation>) {
        for operation in operations {
            self.apply(operation);
            self.check_held();
        }
        self.converge();
    }

    fn apply(&mut self, operation: Operation) {
        match operation {
            Operation::Start(branch) => self.start(branch),
            Operation::Publish(branch) => self.publish(branch),
            Operation::Stop(branch) => {
                self.live.remove(&branch);
            }
            Operation::ReplaceCatalog => self.replace_catalog(),
            Operation::Name(branch) => {
                self.owner_lifecycle.insert(branch);
            }
            Operation::Evict(branch) => {
                self.owner_lifecycle.remove(&branch);
            }
            Operation::SyncLifecycle => self.sync_lifecycle(),
            Operation::Announce(branch) => {
                if let Some(owned) = self.live.get(&branch) {
                    let lsm = owned.lsm;
                    self.receive_announcement(branch, lsm);
                    self.announced.entry(branch).or_default().push(lsm);
                }
            }
            Operation::AnnounceStale(branch) => {
                let stale = match self.announced.get(&branch) {
                    Some(revisions) => revisions.first().copied(),
                    None => None,
                };
                if let Some(stale) = stale {
                    self.receive_announcement(branch, stale);
                }
            }
            Operation::Round {
                catalog_lost,
                failures,
            } => self.round(catalog_lost, failures),
        }
    }

    fn start(&mut self, branch: usize) {
        let lsm = match self.newest.get(&branch) {
            Some(newest) => *newest,
            None => 0,
        };
        let registration = self
            .catalog
            .register(self.branches[branch].clone(), state(), lsm);
        self.live.insert(branch, OwnerBranch { lsm, registration });
    }

    fn publish(&mut self, branch: usize) {
        let Some(owned) = self.live.get_mut(&branch) else {
            return;
        };
        owned.lsm = owned
            .lsm
            .checked_add(1)
            .expect("a property publishes far fewer than 2^64 revisions");
        owned.registration.record(owned.lsm);
        self.newest.insert(branch, owned.lsm);
    }

    fn replace_catalog(&mut self) {
        self.catalog = BranchCheckpointCatalog::with_epoch(self.next_epoch);
        self.next_epoch = self
            .next_epoch
            .checked_add(1)
            .expect("a property replaces far fewer than 2^64 catalogs");
        let restarted = self.live.keys().copied().collect::<Vec<_>>();
        self.live.clear();
        for branch in restarted {
            self.start(branch);
        }
    }

    fn named(&self, branches: &BTreeSet<usize>) -> NamedBranches {
        NamedBranches::from_keys(branches.iter().map(|branch| self.branches[*branch].clone()))
    }

    fn sync_lifecycle(&mut self) {
        let previous = self.named(&self.replica_lifecycle);
        self.replica_lifecycle = self.owner_lifecycle.clone();
        let current = self.named(&self.replica_lifecycle);
        self.replica.follow_lifecycle(Some(&previous), &current);
    }

    fn receive_announcement(&mut self, branch: usize, lsm: u64) {
        let newest = match self.inbox.get(&branch) {
            Some(earlier) => (*earlier).max(lsm),
            None => lsm,
        };
        self.inbox.insert(branch, newest);
    }

    fn index_of(&self, branch: &Option<BranchKey>) -> usize {
        self.branches
            .iter()
            .position(|candidate| candidate == branch)
            .expect("every planned branch is one the property drew")
    }

    fn round(&mut self, catalog_lost: bool, failures: u8) {
        if !catalog_lost {
            loop {
                let listing = self.catalog.changes_after(self.replica.cursor(), PAGE);
                let listing = through_the_wire(listing);
                if !self.replica.apply(listing) {
                    break;
                }
            }
        }
        let inbox = std::mem::take(&mut self.inbox);
        let mut announcements = HashMap::default();
        for (branch, lsm) in inbox {
            announcements.insert(
                self.branches[branch].clone(),
                AnnouncedCheckpoint {
                    state: state(),
                    lsm,
                },
            );
        }
        self.replica.take_announced(announcements);
        let lifecycle = self.named(&self.replica_lifecycle);
        let steps = self.replica.plan(&lifecycle);
        for (position, planned) in steps.into_iter().enumerate() {
            let fails = position < 8 && failures & (1 << position) != 0;
            let outcome = self.execute(&planned, fails);
            self.replica.settle(planned, outcome);
        }
    }

    /// Carry out `planned` the way the replica task does: read this node's copy when the step does
    /// not know it, fetch a newer checkpoint from the owner, and install it.
    fn execute(&mut self, planned: &BranchStep, fails: bool) -> StepOutcome {
        let branch = self.index_of(&planned.branch);
        let held = match planned.held {
            Some(held) => held,
            None => match self.replica_store.get(&branch) {
                Some(lsm) => Held::Revision(*lsm),
                None => Held::Nothing,
            },
        };
        if fails {
            return StepOutcome::Failed(Some(held));
        }
        if held.covers(planned.target) {
            return StepOutcome::Settled(held);
        }
        let Some(owned) = self.live.get(&branch) else {
            return StepOutcome::Settled(held);
        };
        if held.covers(owned.lsm) {
            return StepOutcome::Settled(held);
        }
        self.replica_store.insert(branch, owned.lsm);
        StepOutcome::Settled(Held::Revision(owned.lsm))
    }

    /// What the replica records as held never differs from what its storage holds.
    fn check_held(&self) {
        for (branch, held) in &self.replica.held {
            let index = self.index_of(branch);
            let stored = match self.replica_store.get(&index) {
                Some(lsm) => Held::Revision(*lsm),
                None => Held::Nothing,
            };
            assert_eq!(
                *held, stored,
                "the replica's record of what it holds matches its storage"
            );
        }
    }

    /// Once the owner stops changing, the replica installs its lifecycle and every read and fetch
    /// succeeds, the replica holds every branch its lifecycle names at the owner's revision.
    fn converge(&mut self) {
        self.sync_lifecycle();
        for _ in 0..3 {
            self.round(false, 0);
            self.check_held();
        }
        for branch in &self.replica_lifecycle {
            let Some(owned) = self.live.get(branch) else {
                continue;
            };
            assert_eq!(
                self.replica_store.get(branch).copied(),
                Some(owned.lsm),
                "the replica holds every named branch at its owner's revision"
            );
        }
        let lifecycle = self.named(&self.replica_lifecycle);
        assert_eq!(
            self.replica.plan(&lifecycle),
            Vec::new(),
            "a caught-up replica plans nothing"
        );
    }
}

#[test]
fn bolero_a_replica_catches_up_every_named_branch_with_its_owner() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            let mut property = CatchUpProperty::new();
            property.run(Operation::decode(bytes));
        });
}
