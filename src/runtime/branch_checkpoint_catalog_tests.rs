//! Layer: test harness.
//! Owns: the owner's catalog of an entity's branch checkpoints: registration, revisions, removals,
//! cursors and pages.
//! May depend on: the catalog and typed branch keys.
//! Must not know: replicas, the interconnect, or how a checkpoint is stored.

use nervix_models::SchemaFingerprint;

use super::*;
use crate::runtime::test_fixtures::string_branch_key;

const EPOCH: u64 = 11;

fn page_of(entries: usize) -> NonZeroUsize {
    NonZeroUsize::new(entries).expect("a test page lists at least one change")
}

fn state() -> RuntimeState {
    RuntimeState::Deduplicator {
        schema: SchemaFingerprint::from_digest([7; 32]),
    }
}

fn tenant(name: &str) -> Option<BranchKey> {
    string_branch_key("tenant", name)
}

fn cataloged(branch: &Option<BranchKey>, lsm: u64) -> CatalogedCheckpoint {
    CatalogedCheckpoint {
        branch: branch.clone(),
        state: state(),
        lsm,
    }
}

fn cursor(sequence: u64) -> BranchCheckpointCursor {
    BranchCheckpointCursor {
        epoch: EPOCH,
        sequence,
    }
}

/// The page a listing carries, and whether the listing restarted.
fn page(listing: CheckpointListing) -> (bool, CheckpointPage) {
    match listing {
        CheckpointListing::Restarted(page) => (true, page),
        CheckpointListing::Continued(page) => (false, page),
    }
}

#[test]
fn a_replica_without_a_cursor_learns_every_checkpoint_in_the_order_it_changed() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let acme = tenant("acme");
    let beta = tenant("beta");
    let acme_entry = catalog.register(acme.clone(), state(), 4);
    let _beta_entry = catalog.register(beta.clone(), state(), 2);
    acme_entry.record(5);

    let (restarted, listed) = page(catalog.changes_after(None, page_of(16)));

    assert!(restarted);
    assert_eq!(
        listed,
        CheckpointPage {
            cursor: cursor(3),
            removed: Vec::new(),
            revised: vec![cataloged(&beta, 2), cataloged(&acme, 5)],
            more: false,
        }
    );
}

#[test]
fn a_current_cursor_lists_nothing() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let _entry = catalog.register(tenant("acme"), state(), 1);
    let (_, first) = page(catalog.changes_after(None, page_of(16)));

    let (restarted, listed) = page(catalog.changes_after(Some(first.cursor), page_of(16)));

    assert!(!restarted);
    assert_eq!(
        listed,
        CheckpointPage {
            cursor: first.cursor,
            removed: Vec::new(),
            revised: Vec::new(),
            more: false,
        }
    );
}

#[test]
fn a_cursor_lists_only_the_branches_that_changed_after_it() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let acme = tenant("acme");
    let beta = tenant("beta");
    let acme_entry = catalog.register(acme.clone(), state(), 1);
    let _beta_entry = catalog.register(beta, state(), 1);
    let (_, first) = page(catalog.changes_after(None, page_of(16)));

    acme_entry.record(2);
    acme_entry.record(3);
    let (restarted, listed) = page(catalog.changes_after(Some(first.cursor), page_of(16)));

    assert!(!restarted);
    assert_eq!(
        listed,
        CheckpointPage {
            cursor: cursor(4),
            removed: Vec::new(),
            revised: vec![cataloged(&acme, 3)],
            more: false,
        }
    );
}

#[test]
fn a_revision_that_is_not_newer_changes_nothing() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let entry = catalog.register(tenant("acme"), state(), 5);
    let (_, first) = page(catalog.changes_after(None, page_of(16)));

    entry.record(5);
    entry.record(4);

    let (_, listed) = page(catalog.changes_after(Some(first.cursor), page_of(16)));
    assert_eq!(listed.cursor, first.cursor);
    assert!(listed.revised.is_empty());
}

#[test]
fn a_dropped_branch_state_is_listed_as_removed() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let acme = tenant("acme");
    let entry = catalog.register(acme.clone(), state(), 1);
    let (_, first) = page(catalog.changes_after(None, page_of(16)));

    drop(entry);

    let (restarted, listed) = page(catalog.changes_after(Some(first.cursor), page_of(16)));
    assert!(!restarted);
    assert_eq!(listed.removed, vec![acme]);
    assert!(listed.revised.is_empty());
    let (_, from_scratch) = page(catalog.changes_after(None, page_of(16)));
    assert!(from_scratch.revised.is_empty());
}

#[test]
fn a_replaced_registration_neither_records_nor_removes_its_successors_entry() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let acme = tenant("acme");
    let replaced = catalog.register(acme.clone(), state(), 3);
    let successor = catalog.register(acme.clone(), state(), 1);

    replaced.record(9);
    drop(replaced);
    let (_, listed) = page(catalog.changes_after(None, page_of(16)));
    assert_eq!(listed.revised, vec![cataloged(&acme, 1)]);

    successor.record(2);
    let (_, listed) = page(catalog.changes_after(None, page_of(16)));
    assert_eq!(listed.revised, vec![cataloged(&acme, 2)]);
}

#[test]
fn a_branch_removed_and_registered_again_is_listed_removed_then_revised() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let acme = tenant("acme");
    let first_entry = catalog.register(acme.clone(), state(), 1);
    let (_, first) = page(catalog.changes_after(None, page_of(16)));

    drop(first_entry);
    let _second_entry = catalog.register(acme.clone(), state(), 7);

    let (_, listed) = page(catalog.changes_after(Some(first.cursor), page_of(16)));
    assert_eq!(listed.removed, vec![acme.clone()]);
    assert_eq!(listed.revised, vec![cataloged(&acme, 7)]);
}

#[test]
fn a_cursor_of_another_catalog_restarts_the_listing() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let acme = tenant("acme");
    let _entry = catalog.register(acme.clone(), state(), 1);
    let foreign = BranchCheckpointCursor {
        epoch: EPOCH + 1,
        sequence: 1,
    };

    let (restarted, listed) = page(catalog.changes_after(Some(foreign), page_of(16)));

    assert!(restarted);
    assert_eq!(listed.revised, vec![cataloged(&acme, 1)]);
}

#[test]
fn a_cursor_ahead_of_the_catalog_restarts_the_listing() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let _entry = catalog.register(tenant("acme"), state(), 1);

    let (restarted, _) = page(catalog.changes_after(Some(cursor(9)), page_of(16)));

    assert!(restarted);
}

#[test]
fn a_cursor_older_than_a_discarded_removal_restarts_the_listing() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let survivor = tenant("survivor");
    let _survivor_entry = catalog.register(survivor.clone(), state(), 1);
    let (_, first) = page(catalog.changes_after(None, page_of(16)));
    for index in 0..=RETAINED_REMOVALS {
        let entry = catalog.register(tenant(&format!("churn-{index}")), state(), 1);
        drop(entry);
    }

    let (restarted, listed) = page(catalog.changes_after(Some(first.cursor), page_of(16)));

    assert!(restarted);
    assert_eq!(listed.revised, vec![cataloged(&survivor, 1)]);
    assert!(listed.removed.is_empty());
}

#[test]
fn pages_follow_each_other_until_every_change_is_listed() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let mut entries = Vec::new();
    for index in 0..5 {
        entries.push(catalog.register(tenant(&format!("tenant-{index}")), state(), 1));
    }

    let (restarted, first) = page(catalog.changes_after(None, page_of(2)));
    assert!(restarted);
    assert_eq!(first.revised.len(), 2);
    assert!(first.more);
    assert_eq!(first.cursor, cursor(2));

    let (restarted, second) = page(catalog.changes_after(Some(first.cursor), page_of(2)));
    assert!(!restarted);
    assert_eq!(second.revised.len(), 2);
    assert!(second.more);

    let (_, third) = page(catalog.changes_after(Some(second.cursor), page_of(2)));
    assert_eq!(third.revised.len(), 1);
    assert!(!third.more);
    assert_eq!(third.cursor, cursor(5));
}

#[test]
fn a_page_interleaves_removals_and_revisions_in_the_order_they_happened() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let acme = tenant("acme");
    let beta = tenant("beta");
    let gamma = tenant("gamma");
    let acme_entry = catalog.register(acme.clone(), state(), 1);
    let beta_entry = catalog.register(beta.clone(), state(), 1);
    let (_, first) = page(catalog.changes_after(None, page_of(16)));

    drop(beta_entry);
    acme_entry.record(2);
    let _gamma_entry = catalog.register(gamma.clone(), state(), 1);

    let (_, removal_only) = page(catalog.changes_after(Some(first.cursor), page_of(1)));
    assert_eq!(removal_only.removed, vec![beta]);
    assert!(removal_only.revised.is_empty());
    assert!(removal_only.more);
    let (_, rest) = page(catalog.changes_after(Some(removal_only.cursor), page_of(16)));
    assert!(rest.removed.is_empty());
    assert_eq!(
        rest.revised,
        vec![cataloged(&acme, 2), cataloged(&gamma, 1)]
    );
    assert!(!rest.more);
}

#[test]
fn unbranched_work_is_catalogued_under_the_absent_branch() {
    let catalog = BranchCheckpointCatalog::with_epoch(EPOCH);
    let _entry = catalog.register(None, state(), 3);

    let (_, listed) = page(catalog.changes_after(None, page_of(16)));

    assert_eq!(listed.revised, vec![cataloged(&None, 3)]);
}
