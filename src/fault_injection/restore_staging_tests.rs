//! Restored checkpoint staging failure qualification.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: the persistence, scope and delivery observations of armed staging failures.
//! Depends on: the host's fault controls and vocabulary identities.
//! Must not know: restore steps, command recovery, consensus or the state store.

use meticulous::ResultExt as _;
use nervix_models::DomainName;

use super::{FaultInjection, RestoreStagingKind};

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test domain is a valid literal name")
}

#[test]
fn a_staging_failure_fails_every_staging_of_its_domain_and_kind_until_cleared() {
    let faults = FaultInjection::default();
    let restored = domain("restored");
    let unrelated = domain("unrelated");
    let kind = RestoreStagingKind::Materialized;
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(&restored, kind),
        None
    );
    assert!(!faults.restore_checkpoint_staging_fails(&restored, kind));

    faults.fail_restore_checkpoint_stagings(restored.clone(), kind);
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(&restored, kind),
        Some(false)
    );
    assert!(faults.restore_checkpoint_staging_fails(&restored, kind));
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(&restored, kind),
        Some(true)
    );
    // A restore resumed under the same execution reference stages again and meets the same
    // failure, so the failure stays the restore's outcome.
    assert!(faults.restore_checkpoint_staging_fails(&restored, kind));
    assert!(!faults.restore_checkpoint_staging_fails(&restored, RestoreStagingKind::Guest));
    assert!(!faults.restore_checkpoint_staging_fails(&restored, RestoreStagingKind::BranchState));
    assert!(!faults.restore_checkpoint_staging_fails(&unrelated, kind));
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(&unrelated, kind),
        None
    );

    faults.clear_restore_checkpoint_staging_failure(&restored, kind);
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(&restored, kind),
        None
    );
    assert!(!faults.restore_checkpoint_staging_fails(&restored, kind));
}

#[test]
fn each_staging_kind_of_one_domain_is_armed_delivered_and_cleared_on_its_own() {
    let faults = FaultInjection::default();
    let restored = domain("restored");
    faults.fail_restore_checkpoint_stagings(restored.clone(), RestoreStagingKind::Guest);
    faults.fail_restore_checkpoint_stagings(restored.clone(), RestoreStagingKind::BranchState);

    assert!(faults.restore_checkpoint_staging_fails(&restored, RestoreStagingKind::Guest));
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(&restored, RestoreStagingKind::Guest),
        Some(true)
    );
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(
            &restored,
            RestoreStagingKind::BranchState
        ),
        Some(false)
    );
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(
            &restored,
            RestoreStagingKind::Materialized
        ),
        None
    );

    faults.clear_restore_checkpoint_staging_failure(&restored, RestoreStagingKind::Guest);
    assert!(!faults.restore_checkpoint_staging_fails(&restored, RestoreStagingKind::Guest));
    assert!(faults.restore_checkpoint_staging_fails(&restored, RestoreStagingKind::BranchState));
}

#[test]
fn staging_and_durable_publication_failures_never_consume_each_other() {
    let faults = FaultInjection::default();
    let restored = domain("restored");
    faults.fail_restore_checkpoint_stagings(restored.clone(), RestoreStagingKind::Materialized);
    assert!(!faults.restore_state_publication_fails(&restored));
    assert_eq!(
        faults.restore_state_publication_failure_delivered(&restored),
        None
    );

    faults.fail_restore_state_publications(restored.clone());
    assert!(faults.restore_state_publication_fails(&restored));
    assert_eq!(
        faults.restore_checkpoint_staging_failure_delivered(
            &restored,
            RestoreStagingKind::Materialized
        ),
        Some(false)
    );

    faults.clear_restore_state_publication_failure(&restored);
    assert!(faults.restore_checkpoint_staging_fails(&restored, RestoreStagingKind::Materialized));
}
