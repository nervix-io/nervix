//! Restored-state publication failure qualification.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: the persistence and delivery observations of one armed publication failure.
//! Depends on: the host's fault controls and vocabulary identities.
//! Must not know: restore steps, command recovery, consensus or the state store.

use meticulous::ResultExt as _;
use nervix_models::DomainName;

use super::FaultInjection;

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test domain is a valid literal name")
}

#[test]
fn a_publication_failure_fails_every_publication_of_its_domain_until_cleared() {
    let faults = FaultInjection::default();
    let restored = domain("restored");
    let unrelated = domain("unrelated");
    assert_eq!(
        faults.restore_state_publication_failure_delivered(&restored),
        None
    );
    assert!(!faults.restore_state_publication_fails(&restored));

    faults.fail_restore_state_publications(restored.clone());
    assert_eq!(
        faults.restore_state_publication_failure_delivered(&restored),
        Some(false)
    );
    assert!(faults.restore_state_publication_fails(&restored));
    assert_eq!(
        faults.restore_state_publication_failure_delivered(&restored),
        Some(true)
    );
    // A restore resumed under the same execution reference publishes again and meets the same
    // failure, so the failure stays the restore's outcome.
    assert!(faults.restore_state_publication_fails(&restored));
    assert!(!faults.restore_state_publication_fails(&unrelated));
    assert_eq!(
        faults.restore_state_publication_failure_delivered(&unrelated),
        None
    );

    faults.clear_restore_state_publication_failure(&restored);
    assert_eq!(
        faults.restore_state_publication_failure_delivered(&restored),
        None
    );
    assert!(!faults.restore_state_publication_fails(&restored));
}
