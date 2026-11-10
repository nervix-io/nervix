//! The typed refusal a reorderer or correlator inspection reports when a unit's owner could not
//! read its page.
//!
//! Layer: test harness.
//! - **Owns.** The coordinator's projection of every owner page failure to its documented typed
//!   refusal and that refusal's public spelling, as `SHOW ... STATE` reports it and `DESCRIBE`
//!   prints it in place of a count.
//! - **Depends on.** The data plane's owner page failures and the inspection vocabulary.
//! - **Must not know.** Branch tasks, sessions, the interconnect or Row frames.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use super::*;

/// A processor whose unit task has not restored its branches yet refuses the read as `not_ready`,
/// never as an empty state, while an owner that no longer runs the unit, or a branch task that
/// could not answer, refuses it as `owner_unavailable`.
#[test]
fn an_owner_still_restoring_its_branches_refuses_the_read_as_not_ready() {
    let restoring = StateInspectionRejection::from(&ProcessorStatePageError::Unrestored);
    assert_eq!(restoring, StateInspectionRejection::NotReady);
    assert_eq!(
        restoring.as_ref(),
        "not_ready",
        "DESCRIBE prints the refusal's public spelling in place of each count"
    );

    for (error, refusal) in [
        (
            ProcessorStatePageError::Unavailable,
            StateInspectionRejection::OwnerUnavailable,
        ),
        (
            ProcessorStatePageError::Branch,
            StateInspectionRejection::OwnerUnavailable,
        ),
        (
            ProcessorStatePageError::Position,
            StateInspectionRejection::StaleContinuation,
        ),
        (
            ProcessorStatePageError::Admission,
            StateInspectionRejection::NotReady,
        ),
        (
            ProcessorStatePageError::Columns,
            StateInspectionRejection::NotReady,
        ),
    ] {
        assert_eq!(
            StateInspectionRejection::from(&error),
            refusal,
            "an owner page failure {error:?} reaches the inspection as its documented refusal"
        );
    }
}
