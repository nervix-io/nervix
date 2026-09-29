//! Client producer link tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that the serving side of a link counts a forwarded batch as possibly
//!   admitted only once it cleared that batch, and that losing the owning node answers every batch
//!   it never cleared as not admitted before it ends the producer.
//! - **Depends on.** The serving side's record of forwarded batches and a forwarded route's
//!   events.
//! - **Must not know.** The interconnect transport, sessions, or the owning node's endpoint.

use std::num::NonZeroU64;

use meticulous::OptionExt as _;
use nervix_models::{
    ClientProducerAdmission, ClientProducerEndReason, ClientSubmissionOutcome,
    ClientSubmissionRefusal,
};
use nervix_primitives::sync::{mpsc, watch};

use super::*;

fn submission(id: u64) -> NonZeroU64 {
    NonZeroU64::new(id).assured("a literal non-zero identity")
}

#[test]
fn only_a_batch_that_was_forwarded_and_has_no_outcome_is_cleared() {
    let mut submissions = ForwardedSubmissions::default();
    submissions.track(submission(1));
    submissions.track(submission(2));
    assert!(
        submissions.clear(submission(1)),
        "a forwarded batch is cleared"
    );
    submissions.answered(submission(2));
    assert!(
        !submissions.clear(submission(2)),
        "a batch that already has its outcome is no longer the owning node's to admit"
    );
    assert!(
        !submissions.clear(submission(3)),
        "a batch the serving node never forwarded is never cleared"
    );
    assert_eq!(
        submissions.uncleared(),
        Vec::<NonZeroU64>::new(),
        "the one batch left was cleared"
    );
}

#[test]
fn a_lost_owner_leaves_only_the_batches_it_never_cleared_not_admitted() {
    let (events, mut outcomes) = mpsc::unbounded_channel();
    let (admission, _admission) = watch::channel(ClientProducerAdmission::Open);
    let mut route = ForwardedRoute {
        events,
        admission,
        submissions: ForwardedSubmissions::default(),
    };
    for id in 1..=4 {
        route.submissions.track(submission(id));
    }
    assert!(route.submissions.clear(submission(1)));
    assert!(route.submissions.clear(submission(3)));
    route.end_with_lost_owner();

    let mut refused = Vec::new();
    let mut ended = None;
    while let Ok(event) = outcomes.try_recv() {
        assert!(
            ended.is_none(),
            "an event followed the producer's end: {event:?}"
        );
        match event {
            ClientProducerEvent::Outcome {
                submission,
                outcome,
                detail,
            } => {
                assert_eq!(
                    outcome,
                    ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::ProducerEnded),
                    "a batch the owning node could not have admitted was not admitted"
                );
                assert!(detail.is_some(), "a refusal after a lost owner says why");
                refused.push(submission.get().get());
            }
            ClientProducerEvent::Ended(reason) => ended = Some(reason),
        }
    }
    assert_eq!(
        refused,
        vec![2, 4],
        "only the batches never cleared are refused, in submission order"
    );
    assert_eq!(
        ended,
        Some(ClientProducerEndReason::OwnerLost),
        "the producer ends as lost with its owner, after the refusals"
    );
}
