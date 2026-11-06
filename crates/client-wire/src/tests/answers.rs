//! Which replies answer which requests: the outcome each request is served with, the rejection that
//! can refuse any of them, and the cancellation that ends every request a cancellation can reach.

use strum::IntoEnumIterator as _;

use crate::{ClientRequestKind, ReplyBodyKind};

/// The session contract's requests and the outcome each one is answered with, written out
/// independently of the mapping under test.
const OUTCOMES: [(ClientRequestKind, ReplyBodyKind); 19] = [
    (ClientRequestKind::Command, ReplyBodyKind::Command),
    (ClientRequestKind::Suggest, ReplyBodyKind::Suggest),
    (ClientRequestKind::Choice, ReplyBodyKind::Choice),
    (ClientRequestKind::ListDomains, ReplyBodyKind::DomainList),
    (
        ClientRequestKind::SelectDomain,
        ReplyBodyKind::DomainSelection,
    ),
    (ClientRequestKind::AttachTransaction, ReplyBodyKind::Attach),
    (
        ClientRequestKind::InspectTransaction,
        ReplyBodyKind::Inspection,
    ),
    (ClientRequestKind::Subscribe, ReplyBodyKind::Subscribe),
    (ClientRequestKind::Unsubscribe, ReplyBodyKind::Unsubscribe),
    (ClientRequestKind::Cancel, ReplyBodyKind::Cancel),
    (
        ClientRequestKind::AttachDomainClock,
        ReplyBodyKind::DomainClockAttach,
    ),
    (
        ClientRequestKind::DetachDomainClock,
        ReplyBodyKind::DomainClockDetach,
    ),
    (ClientRequestKind::OpenIngestor, ReplyBodyKind::OpenIngestor),
    (ClientRequestKind::SubmitBatch, ReplyBodyKind::Submission),
    (
        ClientRequestKind::CloseIngestor,
        ReplyBodyKind::CloseIngestor,
    ),
    (ClientRequestKind::OpenEmitter, ReplyBodyKind::OpenEmitter),
    (
        ClientRequestKind::ReadEmitterBatch,
        ReplyBodyKind::ReadEmitterBatch,
    ),
    (
        ClientRequestKind::SettleEmitterBatch,
        ReplyBodyKind::SettleEmitterBatch,
    ),
    (ClientRequestKind::CloseEmitter, ReplyBodyKind::CloseEmitter),
];

/// The requests no `RequestCancelled` ever ends: a cancellation is answered at once, and a
/// submitted batch cannot be withdrawn.
const UNCANCELLABLE: [ClientRequestKind; 2] =
    [ClientRequestKind::Cancel, ClientRequestKind::SubmitBatch];

#[test]
fn the_table_names_every_request_and_gives_every_outcome_to_exactly_one_of_them() {
    let requests: Vec<ClientRequestKind> = OUTCOMES.iter().map(|(request, _)| *request).collect();
    let every_request: Vec<ClientRequestKind> = ClientRequestKind::iter().collect();
    assert_eq!(
        requests, every_request,
        "the table names every request, in the order the contract declares them"
    );

    // Both tables are fixed: every reply kind is compared with the nineteen requests' outcomes.
    for reply in ReplyBodyKind::iter() {
        let mut answered = Vec::new();
        for (request, outcome) in OUTCOMES {
            if outcome == reply {
                answered.push(request);
            }
        }
        let shared = matches!(reply, ReplyBodyKind::Cancelled | ReplyBodyKind::Rejected);
        let owners = if shared { 0 } else { 1 };
        assert_eq!(
            answered.len(),
            owners,
            "a {reply} reply is the outcome of {answered:?}; only a cancellation and a rejection \
             are no request's own outcome"
        );
    }
}

#[test]
fn a_request_is_answered_by_its_outcome_a_rejection_or_a_cancellation_that_can_reach_it() {
    for (request, outcome) in OUTCOMES {
        assert_eq!(request.outcome(), outcome, "the outcome of {request:?}");
        let cancellable = !UNCANCELLABLE.contains(&request);
        assert_eq!(request.is_cancellable(), cancellable, "{request:?}");
        for reply in ReplyBodyKind::iter() {
            let answers = match reply {
                ReplyBodyKind::Rejected => true,
                ReplyBodyKind::Cancelled => cancellable,
                other => other == outcome,
            };
            assert_eq!(
                request.is_answered_by(reply),
                answers,
                "whether a {reply} reply answers {request:?}"
            );
        }
    }
}

#[test]
fn a_reply_kind_is_named_as_its_snake_case_variant() {
    assert_eq!(ReplyBodyKind::DomainList.to_string(), "domain_list");
    assert_eq!(ReplyBodyKind::Command.to_string(), "command");
    assert_eq!(
        <&'static str>::from(ReplyBodyKind::DomainClockAttach),
        "domain_clock_attach"
    );
}
