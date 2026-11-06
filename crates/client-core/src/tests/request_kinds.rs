//! The client's request kinds, as its errors name them, against the requests of the wire contract.
//!
//! Layer: test harness.
//! - **Owns.** The conversion from the wire contract's request kinds.
//! - **Depends on.** The wire contract's request kinds and the client's error vocabulary.
//! - **Must not know.** How requests are sent or answered.

use nervix_client_wire::{ClientRequest, ClientRequestKind};
use strum::IntoEnumIterator as _;

use crate::RequestKind;

#[test]
fn every_wire_request_kind_converts_to_the_client_request_kind_of_the_same_request() {
    // Written out independently of the conversion, in the order the wire contract declares them.
    let expected = [
        (ClientRequestKind::Command, RequestKind::Command),
        (ClientRequestKind::Suggest, RequestKind::Suggest),
        (ClientRequestKind::Choice, RequestKind::Choice),
        (ClientRequestKind::ListDomains, RequestKind::ListDomains),
        (ClientRequestKind::SelectDomain, RequestKind::SelectDomain),
        (
            ClientRequestKind::AttachTransaction,
            RequestKind::AttachTransaction,
        ),
        (
            ClientRequestKind::InspectTransaction,
            RequestKind::InspectTransaction,
        ),
        (ClientRequestKind::Subscribe, RequestKind::Subscribe),
        (ClientRequestKind::Unsubscribe, RequestKind::Unsubscribe),
        (ClientRequestKind::Cancel, RequestKind::Cancel),
        (
            ClientRequestKind::AttachDomainClock,
            RequestKind::AttachDomainClock,
        ),
        (
            ClientRequestKind::DetachDomainClock,
            RequestKind::DetachDomainClock,
        ),
        (ClientRequestKind::OpenIngestor, RequestKind::OpenIngestor),
        (ClientRequestKind::SubmitBatch, RequestKind::SubmitBatch),
        (ClientRequestKind::CloseIngestor, RequestKind::CloseIngestor),
        (ClientRequestKind::OpenEmitter, RequestKind::OpenEmitter),
        (
            ClientRequestKind::ReadEmitterBatch,
            RequestKind::ReadEmitterBatch,
        ),
        (
            ClientRequestKind::SettleEmitterBatch,
            RequestKind::SettleEmitterBatch,
        ),
        (ClientRequestKind::CloseEmitter, RequestKind::CloseEmitter),
    ];
    let listed: Vec<ClientRequestKind> = expected.iter().map(|(wire, _)| *wire).collect();
    let every_request: Vec<ClientRequestKind> = ClientRequestKind::iter().collect();
    assert_eq!(listed, every_request, "the table names every wire request");
    for (wire, client) in expected {
        assert_eq!(RequestKind::from(wire), client, "the kind of {wire:?}");
    }
    assert_eq!(
        RequestKind::from(&ClientRequest::ListDomains),
        RequestKind::ListDomains,
        "a request converts through its wire kind"
    );
}
