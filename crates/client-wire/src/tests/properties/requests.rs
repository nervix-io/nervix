//! Generated requests keep every field and the exact prepared batch bytes.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use std::collections::BTreeSet;

use meticulous::ResultExt as _;
use nervix_models::{
    ClientConsumerLimits, ClientProducerLimits, ImpactPlanningBasis, TransactionInspectionTarget,
    TransactionPosition, TransactionPreviewIdentity,
};

use super::WireValues;
use crate::{
    ChoiceLookupRequest, ChoiceSelection, ChoiceValue, ClientMessage, ClientRequest, ConsumerId,
    EmitterBatchDecision, ProducerId, SessionLimits, SuggestRequest,
    tests::{fixtures::round_trip_client, samples::client_messages},
    wire,
};

impl WireValues<'_> {
    pub(super) fn requests(&mut self) -> Vec<ClientMessage> {
        let mut messages = client_messages();
        for message in &mut messages {
            message.request_id = self.request_id();
            match &mut message.request {
                ClientRequest::Command(command) => {
                    command.query = self.arbitrary.string();
                    command.domain = if self.arbitrary.entropy().flag() {
                        Some(self.arbitrary.name())
                    } else {
                        None
                    };
                    command.execution_reference = self.reference();
                    command.expected_transaction_position = if self.arbitrary.entropy().flag() {
                        Some(TransactionPosition::new(
                            self.arbitrary.entropy().count(usize::MAX),
                        ))
                    } else {
                        None
                    };
                    command.expected_preview = if self.arbitrary.entropy().flag() {
                        Some(TransactionPreviewIdentity {
                            transaction_id: self.arbitrary.string(),
                            position: TransactionPosition::new(
                                self.arbitrary.entropy().count(usize::MAX),
                            ),
                            planning_basis: ImpactPlanningBasis::new(self.digest()),
                        })
                    } else {
                        None
                    };
                }
                ClientRequest::Suggest(suggest) => {
                    let input = self.arbitrary.string();
                    let boundaries: Vec<usize> = input
                        .char_indices()
                        .map(|(position, _)| position)
                        .chain(std::iter::once(input.len()))
                        .collect();
                    let index = self.arbitrary.entropy().count(boundaries.len() - 1);
                    let domain = if self.arbitrary.entropy().flag() {
                        Some(self.arbitrary.name())
                    } else {
                        None
                    };
                    let size = u16::try_from(self.arbitrary.entropy().between(1..=100))
                        .verified("a bounded page");
                    let continuation = if self.arbitrary.entropy().flag() {
                        Some(self.arbitrary.string())
                    } else {
                        None
                    };
                    *suggest = SuggestRequest::new(input, boundaries[index], domain)
                        .verified("cursor chosen from character boundaries")
                        .with_page(size, continuation)
                        .verified("the page has 1..=100 candidates");
                }
                ClientRequest::Choice(choice) => {
                    let target = crate::choice::ALL_CHOICE_TARGETS[self
                        .arbitrary
                        .entropy()
                        .count(crate::choice::ALL_CHOICE_TARGETS.len() - 1)];
                    let selected = vec![
                        ChoiceSelection {
                            value: ChoiceValue::Domain(self.arbitrary.name()),
                        },
                        ChoiceSelection {
                            value: ChoiceValue::Field(self.arbitrary.name()),
                        },
                    ];
                    let size = u16::try_from(self.arbitrary.entropy().between(1..=100))
                        .verified("a bounded page");
                    let continuation = if self.arbitrary.entropy().flag() {
                        Some(self.arbitrary.string())
                    } else {
                        None
                    };
                    *choice = ChoiceLookupRequest::new(target, selected, self.arbitrary.string())
                        .with_page(size, continuation)
                        .verified("the page has 1..=100 choices");
                }
                ClientRequest::ListDomains => {}
                ClientRequest::SelectDomain(select) => select.domain = self.arbitrary.name(),
                ClientRequest::AttachTransaction(attach) => {
                    attach.transaction_id = self.arbitrary.string()
                }
                ClientRequest::InspectTransaction(inspect) => {
                    inspect.target = if self.arbitrary.entropy().flag() {
                        TransactionInspectionTarget::Transaction {
                            transaction_id: self.arbitrary.string(),
                        }
                    } else {
                        TransactionInspectionTarget::Attached
                    };
                }
                ClientRequest::Subscribe(subscribe) => {
                    subscribe.domain = self.arbitrary.name();
                    subscribe.statement = self.arbitrary.string();
                }
                ClientRequest::Unsubscribe(unsubscribe) => {
                    unsubscribe.subscription = self.arbitrary.name()
                }
                ClientRequest::Cancel(cancel) => cancel.target = self.request_id(),
                ClientRequest::AttachDomainClock(attach) => attach.domain = self.arbitrary.name(),
                ClientRequest::DetachDomainClock(detach) => detach.domain = self.arbitrary.name(),
                ClientRequest::OpenIngestor(open) => {
                    open.domain = self.arbitrary.name();
                    open.ingestor = self.arbitrary.name();
                    open.expected_fields = self.arbitrary.schema_fields(1);
                    open.limits = ClientProducerLimits {
                        batches: self.count32(),
                        bytes: self.arbitrary.positive_u64(),
                    };
                }
                ClientRequest::SubmitBatch(submit) => {
                    submit.producer = ProducerId::opened_by(self.request_id());
                    submit.batch = self.bytes(1);
                }
                ClientRequest::CloseIngestor(close) => {
                    close.producer = ProducerId::opened_by(self.request_id())
                }
                ClientRequest::OpenEmitter(open) => {
                    open.domain = self.arbitrary.name();
                    open.emitter = self.arbitrary.name();
                    open.expected_fields = self.arbitrary.schema_fields(1);
                    open.limits = ClientConsumerLimits {
                        batches: self.count32(),
                        bytes: self.arbitrary.positive_u64(),
                    };
                }
                ClientRequest::ReadEmitterBatch(read) => {
                    read.consumer = ConsumerId::opened_by(self.request_id())
                }
                ClientRequest::SettleEmitterBatch(settle) => {
                    settle.consumer = ConsumerId::opened_by(self.request_id());
                    settle.reference = uuid::Uuid::from_bytes(self.digest());
                    let reason = self.arbitrary.string();
                    settle.decision = self.arbitrary.entropy().pick([
                        EmitterBatchDecision::Ack,
                        EmitterBatchDecision::Retry,
                        EmitterBatchDecision::Reject(reason),
                    ]);
                }
                ClientRequest::CloseEmitter(close) => {
                    close.consumer = ConsumerId::opened_by(self.request_id())
                }
            }
        }
        messages
    }
}

pub(super) fn check(bytes: &[u8]) -> BTreeSet<wire::ClientRequest> {
    let mut covered = BTreeSet::new();
    for original in WireValues::new(bytes).requests() {
        let encoded = original
            .encode(&SessionLimits::DEFAULT)
            .assured("bounded valid request fits");
        let prepared = encoded.bytes().clone();
        let frame = encoded
            .verify(&SessionLimits::DEFAULT)
            .assured("typed request verifies");
        covered.insert(frame.root().request_type());
        assert_eq!(frame.request_id(), Some(original.request_id));
        let decoded = ClientMessage::decode(&frame)
            .assured("valid generated requests never skip decode failures");
        drop(frame);
        assert_eq!(decoded, original);
        assert_eq!(
            decoded
                .encode(&SessionLimits::DEFAULT)
                .assured("same request fits")
                .bytes(),
            &prepared
        );
        assert_eq!(round_trip_client(&decoded), original);
    }
    covered
}

#[test]
fn bolero_client_requests_keep_complete_values() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            check(bytes);
        });
}

#[test]
fn request_generator_covers_the_current_schema() {
    let covered = check(&[0xff; 2048]);
    let mut declared: BTreeSet<_> = wire::ClientRequest::ENUM_VALUES.iter().copied().collect();
    declared.remove(&wire::ClientRequest::NONE);
    assert_eq!(covered, declared);
    for seed in [0, 1, 2, 3, 4, 5, 0x80] {
        check(&[seed; 2048]);
    }
}
