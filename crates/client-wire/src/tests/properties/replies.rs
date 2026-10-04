//! Complete replies, including optional metadata, every disposition and ordered diagnostics.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use std::collections::BTreeSet;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{DomainPace, TransactionInspection, TransactionLifecycle};

use super::WireValues;
use crate::{
    tests::{
        fixtures::round_trip_reply,
        samples::{
            command_dispositions, command_outcome, impact_report, transaction, transaction_states,
        },
    },
    *,
};

impl WireValues<'_> {
    pub(super) fn reply_bodies(&mut self) -> Vec<ReplyBody> {
        let mut bodies = Vec::new();
        for disposition in command_dispositions() {
            let mut command = command_outcome(disposition);
            command.execution_reference = self.reference();
            command.origin = self
                .arbitrary
                .entropy()
                .pick([OutcomeOrigin::Executed, OutcomeOrigin::Recovered]);
            command.message = self.arbitrary.string();
            command.diagnostics = self.diagnostics();
            for statement in &mut command.statements {
                statement.message = self.arbitrary.string();
                statement.diagnostics = self.diagnostics();
            }
            if self.arbitrary.entropy().flag() {
                command.statements.clear();
            }
            let states = transaction_states();
            let chosen = self.arbitrary.entropy().count(states.len() - 1);
            command.transaction = if self.arbitrary.entropy().flag() {
                Some(transaction(states[chosen].clone()))
            } else {
                None
            };
            if self.arbitrary.entropy().flag() {
                command.transaction_admission = None;
            }
            self.command_metadata(&mut command);
            bodies.push(ReplyBody::Command(Box::new(command)));
        }
        for state in transaction_states() {
            for disposition in [
                AttachDisposition::Attached(transaction(state.clone())),
                AttachDisposition::AlreadyFinished(transaction(state)),
            ] {
                bodies.push(ReplyBody::Attach(AttachOutcome {
                    disposition,
                    message: self.arbitrary.string(),
                    diagnostics: self.diagnostics(),
                }));
            }
        }
        for disposition in [
            AttachDisposition::Failed,
            AttachDisposition::NotLeader(self.redirect()),
        ] {
            bodies.push(ReplyBody::Attach(AttachOutcome {
                disposition,
                message: self.arbitrary.string(),
                diagnostics: self.diagnostics(),
            }));
        }
        let mut suggestions = Vec::new();
        for &kind in crate::reply::ALL_SUGGESTION_KINDS {
            suggestions.push(Suggestion {
                value: self.arbitrary.string(),
                kind,
                edit: TextEdit {
                    start: 0,
                    end: u32::from(self.arbitrary.entropy().byte()),
                    replacement: self.arbitrary.string(),
                },
            });
        }
        for &status in crate::reply::ALL_SUGGESTION_STATUSES {
            bodies.push(ReplyBody::Suggest(SuggestOutcome {
                status,
                continuation: if self.arbitrary.entropy().flag() {
                    Some(self.arbitrary.string())
                } else {
                    None
                },
                suggestions: suggestions.clone(),
            }));
        }
        let values = [
            ChoiceValue::DomainPace(DomainPaceChoice::Unpaced),
            ChoiceValue::DomainPace(DomainPaceChoice::Paced),
            ChoiceValue::PlacementPolicy(nervix_models::PlacementPolicy::Neutral),
            ChoiceValue::Domain(self.arbitrary.name()),
            ChoiceValue::Resource(self.arbitrary.name()),
            ChoiceValue::ResourceVersion(self.arbitrary.requested_version()),
            ChoiceValue::Model(nervix_models::NodeRef::new(
                nervix_models::ModelKind::Relay,
                self.arbitrary.name::<nervix_models::ModelName>(),
            )),
            ChoiceValue::Field(self.arbitrary.name()),
        ];
        let mut choices = Vec::new();
        for value in values {
            choices.push(Choice {
                value,
                presentation: ChoicePresentation {
                    label: self.arbitrary.string(),
                    detail: if self.arbitrary.entropy().flag() {
                        Some(self.arbitrary.string())
                    } else {
                        None
                    },
                    group: if self.arbitrary.entropy().flag() {
                        Some(self.arbitrary.string())
                    } else {
                        None
                    },
                },
            });
        }
        for &status in crate::choice::ALL_CHOICE_STATUSES {
            bodies.push(ReplyBody::Choice(ChoiceOutcome {
                status,
                choices: choices.clone(),
                page_cursor: if self.arbitrary.entropy().flag() {
                    Some(self.arbitrary.string())
                } else {
                    None
                },
            }));
        }
        let mut domains = Vec::new();
        for status in crate::domain::ALL_DOMAIN_STATUSES {
            let pace = if self.arbitrary.entropy().flag() {
                DomainPace::Paced {
                    period: self.arbitrary.clock_period(),
                    skew: self.arbitrary.clock_skew(),
                }
            } else {
                DomainPace::Unpaced
            };
            domains.push(DomainInfo {
                status: status.clone(),
                domain: self.arbitrary.name(),
                pace,
            });
        }
        bodies.push(ReplyBody::DomainList(DomainList { domains }));
        bodies.push(ReplyBody::DomainList(DomainList {
            domains: Vec::new(),
        }));
        bodies.push(ReplyBody::DomainSelection(DomainSelection::Selected(
            self.arbitrary.name(),
        )));
        bodies.push(ReplyBody::DomainSelection(DomainSelection::NotFound(
            self.arbitrary.name(),
        )));
        bodies.push(ReplyBody::Inspection(InspectionOutcome::Inspected(
            Box::new(TransactionInspection {
                transaction: transaction(TransactionLifecycle::Committed),
                operation: None,
                report: impact_report(),
            }),
        )));
        for &rejection in crate::reply::ALL_INSPECTION_REJECTIONS {
            bodies.push(ReplyBody::Inspection(InspectionOutcome::Rejected {
                rejection,
                message: self.arbitrary.string(),
            }));
        }
        bodies.push(ReplyBody::Inspection(InspectionOutcome::NotLeader(
            self.redirect(),
        )));
        for branched in [false, true] {
            let fields = self.arbitrary.schema_fields(0);
            let branch = if branched {
                Some(
                    RowBranch::new(self.arbitrary.name(), self.arbitrary.schema_fields(1))
                        .assured("a nonempty branch"),
                )
            } else {
                None
            };
            bodies.push(ReplyBody::Subscribe(SubscribeOutcome {
                disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                    subscription: self.subscription(),
                    domain: self.arbitrary.name(),
                    relay: self.arbitrary.name(),
                    subscription_type: SubscriptionType::Row,
                    schema: RowSchema { fields, branch },
                })),
                message: self.arbitrary.string(),
                diagnostics: self.diagnostics(),
            }));
        }
        bodies.push(ReplyBody::Subscribe(SubscribeOutcome {
            disposition: SubscribeDisposition::Failed,
            message: self.arbitrary.string(),
            diagnostics: self.diagnostics(),
        }));
        for disposition in [
            UnsubscribeDisposition::Deleted(self.subscription()),
            UnsubscribeDisposition::Failed,
        ] {
            bodies.push(ReplyBody::Unsubscribe(UnsubscribeOutcome {
                disposition,
                message: self.arbitrary.string(),
                diagnostics: self.diagnostics(),
            }));
        }
        for &state in crate::reply::ALL_CANCEL_STATES {
            bodies.push(ReplyBody::Cancel(CancelOutcome {
                target: self.request_id(),
                state,
            }));
        }
        for &stage in crate::reply::ALL_CANCELLATION_STAGES {
            bodies.push(ReplyBody::Cancelled(RequestCancelled { stage }));
        }
        for &rejection in crate::reply::ALL_REQUEST_REJECTIONS {
            bodies.push(ReplyBody::Rejected(RequestRejected {
                rejection,
                field: if self.arbitrary.entropy().flag() {
                    Some(self.arbitrary.string())
                } else {
                    None
                },
                message: self.arbitrary.string(),
            }));
        }
        for state in 0..4 {
            let clock = self.clock(state);
            bodies.push(ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::Attached {
                    domain: self.arbitrary.name(),
                    clock,
                },
                message: self.arbitrary.string(),
            }));
        }
        for disposition in [
            DomainClockAttachDisposition::AlreadyAttached(self.arbitrary.name()),
            DomainClockAttachDisposition::DomainNotFound(self.arbitrary.name()),
            DomainClockAttachDisposition::Failed,
        ] {
            bodies.push(ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
                disposition,
                message: self.arbitrary.string(),
            }));
        }
        for disposition in [
            DomainClockDetachDisposition::Detached(self.arbitrary.name()),
            DomainClockDetachDisposition::NotAttached(self.arbitrary.name()),
            DomainClockDetachDisposition::Failed,
        ] {
            bodies.push(ReplyBody::DomainClockDetach(DomainClockDetachOutcome {
                disposition,
                message: self.arbitrary.string(),
            }));
        }
        bodies.extend(self.io_replies());
        bodies
    }
}

pub(super) fn check(bytes: &[u8]) -> BTreeSet<wire::ReplyBody> {
    let mut values = WireValues::new(bytes);
    let bodies = values.reply_bodies();
    let mut covered = BTreeSet::new();
    for body in bodies {
        let original = Reply {
            request_id: values.request_id(),
            body,
        };
        let ReplyDelivery::Frame(encoded) = original
            .encode(&SessionLimits::DEFAULT)
            .assured("bounded reply fits")
        else {
            panic!("a bounded generated reply fits one default frame");
        };
        let frame = encoded
            .verify(&SessionLimits::DEFAULT)
            .assured("encoded reply verifies");
        covered.insert(
            frame
                .root()
                .body_as_reply()
                .verified("a typed reply root")
                .body_type(),
        );
        assert_eq!(frame.request_id(), Some(original.request_id));
        let ServerMessage::Reply(decoded) =
            ServerMessage::decode(&frame).assured("valid replies never skip failures")
        else {
            panic!("a reply remains a reply");
        };
        drop(frame);
        assert_eq!(decoded, original);
        assert_eq!(round_trip_reply(&decoded), original);
    }
    covered
}

#[test]
fn bolero_replies_keep_complete_values() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            check(bytes);
        });
}

#[test]
fn reply_generator_covers_the_current_schema() {
    let covered = check(&[0xff; 4096]);
    let mut declared: BTreeSet<_> = wire::ReplyBody::ENUM_VALUES.iter().copied().collect();
    declared.remove(&wire::ReplyBody::NONE);
    // TransferPart is exercised through Reply::encode and TransferAssembly in streams.
    declared.remove(&wire::ReplyBody::TransferPart);
    assert_eq!(covered, declared);
    for seed in [0, 1, 2, 3, 4, 5, 0x80] {
        check(&[seed; 4096]);
    }
}
