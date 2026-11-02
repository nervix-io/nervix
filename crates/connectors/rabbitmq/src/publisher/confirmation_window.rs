//! A bounded RabbitMQ publisher confirmation window and its returned-message attribution.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The driver futures of one window, their individual deadlines, and attribution of
//!   every return before any positive mandatory answer is reported delivered.
//! - **Depends on.** The publisher's messages/outcomes, the connector's confirmation settings,
//!   primitive timers, and the opaque Lapin confirmation contract.
//! - **Must not know.** Graph state, retry scheduling, another window, or the driver's locks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the publisher confirmation boundary")
)]

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
};

use error_stack::Report;
use futures_util::FutureExt as _;
use lapin::{Confirmation, message::BasicReturnMessage, types::ShortString};
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, EmitterRecordId, EmitterWriteError, EmitterWriteResult, PerRecordOutcome,
};
use nervix_models::ConnectorKind;
use nervix_primitives::{
    task::unconstrained,
    time::{Instant, timeout_at},
};
use nervix_recovery::Discarded as _;
use thiserror::Error;

use super::{AnsweredMessage, ConfirmedStop, RABBITMQ, RabbitMqMessage, RabbitMqPublisher};

#[cfg(all(test, not(feature = "shuttle")))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "ordinary confirmation-owner tests")
)]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "modeled confirmation-owner tests")
)]
mod shuttle_tests;

struct PendingConfirmation<A> {
    message: RabbitMqMessage,
    deadline: Instant,
    confirmation: A,
}

pub(super) struct RabbitMqConfirmationWindow<A> {
    settings: AckConfirmation,
    pending: VecDeque<PendingConfirmation<A>>,
    answered: Vec<AnsweredMessage>,
}

enum WindowEnd {
    Answered(Vec<AnsweredMessage>),
    TimedOut(Vec<AnsweredMessage>),
    ChannelLost {
        source: lapin::Error,
        answered: Vec<AnsweredMessage>,
    },
}

/// Whether every positive mandatory answer has a decidable per-record outcome.
pub(super) enum PositiveAnswerAttribution {
    Decided,
    Incomplete,
}

impl PositiveAnswerAttribution {
    pub(super) fn require_decided(self) -> EmitterWriteResult<()> {
        match self {
            Self::Decided => Ok(()),
            Self::Incomplete => Err(RabbitMqConfirmationError::IncompleteAttribution.report()),
        }
    }
}

impl<A> RabbitMqConfirmationWindow<A>
where
    A: Future<Output = lapin::Result<Confirmation>> + Unpin,
{
    pub(super) fn new(settings: AckConfirmation) -> Self {
        Self {
            settings,
            pending: VecDeque::new(),
            answered: Vec::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn is_full(&self) -> bool {
        self.pending.len() == self.settings.max_in_flight.get()
    }

    pub(super) fn push(&mut self, message: RabbitMqMessage, deadline: Instant, confirmation: A) {
        assert!(
            !self.is_full(),
            "the publisher settles a full window before handing over another message"
        );
        self.pending.push_back(PendingConfirmation {
            message,
            deadline,
            confirmation,
        });
    }

    pub(super) fn channel_lost(self, source: lapin::Error) -> ConfirmedStop {
        ConfirmedStop::ChannelLost {
            observed: source,
            answered: self.into_answered(),
        }
    }

    /// Polls every remaining future once without waiting. The driver's budget must not hide an
    /// answer already available when a failure ends this window.
    pub(super) fn into_answered(mut self) -> Vec<AnsweredMessage> {
        for PendingConfirmation {
            message,
            confirmation,
            ..
        } in self.pending
        {
            let answer = match unconstrained(confirmation).now_or_never() {
                Some(result) => match result {
                    Ok(answer) => Some(answer),
                    failed @ Err(_) => {
                        failed.discarded(
                            "the write reports the failure that ends its window; this record's \
                             outcome remains unknown for retry",
                        );
                        None
                    }
                },
                None => None,
            };
            self.answered.push(AnsweredMessage { message, answer });
        }
        self.answered
    }

    async fn read(mut self) -> WindowEnd {
        while let Some(PendingConfirmation {
            message,
            deadline,
            mut confirmation,
        }) = self.pending.pop_front()
        {
            nervix_primitives::task::consume_budget().await;
            let result = timeout_at(deadline, &mut confirmation).await;
            match result {
                Ok(Ok(answer)) => self.answered.push(AnsweredMessage {
                    message,
                    answer: Some(answer),
                }),
                Ok(Err(source)) => {
                    self.answered.push(AnsweredMessage {
                        message,
                        answer: None,
                    });
                    return WindowEnd::ChannelLost {
                        source,
                        answered: self.into_answered(),
                    };
                }
                Err(_) => {
                    self.pending.push_front(PendingConfirmation {
                        message,
                        deadline,
                        confirmation,
                    });
                    return WindowEnd::TimedOut(self.into_answered());
                }
            }
        }
        WindowEnd::Answered(self.answered)
    }

    /// An individual confirm cannot prove the absence of a return: another future can carry it.
    /// Complete the bounded set before attributing its returns and refilling the window.
    pub(super) async fn settle(
        self,
        exchange: &str,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) -> Result<(), ConfirmedStop> {
        let timeout = self.settings.timeout;
        match self.read().await {
            WindowEnd::Answered(answered) => {
                let attribution = RabbitMqPublisher::settle_answered(exchange, &answered, outcome)
                    .map_err(ConfirmedStop::Failed)?;
                attribution.require_decided().map_err(ConfirmedStop::Failed)
            }
            WindowEnd::TimedOut(answered) => {
                RabbitMqPublisher::settle_answered(exchange, &answered, outcome)
                    .map_err(ConfirmedStop::Failed)?;
                Err(ConfirmedStop::Failed(
                    RabbitMqPublisher::confirm_timeout_error(timeout),
                ))
            }
            WindowEnd::ChannelLost { source, answered } => Err(ConfirmedStop::ChannelLost {
                observed: source,
                answered,
            }),
        }
    }
}

#[derive(Debug, Error)]
enum RabbitMqConfirmationError {
    #[error(
        "{connector} returned a message this window did not publish, with reply code {reply_code}: \
         {reply_text}", connector = ConnectorKind::RabbitMq
    )]
    UnattributedReturn {
        reply_code: u16,
        reply_text: ShortString,
    },
    #[error("{connector} publisher confirm returned nack", connector = ConnectorKind::RabbitMq)]
    Nack,
    #[error("{connector} publisher confirms were not enabled", connector = ConnectorKind::RabbitMq)]
    NotRequested,
    #[error(
        "{connector} confirmation window ended before mandatory returns could be fully attributed",
        connector = ConnectorKind::RabbitMq
    )]
    IncompleteAttribution,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ReturnKey<'a> {
    routing_key: &'a str,
    body: &'a [u8],
}

struct ReturnedRecord<'a> {
    message: &'a RabbitMqMessage,
    answer: &'a Option<Confirmation>,
    returned: Option<&'a BasicReturnMessage>,
}

impl ReturnedRecord<'_> {
    fn settle(
        self,
        complete: bool,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) -> EmitterWriteResult<PositiveAnswerAttribution> {
        if let Some(returned) = self.returned {
            outcome.reject(
                self.message
                    .record
                    .rejected(RabbitMqPublisher::returned_message_reason(returned)),
            );
            return Ok(PositiveAnswerAttribution::Decided);
        }
        match self.answer {
            Some(Confirmation::Ack(_)) => {
                // A return for an acknowledged mandatory message can still be hidden in a future
                // that failed or did not resolve. Keep that record for the host's retry.
                if complete || !self.message.record.envelope.mandatory {
                    outcome.deliver(self.message.record.id);
                    Ok(PositiveAnswerAttribution::Decided)
                } else {
                    Ok(PositiveAnswerAttribution::Incomplete)
                }
            }
            Some(Confirmation::Nack(_)) => Err(RabbitMqConfirmationError::Nack.report()),
            Some(Confirmation::NotRequested) => {
                Err(RabbitMqConfirmationError::NotRequested.report())
            }
            None => Ok(PositiveAnswerAttribution::Decided),
        }
    }
}

impl RabbitMqConfirmationError {
    fn report(self) -> Report<EmitterWriteError> {
        let message = self.to_string();
        Report::new(self)
            .change_context(EmitterWriteError::Publish {
                connector: RABBITMQ,
            })
            .attach_printable(message)
    }

    fn unattributed(returned: &BasicReturnMessage) -> Report<EmitterWriteError> {
        Self::UnattributedReturn {
            reply_code: returned.reply_code,
            reply_text: returned.reply_text.clone(),
        }
        .report()
    }
}

impl RabbitMqPublisher {
    /// At most one window's messages, each settled once. Returned-message attachment and the
    /// publisher acknowledgement are independent facts; only message content attributes a return.
    pub(super) fn settle_answered(
        exchange: &str,
        answered: &[AnsweredMessage],
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) -> EmitterWriteResult<PositiveAnswerAttribution> {
        let mut records: Vec<_> = answered
            .iter()
            .map(|entry| ReturnedRecord {
                message: &entry.message,
                answer: &entry.answer,
                returned: None,
            })
            .collect();
        let mut candidates: BTreeMap<ReturnKey<'_>, VecDeque<&mut ReturnedRecord<'_>>> =
            BTreeMap::new();
        for target in &mut records {
            let message = target.message;
            if !message.record.envelope.mandatory {
                continue;
            }
            let key = ReturnKey {
                routing_key: message.routing_key.as_str(),
                body: &message.record.payload,
            };
            candidates.entry(key).or_default().push_back(target);
        }
        for entry in answered {
            let Some(Confirmation::Ack(Some(returned)) | Confirmation::Nack(Some(returned))) =
                &entry.answer
            else {
                continue;
            };
            if returned.delivery.exchange.as_str() != exchange {
                return Err(RabbitMqConfirmationError::unattributed(returned));
            }
            let key = ReturnKey {
                routing_key: returned.delivery.routing_key.as_str(),
                body: &returned.delivery.data,
            };
            let Some(bucket) = candidates.get_mut(&key) else {
                return Err(RabbitMqConfirmationError::unattributed(returned));
            };
            // Only identical routing keys and bodies share this bucket. The remaining exact
            // properties comparison is bounded by the configured confirmation window's members.
            let Some(index) = bucket
                .iter()
                .position(|target| target.message.is_returned_by(exchange, returned))
            else {
                return Err(RabbitMqConfirmationError::unattributed(returned));
            };
            let target = bucket.remove(index).verified(
                "the matching index came from this same bucket, with no intervening mutation",
            );
            target.returned = Some(returned);
        }
        drop(candidates);
        let complete = answered.iter().all(|entry| entry.answer.is_some());
        let mut failure = None;
        let mut attribution = PositiveAnswerAttribution::Decided;
        for record in records {
            match record.settle(complete, outcome) {
                Ok(PositiveAnswerAttribution::Decided) => {}
                Ok(PositiveAnswerAttribution::Incomplete) => {
                    attribution = PositiveAnswerAttribution::Incomplete;
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(attribution),
        }
    }
}
