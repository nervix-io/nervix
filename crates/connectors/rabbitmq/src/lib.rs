//! RabbitMQ source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The AMQP connection and channel a client configures, queue declaration,
//!   the consumer and prefetch window a source reads through, AMQP headers in both
//!   directions, per-delivery acknowledgement and requeue, publisher confirms, the channel round
//!   trip that completes a `NO_ACK` write, returned-message classification, and the broker's
//!   refusal of a message above its `max_message_size`.
//! - **Depends on.** The connector contract, vocabulary values, the node resolver, `error-stack`,
//!   Tokio and `lapin`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.
//!
//! # A message the broker refuses
//!
//! RabbitMQ's `max_message_size` is a broker setting that AMQP 0-9-1 never tells a client, so the
//! sink cannot measure a message against it before writing. The broker refuses a larger body by
//! closing the channel the message arrived on, and it discards every message written on that
//! channel after the refused one. The `channel_closure` module reads the broker's reason for every
//! channel the sink loses. When the reason is that refusal, the first message of the write the
//! broker had not answered whose body exceeds the limit it names is the one it refused: the sink
//! rejects it, settles the messages before it, and writes the messages after it again on a new
//! channel of the same connection, which the refusal left open. A message the broker answered
//! first stays answered, and an unanswered message written before the refused one leaves the write
//! to the host's retry, because it may have reached its queue.
//!
//! The sink does not remember the limit a refusal named, and checks no later message against it:
//! the broker's setting may change, and a message is rejected only on the broker's own answer.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

mod channel_closure;
mod connection;
mod source;

use std::{cmp::Ordering, collections::VecDeque, time::Duration};

use async_trait::async_trait;
use channel_closure::{ChannelClosure, ChannelClosures};
use connection::RabbitMqBroker;
pub use connection::RabbitMqConnectError;
use error_stack::Report;
use futures_util::FutureExt;
use lapin::{
    Confirmation, PublisherConfirm,
    message::BasicReturnMessage,
    options::{BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
};
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, PerRecordOutcome, RecordSink, RejectedSinkRecord,
    SinkHost, SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordId,
    SinkStartError, SinkStartResult,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, QueueName};
pub use source::{
    RabbitMqDeliveryHeaders, RabbitMqSource, RabbitMqSourceError, RabbitMqSourceMessage,
    RabbitMqSourcePlan, RabbitMqSourcePosition,
};
use thiserror::Error;
use tokio::time::{Instant, sleep};

const RABBITMQ: &str = "rabbitmq";

/// What one RabbitMQ sink publishes through: its client entries, the node resolver its broker host
/// resolves through, its queue and its publishing mode.
pub struct RabbitMqSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
    pub queue: QueueName,
    pub mode: BrokerPublishingMode,
}

pub struct RabbitMqSink {
    /// The connection the sink's channels run on. The broker's refusal of a message closes only
    /// the channel, so the sink opens the next channel on the same connection.
    connection: lapin::Connection,
    /// Why the broker closed each channel the sink loses. Its event stream is `Send` but not
    /// `Sync`, so the methods that await while they borrow the sink borrow it mutably.
    closures: ChannelClosures,
    channel: lapin::Channel,
    queue: QueueName,
    mode: BrokerPublishingMode,
}

/// Why the broker refused one message the sink wrote.
#[derive(Debug, Error)]
enum RabbitMqRecordError {
    #[error(
        "message body of {body_bytes} bytes exceeds the broker's max_message_size of {limit} bytes"
    )]
    AboveMaxMessageSize { body_bytes: usize, limit: usize },
}

/// One message written on a channel in confirm mode whose publisher confirm the sink has not read.
struct PendingRabbitMqConfirmation {
    /// The whole record, because the broker may discard the message and it is then written again.
    record: SinkRecord,
    deadline: Instant,
    confirmation: PublisherConfirm,
}

/// A message a confirmed write had on its channel when the channel was lost, with the broker's
/// answer for it when the broker answered first.
struct AnsweredMessage {
    record: SinkRecord,
    answer: Option<Confirmation>,
}

/// What the broker's publisher confirm settles for the message it answers.
enum ConfirmedAnswer {
    Delivered,
    Rejected(RejectedSinkRecord<SinkRecordId>),
    /// The answer fails the write: a nack, a return that does not reject the message itself, or a
    /// confirm from a channel that is not in confirm mode.
    Failed(Report<SinkPublishError>),
}

/// Why a confirmed write stopped writing on its channel.
enum ConfirmedStop {
    /// The write failed for a reason other than a lost channel: a nack, a return that does not
    /// reject the message itself, or `ACK TIMEOUT`. What the broker answered meanwhile is settled.
    Failed(Report<SinkPublishError>),
    /// The channel is gone, as the failure the sink saw shows. The messages written on it stay in
    /// the confirmation window.
    ChannelLost(lapin::Error),
}

impl RabbitMqSink {
    pub async fn new(config: RabbitMqSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let mode = config.mode;
        let broker =
            RabbitMqBroker::from_config(&config.config, config.dns).map_err(Self::connect_error)?;
        let connection = broker.connect().await.map_err(Self::connect_error)?;
        let closures = ChannelClosures::listen(&connection);
        let channel = Self::open_channel(&connection, mode)
            .await
            .map_err(Self::start_error)?;
        channel
            .queue_declare(
                config.queue.as_str().into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(Self::start_error)?;
        Ok(Self {
            connection,
            closures,
            channel,
            queue: config.queue,
            mode,
        })
    }

    /// A new channel on `connection`, in confirm mode for `MODE ACK`.
    async fn open_channel(
        connection: &lapin::Connection,
        mode: BrokerPublishingMode,
    ) -> lapin::Result<lapin::Channel> {
        let channel = connection.create_channel().await?;
        if let BrokerPublishingMode::Ack { .. } = mode {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await?;
        }
        Ok(channel)
    }

    /// Replaces the channel the broker closed with a new one on the same connection.
    async fn replace_channel(&mut self) -> SinkPublishResult<()> {
        let channel = Self::open_channel(&self.connection, self.mode)
            .await
            .map_err(Self::publish_error)?;
        self.channel = channel;
        Ok(())
    }

    fn properties(headers: &[(String, String)]) -> lapin::BasicProperties {
        if headers.is_empty() {
            lapin::BasicProperties::default()
        } else {
            let mut table = FieldTable::default();
            for (name, value) in headers {
                table.insert(
                    name.as_str().into(),
                    AMQPValue::LongString(value.as_str().into()),
                );
            }
            lapin::BasicProperties::default().with_headers(table)
        }
    }

    async fn publish_message(&mut self, record: &SinkRecord) -> lapin::Result<PublisherConfirm> {
        self.channel
            .basic_publish(
                "".into(),
                self.queue.as_str().into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..Default::default()
                },
                &record.payload,
                Self::properties(&record.headers),
            )
            .await
    }

    /// `MODE NO_ACK`: the channel is not in confirm mode, so the broker answers no message on its
    /// own. Once the messages are written, the sink asks the channel for one round trip, which the
    /// broker answers only after it has taken every message written before the request, and that
    /// answer delivers them. A write therefore costs one round trip, however many messages it
    /// carries. A channel lost on the way is settled by [`Self::settle_unconfirmed_loss`].
    async fn publish_unconfirmed(
        &mut self,
        mut unsent: VecDeque<SinkRecord>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        loop {
            tokio::task::consume_budget().await;
            let mut written = Vec::with_capacity(unsent.len());
            let observed = match self.write_unconfirmed(&mut unsent, &mut written).await {
                Ok(()) => {
                    for record in written {
                        outcome.deliver(record.id);
                    }
                    return;
                }
                Err(observed) => observed,
            };
            let closure = self
                .closures
                .next(&self.connection, &self.channel, observed)
                .await;
            if let Err(error) =
                Self::settle_unconfirmed_loss(closure, written, &mut unsent, outcome)
            {
                outcome.fail(error);
                return;
            }
            if let Err(error) = self.replace_channel().await {
                outcome.fail(error);
                return;
            }
        }
    }

    /// Writes `unsent` on the channel in order, moving each message it writes into `written`, and
    /// then completes the channel's round trip. A publish that fails leaves its message at the
    /// front of `unsent`; the failure is the channel's.
    async fn write_unconfirmed(
        &mut self,
        unsent: &mut VecDeque<SinkRecord>,
        written: &mut Vec<SinkRecord>,
    ) -> lapin::Result<()> {
        while let Some(record) = unsent.pop_front() {
            tokio::task::consume_budget().await;
            match self.publish_message(&record).await {
                // Outside confirm mode Lapin resolves the confirm at once, and it carries nothing.
                Ok(_not_requested) => written.push(record),
                Err(error) => {
                    unsent.push_front(record);
                    return Err(error);
                }
            }
        }
        if written.is_empty() {
            return Ok(());
        }
        self.channel.basic_qos(0, BasicQosOptions::default()).await
    }

    /// Settles the messages a `NO_ACK` write had on a channel it lost.
    ///
    /// When the broker refused a message for its size, the first message of `written` whose body
    /// exceeds the limit is that message; the broker took every message before it, which delivers
    /// them, and discarded every message after it, which return to the front of `unsent` to be
    /// written on a new channel. Any other loss leaves `written` unresolved for the host's retry,
    /// because the round trip that would have delivered them never completed.
    fn settle_unconfirmed_loss(
        closure: ChannelClosure,
        written: Vec<SinkRecord>,
        unsent: &mut VecDeque<SinkRecord>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> SinkPublishResult<()> {
        let (limit, reason) = match closure {
            ChannelClosure::MessageTooLarge { limit, reason } => (limit, reason),
            ChannelClosure::Unattributed(reason) => return Err(Self::publish_error(reason)),
        };
        // The scan walks this write's messages on the lost channel, in the order they were written.
        let Some(refused) = written
            .iter()
            .position(|record| record.payload.len() > limit)
        else {
            return Err(Self::publish_error(reason));
        };
        let mut discarded = Vec::new();
        for (index, record) in written.into_iter().enumerate() {
            match index.cmp(&refused) {
                Ordering::Less => outcome.deliver(record.id),
                Ordering::Equal => outcome.reject(Self::refused_for_size(&record, limit)),
                Ordering::Greater => discarded.push(record),
            }
        }
        for record in discarded.into_iter().rev() {
            unsent.push_front(record);
        }
        Ok(())
    }

    /// `MODE ACK`: the channel is in confirm mode, at most `max_in_flight` publisher confirms are
    /// outstanding at once, and every one is awaited before the batch finishes. A channel lost on
    /// the way is settled by [`Self::settle_confirmed_loss`].
    async fn publish_confirmed(
        &mut self,
        mut unsent: VecDeque<SinkRecord>,
        confirmation: AckConfirmation,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        loop {
            tokio::task::consume_budget().await;
            let mut pending = VecDeque::new();
            let written = self
                .write_confirmed(&mut unsent, &mut pending, confirmation, outcome)
                .await;
            let observed = match written {
                Ok(()) => return,
                Err(ConfirmedStop::Failed(error)) => {
                    outcome.fail(error);
                    return;
                }
                Err(ConfirmedStop::ChannelLost(observed)) => observed,
            };
            let closure = self
                .closures
                .next(&self.connection, &self.channel, observed)
                .await;
            let answered = Self::answered_before_loss(pending);
            if let Err(error) = Self::settle_confirmed_loss(closure, answered, &mut unsent, outcome)
            {
                outcome.fail(error);
                return;
            }
            if let Err(error) = self.replace_channel().await {
                outcome.fail(error);
                return;
            }
        }
    }

    /// Writes `unsent` on the channel in order within the confirmation window, reading each
    /// publisher confirm the window requires and every one still outstanding at the end. A
    /// publish that fails leaves its message at the front of `unsent`, and a lost channel leaves
    /// the messages written on it in `pending`. The window carries the confirmation settings, so
    /// the drain below never has to ask a mode that has no confirmations what its timeout is.
    async fn write_confirmed(
        &mut self,
        unsent: &mut VecDeque<SinkRecord>,
        pending: &mut VecDeque<PendingRabbitMqConfirmation>,
        AckConfirmation {
            max_in_flight,
            timeout,
        }: AckConfirmation,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> Result<(), ConfirmedStop> {
        while let Some(record) = unsent.pop_front() {
            tokio::task::consume_budget().await;
            let confirmation = match self.publish_message(&record).await {
                Ok(confirmation) => confirmation,
                Err(observed) => {
                    unsent.push_front(record);
                    return Err(ConfirmedStop::ChannelLost(observed));
                }
            };
            let Some(deadline) = Instant::now().checked_add(timeout) else {
                return Err(ConfirmedStop::Failed(Self::publish_error(
                    "rabbitmq ACK TIMEOUT exceeds the monotonic clock range",
                )));
            };
            pending.push_back(PendingRabbitMqConfirmation {
                record,
                deadline,
                confirmation,
            });
            if pending.len() >= max_in_flight.get() {
                Self::confirm_oldest(pending, timeout, outcome).await?;
            }
        }
        while !pending.is_empty() {
            tokio::task::consume_budget().await;
            Self::confirm_oldest(pending, timeout, outcome).await?;
        }
        Ok(())
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingRabbitMqConfirmation>,
        timeout: Duration,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> Result<(), ConfirmedStop> {
        let Some(oldest) = pending.front_mut() else {
            return Err(ConfirmedStop::Failed(Self::publish_error(
                "rabbitmq acknowledgment window unexpectedly became empty",
            )));
        };
        let remaining = oldest
            .deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(ConfirmedStop::Failed(Self::confirm_timeout_error(timeout)));
        }
        let result = tokio::select! {
            biased;
            result = &mut oldest.confirmation => Some(result),
            _ = sleep(remaining) => None,
        };
        let Some(result) = result else {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(ConfirmedStop::Failed(Self::confirm_timeout_error(timeout)));
        };
        let confirmation = match result {
            Ok(confirmation) => confirmation,
            Err(observed) => return Err(ConfirmedStop::ChannelLost(observed)),
        };
        let answer = Self::confirmed_answer(&oldest.record, confirmation);
        match answer {
            ConfirmedAnswer::Delivered => {
                let delivered = pending.pop_front().verified(
                    "the oldest confirmation was read from the front of this window, which \
                     nothing else removes from",
                );
                outcome.deliver(delivered.record.id);
                Ok(())
            }
            ConfirmedAnswer::Rejected(rejected) => {
                pending.pop_front();
                outcome.reject(rejected);
                Ok(())
            }
            ConfirmedAnswer::Failed(error) => {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(ConfirmedStop::Failed(error))
            }
        }
    }

    /// Collects the records behind the oldest one whose confirmation already resolved.
    ///
    /// The caller reached here because the oldest record failed or timed out, and it is about to
    /// return that failure for the whole publish. Records behind it that already succeeded or were
    /// individually rejected are recorded so the retry does not send them again. Anything else is
    /// deliberately left in neither list: its failure is the same infrastructure failure the
    /// caller is returning, and classifying it per record would report one outage many times.
    fn harvest_ready_after_oldest_failure(
        pending: &mut VecDeque<PendingRabbitMqConfirmation>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        let mut index = 1;
        while index < pending.len() {
            let ready = pending
                .get_mut(index)
                .and_then(|confirmation| (&mut confirmation.confirmation).now_or_never());
            let Some(result) = ready else {
                index += 1;
                continue;
            };
            let settled = pending.remove(index).verified(
                "the index came from scanning this same pending window, which nothing else \
                 removes from",
            );
            let Ok(confirmation) = result else {
                continue;
            };
            match Self::confirmed_answer(&settled.record, confirmation) {
                ConfirmedAnswer::Delivered => outcome.deliver(settled.record.id),
                ConfirmedAnswer::Rejected(rejected) => outcome.reject(rejected),
                ConfirmedAnswer::Failed(_) => {}
            }
        }
    }

    /// The messages a confirmed write had on the channel it lost, in the order they were written,
    /// each with the answer the broker gave for it first.
    ///
    /// Lapin resolves every confirm still outstanding when the broker closes a channel, one the
    /// broker never answered with the loss, so once the loss is known each confirm holds the
    /// broker's answer or that failure. The oldest message's confirm is spent when it is what
    /// reported the loss, and Lapin answers a second read of a spent confirm with
    /// `FutureCompleted`, which counts as no answer too.
    fn answered_before_loss(
        pending: VecDeque<PendingRabbitMqConfirmation>,
    ) -> Vec<AnsweredMessage> {
        let mut answered = Vec::with_capacity(pending.len());
        for PendingRabbitMqConfirmation {
            record,
            confirmation,
            ..
        } in pending
        {
            let answer = match confirmation.now_or_never() {
                Some(Ok(confirmation)) => Some(confirmation),
                Some(Err(_)) | None => None,
            };
            answered.push(AnsweredMessage { record, answer });
        }
        answered
    }

    /// Settles the messages a confirmed write had on a channel it lost.
    ///
    /// Each answer the broker gave before the loss settles its message as it would have on an open
    /// channel. When the broker refused a message for its size, the first message it left
    /// unanswered whose body exceeds the limit is that message, and it is rejected. The broker
    /// discarded every message after it, and when it answered every message before it, those
    /// return to the front of `unsent` to be written on a new channel. An unanswered message before
    /// the refused one may have reached its queue all the same, as it can on a quorum queue, and a
    /// failing answer fails the write, so either leaves the write to the host's retry, as does any
    /// other loss.
    fn settle_confirmed_loss(
        closure: ChannelClosure,
        answered: Vec<AnsweredMessage>,
        unsent: &mut VecDeque<SinkRecord>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> SinkPublishResult<()> {
        let (limit, reason) = match closure {
            ChannelClosure::MessageTooLarge { limit, reason } => (limit, reason),
            ChannelClosure::Unattributed(reason) => {
                Self::settle_answered(answered, outcome);
                return Err(Self::publish_error(reason));
            }
        };
        // The scan walks this write's messages on the lost channel, in the order they were written.
        let Some(refused) = answered
            .iter()
            .position(|message| message.answer.is_none() && message.record.payload.len() > limit)
        else {
            Self::settle_answered(answered, outcome);
            return Err(Self::publish_error(reason));
        };
        let mut failure = None;
        let mut discarded = Vec::new();
        for (index, AnsweredMessage { record, answer }) in answered.into_iter().enumerate() {
            let Some(confirmation) = answer else {
                match index.cmp(&refused) {
                    Ordering::Less => {
                        failure.get_or_insert_with(|| {
                            Self::publish_error(
                                "rabbitmq closed the channel before confirming a message written \
                                 ahead of the one it refused",
                            )
                        });
                    }
                    Ordering::Equal => outcome.reject(Self::refused_for_size(&record, limit)),
                    Ordering::Greater => discarded.push(record),
                }
                continue;
            };
            match Self::confirmed_answer(&record, confirmation) {
                ConfirmedAnswer::Delivered => outcome.deliver(record.id),
                ConfirmedAnswer::Rejected(rejected) => outcome.reject(rejected),
                ConfirmedAnswer::Failed(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error.attach_printable(reason.to_string()));
        }
        for record in discarded.into_iter().rev() {
            unsent.push_front(record);
        }
        Ok(())
    }

    /// Settles every message the broker answered before a loss that leaves the rest unresolved.
    fn settle_answered(
        answered: Vec<AnsweredMessage>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        for AnsweredMessage { record, answer } in answered {
            let Some(confirmation) = answer else {
                continue;
            };
            match Self::confirmed_answer(&record, confirmation) {
                ConfirmedAnswer::Delivered => outcome.deliver(record.id),
                ConfirmedAnswer::Rejected(rejected) => outcome.reject(rejected),
                ConfirmedAnswer::Failed(_) => {}
            }
        }
    }

    /// What the broker's publisher confirm for `record` settles.
    fn confirmed_answer(record: &SinkRecord, confirmation: Confirmation) -> ConfirmedAnswer {
        match confirmation {
            Confirmation::Ack(None) => ConfirmedAnswer::Delivered,
            Confirmation::Ack(Some(returned)) | Confirmation::Nack(Some(returned))
                if Self::is_returned_record_rejection(&returned) =>
            {
                ConfirmedAnswer::Rejected(record.rejected(Self::returned_message_reason(&returned)))
            }
            Confirmation::Ack(Some(returned)) => {
                ConfirmedAnswer::Failed(Self::returned_message_error(&returned))
            }
            Confirmation::Nack(_) => ConfirmedAnswer::Failed(Self::publish_error(
                "rabbitmq publisher confirm returned nack",
            )),
            Confirmation::NotRequested => ConfirmedAnswer::Failed(Self::publish_error(
                "rabbitmq publisher confirms were not enabled",
            )),
        }
    }

    /// `record` rejected because the broker refused its body as larger than `limit`, its
    /// `max_message_size`.
    fn refused_for_size(record: &SinkRecord, limit: usize) -> RejectedSinkRecord<SinkRecordId> {
        let error = RabbitMqRecordError::AboveMaxMessageSize {
            body_bytes: record.payload.len(),
            limit,
        };
        record.rejected(format!("rabbitmq rejected record: {error}"))
    }

    fn is_returned_record_rejection(returned: &BasicReturnMessage) -> bool {
        returned.reply_code == 311
    }

    fn returned_message_reason(returned: &BasicReturnMessage) -> String {
        format!(
            "rabbitmq returned message with reply code {}: {}",
            returned.reply_code, returned.reply_text
        )
    }

    fn returned_message_error(returned: &BasicReturnMessage) -> Report<SinkPublishError> {
        Self::publish_error(Self::returned_message_reason(returned))
    }

    fn confirm_timeout_error(timeout: Duration) -> Report<SinkPublishError> {
        Self::publish_error(format!(
            "rabbitmq publisher confirm exceeded ACK TIMEOUT {}",
            humantime::format_duration(timeout)
        ))
    }

    /// A failed connection as a start failure: the client's configuration when it is at fault,
    /// otherwise the sink's initialization, which the host retries. The connection failure is kept
    /// as the typed cause and its message leads the report.
    fn connect_error(report: Report<RabbitMqConnectError>) -> Report<SinkStartError> {
        let context = if report.current_context().is_configuration() {
            SinkStartError::InvalidConfiguration { sink: RABBITMQ }
        } else {
            SinkStartError::Initialize { sink: RABBITMQ }
        };
        let message = report.current_context().to_string();
        report.change_context(context).attach_printable(message)
    }

    fn start_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::Initialize { sink: RABBITMQ })
            .attach_printable(error.to_string())
    }

    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: RABBITMQ })
            .attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for RabbitMqSink {}

#[async_trait]
impl RecordSink for RabbitMqSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let unsent = VecDeque::from(records);
        match self.mode {
            BrokerPublishingMode::NoAck => {
                self.publish_unconfirmed(unsent, &mut outcome).await;
            }
            BrokerPublishingMode::Ack(confirmation) => {
                self.publish_confirmed(unsent, confirmation, &mut outcome)
                    .await;
            }
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use lapin::{ChannelState, ErrorKind, message::Delivery};
    use meticulous::ResultExt as _;
    use nervix_dns::DnsLookupFailure;
    use nervix_models::Timestamp;

    use super::*;

    /// The `max_message_size` every refusal below names.
    const LIMIT: usize = 1000;

    fn record(index: usize, body_bytes: usize) -> SinkRecord {
        SinkRecord::new(
            SinkRecordId::new(index),
            None,
            vec![b'x'; body_bytes],
            Vec::new(),
            Timestamp::from_unix_nanos(0),
        )
    }

    fn answered(index: usize, body_bytes: usize, answer: Option<Confirmation>) -> AnsweredMessage {
        AnsweredMessage {
            record: record(index, body_bytes),
            answer,
        }
    }

    fn lost_channel() -> lapin::Error {
        lapin::Error::from(ErrorKind::InvalidChannelState(
            ChannelState::Closed,
            "basic.publish",
        ))
    }

    fn size_refusal() -> ChannelClosure {
        ChannelClosure::MessageTooLarge {
            limit: LIMIT,
            reason: lost_channel(),
        }
    }

    fn ids(records: &VecDeque<SinkRecord>) -> Vec<usize> {
        records.iter().map(|record| record.id.index()).collect()
    }

    /// What `outcome` delivered and rejected, by record index, with each rejection's message.
    fn settled(outcome: PerRecordOutcome<SinkRecordId>) -> (Vec<usize>, Vec<(usize, String)>) {
        let parts = outcome.into_parts();
        assert!(parts.infrastructure_error.is_none());
        let delivered = parts.delivered.iter().map(|id| id.index()).collect();
        let rejected = parts
            .rejected
            .into_iter()
            .map(|rejected| (rejected.id.index(), rejected.error.message))
            .collect();
        (delivered, rejected)
    }

    fn too_large(body_bytes: usize) -> String {
        format!(
            "rabbitmq rejected record: message body of {body_bytes} bytes exceeds the broker's \
             max_message_size of {LIMIT} bytes"
        )
    }

    #[test]
    fn an_unconfirmed_refusal_delivers_what_came_before_and_writes_the_rest_again() {
        let written = vec![
            record(0, 10),
            record(1, 2000),
            record(2, 10),
            record(3, 5000),
        ];
        let mut unsent = VecDeque::from([record(4, 10)]);
        let mut outcome = PerRecordOutcome::empty();

        RabbitMqSink::settle_unconfirmed_loss(size_refusal(), written, &mut unsent, &mut outcome)
            .assured("the refused message is among the written ones");

        assert_eq!(ids(&unsent), vec![2, 3, 4]);
        assert_eq!(settled(outcome), (vec![0], vec![(1, too_large(2000))]));
    }

    #[test]
    fn an_unconfirmed_loss_no_refusal_explains_leaves_the_write_unresolved() {
        for closure in [
            ChannelClosure::Unattributed(lost_channel()),
            // Every written body fits the limit the refusal names, so none of them is the one the
            // broker refused.
            size_refusal(),
        ] {
            let written = vec![record(0, 10), record(1, LIMIT)];
            let mut unsent = VecDeque::from([record(2, 10)]);
            let mut outcome = PerRecordOutcome::empty();

            let settled_loss =
                RabbitMqSink::settle_unconfirmed_loss(closure, written, &mut unsent, &mut outcome);

            assert!(settled_loss.is_err());
            assert_eq!(ids(&unsent), vec![2]);
            assert_eq!(settled(outcome), (Vec::new(), Vec::new()));
        }
    }

    #[test]
    fn a_confirmed_refusal_keeps_earlier_answers_and_writes_the_discarded_messages_again() {
        let answered = vec![
            answered(0, 10, Some(Confirmation::Ack(None))),
            answered(
                1,
                10,
                Some(Confirmation::Ack(Some(returned_message(
                    311,
                    "CONTENT_TOO_LARGE",
                )))),
            ),
            answered(2, 2000, None),
            answered(3, 10, None),
        ];
        let mut unsent = VecDeque::from([record(4, 10)]);
        let mut outcome = PerRecordOutcome::empty();

        RabbitMqSink::settle_confirmed_loss(size_refusal(), answered, &mut unsent, &mut outcome)
            .assured("every message ahead of the refused one was answered");

        assert_eq!(ids(&unsent), vec![3, 4]);
        assert_eq!(
            settled(outcome),
            (
                vec![0],
                vec![
                    (
                        1,
                        "rabbitmq returned message with reply code 311: CONTENT_TOO_LARGE"
                            .to_string()
                    ),
                    (2, too_large(2000)),
                ]
            )
        );
    }

    #[test]
    fn a_confirmed_refusal_behind_an_open_question_leaves_the_rest_to_the_retry() {
        for ahead in [
            // The broker may have queued it and lost only its confirm, as a quorum queue can.
            None,
            // A nack fails the write wherever it stands.
            Some(Confirmation::Nack(None)),
        ] {
            let answered = vec![
                answered(0, 10, ahead),
                answered(1, 2000, None),
                answered(2, 10, None),
            ];
            let mut unsent = VecDeque::from([record(3, 10)]);
            let mut outcome = PerRecordOutcome::empty();

            let settled_loss = RabbitMqSink::settle_confirmed_loss(
                size_refusal(),
                answered,
                &mut unsent,
                &mut outcome,
            );

            assert!(settled_loss.is_err());
            assert_eq!(ids(&unsent), vec![3]);
            assert_eq!(settled(outcome), (Vec::new(), vec![(1, too_large(2000))]));
        }
    }

    #[test]
    fn a_confirmed_loss_no_refusal_explains_settles_only_the_answers() {
        for closure in [
            ChannelClosure::Unattributed(lost_channel()),
            // The one body above the limit was confirmed, so the broker did not refuse it.
            size_refusal(),
        ] {
            let answered = vec![
                answered(0, 2000, Some(Confirmation::Ack(None))),
                answered(1, 10, None),
            ];
            let mut unsent = VecDeque::from([record(2, 10)]);
            let mut outcome = PerRecordOutcome::empty();

            let settled_loss =
                RabbitMqSink::settle_confirmed_loss(closure, answered, &mut unsent, &mut outcome);

            assert!(settled_loss.is_err());
            assert_eq!(ids(&unsent), vec![2]);
            assert_eq!(settled(outcome), (vec![0], Vec::new()));
        }
    }

    #[test]
    fn publisher_confirms_deliver_reject_or_fail_their_message() {
        let message = record(0, 10);
        let delivered = RabbitMqSink::confirmed_answer(&message, Confirmation::Ack(None));
        assert!(matches!(delivered, ConfirmedAnswer::Delivered));
        for rejection in [
            Confirmation::Ack(Some(returned_message(311, "CONTENT_TOO_LARGE"))),
            Confirmation::Nack(Some(returned_message(311, "CONTENT_TOO_LARGE"))),
        ] {
            let answer = RabbitMqSink::confirmed_answer(&message, rejection);
            assert!(matches!(answer, ConfirmedAnswer::Rejected(_)));
        }
        for failure in [
            Confirmation::Ack(Some(returned_message(312, "NO_ROUTE"))),
            Confirmation::Nack(None),
            Confirmation::NotRequested,
        ] {
            let answer = RabbitMqSink::confirmed_answer(&message, failure);
            assert!(matches!(answer, ConfirmedAnswer::Failed(_)));
        }
    }

    #[test]
    fn connection_failures_start_as_configuration_or_initialization_failures() {
        let invalid =
            RabbitMqSink::connect_error(Report::new(RabbitMqConnectError::InvalidCaCertificate));
        assert!(matches!(
            invalid.current_context(),
            SinkStartError::InvalidConfiguration { sink: RABBITMQ }
        ));

        let unresolved = RabbitMqSink::connect_error(Report::new(RabbitMqConnectError::Resolve {
            host: "rabbitmq.nervix.test".to_string(),
            failure: DnsLookupFailure::NameNotFound,
        }));
        assert!(matches!(
            unresolved.current_context(),
            SinkStartError::Initialize { sink: RABBITMQ }
        ));
        let leading = unresolved
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            leading.map(String::as_str),
            Some("resolving RabbitMQ host 'rabbitmq.nervix.test' failed: the name does not exist")
        );
        assert!(unresolved.downcast_ref::<RabbitMqConnectError>().is_some());
    }

    fn returned_message(reply_code: u16, reply_text: &str) -> BasicReturnMessage {
        BasicReturnMessage {
            delivery: Delivery::mock(0, "".into(), "notifications".into(), false, Vec::new()),
            reply_code,
            reply_text: reply_text.into(),
        }
    }

    #[test]
    fn content_too_large_return_is_a_record_rejection() {
        assert!(RabbitMqSink::is_returned_record_rejection(
            &returned_message(311, "CONTENT_TOO_LARGE")
        ));
    }

    #[test]
    fn no_route_return_remains_an_infrastructure_failure() {
        assert!(!RabbitMqSink::is_returned_record_rejection(
            &returned_message(312, "NO_ROUTE")
        ));
    }
}
