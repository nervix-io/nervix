//! Publishing to a RabbitMQ broker, for every emitter entity.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The channel an emitter publishes through, the check of its queue or exchange that
//!   opens it, where each message goes and under which routing key, AMQP headers on the messages it
//!   writes, mandatory publishing and the messages the broker returns, publisher confirms, the
//!   channel round trip that completes a `NO ACK` write and the client `timeout_ms` that bounds it,
//!   and the broker's refusal of a message above its `max_message_size`.
//! - **Depends on.** The connector contract, vocabulary values, the node resolver, `error-stack`,
//!   Tokio and `lapin`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.
//!
//! # A message the broker refuses
//!
//! RabbitMQ's `max_message_size` is a broker setting that AMQP 0-9-1 never tells a client, so the
//! emitter cannot measure a message against it before writing. The broker refuses a larger body by
//! closing the channel the message arrived on, and it discards every message written on that
//! channel after the refused one. The `channel_closure` module reads the broker's reason for every
//! channel the emitter loses. When the reason is that refusal, the first message of the write the
//! broker had not answered whose body exceeds the limit it names is the one it refused: the emitter
//! rejects it, settles the messages before it, and writes the messages after it again on a new
//! channel of the same connection, which the refusal left open. A message the broker answered
//! first stays answered, and an unanswered message written before the refused one leaves the write
//! to the host's retry, because it may have reached its queue.
//!
//! The emitter does not remember the limit a refusal named, and checks no later message against it:
//! the broker's setting may change, and a message is rejected only on the broker's own answer.
//!
//! # A message the broker returns
//!
//! A queue emitter publishes to the default exchange under its queue's name, and an exchange
//! emitter to its exchange under the routing key the host evaluated for each record. A record whose
//! envelope carries `MANDATORY` is published mandatory, so the broker returns its message when no
//! binding routes it, instead of dropping it, and the emitter rejects the record. Under `ACK` the
//! return arrives ahead of the message's publisher confirm, but the driver can attach it to any
//! confirm. The confirmation owner attributes all returns across the bounded window before
//! reporting mandatory delivery or refilling it. Under `NO ACK` a return arrives ahead of the
//! broker's answer to the round trip that ends the write, so once that answer
//! arrives every return of the write is on the channel, and the emitter takes them all before the
//! write ends: a return never outlives the write that published its message. A return names the
//! message's exchange, routing key, properties and body, not which record it was, and the broker
//! returns messages in the order it took them, so the emitter attributes each return to the first
//! message in the window or write, in written order, that carries the same four; of
//! messages identical in all four, the earlier one is rejected. A return the emitter cannot
//! attribute fails the write instead.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

mod confirmation_window;

use std::{cmp::Ordering, collections::VecDeque, time::Duration};

use confirmation_window::RabbitMqConfirmationWindow;
use error_stack::{Report, ResultExt as _};
use lapin::{
    Confirmation, ErrorKind, ExchangeKind, PublisherConfirm,
    message::BasicReturnMessage,
    options::{
        BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
        QueueDeclareOptions,
    },
    protocol::{AMQPErrorKind, AMQPSoftError},
    types::{AMQPValue, FieldTable, MAX_SHORT_STRING_LENGTH, ShortString, ShortStringError},
};
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, ClientConfigError, ConfiguredDeadline, DeadlineElapsed,
    EmitterFinish, EmitterFinishResult, EmitterOpenError, EmitterRecord, EmitterRecordId,
    EmitterStartResult, EmitterWriteError, EmitterWriteResult, PerRecordOutcome,
    RejectedEmitterMember,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, RabbitMqExchangeName, RabbitMqQueueName};
use nervix_primitives::time::{Instant, timeout};
use thiserror::Error;

use crate::{
    channel_closure::{ChannelClosure, ChannelClosures},
    connection::{OpeningStepFailure, RabbitMqBroker, RabbitMqConnectError},
};

mod properties;
#[cfg(test)]
mod properties_tests;

const RABBITMQ: &str = "rabbitmq";

/// Where a RabbitMQ emitter's messages go.
#[derive(Clone)]
pub(crate) enum RabbitMqDestination {
    /// The broker's default exchange, which routes a message to the queue its routing key names:
    /// this queue.
    Queue(RabbitMqQueueName),
    /// A declared exchange, which routes each message by the key the host evaluated for its record.
    Exchange(RabbitMqExchangeName),
}

impl RabbitMqDestination {
    /// The exchange a message is published to: the default exchange's empty name for a queue.
    fn exchange(&self) -> &str {
        match self {
            Self::Queue(_) => "",
            Self::Exchange(exchange) => exchange.as_str(),
        }
    }

    /// `record` as the message its destination publishes: under its queue's name for the default
    /// exchange, or under the routing key the host evaluated for it for an exchange.
    fn message(&self, record: EmitterRecord) -> Result<RabbitMqMessage, Unaddressed> {
        let key = match self {
            Self::Queue(queue) => queue.as_str(),
            Self::Exchange(_) => match record.envelope.routing_key.as_deref() {
                Some(key) => key,
                None => {
                    return Err(Unaddressed::Missing(
                        Report::new(EmitterWriteError::Misconfigured {
                            connector: RABBITMQ,
                        })
                        .attach_printable(
                            "a RabbitMQ exchange emitter received a record without the routing \
                             key its ROUTING KEY computes",
                        ),
                    ));
                }
            },
        };
        let key_bytes = key.len();
        match ShortString::try_new(key) {
            Ok(routing_key) => match RabbitMqMessage::properties(&record) {
                Ok(properties) => Ok(RabbitMqMessage {
                    record,
                    routing_key,
                    properties,
                }),
                Err(error) => Err(Unaddressed::Rejected(record.rejected(format!(
                    "{} rejected record: {error:#}",
                    nervix_models::ConnectorKind::RabbitMq.to_string().to_ascii_lowercase()
                )))),
            },
            Err(source) => {
                let error = RabbitMqRecordError::RoutingKeyTooLong { key_bytes, source };
                Err(Unaddressed::Rejected(
                    record.rejected(format!("rabbitmq rejected record: {error}")),
                ))
            }
        }
    }
}

/// What one RabbitMQ emitter publishes through: its client entries, the node resolver its broker host
/// resolves through, where its messages go and its publishing mode.
pub(crate) struct RabbitMqPublisherConfig {
    pub(crate) config: Vec<ClientConfigEntry>,
    pub(crate) dns: DnsResolver,
    pub(crate) destination: RabbitMqDestination,
    pub(crate) mode: BrokerPublishingMode,
}

/// A retained publisher, including its prepared TLS broker and exact external destination.
pub(crate) struct RabbitMqPublisher {
    broker: RabbitMqBroker,
    destination: RabbitMqDestination,
    mode: BrokerPublishingMode,
    round_trip: Option<ConfiguredDeadline>,
    transport: Option<RabbitMqChannel>,
}

impl RabbitMqPublisher {
    pub(crate) fn open(config: RabbitMqPublisherConfig) -> EmitterStartResult<Self> {
        let round_trip = ConfiguredDeadline::timeout(&config.config)
            .map_err(RabbitMqChannel::configuration_error)?;
        let broker = RabbitMqBroker::from_config(&config.config, config.dns)
            .map_err(RabbitMqChannel::connect_error)?;
        Ok(Self {
            broker,
            destination: config.destination,
            mode: config.mode,
            round_trip,
            transport: None,
        })
    }

    /// An uncertain write must not share pending returns or confirmations with its retry.
    pub(crate) fn suspend(&mut self) {
        self.transport = None;
    }

    pub(crate) fn needs_resume(&self) -> bool {
        self.transport.as_ref().is_none_or(|transport| {
            !transport.connection.status().connected() || !transport.channel.status().connected()
        })
    }

    pub(crate) async fn resume(
        &mut self,
    ) -> nervix_connector::EmitterLifecycleResult<nervix_connector::EmitterResume> {
        let resuming = nervix_connector::EmitterLifecycleError::Resume {
            connector: RABBITMQ,
        };
        if self.transport.as_ref().is_some_and(|transport| {
            transport.connection.status().connected() && transport.channel.status().connected()
        }) {
            return Ok(nervix_connector::EmitterResume::Ready);
        }
        let transport = RabbitMqChannel::connect(
            &self.broker,
            self.destination.clone(),
            self.mode,
            self.round_trip,
        )
        .await
        .change_context(resuming)?;
        self.transport = Some(transport);
        Ok(nervix_connector::EmitterResume::Ready)
    }

    pub(crate) fn finish(&self) -> EmitterFinishResult {
        match &self.transport {
            Some(transport) => transport.finish(),
            None => Ok(EmitterFinish::NothingHeld),
        }
    }

    pub(crate) async fn publish(
        &mut self,
        records: Vec<EmitterRecord>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let Some(transport) = &mut self.transport else {
            let mut outcome = PerRecordOutcome::with_capacity(records.len());
            outcome.fail(Report::new(EmitterWriteError::NotInitialized {
                connector: RABBITMQ,
            }));
            return outcome;
        };
        transport.publish(records).await
    }
}

/// The connection and channel one RabbitMQ emitter publishes through, whichever entity it is.
pub(crate) struct RabbitMqChannel {
    /// The connection the emitter's channels run on. The broker's refusal of a message closes only
    /// the channel, so the emitter opens the next channel on the same connection.
    connection: lapin::Connection,
    /// Why the broker closed each channel the emitter loses. Its event stream is `Send` but not
    /// `Sync`, so the methods that await while they borrow the emitter borrow it mutably.
    closures: ChannelClosures,
    channel: lapin::Channel,
    destination: RabbitMqDestination,
    mode: BrokerPublishingMode,
    /// The client's `timeout_ms`, which bounds the round trip that ends a `NO ACK` write when the
    /// client sets it.
    round_trip: Option<ConfiguredDeadline>,
}

/// Why one record cannot reach its destination.
#[derive(Debug, Error)]
enum RabbitMqRecordError {
    /// The broker refused the message's body as larger than its `max_message_size`.
    #[error(
        "message body of {body_bytes} bytes exceeds the broker's max_message_size of {limit} bytes"
    )]
    AboveMaxMessageSize { body_bytes: usize, limit: usize },
    /// The record's routing key does not fit the short string AMQP carries a routing key in.
    #[error(
        "routing key of {key_bytes} bytes is longer than the {MAX_SHORT_STRING_LENGTH} bytes an \
         AMQP short string carries"
    )]
    RoutingKeyTooLong {
        key_bytes: usize,
        #[source]
        source: ShortStringError,
    },
}

/// One record ready for the channel, with the routing key its destination routes it by.
struct RabbitMqMessage {
    /// The whole record, because the broker may discard the message and it is then written again.
    record: EmitterRecord,
    routing_key: ShortString,
    properties: lapin::BasicProperties,
}

impl RabbitMqMessage {
    /// Whether `returned` is this message coming back unrouted from `exchange`: the broker returns
    /// a message with the exchange, routing key, properties and body it was published with.
    fn is_returned_by(&self, exchange: &str, returned: &BasicReturnMessage) -> bool {
        let delivery = &returned.delivery;
        delivery.exchange.as_str() == exchange
            && delivery.routing_key == self.routing_key
            && delivery.data == self.record.payload
            && delivery.properties == self.properties
    }
}

/// Why a record cannot be written under the routing key its destination routes it by.
enum Unaddressed {
    /// Its routing key is longer than AMQP carries, which rejects the record alone.
    Rejected(RejectedEmitterMember<EmitterRecordId>),
    /// The host handed an exchange emitter a record without the routing key its `ROUTING KEY`
    /// computes, which no write of the emitter can then be trusted with.
    Missing(Report<EmitterWriteError>),
}

/// A message a confirmed write had on its channel when the channel was lost, with the broker's
/// answer for it when the broker answered first.
struct AnsweredMessage {
    message: RabbitMqMessage,
    answer: Option<Confirmation>,
}

/// Why a `NO ACK` write stopped before the broker answered its round trip.
enum UnconfirmedStop {
    /// The channel is gone, as the failure the emitter saw shows. The messages written on it stay
    /// unresolved until the loss is settled.
    ChannelLost(lapin::Error),
    /// The broker did not answer the round trip within the client's `timeout_ms`. The messages the
    /// write put on the channel stay unresolved, for the host's retry.
    RoundTripElapsed(DeadlineElapsed),
}

/// Why a confirmed write stopped writing on its channel.
enum ConfirmedStop {
    /// The write failed for a reason other than a lost channel: a nack, a return that does not
    /// reject the message itself, or `ACK TIMEOUT`. What the broker answered meanwhile is settled.
    Failed(Report<EmitterWriteError>),
    /// The channel is gone, as the failure the emitter saw shows. The messages written on it stay in
    /// the confirmation window.
    ChannelLost {
        observed: lapin::Error,
        answered: Vec<AnsweredMessage>,
    },
}

impl RabbitMqChannel {
    /// Connects, opens the channel the emitter publishes through and checks that its queue or its
    /// exchange exists, every step within the client's `connect_timeout_ms`.
    async fn connect(
        broker: &RabbitMqBroker,
        destination: RabbitMqDestination,
        mode: BrokerPublishingMode,
        round_trip: Option<ConfiguredDeadline>,
    ) -> EmitterStartResult<Self> {
        let opening = broker.connect().await.map_err(Self::connect_error)?;
        let closures = ChannelClosures::listen(opening.connection());
        let channel = opening
            .within_budget(
                "opening the RabbitMQ publishing channel",
                Self::open_channel(opening.connection(), mode),
            )
            .await
            .map_err(|failure| {
                failure.beneath(EmitterOpenError::Initialize {
                    connector: RABBITMQ,
                })
            })?;
        match &destination {
            RabbitMqDestination::Queue(queue) => {
                opening
                    .within_budget(
                        "checking the RabbitMQ queue",
                        channel.queue_declare(
                            queue.as_str().into(),
                            QueueDeclareOptions {
                                passive: true,
                                ..Default::default()
                            },
                            FieldTable::default(),
                        ),
                    )
                    .await
                    .map_err(|failure| {
                        failure.beneath(EmitterOpenError::Initialize {
                            connector: RABBITMQ,
                        })
                    })?;
            }
            RabbitMqDestination::Exchange(exchange) => {
                // A passive declaration only looks the exchange up, so the broker reads no type.
                opening
                    .within_budget(
                        "checking the RabbitMQ exchange",
                        channel.exchange_declare(
                            exchange.as_str().into(),
                            ExchangeKind::Direct,
                            ExchangeDeclareOptions {
                                passive: true,
                                ..Default::default()
                            },
                            FieldTable::default(),
                        ),
                    )
                    .await
                    .map_err(|failure| Self::exchange_check_error(exchange, failure))?;
            }
        }
        Ok(Self {
            connection: opening.opened(),
            closures,
            channel,
            destination,
            mode,
            round_trip,
        })
    }

    /// The failed check of `exchange` as a start failure: the missing entity the contract names when
    /// the broker answers that it has no such exchange, keeping its answer as the typed cause, and
    /// otherwise the emitter's initialization. The host retries both. The missing exchange's own
    /// message leads the report.
    fn exchange_check_error(
        exchange: &RabbitMqExchangeName,
        failure: OpeningStepFailure,
    ) -> Report<EmitterOpenError> {
        let error = match failure {
            OpeningStepFailure::Failed(error) if Self::is_not_found(&error) => error,
            failure => {
                return failure.beneath(EmitterOpenError::Initialize {
                    connector: RABBITMQ,
                });
            }
        };
        let missing = EmitterOpenError::MissingExternalEntity {
            connector: RABBITMQ,
            kind: "RabbitMQ exchange",
            name: exchange.as_str().to_string(),
        };
        let message = missing.to_string();
        Report::new(error)
            .change_context(missing)
            .attach_printable(message)
    }

    /// Whether the broker answered `error` because it has no entity of the name it was asked for.
    fn is_not_found(error: &lapin::Error) -> bool {
        let ErrorKind::ProtocolError(reply) = error.kind() else {
            return false;
        };
        let AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND) = reply.kind() else {
            return false;
        };
        true
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
    async fn replace_channel(&mut self) -> EmitterWriteResult<()> {
        let channel = Self::open_channel(&self.connection, self.mode)
            .await
            .map_err(Self::publish_error)?;
        self.channel = channel;
        Ok(())
    }

    pub(crate) fn properties(headers: &[(String, String)]) -> lapin::BasicProperties {
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

    async fn publish_message(
        &mut self,
        message: &RabbitMqMessage,
    ) -> lapin::Result<PublisherConfirm> {
        self.channel
            .basic_publish(
                self.destination.exchange().into(),
                message.routing_key.clone(),
                BasicPublishOptions {
                    mandatory: message.record.envelope.mandatory,
                    ..Default::default()
                },
                &message.record.payload,
                message.properties.clone(),
            )
            .await
    }

    /// Every message the broker returned on the channel since the emitter last asked, in the order it
    /// returned them. Outside confirm mode no confirm is outstanding, so Lapin hands them over without
    /// waiting. It borrows the publisher mutably, as every method that awaits does, because the
    /// closure listener is not `Sync`.
    async fn returned_messages(&mut self) -> lapin::Result<Vec<BasicReturnMessage>> {
        self.channel.wait_for_confirms().await
    }

    /// `MODE NO ACK`: the channel is not in confirm mode, so the broker answers no message on its
    /// own. Once the messages are written, the emitter asks the channel for one round trip, which the
    /// broker answers only after it has taken every message written before the request, and that
    /// answer delivers them. A write therefore costs one round trip, however many messages it
    /// carries. The answer follows every message the broker returned of the write, which the emitter
    /// then reads and rejects. A channel lost on the way is settled by
    /// [`Self::settle_unconfirmed_loss`], and a round trip that runs out of the client's
    /// `timeout_ms` fails the write, leaving what it wrote to the host's retry, which reopens the
    /// emitter and so drops the channel with whatever it returned meanwhile.
    async fn publish_unconfirmed(
        &mut self,
        mut unsent: VecDeque<RabbitMqMessage>,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut written = Vec::with_capacity(unsent.len());
            let observed = match self.write_unconfirmed(&mut unsent, &mut written).await {
                Ok(()) => {
                    let settled = match self.returned_messages().await {
                        Ok(returned) => Self::settle_taken(
                            self.destination.exchange(),
                            written,
                            returned,
                            outcome,
                        ),
                        Err(error) => Err(Self::publish_error(error)),
                    };
                    if let Err(error) = settled {
                        outcome.fail(error);
                    }
                    return;
                }
                Err(UnconfirmedStop::RoundTripElapsed(elapsed)) => {
                    outcome.fail(Self::round_trip_error(elapsed));
                    return;
                }
                Err(UnconfirmedStop::ChannelLost(observed)) => observed,
            };
            let closure = self
                .closures
                .next(&self.connection, &self.channel, observed)
                .await;
            // The channel is gone, but what the broker returned on it before closing it is still
            // held, and only the messages it took ahead of a refusal can be among those returns.
            let returned = match self.returned_messages().await {
                Ok(returned) => returned,
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    return;
                }
            };
            if let Err(error) = Self::settle_unconfirmed_loss(
                self.destination.exchange(),
                closure,
                written,
                returned,
                &mut unsent,
                outcome,
            ) {
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
        unsent: &mut VecDeque<RabbitMqMessage>,
        written: &mut Vec<RabbitMqMessage>,
    ) -> Result<(), UnconfirmedStop> {
        while let Some(message) = unsent.pop_front() {
            nervix_primitives::task::consume_budget().await;
            match self.publish_message(&message).await {
                // Outside confirm mode Lapin resolves the confirm at once, and it carries nothing.
                Ok(_not_requested) => written.push(message),
                Err(error) => {
                    unsent.push_front(message);
                    return Err(UnconfirmedStop::ChannelLost(error));
                }
            }
        }
        if written.is_empty() {
            return Ok(());
        }
        self.round_trip().await
    }

    /// The channel's round trip that ends a `NO ACK` write, within the client's `timeout_ms` when
    /// it sets one.
    async fn round_trip(&mut self) -> Result<(), UnconfirmedStop> {
        let answered = self.channel.basic_qos(0, BasicQosOptions::default());
        Self::answered_within(self.round_trip, answered).await
    }

    /// The broker's answer to a round trip, `answered`, within `deadline` when the client sets
    /// one; a failed answer is the channel's.
    async fn answered_within(
        deadline: Option<ConfiguredDeadline>,
        answered: impl Future<Output = lapin::Result<()>>,
    ) -> Result<(), UnconfirmedStop> {
        let Some(deadline) = deadline else {
            return answered.await.map_err(UnconfirmedStop::ChannelLost);
        };
        match timeout(deadline.bound(), answered).await {
            Ok(answer) => answer.map_err(UnconfirmedStop::ChannelLost),
            Err(_) => Err(UnconfirmedStop::RoundTripElapsed(
                deadline.elapsed("the round trip that ends a RabbitMQ NO ACK write"),
            )),
        }
    }

    /// Settles the messages a `NO ACK` write had on a channel it lost.
    ///
    /// When the broker refused a message for its size, the first message of `written` whose body
    /// exceeds the limit is that message; the broker took every message before it, which delivers
    /// them but for those it `returned`, and discarded every message after it, which return to the
    /// front of `unsent` to be written on a new channel. Any other loss leaves `written` unresolved
    /// for the host's retry, because the round trip that would have delivered them never completed.
    fn settle_unconfirmed_loss(
        exchange: &str,
        closure: ChannelClosure,
        written: Vec<RabbitMqMessage>,
        returned: Vec<BasicReturnMessage>,
        unsent: &mut VecDeque<RabbitMqMessage>,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) -> EmitterWriteResult<()> {
        let (limit, reason) = match closure {
            ChannelClosure::MessageTooLarge { limit, reason } => (limit, reason),
            ChannelClosure::Unattributed(reason) => return Err(Self::publish_error(reason)),
        };
        // The scan walks this write's messages on the lost channel, in the order they were written.
        let Some(refused) = written
            .iter()
            .position(|message| message.record.payload.len() > limit)
        else {
            return Err(Self::publish_error(reason));
        };
        // The broker took every message before the refused one and discarded every message after it,
        // and the outcome settles them in the order they were written.
        let mut taken = written;
        let mut discarded = taken.split_off(refused).into_iter();
        let refused_message = discarded
            .next()
            .verified("`refused` is the position of a message of `written`");
        Self::settle_taken(exchange, taken, returned, outcome)?;
        outcome.reject(Self::refused_for_size(&refused_message.record, limit));
        for message in discarded.rev() {
            unsent.push_front(message);
        }
        Ok(())
    }

    /// Settles the messages the broker took of a `NO ACK` write, in written order, together with
    /// the messages it `returned` of them: each returned message is rejected and every other one
    /// delivered.
    ///
    /// The broker returns messages in the order it took them, so each return belongs to the first
    /// message after the previous return's that carries it. A return no message of `taken` carries
    /// fails the write and settles none of it, because the message it stands for cannot be named.
    fn settle_taken(
        exchange: &str,
        taken: Vec<RabbitMqMessage>,
        returned: Vec<BasicReturnMessage>,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) -> EmitterWriteResult<()> {
        let mut returns = returned.into_iter().peekable();
        let mut answers = Vec::with_capacity(taken.len());
        for message in &taken {
            let returned_here = match returns.peek() {
                Some(next) => message.is_returned_by(exchange, next),
                None => false,
            };
            if returned_here {
                answers.push(returns.next());
            } else {
                answers.push(None);
            }
        }
        if let Some(stray) = returns.next() {
            return Err(Self::publish_error(format!(
                "rabbitmq returned a message this write did not publish, with reply code {}: {}",
                stray.reply_code, stray.reply_text
            )));
        }
        for (message, answer) in taken.into_iter().zip(answers) {
            match answer {
                Some(returned) => outcome.reject(
                    message
                        .record
                        .rejected(Self::returned_message_reason(&returned)),
                ),
                None => outcome.deliver(message.record.id),
            }
        }
        Ok(())
    }

    /// `MODE ACK`: publish at most MAX messages together, then attribute every return across
    /// that window before refilling it. The driver can attach a return to another message's
    /// confirm, so an individual positive answer cannot settle a mandatory record on its own.
    async fn publish_confirmed(
        &mut self,
        mut unsent: VecDeque<RabbitMqMessage>,
        confirmation: AckConfirmation,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let (observed, answered) = match self
                .write_confirmed(&mut unsent, confirmation, outcome)
                .await
            {
                Ok(()) => return,
                Err(ConfirmedStop::Failed(error)) => {
                    outcome.fail(error);
                    return;
                }
                Err(ConfirmedStop::ChannelLost { observed, answered }) => (observed, answered),
            };
            let closure = self
                .closures
                .next(&self.connection, &self.channel, observed)
                .await;
            if let Err(error) = Self::settle_confirmed_loss(
                self.destination.exchange(),
                closure,
                answered,
                &mut unsent,
                outcome,
            ) {
                outcome.fail(error);
                return;
            }
            if let Err(error) = self.replace_channel().await {
                outcome.fail(error);
                return;
            }
        }
    }

    async fn write_confirmed(
        &mut self,
        unsent: &mut VecDeque<RabbitMqMessage>,
        settings: AckConfirmation,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) -> Result<(), ConfirmedStop> {
        let mut window = RabbitMqConfirmationWindow::new(settings);
        while let Some(message) = unsent.pop_front() {
            nervix_primitives::task::consume_budget().await;
            let confirmation = match self.publish_message(&message).await {
                Ok(confirmation) => confirmation,
                Err(observed) => {
                    unsent.push_front(message);
                    return Err(window.channel_lost(observed));
                }
            };
            let Some(deadline) = Instant::now().checked_add(settings.timeout) else {
                return Err(ConfirmedStop::Failed(Self::publish_error(
                    "rabbitmq ACK TIMEOUT exceeds the monotonic clock range",
                )));
            };
            window.push(message, deadline, confirmation);
            if window.is_full() {
                window.settle(self.destination.exchange(), outcome).await?;
                window = RabbitMqConfirmationWindow::new(settings);
            }
        }
        if !window.is_empty() {
            window.settle(self.destination.exchange(), outcome).await?;
        }
        Ok(())
    }

    /// Settle all attributable answers before deciding whether a broker size refusal permits
    /// the channel's discarded suffix to be written again. A mandatory answer without complete
    /// attribution remains unresolved; a known return is a definitive rejection.
    fn settle_confirmed_loss(
        exchange: &str,
        closure: ChannelClosure,
        answered: Vec<AnsweredMessage>,
        unsent: &mut VecDeque<RabbitMqMessage>,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) -> EmitterWriteResult<()> {
        let settlement = Self::settle_answered(exchange, &answered, outcome);
        let (limit, reason) = match closure {
            ChannelClosure::MessageTooLarge { limit, reason } => (limit, reason),
            ChannelClosure::Unattributed(reason) => {
                settlement?;
                return Err(Self::publish_error(reason));
            }
        };
        // This is the current bounded window in publication order, not a shared lookup.
        let Some(refused) = answered
            .iter()
            .position(|entry| entry.answer.is_none() && entry.message.record.payload.len() > limit)
        else {
            settlement?;
            return Err(Self::publish_error(reason));
        };
        let mut failure = match settlement {
            Ok(attribution) => attribution.require_decided().err(),
            Err(error) => Some(error),
        };
        let mut discarded = Vec::new();
        for (index, AnsweredMessage { message, answer }) in answered.into_iter().enumerate() {
            if answer.is_some() {
                continue;
            }
            match index.cmp(&refused) {
                Ordering::Less => {
                    failure.get_or_insert_with(|| {
                        Self::publish_error(
                            "rabbitmq closed the channel before confirming a message written \
                             ahead of the one it refused",
                        )
                    });
                }
                Ordering::Equal => outcome.reject(Self::refused_for_size(&message.record, limit)),
                Ordering::Greater => discarded.push(message),
            }
        }
        if let Some(error) = failure {
            return Err(error.attach_printable(reason));
        }
        for message in discarded.into_iter().rev() {
            unsent.push_front(message);
        }
        Ok(())
    }

    /// `record` rejected because the broker refused its body as larger than `limit`, its
    /// `max_message_size`.
    fn refused_for_size(
        record: &EmitterRecord,
        limit: usize,
    ) -> RejectedEmitterMember<EmitterRecordId> {
        let error = RabbitMqRecordError::AboveMaxMessageSize {
            body_bytes: record.payload.len(),
            limit,
        };
        record.rejected(format!("rabbitmq rejected record: {error}"))
    }

    fn returned_message_reason(returned: &BasicReturnMessage) -> String {
        format!(
            "rabbitmq returned the message unrouted with reply code {}: {}",
            returned.reply_code, returned.reply_text
        )
    }

    fn confirm_timeout_error(timeout: Duration) -> Report<EmitterWriteError> {
        Self::publish_error(format!(
            "rabbitmq publisher confirm exceeded ACK TIMEOUT {}",
            humantime::format_duration(timeout)
        ))
    }

    /// A round trip that ran out of the client's `timeout_ms` as the write's failure, which the
    /// host retries: the deadline stays its typed cause, and its text leads the report.
    fn round_trip_error(elapsed: DeadlineElapsed) -> Report<EmitterWriteError> {
        let message = elapsed.to_string();
        Report::new(elapsed)
            .change_context(EmitterWriteError::Publish {
                connector: RABBITMQ,
            })
            .attach_printable(message)
    }

    /// A client configuration key the emitter cannot read as a configuration failure, with its chain
    /// leading the report.
    fn configuration_error(report: Report<ClientConfigError>) -> Report<EmitterOpenError> {
        let message = format!("{report:#}");
        report
            .change_context(EmitterOpenError::InvalidConfiguration {
                connector: RABBITMQ,
            })
            .attach_printable(message)
    }

    /// A failed connection as a start failure: the client's configuration when it is at fault,
    /// otherwise the emitter's initialization, which the host retries. The connection failure is kept
    /// as the typed cause, and its chain, down to a `connect_timeout_ms` it ran out of, leads the
    /// report.
    fn connect_error(report: Report<RabbitMqConnectError>) -> Report<EmitterOpenError> {
        let context = if report.current_context().is_configuration() {
            EmitterOpenError::InvalidConfiguration {
                connector: RABBITMQ,
            }
        } else {
            EmitterOpenError::Initialize {
                connector: RABBITMQ,
            }
        };
        let message = format!("{report:#}");
        report.change_context(context).attach_printable(message)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external RabbitMQ driver failure; no \
                                   internal runtime ownership is inferred")
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<EmitterWriteError> {
        Report::new(EmitterWriteError::Publish {
            connector: RABBITMQ,
        })
        .attach_printable(error.to_string())
    }
}

impl RabbitMqChannel {
    /// A `NO ACK` write ends with a round trip the broker answers only after it took every
    /// message written before it, and a confirmed write awaits each confirm, so the channel holds
    /// nothing once a publish returns.
    pub(crate) fn finish(&self) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }

    /// Publishes `records` and answers for each of them: a record whose routing key AMQP cannot
    /// carry is rejected before anything is written, and the rest are written in order.
    pub(crate) async fn publish(
        &mut self,
        records: Vec<EmitterRecord>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let mut unsent = VecDeque::with_capacity(records.len());
        for record in records {
            match self.destination.message(record) {
                Ok(message) => unsent.push_back(message),
                Err(Unaddressed::Rejected(rejected)) => outcome.reject(rejected),
                Err(Unaddressed::Missing(error)) => {
                    outcome.fail(error);
                    return outcome;
                }
            }
        }
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use lapin::{
        ChannelState,
        message::Delivery,
        protocol::{AMQPError, AMQPHardError},
    };
    use meticulous::ResultExt as _;
    use nervix_connector::RecordEnvelope;
    use nervix_dns::DnsLookupFailure;
    use nervix_models::{DeadlineKey, Timestamp};

    use super::*;

    /// The `max_message_size` every refusal below names.
    const LIMIT: usize = 1000;

    fn record(index: usize, body_bytes: usize) -> EmitterRecord {
        EmitterRecord::new(
            EmitterRecordId::new(index),
            None,
            vec![b'x'; body_bytes],
            Vec::new(),
            Timestamp::from_unix_nanos(0),
        )
    }

    /// The routing key every message below goes under.
    const QUEUE: &str = "notifications";

    fn message(index: usize, body_bytes: usize) -> RabbitMqMessage {
        let record = record(index, body_bytes);
        let properties =
            RabbitMqMessage::properties(&record).assured("the fixture envelope is valid");
        RabbitMqMessage {
            record,
            routing_key: QUEUE.into(),
            properties,
        }
    }

    /// A mandatory message written under `routing_key`, whose body is `body`.
    pub(super) fn keyed(index: usize, routing_key: &str, body: &[u8]) -> RabbitMqMessage {
        let mut record = record(index, 0);
        record.payload = body.to_vec();
        record.envelope.mandatory = true;
        let properties =
            RabbitMqMessage::properties(&record).assured("the fixture envelope is valid");
        RabbitMqMessage {
            record,
            routing_key: routing_key.into(),
            properties,
        }
    }

    /// `message` as the broker returns it unrouted from `exchange`.
    pub(super) fn returned(exchange: &str, message: &RabbitMqMessage) -> BasicReturnMessage {
        let mut delivery = Delivery::mock(
            0,
            exchange.into(),
            message.routing_key.clone(),
            false,
            message.record.payload.clone(),
        );
        delivery.properties = message.properties.clone();
        BasicReturnMessage {
            delivery,
            reply_code: 312,
            reply_text: "NO_ROUTE".into(),
        }
    }

    fn answered(index: usize, body_bytes: usize, answer: Option<Confirmation>) -> AnsweredMessage {
        AnsweredMessage {
            message: message(index, body_bytes),
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

    fn ids(messages: &VecDeque<RabbitMqMessage>) -> Vec<usize> {
        messages
            .iter()
            .map(|message| message.record.id.index())
            .collect()
    }

    /// What `outcome` delivered and rejected, by record index, with each rejection's message.
    pub(super) fn settled(
        outcome: PerRecordOutcome<EmitterRecordId>,
    ) -> (Vec<usize>, Vec<(usize, String)>) {
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

    pub(super) fn no_route() -> String {
        "rabbitmq returned the message unrouted with reply code 312: NO_ROUTE".to_string()
    }

    #[test]
    fn a_multiple_confirmation_rejects_only_the_message_the_broker_returned() {
        let routed = keyed(0, "order.created", b"first");
        let unrouted = keyed(1, "order.unrouted", b"second");
        // Lapin can attach the return to either confirm covered by a multiple acknowledgement.
        let returned = returned("events", &unrouted);
        let answered = vec![
            AnsweredMessage {
                message: routed,
                answer: Some(Confirmation::Ack(Some(returned))),
            },
            AnsweredMessage {
                message: unrouted,
                answer: Some(Confirmation::Ack(None)),
            },
        ];
        let mut outcome = PerRecordOutcome::empty();

        RabbitMqChannel::settle_answered("events", &answered, &mut outcome)
            .assured("every return names a message in this window");

        assert_eq!(settled(outcome), (vec![0], vec![(1, no_route())]));
    }

    #[test]
    fn a_return_carried_by_a_later_confirm_still_rejects_the_first_message() {
        let unrouted = keyed(0, "order.unrouted", b"first");
        let routed = keyed(1, "order.created", b"second");
        let returned = returned("events", &unrouted);
        let answered = vec![
            AnsweredMessage {
                message: unrouted,
                answer: Some(Confirmation::Ack(None)),
            },
            AnsweredMessage {
                message: routed,
                answer: Some(Confirmation::Ack(Some(returned))),
            },
        ];
        let mut outcome = PerRecordOutcome::empty();
        RabbitMqChannel::settle_answered("events", &answered, &mut outcome)
            .assured("every return names a message in this window");
        assert_eq!(settled(outcome), (vec![1], vec![(0, no_route())]));
    }

    #[test]
    fn indistinguishable_confirmed_messages_take_returns_in_written_order() {
        let first = keyed(0, "order.created", b"same");
        let second = keyed(1, "order.created", b"same");
        let returned = returned("events", &second);
        let answered = vec![
            AnsweredMessage {
                message: first,
                answer: Some(Confirmation::Ack(None)),
            },
            AnsweredMessage {
                message: second,
                answer: Some(Confirmation::Ack(Some(returned))),
            },
        ];
        let mut outcome = PerRecordOutcome::empty();
        RabbitMqChannel::settle_answered("events", &answered, &mut outcome)
            .assured("every return names a message in this window");
        assert_eq!(settled(outcome), (vec![1], vec![(0, no_route())]));
    }

    #[test]
    fn return_attribution_compares_properties_when_the_key_and_body_are_identical() {
        let mut first = keyed(0, "order.created", b"same");
        first.record.headers = vec![("event".to_string(), "first".to_string())];
        first.properties = RabbitMqMessage::properties(&first.record).assured("valid headers");
        let mut second = keyed(1, "order.created", b"same");
        second.record.headers = vec![("event".to_string(), "second".to_string())];
        second.properties = RabbitMqMessage::properties(&second.record).assured("valid headers");
        let returned = returned("events", &second);
        let answered = vec![
            AnsweredMessage {
                message: first,
                answer: Some(Confirmation::Ack(Some(returned))),
            },
            AnsweredMessage {
                message: second,
                answer: Some(Confirmation::Ack(None)),
            },
        ];
        let mut outcome = PerRecordOutcome::empty();
        RabbitMqChannel::settle_answered("events", &answered, &mut outcome)
            .assured("every return names a message in this window");
        assert_eq!(settled(outcome), (vec![0], vec![(1, no_route())]));
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
            message(0, 10),
            message(1, 2000),
            message(2, 10),
            message(3, 5000),
        ];
        let mut unsent = VecDeque::from([message(4, 10)]);
        let mut outcome = PerRecordOutcome::empty();

        RabbitMqChannel::settle_unconfirmed_loss(
            "",
            size_refusal(),
            written,
            Vec::new(),
            &mut unsent,
            &mut outcome,
        )
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
            let written = vec![message(0, 10), message(1, LIMIT)];
            let mut unsent = VecDeque::from([message(2, 10)]);
            let mut outcome = PerRecordOutcome::empty();

            let settled_loss = RabbitMqChannel::settle_unconfirmed_loss(
                "",
                closure,
                written,
                Vec::new(),
                &mut unsent,
                &mut outcome,
            );

            assert!(settled_loss.is_err());
            assert_eq!(ids(&unsent), vec![2]);
            assert_eq!(settled(outcome), (Vec::new(), Vec::new()));
        }
    }

    #[test]
    fn a_confirmed_refusal_keeps_earlier_answers_and_writes_the_discarded_messages_again() {
        let mut rejected = message(1, 10);
        rejected.record.envelope.mandatory = true;
        let mut returned = returned("", &rejected);
        returned.reply_code = 311;
        returned.reply_text = "CONTENT_TOO_LARGE".into();
        let answered = vec![
            answered(0, 10, Some(Confirmation::Ack(None))),
            AnsweredMessage {
                message: rejected,
                answer: Some(Confirmation::Ack(Some(returned))),
            },
            answered(2, 2000, None),
            answered(3, 10, None),
        ];
        let mut unsent = VecDeque::from([message(4, 10)]);
        let mut outcome = PerRecordOutcome::empty();

        RabbitMqChannel::settle_confirmed_loss(
            "",
            size_refusal(),
            answered,
            &mut unsent,
            &mut outcome,
        )
        .assured("every message ahead of the refused one was answered");

        assert_eq!(ids(&unsent), vec![3, 4]);
        assert_eq!(
            settled(outcome),
            (
                vec![0],
                vec![
                    (
                        1,
                        "rabbitmq returned the message unrouted with reply code 311: \
                         CONTENT_TOO_LARGE"
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
            let mut unsent = VecDeque::from([message(3, 10)]);
            let mut outcome = PerRecordOutcome::empty();

            let settled_loss = RabbitMqChannel::settle_confirmed_loss(
                "",
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
    fn size_recovery_keeps_a_partially_attributed_mandatory_prefix_for_retry() {
        let mut first = message(0, 10);
        first.record.envelope.mandatory = true;
        let answered = vec![
            AnsweredMessage {
                message: first,
                answer: Some(Confirmation::Ack(None)),
            },
            answered(1, 2000, None),
            answered(2, 10, None),
        ];
        let mut unsent = VecDeque::from([message(3, 10)]);
        let mut outcome = PerRecordOutcome::empty();
        let result = RabbitMqChannel::settle_confirmed_loss(
            "",
            size_refusal(),
            answered,
            &mut unsent,
            &mut outcome,
        );
        assert!(
            result.is_err(),
            "the mandatory prefix still has an unknown return outcome"
        );
        assert_eq!(ids(&unsent), vec![3]);
        assert_eq!(settled(outcome), (Vec::new(), vec![(1, too_large(2000))]));
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
            let mut unsent = VecDeque::from([message(2, 10)]);
            let mut outcome = PerRecordOutcome::empty();

            let settled_loss = RabbitMqChannel::settle_confirmed_loss(
                "",
                closure,
                answered,
                &mut unsent,
                &mut outcome,
            );

            assert!(settled_loss.is_err());
            assert_eq!(ids(&unsent), vec![2]);
            assert_eq!(settled(outcome), (vec![0], Vec::new()));
        }
    }

    #[test]
    fn a_no_ack_write_rejects_each_returned_message_and_delivers_the_rest() {
        let taken = vec![
            keyed(0, "order.created", b"first"),
            keyed(1, "order.unrouted", b"second"),
            keyed(2, "order.created", b"third"),
        ];
        let returns = vec![returned("events", &taken[1])];
        let mut outcome = PerRecordOutcome::empty();

        RabbitMqChannel::settle_taken("events", taken, returns, &mut outcome)
            .assured("the write's one return is its second message");

        assert_eq!(settled(outcome), (vec![0, 2], vec![(1, no_route())]));
    }

    /// Two messages identical in exchange, routing key, properties and body cannot be told apart by
    /// their return, so the earlier one takes it, and the later one is delivered.
    #[test]
    fn identical_messages_take_their_returns_in_written_order() {
        let taken = vec![
            keyed(0, "order.created", b"same"),
            keyed(1, "order.created", b"same"),
            keyed(2, "order.created", b"other"),
        ];
        let returns = vec![returned("events", &taken[0]), returned("events", &taken[2])];
        let mut outcome = PerRecordOutcome::empty();

        RabbitMqChannel::settle_taken("events", taken, returns, &mut outcome)
            .assured("both returns match a message of the write in order");

        assert_eq!(
            settled(outcome),
            (vec![1], vec![(0, no_route()), (2, no_route())])
        );
    }

    #[test]
    fn a_return_matches_the_complete_prepared_properties() {
        let first = keyed(0, "order.created", b"same");
        let mut second = keyed(1, "order.created", b"same");
        second.record.envelope = RecordEnvelope {
            mandatory: true,
            persistent: true,
            timestamp: Some(Timestamp::from_unix_nanos(0)),
            properties: vec![nervix_connector::RecordProperty {
                name: "message_id",
                value: nervix_connector::PropertyValue::String("second".to_owned()),
            }],
            ..Default::default()
        };
        second.properties =
            RabbitMqMessage::properties(&second.record).assured("the envelope is valid");
        let returned = returned("events", &second);
        assert!(!first.is_returned_by("events", &returned));
        assert!(second.is_returned_by("events", &returned));
        let mut outcome = PerRecordOutcome::empty();
        RabbitMqChannel::settle_answered(
            "events",
            &[
                AnsweredMessage {
                    message: first,
                    answer: Some(Confirmation::Ack(Some(returned))),
                },
                AnsweredMessage {
                    message: second,
                    answer: Some(Confirmation::Ack(None)),
                },
            ],
            &mut outcome,
        )
        .assured("only the second message carries the returned properties");
        assert_eq!(settled(outcome), (vec![0], vec![(1, no_route())]));
    }

    #[test]
    fn a_return_no_message_of_the_write_carries_fails_the_write_and_settles_nothing() {
        let written = keyed(0, "order.created", b"first");
        for stray in [
            // Another body.
            returned("events", &keyed(9, "order.created", b"elsewhere")),
            // Another routing key.
            returned("events", &keyed(9, "order.other", b"first")),
            // Another exchange.
            returned("audit", &written),
        ] {
            let mut outcome = PerRecordOutcome::empty();
            let settled_write = RabbitMqChannel::settle_taken(
                "events",
                vec![keyed(0, "order.created", b"first")],
                vec![stray],
                &mut outcome,
            );

            assert!(settled_write.is_err());
            assert_eq!(settled(outcome), (Vec::new(), Vec::new()));
        }
    }

    #[test]
    fn a_return_ahead_of_a_size_refusal_rejects_its_message_with_the_refused_one() {
        let written = vec![
            message(0, 10),
            message(1, 20),
            message(2, 2000),
            message(3, 10),
        ];
        let returns = vec![returned("", &written[1])];
        let mut unsent = VecDeque::new();
        let mut outcome = PerRecordOutcome::empty();

        RabbitMqChannel::settle_unconfirmed_loss(
            "",
            size_refusal(),
            written,
            returns,
            &mut unsent,
            &mut outcome,
        )
        .assured("the broker refused the third message and returned the second");

        assert_eq!(ids(&unsent), vec![3]);
        assert_eq!(
            settled(outcome),
            (vec![0], vec![(1, no_route()), (2, too_large(2000))])
        );
    }

    fn keyed_record(routing_key: Option<String>) -> EmitterRecord {
        record(0, 10).with_envelope(RecordEnvelope {
            routing_key,
            ..RecordEnvelope::default()
        })
    }

    fn exchange() -> RabbitMqDestination {
        RabbitMqDestination::Exchange(
            RabbitMqExchangeName::parse("events").assured("events is an exchange name"),
        )
    }

    #[test]
    fn a_queue_routes_by_its_name_and_an_exchange_by_each_record_key() {
        let queue = RabbitMqDestination::Queue(
            RabbitMqQueueName::parse(QUEUE).assured("notifications is a queue name"),
        );
        let Ok(queued) = queue.message(keyed_record(Some("ignored".to_string()))) else {
            panic!("a queue routes every record by its name");
        };
        assert_eq!((queue.exchange(), queued.routing_key.as_str()), ("", QUEUE));

        let longest = "k".repeat(255);
        let destination = exchange();
        let Ok(routed) = destination.message(keyed_record(Some(longest.clone()))) else {
            panic!("a 255-byte key fits an AMQP short string");
        };
        assert_eq!(
            (destination.exchange(), routed.routing_key.as_str()),
            ("events", longest.as_str())
        );
    }

    #[test]
    fn a_routing_key_longer_than_a_short_string_rejects_its_record() {
        // 128 two-byte characters: 256 bytes in 128 characters.
        for key in ["k".repeat(256), "é".repeat(128)] {
            let Err(Unaddressed::Rejected(rejected)) = exchange().message(keyed_record(Some(key)))
            else {
                panic!("a 256-byte key does not fit an AMQP short string");
            };
            assert_eq!(rejected.id.index(), 0);
            assert_eq!(
                rejected.error.message,
                "rabbitmq rejected record: routing key of 256 bytes is longer than the 255 bytes \
                 an AMQP short string carries"
            );
        }
    }

    #[test]
    fn an_exchange_record_without_its_routing_key_is_a_misconfigured_write() {
        let Err(Unaddressed::Missing(error)) = exchange().message(keyed_record(None)) else {
            panic!("an exchange record carries the key its ROUTING KEY computes");
        };
        assert!(matches!(
            error.current_context(),
            EmitterWriteError::Misconfigured {
                connector: RABBITMQ
            }
        ));
    }

    fn protocol_error(kind: AMQPErrorKind, text: &str) -> lapin::Error {
        lapin::Error::from(ErrorKind::ProtocolError(AMQPError::new(kind, text.into())))
    }

    /// The broker answers a passive declaration of an exchange it does not have with `NOT_FOUND`,
    /// which is the contract's missing entity; every other failure of the check is the emitter's
    /// initialization, which the host retries all the same.
    #[test]
    fn a_missing_exchange_is_the_missing_entity_and_any_other_check_failure_initializes() {
        let name = RabbitMqExchangeName::parse("events").assured("events is an exchange name");
        let missing = RabbitMqChannel::exchange_check_error(
            &name,
            OpeningStepFailure::Failed(protocol_error(
                AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND),
                "NOT_FOUND - no exchange 'events' in vhost '/'",
            )),
        );
        assert!(matches!(
            missing.current_context(),
            EmitterOpenError::MissingExternalEntity {
                connector: RABBITMQ,
                kind: "RabbitMQ exchange",
                name,
            } if name == "events"
        ));
        let leading = missing
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            leading.map(String::as_str),
            Some("RabbitMQ exchange 'events' does not exist")
        );
        assert!(missing.downcast_ref::<lapin::Error>().is_some());

        let connect_timeout = nervix_connector::ConfiguredDeadline::connect(&[])
            .expect("a client without the key takes the default");
        for failure in [
            OpeningStepFailure::Failed(protocol_error(
                AMQPErrorKind::Soft(AMQPSoftError::ACCESSREFUSED),
                "ACCESS_REFUSED - access to exchange 'events' refused",
            )),
            OpeningStepFailure::Failed(protocol_error(
                AMQPErrorKind::Hard(AMQPHardError::CONNECTIONFORCED),
                "CONNECTION_FORCED - broker forced connection closure",
            )),
            OpeningStepFailure::Failed(lost_channel()),
            OpeningStepFailure::Elapsed(connect_timeout.elapsed("checking the RabbitMQ exchange")),
        ] {
            let initialize = RabbitMqChannel::exchange_check_error(&name, failure);
            assert!(matches!(
                initialize.current_context(),
                EmitterOpenError::Initialize {
                    connector: RABBITMQ
                }
            ));
        }
    }

    #[test]
    fn connection_failures_start_as_configuration_or_initialization_failures() {
        let invalid =
            RabbitMqChannel::connect_error(Report::new(RabbitMqConnectError::InvalidCaCertificate));
        assert!(matches!(
            invalid.current_context(),
            EmitterOpenError::InvalidConfiguration {
                connector: RABBITMQ
            }
        ));

        let unresolved =
            RabbitMqChannel::connect_error(Report::new(RabbitMqConnectError::Resolve {
                host: "rabbitmq.nervix.test".to_string(),
                failure: DnsLookupFailure::NameNotFound,
            }));
        assert!(matches!(
            unresolved.current_context(),
            EmitterOpenError::Initialize {
                connector: RABBITMQ
            }
        ));
        let leading = unresolved
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            leading.map(String::as_str),
            Some("resolving RabbitMQ host 'rabbitmq.nervix.test' failed: the name does not exist")
        );
        assert!(unresolved.downcast_ref::<RabbitMqConnectError>().is_some());

        let connect_timeout = nervix_connector::ConfiguredDeadline::connect(&[])
            .expect("a client without the key takes the default");
        let stalled = RabbitMqChannel::connect_error(
            Report::new(connect_timeout.elapsed("the TLS handshake with the RabbitMQ broker"))
                .change_context(RabbitMqConnectError::TlsHandshake {
                    host: "rabbitmq.nervix.test".to_string(),
                }),
        );
        let leading = stalled
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            leading.map(String::as_str),
            Some(
                "the TLS handshake with RabbitMQ host 'rabbitmq.nervix.test' failed: the TLS \
                 handshake with the RabbitMQ broker did not finish within connect_timeout_ms 30s"
            )
        );
    }

    fn timeout_ms(value: &str) -> Vec<ClientConfigEntry> {
        vec![ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: value.to_string(),
        }]
    }

    #[nervix_primitives::test]
    async fn a_no_ack_round_trip_the_broker_does_not_answer_ends_within_timeout_ms() {
        let deadline = ConfiguredDeadline::timeout(&timeout_ms("50"))
            .assured("50 ms is a valid deadline")
            .assured("the entries set timeout_ms");

        let stalled =
            RabbitMqChannel::answered_within(Some(deadline), std::future::pending()).await;
        let Err(UnconfirmedStop::RoundTripElapsed(elapsed)) = stalled else {
            panic!("a round trip the broker never answers runs out of timeout_ms");
        };
        assert_eq!(
            (elapsed.key, elapsed.bound),
            (DeadlineKey::Timeout, Duration::from_millis(50))
        );
        let report = RabbitMqChannel::round_trip_error(elapsed);
        assert!(matches!(
            report.current_context(),
            EmitterWriteError::Publish {
                connector: RABBITMQ
            }
        ));
        assert_eq!(
            report.downcast_ref::<DeadlineElapsed>().copied(),
            Some(elapsed)
        );
        let leading = report
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            leading.map(String::as_str),
            Some(
                "the round trip that ends a RabbitMQ NO ACK write did not finish within \
                 timeout_ms 50ms"
            )
        );

        for deadline in [Some(deadline), None] {
            let answered = RabbitMqChannel::answered_within(deadline, async { Ok(()) }).await;
            assert!(matches!(answered, Ok(())));
            let lost =
                RabbitMqChannel::answered_within(deadline, async { Err(lost_channel()) }).await;
            assert!(matches!(lost, Err(UnconfirmedStop::ChannelLost(_))));
        }
    }

    #[test]
    fn a_timeout_ms_that_breaks_the_deadline_rule_is_a_configuration_failure() {
        let Err(invalid) = ConfiguredDeadline::timeout(&timeout_ms("0")) else {
            panic!("zero milliseconds is not a deadline");
        };
        let report = RabbitMqChannel::configuration_error(invalid);
        assert!(matches!(
            report.current_context(),
            EmitterOpenError::InvalidConfiguration {
                connector: RABBITMQ
            }
        ));
        let leading = report
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            leading.map(String::as_str),
            Some(
                "invalid client config key 'timeout_ms': not a positive whole number of \
                 milliseconds"
            )
        );
    }
}
