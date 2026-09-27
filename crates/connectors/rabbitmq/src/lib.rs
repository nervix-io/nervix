//! RabbitMQ source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The AMQP connection and channel a client configures, queue declaration,
//!   the consumer and prefetch window a source reads through, AMQP headers in both
//!   directions, per-delivery acknowledgement and requeue, publisher confirms, and
//!   returned-message classification.
//! - **Depends on.** The connector contract, vocabulary values, the node resolver, `error-stack`,
//!   Tokio and `lapin`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

mod connection;
mod source;

use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use connection::RabbitMqBroker;
pub use connection::RabbitMqConnectError;
use error_stack::Report;
use futures_util::FutureExt;
use lapin::{
    Confirmation, PublisherConfirm,
    message::BasicReturnMessage,
    options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
};
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, PerRecordOutcome, RecordSink, RejectedSinkRecord,
    SinkHost, SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordId,
    SinkStartError, SinkStartResult,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, QueueName, Timestamp};
pub use source::{
    RabbitMqDeliveryHeaders, RabbitMqSource, RabbitMqSourceError, RabbitMqSourceMessage,
    RabbitMqSourcePlan, RabbitMqSourcePosition,
};
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
    channel: lapin::Channel,
    queue: QueueName,
    mode: BrokerPublishingMode,
}

struct PendingRabbitMqConfirmation {
    record: SinkRecordId,
    occurred_at: Timestamp,
    deadline: Instant,
    confirmation: PublisherConfirm,
}

impl RabbitMqSink {
    pub async fn new(config: RabbitMqSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let mode = config.mode;
        let channel = Self::open_channel(&config.config, config.dns).await?;
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
        if let BrokerPublishingMode::Ack { .. } = mode {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(Self::start_error)?;
        }
        Ok(Self {
            channel,
            queue: config.queue,
            mode,
        })
    }

    /// A channel on a new connection to the broker `config` names.
    async fn open_channel(
        config: &[ClientConfigEntry],
        dns: DnsResolver,
    ) -> SinkStartResult<lapin::Channel> {
        let broker = RabbitMqBroker::from_config(config, dns).map_err(Self::connect_error)?;
        let connection = broker.connect().await.map_err(Self::connect_error)?;
        connection.create_channel().await.map_err(Self::start_error)
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

    async fn publish_message(&self, record: &SinkRecord) -> SinkPublishResult<PublisherConfirm> {
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
            .map_err(Self::publish_error)
    }

    /// `MODE NO_ACK`: the channel is not in confirm mode, so the publisher confirm resolves to the
    /// channel's acceptance and a record is delivered as soon as the broker takes it.
    async fn publish_unconfirmed(
        &self,
        records: Vec<SinkRecord>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        for record in records {
            tokio::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let confirmation = match self.publish_message(&record).await {
                Ok(confirmation) => confirmation,
                Err(error) => {
                    outcome.fail(error);
                    return;
                }
            };
            match confirmation.await {
                Ok(Confirmation::NotRequested | Confirmation::Ack(None)) => {
                    outcome.deliver(record_id);
                }
                Ok(Confirmation::Ack(Some(returned))) => {
                    if Self::is_returned_record_rejection(&returned) {
                        outcome.reject(RejectedSinkRecord::external(
                            record_id,
                            occurred_at,
                            Self::returned_message_reason(&returned),
                        ));
                    } else {
                        outcome.fail(Self::returned_message_error(&returned));
                        return;
                    }
                }
                Ok(Confirmation::Nack(_)) => {
                    outcome.fail(Self::publish_error(
                        "rabbitmq channel acceptance returned nack",
                    ));
                    return;
                }
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    return;
                }
            }
        }
    }

    /// `MODE ACK`: the channel is in confirm mode, at most `max_in_flight` publisher confirms are
    /// outstanding at once, and every one is awaited before the batch finishes. The window carries
    /// the confirmation settings, so the drain below never has to ask a mode that has no
    /// confirmations what its timeout is.
    async fn publish_confirmed(
        &self,
        records: Vec<SinkRecord>,
        AckConfirmation {
            max_in_flight,
            timeout,
        }: AckConfirmation,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        let mut pending: VecDeque<PendingRabbitMqConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let confirmation = match self.publish_message(&record).await {
                Ok(confirmation) => confirmation,
                Err(error) => {
                    outcome.fail(error);
                    return;
                }
            };
            let Some(deadline) = Instant::now().checked_add(timeout) else {
                outcome.fail(Self::publish_error(
                    "rabbitmq ACK TIMEOUT exceeds the monotonic clock range",
                ));
                return;
            };
            pending.push_back(PendingRabbitMqConfirmation {
                record: record_id,
                occurred_at,
                deadline,
                confirmation,
            });
            if pending.len() >= max_in_flight.get()
                && let Err(error) = Self::confirm_oldest(&mut pending, timeout, outcome).await
            {
                outcome.fail(error);
                return;
            }
        }
        while !pending.is_empty() {
            tokio::task::consume_budget().await;
            if let Err(error) = Self::confirm_oldest(&mut pending, timeout, outcome).await {
                outcome.fail(error);
                return;
            }
        }
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingRabbitMqConfirmation>,
        timeout: Duration,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> SinkPublishResult<()> {
        let Some(oldest) = pending.front_mut() else {
            return Err(Self::publish_error(
                "rabbitmq acknowledgment window unexpectedly became empty",
            ));
        };
        let remaining = oldest
            .deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(Self::confirm_timeout_error(timeout));
        }
        let result = tokio::select! {
            biased;
            result = &mut oldest.confirmation => Some(result),
            _ = sleep(remaining) => None,
        };
        let Some(result) = result else {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(Self::confirm_timeout_error(timeout));
        };
        let record_id = oldest.record;
        let occurred_at = oldest.occurred_at;
        match result {
            Ok(Confirmation::Ack(None)) => {
                pending.pop_front();
                outcome.deliver(record_id);
                Ok(())
            }
            Ok(Confirmation::Ack(Some(returned))) => {
                if Self::is_returned_record_rejection(&returned) {
                    pending.pop_front();
                    outcome.reject(RejectedSinkRecord::external(
                        record_id,
                        occurred_at,
                        Self::returned_message_reason(&returned),
                    ));
                    return Ok(());
                }
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(Self::returned_message_error(&returned))
            }
            Ok(Confirmation::Nack(Some(returned)))
                if Self::is_returned_record_rejection(&returned) =>
            {
                pending.pop_front();
                outcome.reject(RejectedSinkRecord::external(
                    record_id,
                    occurred_at,
                    Self::returned_message_reason(&returned),
                ));
                Ok(())
            }
            Ok(Confirmation::Nack(_)) => {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(Self::publish_error(
                    "rabbitmq publisher confirm returned nack",
                ))
            }
            Ok(Confirmation::NotRequested) => {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(Self::publish_error(
                    "rabbitmq publisher confirms were not enabled",
                ))
            }
            Err(source) => {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(Self::publish_error(source))
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
            let confirmation = pending.remove(index).verified(
                "the index came from scanning this same pending window, which nothing else \
                 removes from",
            );
            match result {
                Ok(Confirmation::Ack(None)) => outcome.deliver(confirmation.record),
                Ok(Confirmation::Ack(Some(returned)) | Confirmation::Nack(Some(returned)))
                    if Self::is_returned_record_rejection(&returned) =>
                {
                    outcome.reject(RejectedSinkRecord::external(
                        confirmation.record,
                        confirmation.occurred_at,
                        Self::returned_message_reason(&returned),
                    ));
                }
                Ok(
                    Confirmation::Ack(Some(_)) | Confirmation::Nack(_) | Confirmation::NotRequested,
                )
                | Err(_) => {}
            }
        }
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
        match self.mode {
            BrokerPublishingMode::NoAck => {
                self.publish_unconfirmed(records, &mut outcome).await;
            }
            BrokerPublishingMode::Ack(confirmation) => {
                self.publish_confirmed(records, confirmation, &mut outcome)
                    .await;
            }
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use lapin::message::Delivery;
    use nervix_dns::DnsLookupFailure;

    use super::*;

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
