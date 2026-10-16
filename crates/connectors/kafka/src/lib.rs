//! Kafka source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Kafka consumer and producer configuration, source polling and offset handling,
//!   record and header publication, delivery-report classification, the delivery bound a
//!   confirmed publish gives the producer, and producer queue shutdown.
//! - **Depends on.** The connector contract and its confirmation window, vocabulary values,
//!   `error-stack`, Tokio, and `rust-rdkafka`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

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

#[cfg(feature = "testing")]
#[doc(hidden)]
pub use rdkafka as testing_rdkafka;

mod source;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    AckConfirmation, BrokerAnswer, BrokerPublishingMode, BrokerVerdict, ConfirmationWindow,
    PerRecordOutcome, RecordSink, SinkFinish, SinkFinishError, SinkFinishResult, SinkHost,
    SinkLifecycle, SinkPublishError, SinkRecord, SinkRecordId, SinkStartError, SinkStartResult,
};
use nervix_models::{ClientConfigEntry, TopicName};
use nervix_primitives::time::Instant;
use rdkafka::{
    config::ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header as KafkaHeader, OwnedHeaders},
    producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer},
};
pub use source::{
    KafkaDomainOffsetError, KafkaDomainOffsetHost, KafkaDomainOffsetInitialization,
    KafkaDomainOffsetResult, KafkaDomainOffsetServices, KafkaDomainOffsetStart,
    KafkaOffsetPosition, KafkaSource, KafkaSourceError, KafkaSourceMessage, KafkaSourceOffsetMode,
    KafkaSourcePlan, TopicPartitionInspector,
};
// Criterion measures the per-partition folds an acknowledgement and a rejection run.
#[cfg(feature = "benchmarks")]
pub use source::{earliest_message_offsets, latest_positions};
use thiserror::Error;

const KAFKA: &str = "kafka";

/// The librdkafka property that bounds how long the producer keeps trying to deliver a record it
/// took, its own retries included.
const MESSAGE_TIMEOUT_PROPERTY: &str = "message.timeout.ms";

/// The other name librdkafka reads `message.timeout.ms` from.
const DELIVERY_TIMEOUT_PROPERTY: &str = "delivery.timeout.ms";

/// The longest `message.timeout.ms` librdkafka accepts, which it holds as a C `int` of
/// milliseconds.
const LONGEST_MESSAGE_TIMEOUT_MILLIS: u32 = i32::MAX.unsigned_abs();

/// Nanoseconds in one millisecond, the unit `message.timeout.ms` counts in.
const NANOS_PER_MILLI: u32 = 1_000_000;

pub struct KafkaSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub topic: TopicName,
    pub mode: BrokerPublishingMode,
}

pub struct KafkaSink {
    producer: FutureProducer,
    topic: TopicName,
    mode: BrokerPublishingMode,
}

/// How long librdkafka keeps trying to deliver a record a confirmed publish handed it, in the
/// whole milliseconds `message.timeout.ms` counts.
///
/// A confirmed publish stops waiting for a record's answer `ACK TIMEOUT` after it handed the
/// record over, and the host's retry then sends the record again. A producer that went on trying
/// past that point could still deliver the copy it was handed first beside the retry's copy, so it
/// gives up after the same `ACK TIMEOUT`, rounded up to the millisecond it counts in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KafkaMessageTimeout {
    millis: u32,
}

/// Why a confirmed publish's `ACK TIMEOUT` cannot bound how long librdkafka tries to deliver a
/// record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the conversion returns a pure range validation refusal"
    )
)]
enum KafkaMessageTimeoutError {
    /// A zero `ACK TIMEOUT` would set `message.timeout.ms` to `0`, which librdkafka reads as no
    /// bound at all.
    #[error("ACK TIMEOUT 0s cannot bound librdkafka's delivery of a record")]
    Zero,
    /// librdkafka cannot hold a `message.timeout.ms` this long.
    #[error(
        "ACK TIMEOUT {} is longer than the {LONGEST_MESSAGE_TIMEOUT_MILLIS}ms librdkafka's \
         message.timeout.ms can hold",
        humantime::format_duration(*.timeout)
    )]
    TooLong { timeout: Duration },
}

impl TryFrom<Duration> for KafkaMessageTimeout {
    type Error = KafkaMessageTimeoutError;

    fn try_from(timeout: Duration) -> Result<Self, Self::Error> {
        let whole_millis = timeout.as_millis();
        let rounded_millis = if timeout.subsec_nanos().is_multiple_of(NANOS_PER_MILLI) {
            whole_millis
        } else {
            whole_millis
                .checked_add(1)
                .assured("a duration's whole milliseconds stay far below u128::MAX")
        };
        let Ok(millis) = u32::try_from(rounded_millis) else {
            return Err(KafkaMessageTimeoutError::TooLong { timeout });
        };
        if millis > LONGEST_MESSAGE_TIMEOUT_MILLIS {
            return Err(KafkaMessageTimeoutError::TooLong { timeout });
        }
        if millis == 0 {
            return Err(KafkaMessageTimeoutError::Zero);
        }
        Ok(Self { millis })
    }
}

/// The broker's answer for one record handed to the producer, read from the delivery report
/// librdkafka gives for it.
struct KafkaAnswer {
    delivery: DeliveryFuture,
}

impl Future for KafkaAnswer {
    type Output = BrokerAnswer;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<BrokerAnswer> {
        let report = match Pin::new(&mut self.delivery).poll(context) {
            Poll::Ready(report) => report,
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(KafkaSink::broker_answer(report))
    }
}

impl KafkaSink {
    pub fn new(config: KafkaSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let producer_config = Self::producer_config(&config.config, config.mode)?;
        let producer = Self::create_producer(&producer_config)?;
        Ok(Self {
            producer,
            topic: config.topic,
            mode: config.mode,
        })
    }

    /// The configuration of a producer that publishes in `mode` through a client whose entries are
    /// `entries`.
    ///
    /// The entries pass through as written. A confirmed publish then sets `message.timeout.ms` to
    /// its `ACK TIMEOUT` and drops the other name librdkafka reads that bound from, so no entry can
    /// keep the producer trying to deliver a record after the publish stopped waiting for it.
    fn producer_config(
        entries: &[ClientConfigEntry],
        mode: BrokerPublishingMode,
    ) -> SinkStartResult<ClientConfig> {
        let mut config = ClientConfig::new();
        for entry in entries {
            config.set(&entry.key, &entry.value);
        }
        let BrokerPublishingMode::Ack(settings) = mode else {
            return Ok(config);
        };
        let message_timeout = match KafkaMessageTimeout::try_from(settings.timeout) {
            Ok(message_timeout) => message_timeout,
            Err(error) => {
                let description = error.to_string();
                return Err(Report::new(error)
                    .change_context(SinkStartError::InvalidConfiguration { sink: KAFKA })
                    .attach_printable(description));
            }
        };
        config.remove(DELIVERY_TIMEOUT_PROPERTY);
        config.set(MESSAGE_TIMEOUT_PROPERTY, message_timeout.millis.to_string());
        Ok(config)
    }

    fn create_producer(config: &ClientConfig) -> SinkStartResult<FutureProducer> {
        match config.create() {
            Ok(producer) => Ok(producer),
            Err(error) => {
                let description = error.to_string();
                Err(Report::new(error)
                    .change_context(SinkStartError::Initialize { sink: KAFKA })
                    .attach_printable(description))
            }
        }
    }

    /// Publishes `records` without awaiting delivery reports: the producer taking a record into
    /// its queue delivers it.
    async fn publish_unconfirmed(
        &self,
        records: Vec<SinkRecord>,
    ) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            match self.enqueue(&record) {
                Ok(delivery) => {
                    drop(delivery);
                    outcome.deliver(record.id);
                }
                Err(error) if Self::is_record_rejection(&error) => {
                    outcome.reject(record.rejected(format!("kafka rejected record: {error}")));
                }
                Err(error) => {
                    outcome.fail(Self::refused_hand_over(error));
                    return outcome;
                }
            }
        }
        outcome
    }

    /// Publishes `records` and awaits librdkafka's delivery report for each through the
    /// confirmation window `settings` declare.
    ///
    /// Every way the publish can fail ends it through the window, which first settles each answer
    /// the broker already gave, so the host's retry never resends a record the broker confirmed.
    /// That includes a producer queue too full to take the next record.
    async fn publish_confirmed(
        &self,
        records: Vec<SinkRecord>,
        settings: AckConfirmation,
    ) -> PerRecordOutcome<SinkRecordId> {
        let mut window: ConfirmationWindow<KafkaAnswer> =
            ConfirmationWindow::new(KAFKA, settings, records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let handed_over = self.enqueue(&record);
            let delivery = match handed_over {
                Ok(delivery) => delivery,
                Err(error) if Self::is_record_rejection(&error) => {
                    window.reject(record.rejected(format!("kafka rejected record: {error}")));
                    continue;
                }
                Err(error) => return window.fail(Self::refused_hand_over(error)),
            };
            let answer = KafkaAnswer { delivery };
            if let Err(failure) = window.admit(record.id, record.occurred_at, answer).await {
                return window.fail(failure);
            }
        }
        window.finish().await
    }

    fn enqueue(&self, message: &SinkRecord) -> Result<DeliveryFuture, KafkaError> {
        let mut record =
            FutureRecord::<str, [u8]>::to(self.topic.as_str()).payload(message.payload.as_slice());
        if let Some(key) = message.key.as_deref() {
            record = record.key(key);
        }
        if !message.headers.is_empty() {
            let owned_headers = message.headers.iter().fold(
                OwnedHeaders::new_with_capacity(message.headers.len()),
                |owned_headers, (key, value)| {
                    owned_headers.insert(KafkaHeader {
                        key,
                        value: Some(value.as_str()),
                    })
                },
            );
            record = record.headers(owned_headers);
        }
        self.producer
            .send_result(record)
            .map_err(|(source, _record)| source)
    }

    /// The broker's answer for a record whose delivery report is `report`.
    ///
    /// A delivered record is confirmed, and a delivery error about the record itself refuses it
    /// for good. Any other ending leaves the record's outcome unknown: the broker, the connection
    /// or the producer failed, or the producer dropped the record without a report.
    fn broker_answer(report: <DeliveryFuture as Future>::Output) -> BrokerAnswer {
        match report {
            Ok(Ok(_delivery)) => Ok(BrokerVerdict::Confirmed),
            Ok(Err((error, _record))) if Self::is_record_rejection(&error) => {
                Ok(BrokerVerdict::Rejected {
                    message: format!("kafka rejected record: {error}"),
                })
            }
            Ok(Err((error, _record))) => {
                let description = format!("kafka delivery failed: {error}");
                Err(Report::new(error)
                    .change_context(SinkPublishError::Publish { sink: KAFKA })
                    .attach_printable(description))
            }
            Err(dropped) => Err(Report::new(dropped)
                .change_context(SinkPublishError::Publish { sink: KAFKA })
                .attach_printable("kafka producer dropped the record without a delivery report")),
        }
    }

    /// The failure that ends a publish whose record the producer refused to take for a reason
    /// that is not the record's own, such as a full producer queue.
    fn refused_hand_over(error: KafkaError) -> Report<SinkPublishError> {
        let description = format!("kafka producer refused the record: {error}");
        Report::new(error)
            .change_context(SinkPublishError::Publish { sink: KAFKA })
            .attach_printable(description)
    }

    /// How many records the producer holds that have no delivery report yet.
    fn queued_records(&self) -> u64 {
        u64::try_from(self.producer.in_flight_count())
            .assured("librdkafka counts the messages a producer holds with a non-negative int")
    }

    fn is_record_rejection(error: &KafkaError) -> bool {
        let KafkaError::MessageProduction(code) = error else {
            return false;
        };
        matches!(
            code,
            RDKafkaErrorCode::InvalidMessage
                | RDKafkaErrorCode::InvalidMessageSize
                | RDKafkaErrorCode::MessageSizeTooLarge
                | RDKafkaErrorCode::InvalidTimestamp
                | RDKafkaErrorCode::InvalidRecord
        )
    }
}

#[async_trait]
impl SinkLifecycle for KafkaSink {
    /// Flushes the producer's queue, which holds the records a `NO ACK` publish delivered and
    /// librdkafka has not sent yet, and any record a publish handed over and then gave up on. A
    /// flush sends them at once, whatever `linger.ms` says.
    async fn finish(&mut self, deadline: Instant) -> SinkFinishResult {
        if self.queued_records() == 0 {
            return Ok(SinkFinish::NothingHeld);
        }
        let producer = self.producer.clone();
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        let flushed = nervix_primitives::task::spawn_blocking(move || producer.flush(remaining))
            .await
            .map_err(|source| {
                Report::new(source).change_context(SinkFinishError::Flush { sink: KAFKA })
            })?;
        match flushed {
            Ok(()) => Ok(SinkFinish::Flushed),
            Err(error @ KafkaError::Flush(RDKafkaErrorCode::OperationTimedOut)) => {
                let records = self.queued_records();
                Err(Report::new(error).change_context(SinkFinishError::Unsent {
                    sink: KAFKA,
                    records,
                }))
            }
            Err(error) => {
                Err(Report::new(error).change_context(SinkFinishError::Flush { sink: KAFKA }))
            }
        }
    }

    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }
}

#[async_trait]
impl RecordSink for KafkaSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        match self.mode {
            BrokerPublishingMode::NoAck => self.publish_unconfirmed(records).await,
            BrokerPublishingMode::Ack(settings) => self.publish_confirmed(records, settings).await,
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use error_stack::{AttachmentKind, Context, FrameKind, Report};
    use nervix_connector::{
        AckConfirmation, BrokerPublishingMode, BrokerVerdict, SinkPublishError, SinkStartError,
    };
    use nervix_models::ClientConfigEntry;
    use rdkafka::{
        Timestamp as KafkaTimestamp,
        error::{KafkaError, RDKafkaErrorCode},
        message::OwnedMessage,
        producer::future_producer::Delivery,
    };

    use super::{
        KafkaMessageTimeout, KafkaMessageTimeoutError, KafkaSink, LONGEST_MESSAGE_TIMEOUT_MILLIS,
    };

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn confirmed(timeout: Duration) -> BrokerPublishingMode {
        BrokerPublishingMode::Ack(AckConfirmation {
            max_in_flight: NonZeroUsize::MIN,
            timeout,
        })
    }

    /// The record a failed delivery report hands back.
    fn returned_record() -> OwnedMessage {
        OwnedMessage::new(
            Some(b"{\"user_id\":42}".to_vec()),
            None,
            "orders".to_string(),
            KafkaTimestamp::NotAvailable,
            0,
            0,
            None,
        )
    }

    /// How an emitter describes `report`: its first printable attachment.
    fn description<C: Context>(report: &Report<C>) -> Option<String> {
        report.frames().find_map(|frame| match frame.kind() {
            FrameKind::Attachment(AttachmentKind::Printable(attachment)) => {
                Some(attachment.to_string())
            }
            FrameKind::Context(_) | FrameKind::Attachment(_) => None,
        })
    }

    #[test]
    fn a_delivered_record_is_confirmed() {
        let delivery = Delivery {
            partition: 0,
            offset: 7,
            timestamp: KafkaTimestamp::NotAvailable,
        };

        let answer = KafkaSink::broker_answer(Ok(Ok(delivery)));

        let Ok(verdict) = answer else {
            panic!("a delivered record has an answer, got {answer:?}");
        };
        assert_eq!(verdict, BrokerVerdict::Confirmed);
    }

    #[test]
    fn a_delivery_error_about_the_record_refuses_it_for_good() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);

        let answer = KafkaSink::broker_answer(Ok(Err((error.clone(), returned_record()))));

        let Ok(verdict) = answer else {
            panic!("a refused record has an answer, got {answer:?}");
        };
        assert_eq!(
            verdict,
            BrokerVerdict::Rejected {
                message: format!("kafka rejected record: {error}"),
            }
        );
    }

    #[test]
    fn any_other_delivery_error_leaves_the_record_unknown_under_its_typed_cause() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);

        let answer = KafkaSink::broker_answer(Ok(Err((error.clone(), returned_record()))));

        let Err(failure) = answer else {
            panic!("a timed out record has no answer, got {answer:?}");
        };
        assert_eq!(
            failure.current_context(),
            &SinkPublishError::Publish { sink: "kafka" }
        );
        assert_eq!(failure.downcast_ref::<KafkaError>(), Some(&error));
        assert_eq!(
            description(&failure),
            Some(format!("kafka delivery failed: {error}"))
        );
    }

    #[test]
    fn a_refused_hand_over_keeps_the_producer_error_beneath_the_publish_failure() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull);

        let failure = KafkaSink::refused_hand_over(error.clone());

        assert_eq!(
            failure.current_context(),
            &SinkPublishError::Publish { sink: "kafka" }
        );
        assert_eq!(failure.downcast_ref::<KafkaError>(), Some(&error));
        assert_eq!(
            description(&failure),
            Some(format!("kafka producer refused the record: {error}"))
        );
    }

    #[test]
    fn the_message_timeout_is_ack_timeout_rounded_up_to_its_millisecond() {
        let longest = Duration::from_millis(u64::from(LONGEST_MESSAGE_TIMEOUT_MILLIS));
        for (timeout, millis) in [
            (Duration::from_secs(3), 3_000),
            (Duration::from_millis(1_500), 1_500),
            (Duration::from_micros(1_500), 2),
            (Duration::from_nanos(1), 1),
            (longest, LONGEST_MESSAGE_TIMEOUT_MILLIS),
        ] {
            assert_eq!(
                KafkaMessageTimeout::try_from(timeout),
                Ok(KafkaMessageTimeout { millis }),
                "ACK TIMEOUT {timeout:?}"
            );
        }
    }

    #[test]
    fn an_ack_timeout_librdkafka_cannot_hold_is_refused() {
        let longest = Duration::from_millis(u64::from(LONGEST_MESSAGE_TIMEOUT_MILLIS));
        for timeout in [
            longest + Duration::from_nanos(1),
            longest + Duration::from_millis(1),
            Duration::MAX,
        ] {
            assert_eq!(
                KafkaMessageTimeout::try_from(timeout),
                Err(KafkaMessageTimeoutError::TooLong { timeout }),
                "ACK TIMEOUT {timeout:?}"
            );
        }
        assert_eq!(
            KafkaMessageTimeout::try_from(Duration::ZERO),
            Err(KafkaMessageTimeoutError::Zero)
        );
    }

    #[test]
    fn a_confirmed_producer_gives_up_on_a_record_after_ack_timeout() {
        let entries = [
            entry("bootstrap.servers", "127.0.0.1:9092"),
            entry("message.timeout.ms", "300000"),
            entry("delivery.timeout.ms", "300000"),
        ];

        let config = KafkaSink::producer_config(&entries, confirmed(Duration::from_millis(2_500)))
            .expect("an ACK TIMEOUT of seconds fits librdkafka's message timeout");

        assert_eq!(config.get("message.timeout.ms"), Some("2500"));
        assert_eq!(config.get("delivery.timeout.ms"), None);
        assert_eq!(config.get("bootstrap.servers"), Some("127.0.0.1:9092"));
    }

    #[test]
    fn an_unconfirmed_producer_keeps_the_client_delivery_bound() {
        let entries = [
            entry("bootstrap.servers", "127.0.0.1:9092"),
            entry("delivery.timeout.ms", "300000"),
        ];

        let config = KafkaSink::producer_config(&entries, BrokerPublishingMode::NoAck)
            .expect("the client entries pass through");

        assert_eq!(config.get("delivery.timeout.ms"), Some("300000"));
        assert_eq!(config.get("message.timeout.ms"), None);
    }

    #[test]
    fn an_ack_timeout_librdkafka_cannot_hold_refuses_the_sink_configuration() {
        let timeout = Duration::from_secs(30 * 24 * 60 * 60);

        let Err(error) = KafkaSink::producer_config(&[], confirmed(timeout)) else {
            panic!("an ACK TIMEOUT of 30 days exceeds librdkafka's message timeout");
        };

        assert_eq!(
            error.current_context(),
            &SinkStartError::InvalidConfiguration { sink: "kafka" }
        );
        assert_eq!(
            error.downcast_ref::<KafkaMessageTimeoutError>(),
            Some(&KafkaMessageTimeoutError::TooLong { timeout })
        );
        assert_eq!(
            description(&error),
            Some(KafkaMessageTimeoutError::TooLong { timeout }.to_string())
        );
    }

    #[test]
    fn oversized_and_invalid_records_are_definitive_rejections() {
        for code in [
            RDKafkaErrorCode::InvalidMessage,
            RDKafkaErrorCode::InvalidMessageSize,
            RDKafkaErrorCode::MessageSizeTooLarge,
            RDKafkaErrorCode::InvalidTimestamp,
            RDKafkaErrorCode::InvalidRecord,
        ] {
            assert!(KafkaSink::is_record_rejection(
                &KafkaError::MessageProduction(code)
            ));
        }
    }

    #[test]
    fn broker_availability_errors_remain_infrastructure_failures() {
        for code in [
            RDKafkaErrorCode::QueueFull,
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::MessageTimedOut,
        ] {
            assert!(!KafkaSink::is_record_rejection(
                &KafkaError::MessageProduction(code)
            ));
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "publish_tests.rs"]
mod publish_tests;
