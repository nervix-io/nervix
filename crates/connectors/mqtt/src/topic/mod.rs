//! MQTT ingestor and emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The MQTT client a configuration declares and the one set of connection options
//!   both sides build from it, its event loop and reconnect backoff,
//!   the connection it opens to the broker through the node resolver, the session, quality of
//!   service and shared subscription an ingestor reads through, the Maximum Packet Size an ingestor
//!   advertises, manual acknowledgement and local replay of publishes, the quality of service each
//!   record is published at, the Maximum Packet Size the broker declares and the rejection of a
//!   record whose PUBLISH packet would exceed it, the QoS 0 publishes an emitter's client still holds
//!   until it sends them, and confirmation classification.
//! - **Depends on.** The connector contract, the node DNS resolver, vocabulary values,
//!   `error-stack`, Tokio and `rumqttc`.
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

mod client_options;
mod connection;
mod ingestor;
#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod scripted_broker;
#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod test_fixtures;

use std::{
    collections::VecDeque,
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    time::Duration,
};

use arch_into::ArchInto as _;
use async_trait::async_trait;
use client_options::MqttClientOptions;
pub use client_options::MqttClientOptionsError;
pub use connection::MqttConnectionError;
use error_stack::{Report, ResultExt as _};
use futures_util::FutureExt as _;
pub use ingestor::{
    MqttClientIdError, MqttTopicIngestor, MqttTopicIngestorError, MqttTopicIngestorMessage,
    MqttTopicIngestorPlan, MqttTopicIngestorPosition, MqttTopicIngestorSessions,
    MqttTopicIngestorSettings,
};
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerAnswer, BrokerVerdict, ConfirmationWindow, EmitterFinish,
    EmitterFinishError, EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterOpenError,
    EmitterRecord, EmitterRecordId, EmitterStartResult, EmitterWriteError, ParsedRetryPolicy,
    PerRecordOutcome, RecordEmitter, RejectedEmitterMember, next_retry_delay,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, MqttTopicName};
use nervix_primitives::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
        watch,
    },
    task::unconstrained,
    time::{Instant, sleep, timeout_at},
};
use nervix_recovery::Discarded as _;
use rumqttc::{
    AsyncClient, ClientError as MqttClientError, ConnAck, Event, Incoming,
    PubAckReason as MqttPubAckReason, PubRecReason as MqttPubRecReason, PublishNotice,
    PublishNoticeError, PublishOptions, SessionMode, ValidatedTopic,
};
use thiserror::Error;
use tracing::warn;

const MQTT: &str = "mqtt";

/// The largest Remaining Length an MQTT packet can declare. Its variable byte integer holds at most
/// four bytes of seven value bits each.
const MQTT_MAX_REMAINING_LENGTH: usize = 268_435_455;

/// The quality of service an MQTT emitter publishes at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttPublishingMode {
    Qos0,
    Qos1(AckConfirmation),
    Qos2(AckConfirmation),
}

impl MqttPublishingMode {
    fn publish_options(self) -> PublishOptions {
        match self {
            Self::Qos0 => PublishOptions::at_most_once(),
            Self::Qos1(_) => PublishOptions::at_least_once(),
            Self::Qos2(_) => PublishOptions::exactly_once(),
        }
    }
}

/// What one MQTT emitter publishes through: its client entries, topic, quality of service, the
/// backoff its event loop reconnects on, the client id to use when the configuration declares
/// none, and the node resolver every connection resolves the broker host through.
pub struct MqttTopicEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub topic: MqttTopicName,
    pub mode: MqttPublishingMode,
    pub retry_policy: ParsedRetryPolicy,
    pub default_client_id: String,
    pub dns: DnsResolver,
}

pub struct MqttTopicEmitter {
    client: AsyncClient,
    topic: MqttTopicName,
    mode: MqttPublishingMode,
    framing: MqttPublishFraming,
    broker_limit: Arc<MqttBrokerPacketLimit>,
    /// The QoS 0 publishes the client took and has not yet sent, which stay empty under QoS 1
    /// and 2.
    unsent: MqttUnsentPublishes,
    eventloop_shutdown: watch::Sender<bool>,
}

/// Why one record is not a PUBLISH packet this emitter can send.
#[derive(Debug, PartialEq, Eq, Error)]
enum MqttRecordError {
    #[error(
        "MQTT PUBLISH packet of {packet_bytes} bytes exceeds the broker's maximum packet size of \
         {maximum} bytes"
    )]
    PacketTooLarge {
        packet_bytes: usize,
        maximum: NonZeroU32,
    },
    #[error(
        "MQTT PUBLISH payload of {payload_bytes} bytes does not fit the largest packet the \
         protocol can express"
    )]
    BeyondProtocol { payload_bytes: usize },
}

type MqttRecordResult<T> = Result<T, Report<MqttRecordError>>;

/// What the client writes around every payload this emitter publishes, which its topic and quality
/// of service fix for the emitter's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MqttPublishFraming {
    /// The variable header in front of the payload: the topic's two-byte length prefix and its
    /// bytes, the packet identifier QoS 1 and 2 carry, and the one-byte length of the empty
    /// property section.
    variable_header_bytes: usize,
}

impl MqttPublishFraming {
    fn new(topic: &MqttTopicName, mode: MqttPublishingMode) -> Self {
        const TOPIC_LENGTH_PREFIX_BYTES: usize = 2;
        const PROPERTY_LENGTH_BYTES: usize = 1;
        const IN_MEMORY: &str = "a topic name and a few framing bytes fit in usize";
        let packet_identifier_bytes = match mode {
            MqttPublishingMode::Qos0 => 0,
            MqttPublishingMode::Qos1(_) | MqttPublishingMode::Qos2(_) => 2,
        };
        let topic_bytes = TOPIC_LENGTH_PREFIX_BYTES
            .checked_add(topic.as_str().len())
            .assured(IN_MEMORY);
        let identified_bytes = topic_bytes
            .checked_add(packet_identifier_bytes)
            .assured(IN_MEMORY);
        let variable_header_bytes = identified_bytes
            .checked_add(PROPERTY_LENGTH_BYTES)
            .assured(IN_MEMORY);
        Self {
            variable_header_bytes,
        }
    }

    /// The size of the whole PUBLISH packet the client writes for a payload of `payload_bytes`:
    /// the fixed header byte, the Remaining Length, the variable header and the payload.
    fn packet_bytes(self, payload_bytes: usize) -> MqttRecordResult<usize> {
        const BOUNDED: &str = "the remaining length was checked against the MQTT maximum above";
        let Some(remaining_bytes) = self.variable_header_bytes.checked_add(payload_bytes) else {
            return Err(Report::new(MqttRecordError::BeyondProtocol {
                payload_bytes,
            }));
        };
        if remaining_bytes > MQTT_MAX_REMAINING_LENGTH {
            return Err(Report::new(MqttRecordError::BeyondProtocol {
                payload_bytes,
            }));
        }
        let length_bytes: usize = match remaining_bytes {
            0..=127 => 1,
            128..=16_383 => 2,
            16_384..=2_097_151 => 3,
            _ => 4,
        };
        let fixed_header_bytes = length_bytes.checked_add(1).verified(BOUNDED);
        let packet_bytes = remaining_bytes
            .checked_add(fixed_header_bytes)
            .verified(BOUNDED);
        Ok(packet_bytes)
    }
}

/// The Maximum Packet Size the broker declared in the CONNACK of the client's current connection.
///
/// The broker declares it on every connection, and a reconnect may change or withdraw it, so the
/// event loop replaces it with each CONNACK and a publish reads the latest one. The atomic holds
/// the declared size, or zero while the broker has declared none: MQTT forbids a Maximum Packet
/// Size of zero, so zero never stands for a declared value. It is a single independent value, so
/// relaxed ordering is its whole contract.
#[derive(Debug, Default)]
struct MqttBrokerPacketLimit(AtomicU32);

impl MqttBrokerPacketLimit {
    /// The value the atomic holds while the broker has declared no maximum.
    const UNDECLARED: u32 = 0;

    fn declare(&self, connack: &ConnAck) {
        let declared = match &connack.properties {
            Some(properties) => properties.max_packet_size,
            None => None,
        };
        self.0
            .store(declared.unwrap_or(Self::UNDECLARED), Ordering::Relaxed);
    }

    fn declared(&self) -> Option<NonZeroU32> {
        NonZeroU32::new(self.0.load(Ordering::Relaxed))
    }

    /// Accepts a PUBLISH packet of `packet_bytes` bytes, or names the declared maximum it exceeds.
    fn admit(&self, packet_bytes: usize) -> MqttRecordResult<()> {
        let Some(maximum) = self.declared() else {
            return Ok(());
        };
        let maximum_bytes: usize = maximum.get().arch_into();
        if packet_bytes <= maximum_bytes {
            return Ok(());
        }
        Err(Report::new(MqttRecordError::PacketTooLarge {
            packet_bytes,
            maximum,
        }))
    }
}

/// The broker's answer for one QoS 1 or 2 record, which resolves when its `PUBACK` or the last
/// packet of its QoS 2 handshake arrives, or when the client gives the record up.
type MqttAnswer = Pin<Box<dyn Future<Output = BrokerAnswer> + Send>>;

/// The flush of one QoS 0 publish the client took, which ends once the client's event loop has
/// written the publish and flushed the connection, or failed to: the write or the flush failed, or
/// a clean session's next connection dropped the publish a lost connection left behind.
type MqttFlush = Pin<Box<dyn Future<Output = Result<(), PublishNoticeError>> + Send>>;

/// The QoS 0 publishes an emitter reported delivered that its client has not yet sent.
///
/// A QoS 0 record is delivered once the client takes it, and the client's event loop writes it to
/// the broker connection afterwards: at once while it is connected, or once it connects again. The
/// emitter keeps the flush of every publish the client took until that flush ends, so finishing the
/// emitter can wait for what the client still holds. Dropping the ended flushes at the start of each
/// publish keeps one flush for each publish the client still holds: the one request its queue
/// takes, and those a lost connection left for the next.
#[derive(Default)]
struct MqttUnsentPublishes {
    flushes: VecDeque<MqttFlush>,
}

impl MqttUnsentPublishes {
    fn hold(&mut self, flush: MqttFlush) {
        self.flushes.push_back(flush);
    }

    /// Drops every flush that has ended, without waiting, and answers the first failure among
    /// them. Visiting each flush once leaves every pending one held even if a surrounding finish
    /// is cancelled.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "polls the MQTT driver's flush notices without waiting; their \
                      implementations belong to the driver"
        )
    )]
    fn drop_ended(&mut self) -> Option<PublishNoticeError> {
        let mut failure = None;
        let flushes = std::mem::take(&mut self.flushes);
        for mut flush in flushes {
            match unconstrained(&mut flush).now_or_never() {
                Some(Ok(())) => {}
                Some(Err(error)) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
                None => self.flushes.push_back(flush),
            }
        }
        failure
    }

    /// Drops the flushes that ended while the client was not being finished. One that failed lost
    /// its publish with the connection whose failure the event loop reported.
    fn drop_settled(&mut self) {
        self.drop_ended().discarded(
            "the event loop reported the lost connection that failed the flush as the emitter's \
             transient error and an error event",
        );
    }

    /// Waits until the client has sent every publish it still holds, or `deadline` passes.
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        self.drop_settled();
        if self.flushes.is_empty() {
            return Ok(EmitterFinish::NothingHeld);
        }
        let mut failure = None;
        while let Some(flush) = self.flushes.front_mut() {
            nervix_primitives::task::consume_budget().await;
            let waited = timeout_at(deadline, flush).await;
            let Ok(ended) = waited else {
                let failed = self.drop_ended();
                if failure.is_none() {
                    failure = failed;
                }
                break;
            };
            self.flushes.pop_front();
            if let Err(error) = ended
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        self.finish_outcome(failure)
    }

    /// What a finish that saw `failure` among the flushes it waited for answers: the count still
    /// held when its deadline passed, then a failed flush, then a flush of everything held.
    fn finish_outcome(&self, failure: Option<PublishNoticeError>) -> EmitterFinishResult {
        if !self.flushes.is_empty() {
            let unsent = EmitterFinishError::Unsent {
                connector: MQTT,
                records: self.flushes.len().arch_into(),
            };
            let report = match failure {
                Some(error) => Report::new(error).change_context(unsent),
                None => Report::new(unsent),
            };
            return Err(report);
        }
        match failure {
            Some(error) => {
                Err(Report::new(error)
                    .change_context(EmitterFinishError::Flush { connector: MQTT }))
            }
            None => Ok(EmitterFinish::Flushed),
        }
    }
}

/// The delay before the event loop's next reconnect attempt, doubling up to the declared ceiling.
struct MqttReconnectBackoff {
    policy: ParsedRetryPolicy,
    next: Duration,
}

impl MqttReconnectBackoff {
    fn from_policy(policy: ParsedRetryPolicy) -> Self {
        Self {
            policy,
            next: policy.backoff,
        }
    }

    fn reset(&mut self) {
        self.next = self.policy.backoff;
    }

    fn take_next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = next_retry_delay(self.next, self.policy);
        delay
    }
}

impl MqttTopicEmitter {
    pub fn new(config: MqttTopicEmitterConfig, host: EmitterHost) -> EmitterStartResult<Self> {
        let mode = config.mode;
        let (client, mut eventloop) =
            Self::client_from_config(&config.config, &config.default_client_id, mode, config.dns)?;
        let retry_policy = config.retry_policy;
        let broker_limit = Arc::new(MqttBrokerPacketLimit::default());
        let declared_limit = broker_limit.clone();
        let (eventloop_shutdown, mut eventloop_shutdown_rx) = watch::channel(false);
        nervix_primitives::task::spawn(async move {
            let mut backoff = MqttReconnectBackoff::from_policy(retry_policy);
            loop {
                nervix_primitives::task::consume_budget().await;
                let polled = nervix_primitives::select! {
                    changed = eventloop_shutdown_rx.changed() => {
                        if changed.is_err() || *eventloop_shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    polled = eventloop.poll() => polled,
                };
                match polled {
                    Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                        declared_limit.declare(&connack);
                        backoff.reset();
                        host.clear_transient_error();
                    }
                    Ok(Event::Incoming(_) | Event::Outgoing(_) | Event::Auth(_)) => {
                        backoff.reset();
                        host.clear_transient_error();
                    }
                    Err(error) => {
                        let failure = MqttConnectionError::report(error);
                        let reason = failure.current_context().to_string();
                        let wait = backoff.take_next_delay();
                        host.record_transient_error(reason.clone(), wait);
                        host.report_error(format!(
                            "mqtt event loop failed; reconnecting in {}: {reason}",
                            humantime::format_duration(wait),
                        ));
                        warn!(
                            error = %reason,
                            retry_in = %humantime::format_duration(wait),
                            "mqtt emitter event loop reconnecting"
                        );
                        nervix_primitives::select! {
                            changed = eventloop_shutdown_rx.changed() => {
                                if changed.is_err() || *eventloop_shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = sleep(wait) => {}
                        }
                    }
                }
            }
        });
        ValidatedTopic::new(config.topic.as_str()).map_err(Self::config_error)?;
        let framing = MqttPublishFraming::new(&config.topic, mode);
        Ok(Self {
            client,
            topic: config.topic,
            mode,
            framing,
            broker_limit,
            unsent: MqttUnsentPublishes::default(),
            eventloop_shutdown,
        })
    }

    /// The client and event loop of the connection `config` declares: the options it shares with
    /// every ingestor, completed with the session the emitter's quality of service needs and room
    /// for the publishes its confirmation window lets wait.
    fn client_from_config(
        config: &[ClientConfigEntry],
        default_client_id: &str,
        mode: MqttPublishingMode,
        dns: DnsResolver,
    ) -> EmitterStartResult<(AsyncClient, rumqttc::EventLoop)> {
        let invalid = || EmitterOpenError::InvalidConfiguration { connector: MQTT };
        let mut client = MqttClientOptions::from_config(config, default_client_id)
            .change_context_lazy(invalid)?;
        let session = match mode {
            MqttPublishingMode::Qos0 => SessionMode::Clean,
            MqttPublishingMode::Qos1 { .. } | MqttPublishingMode::Qos2 { .. } => {
                SessionMode::Persistent
            }
        };
        client.options_mut().set_session_mode(session);
        let request_capacity = match mode {
            MqttPublishingMode::Qos0 => NonZeroUsize::MIN,
            MqttPublishingMode::Qos1(confirmation) | MqttPublishingMode::Qos2(confirmation) => {
                confirmation.max_in_flight
            }
        };
        let options = client.dialing_through(dns);
        AsyncClient::builder(options)
            .capacity(request_capacity.get())
            .try_build()
            .map_err(|error| Report::new(error).change_context(invalid()))
    }

    /// Accepts a payload of `payload_bytes` whose PUBLISH packet the protocol can express and the
    /// broker declared it will receive, or says why no packet of it can be sent.
    fn admit(&self, payload_bytes: usize) -> MqttRecordResult<()> {
        let packet_bytes = self.framing.packet_bytes(payload_bytes)?;
        self.broker_limit.admit(packet_bytes)
    }

    /// Whether the client refused to take a record because of the record itself, so handing it over
    /// again cannot succeed.
    fn is_record_client_rejection(error: &MqttClientError) -> bool {
        matches!(error, MqttClientError::InvalidRequest(_))
    }

    /// Whether the broker refused a record for good: a v5 reason code about the record itself.
    fn is_record_notice_rejection(error: &PublishNoticeError) -> bool {
        matches!(
            error,
            PublishNoticeError::V5PubAck(
                MqttPubAckReason::NotAuthorized
                    | MqttPubAckReason::TopicNameInvalid
                    | MqttPubAckReason::PayloadFormatInvalid
            ) | PublishNoticeError::V5PubRec(
                MqttPubRecReason::NotAuthorized
                    | MqttPubRecReason::TopicNameInvalid
                    | MqttPubRecReason::PayloadFormatInvalid
            )
        )
    }

    /// The broker's answer for a QoS 1 or 2 record whose handshake ended as `completion`.
    ///
    /// A completed handshake confirms the record, and a reason code about the record itself refuses
    /// it for good. Any other ending leaves the record's outcome unknown: the client, its
    /// connection or its session failed, or the broker's reason code is not about the record.
    fn broker_answer(completion: Result<(), PublishNoticeError>) -> BrokerAnswer {
        match completion {
            Ok(()) => Ok(BrokerVerdict::Confirmed),
            Err(error) if Self::is_record_notice_rejection(&error) => Ok(BrokerVerdict::Rejected {
                message: format!("mqtt rejected record: {error}"),
            }),
            Err(error) => {
                let description = format!("mqtt publish failed: {error}");
                Err(Report::new(error)
                    .change_context(EmitterWriteError::Publish { connector: MQTT })
                    .attach_printable(description))
            }
        }
    }

    /// Waits for the end of the handshake `notice` tracks and reads the broker's answer from it.
    async fn await_answer(notice: PublishNotice) -> BrokerAnswer {
        let completion = notice.wait_completion_async().await;
        Self::broker_answer(completion)
    }

    /// The failure that ends a publish whose record the client refused to take for a reason that
    /// is not the record's own, such as a full request queue.
    fn refused_hand_over(error: MqttClientError) -> Report<EmitterWriteError> {
        let description = format!("mqtt client refused the record: {error}");
        Report::new(error)
            .change_context(EmitterWriteError::Publish { connector: MQTT })
            .attach_printable(description)
    }

    /// Publishes `records` at QoS 0, where the client taking a record delivers it, and keeps the
    /// flush of every record the client took until the client sends it.
    async fn publish_at_most_once(
        &mut self,
        records: Vec<EmitterRecord>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        self.unsent.drop_settled();
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            // The client would fail its connection on a packet the broker refuses to receive, so
            // a record whose packet cannot be sent is rejected before it is handed over.
            if let Err(error) = self.admit(record.payload.len()) {
                outcome.reject(RejectedEmitterMember::external(
                    record_id,
                    occurred_at,
                    format!("mqtt rejected record: {error}"),
                ));
                continue;
            }
            let handed_over = self.client.try_publish_tracked(
                self.topic.as_str(),
                record.payload,
                PublishOptions::at_most_once(),
            );
            match handed_over {
                Ok(notice) => {
                    self.unsent.hold(Box::pin(notice.wait_completion_async()));
                    outcome.deliver(record_id);
                }
                Err(error) if Self::is_record_client_rejection(&error) => {
                    outcome.reject(RejectedEmitterMember::external(
                        record_id,
                        occurred_at,
                        format!("mqtt rejected record: {error}"),
                    ));
                }
                Err(error) => {
                    outcome.fail(Self::refused_hand_over(error));
                    return outcome;
                }
            }
        }
        outcome
    }

    /// Publishes `records` at QoS 1 or 2 and awaits the broker's answers through the confirmation
    /// window `settings` declare.
    ///
    /// Every way the publish can fail ends it through the window, which first settles each answer
    /// the broker already gave, so the host's retry never resends a record the broker confirmed.
    async fn publish_confirmed(
        &mut self,
        records: Vec<EmitterRecord>,
        settings: AckConfirmation,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let mut window: ConfirmationWindow<MqttAnswer> =
            ConfirmationWindow::new(MQTT, settings, records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            // The client would fail its connection on a packet the broker refuses to receive, so
            // a record whose packet cannot be sent is rejected before it is handed over.
            if let Err(error) = self.admit(record.payload.len()) {
                window.reject(RejectedEmitterMember::external(
                    record_id,
                    occurred_at,
                    format!("mqtt rejected record: {error}"),
                ));
                continue;
            }
            let handed_over = self.client.try_publish_tracked(
                self.topic.as_str(),
                record.payload,
                self.mode.publish_options(),
            );
            let notice = match handed_over {
                Ok(notice) => notice,
                Err(error) if Self::is_record_client_rejection(&error) => {
                    window.reject(RejectedEmitterMember::external(
                        record_id,
                        occurred_at,
                        format!("mqtt rejected record: {error}"),
                    ));
                    continue;
                }
                Err(error) => return window.fail(Self::refused_hand_over(error)),
            };
            let answer: MqttAnswer = Box::pin(Self::await_answer(notice));
            if let Err(failure) = window.admit(record_id, occurred_at, answer).await {
                return window.fail(failure);
            }
        }
        window.finish().await
    }

    fn config_error(error: impl std::fmt::Display) -> Report<EmitterOpenError> {
        Report::new(EmitterOpenError::InvalidConfiguration { connector: MQTT })
            .attach_printable(error.to_string())
    }
}

impl Drop for MqttTopicEmitter {
    fn drop(&mut self) {
        self.eventloop_shutdown.send_replace(true);
    }
}

impl MqttTopicEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Mqtt(nervix_models::MqttEmitterEntity::Topic);
}

#[async_trait]
impl EmitterLifecycle for MqttTopicEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// A QoS 0 publish is delivered once the client takes it, and the event loop sends it later,
    /// so the client can still hold delivered publishes, which the finish waits for the event loop
    /// to send. A QoS 1 or 2 publish is delivered only once the broker answered for it.
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        match self.mode {
            MqttPublishingMode::Qos0 => self.unsent.finish(deadline).await,
            MqttPublishingMode::Qos1(_) | MqttPublishingMode::Qos2(_) => {
                Ok(EmitterFinish::NothingHeld)
            }
        }
    }

    /// A publish failure leaves the client connected: its event loop reconnects on its own
    /// schedule, and dropping it would discard the session the broker still holds.
    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }
}

#[async_trait]
impl RecordEmitter for MqttTopicEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        match self.mode {
            MqttPublishingMode::Qos0 => self.publish_at_most_once(records).await,
            MqttPublishingMode::Qos1(settings) | MqttPublishingMode::Qos2(settings) => {
                self.publish_confirmed(records, settings).await
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use error_stack::{AttachmentKind, FrameKind};
    use meticulous::ResultExt as _;
    use nervix_connector::{ConfirmationWindowError, PerRecordOutcomeParts};
    use nervix_models::Timestamp;

    use super::*;
    use crate::topic::{scripted_broker::ScriptedBroker, test_fixtures::DnsFixture};

    #[test]
    fn a_completed_handshake_confirms_the_record() {
        let verdict = MqttTopicEmitter::broker_answer(Ok(()))
            .expect("a completed handshake is the broker's verdict on the record");

        assert_eq!(verdict, BrokerVerdict::Confirmed);
    }

    #[test]
    fn a_reason_code_about_the_record_refuses_it_for_good() {
        let refusals = [
            (
                PublishNoticeError::V5PubAck(MqttPubAckReason::NotAuthorized),
                "mqtt rejected record: v5 puback returned non-success reason: NotAuthorized",
            ),
            (
                PublishNoticeError::V5PubAck(MqttPubAckReason::TopicNameInvalid),
                "mqtt rejected record: v5 puback returned non-success reason: TopicNameInvalid",
            ),
            (
                PublishNoticeError::V5PubAck(MqttPubAckReason::PayloadFormatInvalid),
                "mqtt rejected record: v5 puback returned non-success reason: PayloadFormatInvalid",
            ),
            (
                PublishNoticeError::V5PubRec(MqttPubRecReason::NotAuthorized),
                "mqtt rejected record: v5 pubrec returned non-success reason: NotAuthorized",
            ),
            (
                PublishNoticeError::V5PubRec(MqttPubRecReason::TopicNameInvalid),
                "mqtt rejected record: v5 pubrec returned non-success reason: TopicNameInvalid",
            ),
            (
                PublishNoticeError::V5PubRec(MqttPubRecReason::PayloadFormatInvalid),
                "mqtt rejected record: v5 pubrec returned non-success reason: PayloadFormatInvalid",
            ),
        ];
        for (refusal, message) in refusals {
            let verdict = MqttTopicEmitter::broker_answer(Err(refusal))
                .expect("a refusal of the record itself is the broker's verdict on it");

            assert_eq!(
                verdict,
                BrokerVerdict::Rejected {
                    message: message.to_string()
                }
            );
        }
    }

    fn topic(name: &str) -> MqttTopicName {
        MqttTopicName::parse(name).expect("the test topic is a valid topic name")
    }

    fn confirmed() -> AckConfirmation {
        AckConfirmation {
            max_in_flight: NonZeroUsize::MIN,
            timeout: Duration::from_secs(1),
        }
    }

    /// The size of the packet the client itself encodes for `payload_bytes` on `topic`, with the
    /// packet identifier it assigns to QoS 1 and 2 publishes.
    fn encoded_packet_bytes(topic: &str, qos: rumqttc::QoS, payload_bytes: usize) -> usize {
        let mut publish = rumqttc::Publish::new(topic, qos, vec![0_u8; payload_bytes], None);
        if qos != rumqttc::QoS::AtMostOnce {
            publish.pkid = 1;
        }
        publish.size()
    }

    #[test]
    fn packet_size_matches_what_the_client_writes_across_remaining_length_widths() {
        let cases = [
            (MqttPublishingMode::Qos0, rumqttc::QoS::AtMostOnce),
            (
                MqttPublishingMode::Qos1(confirmed()),
                rumqttc::QoS::AtLeastOnce,
            ),
            (
                MqttPublishingMode::Qos2(confirmed()),
                rumqttc::QoS::ExactlyOnce,
            ),
        ];
        for (mode, qos) in cases {
            let framing = MqttPublishFraming::new(&topic("batched_out"), mode);
            for payload_bytes in [0, 1, 100, 127, 128, 16_000, 16_383, 16_384, 2_097_000] {
                let packet_bytes = framing
                    .packet_bytes(payload_bytes)
                    .expect("every tested payload fits an MQTT packet");
                assert_eq!(
                    packet_bytes,
                    encoded_packet_bytes("batched_out", qos, payload_bytes),
                    "{mode:?} with a {payload_bytes}-byte payload"
                );
            }
        }
    }

    #[test]
    fn a_payload_past_the_largest_remaining_length_has_no_packet() {
        let framing = MqttPublishFraming::new(&topic("t"), MqttPublishingMode::Qos0);
        // Two topic-length bytes, one topic byte and one property-length byte precede the payload.
        let largest_payload = MQTT_MAX_REMAINING_LENGTH - 4;

        let largest_packet = framing
            .packet_bytes(largest_payload)
            .expect("the largest remaining length is still a packet");
        assert_eq!(largest_packet, MQTT_MAX_REMAINING_LENGTH + 5);
        for payload_bytes in [largest_payload + 1, usize::MAX] {
            let error = framing
                .packet_bytes(payload_bytes)
                .expect_err("a payload past the largest remaining length has no packet");
            assert_eq!(
                error.current_context(),
                &MqttRecordError::BeyondProtocol { payload_bytes }
            );
        }
    }

    fn connack(max_packet_size: Option<u32>) -> ConnAck {
        let properties = rumqttc::ConnAckProperties {
            session_expiry_interval: None,
            receive_max: None,
            max_qos: None,
            retain_available: None,
            max_packet_size,
            assigned_client_identifier: None,
            topic_alias_max: None,
            reason_string: None,
            user_properties: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: None,
        };
        ConnAck {
            session_present: false,
            code: rumqttc::ConnectReturnCode::Success,
            properties: Some(properties),
        }
    }

    #[test]
    fn the_latest_connack_decides_which_packets_the_broker_receives() {
        let limit = MqttBrokerPacketLimit::default();
        assert!(
            limit.admit(usize::MAX).is_ok(),
            "no CONNACK declared a limit yet"
        );

        limit.declare(&connack(Some(1_048_576)));
        assert!(limit.admit(1_048_576).is_ok());
        let refused = limit
            .admit(1_048_577)
            .expect_err("a packet past the declared maximum is refused");
        assert_eq!(
            refused.current_context(),
            &MqttRecordError::PacketTooLarge {
                packet_bytes: 1_048_577,
                maximum: NonZeroU32::new(1_048_576).expect("the test maximum is positive"),
            }
        );

        // A reconnect to a broker that declares no maximum withdraws the previous one.
        limit.declare(&connack(None));
        assert!(limit.admit(1_048_577).is_ok());
        limit.declare(&ConnAck {
            session_present: true,
            code: rumqttc::ConnectReturnCode::Success,
            properties: None,
        });
        assert_eq!(limit.declared(), None);
    }

    /// The characters a generated topic is drawn from: every character a topic name holds.
    const TOPIC_CHARACTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-_~.";

    /// The Remaining Lengths at which its variable byte integer grows by one byte.
    const REMAINING_LENGTH_WIDTHS: [usize; 3] = [128, 16_384, 2_097_152];

    /// A generated payload length: small, beside a Remaining Length width boundary, or beside the
    /// largest Remaining Length the protocol expresses.
    #[derive(Debug, bolero::TypeGenerator)]
    enum PayloadLength {
        Small(u16),
        NearWidth { width: u8, offset: i8 },
        NearProtocolLimit(i16),
    }

    /// A generated PUBLISH: its topic, quality of service, payload length, and the Maximum Packet
    /// Size the broker's latest CONNACK declared.
    #[derive(Debug, bolero::TypeGenerator)]
    struct PublishCase {
        #[generator(bolero::generator::produce_with::<Vec<u8>>().len(1_usize..=128))]
        topic: Vec<u8>,
        qos: u8,
        payload: PayloadLength,
        broker_maximum: Option<u32>,
    }

    impl PublishCase {
        fn topic(&self) -> String {
            let mut topic = String::with_capacity(self.topic.len());
            for byte in &self.topic {
                let index = usize::from(*byte) % TOPIC_CHARACTERS.len();
                topic.push(char::from(TOPIC_CHARACTERS[index]));
            }
            topic
        }

        fn mode(&self) -> (MqttPublishingMode, rumqttc::QoS) {
            match self.qos % 3 {
                0 => (MqttPublishingMode::Qos0, rumqttc::QoS::AtMostOnce),
                1 => (
                    MqttPublishingMode::Qos1(confirmed()),
                    rumqttc::QoS::AtLeastOnce,
                ),
                _ => (
                    MqttPublishingMode::Qos2(confirmed()),
                    rumqttc::QoS::ExactlyOnce,
                ),
            }
        }

        /// The payload length, placed relative to the Remaining Length that `header` bytes of
        /// variable header leave for it.
        fn payload_bytes(&self, header: usize) -> usize {
            let (target, offset) = match self.payload {
                PayloadLength::Small(bytes) => return usize::from(bytes),
                PayloadLength::NearWidth { width, offset } => {
                    let boundary = REMAINING_LENGTH_WIDTHS[usize::from(width) % 3];
                    (boundary, i64::from(offset))
                }
                PayloadLength::NearProtocolLimit(offset) => {
                    (MQTT_MAX_REMAINING_LENGTH, i64::from(offset))
                }
            };
            let target = i64::try_from(target).assured("the protocol's lengths fit i64");
            let header = i64::try_from(header).assured("a header of a short topic fits i64");
            let payload = (target - header + offset).max(0);
            usize::try_from(payload).assured("the payload length is not negative")
        }
    }

    /// The bytes of a variable byte integer holding `value`: seven value bits a byte.
    fn variable_integer_bytes(value: usize) -> usize {
        let mut bytes = 1;
        let mut rest = value >> 7;
        while rest > 0 {
            bytes += 1;
            rest >>= 7;
        }
        bytes
    }

    /// The size an emitter measures a PUBLISH at is the length of the packet the client writes for it:
    /// the fixed header, a Remaining Length of the variable header and payload, the topic, the
    /// packet identifier QoS 1 and 2 carry, the property length and the payload, across every
    /// Remaining Length width. A payload past the largest Remaining Length has no packet, and the
    /// broker's latest declared maximum admits exactly the packets at most that size.
    #[test]
    fn bolero_measured_packets_are_the_bytes_the_client_writes() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(512)
            .with_type::<PublishCase>()
            .for_each(|case| {
                let name = case.topic();
                let (mode, qos) = case.mode();
                let framing = MqttPublishFraming::new(&topic(&name), mode);
                let identifier = if qos == rumqttc::QoS::AtMostOnce {
                    0
                } else {
                    2
                };
                let header = 2 + name.len() + identifier + 1;
                assert_eq!(framing.variable_header_bytes, header);
                let payload_bytes = case.payload_bytes(header);
                let remaining = header + payload_bytes;
                let measured = framing.packet_bytes(payload_bytes);
                if remaining > MQTT_MAX_REMAINING_LENGTH {
                    let Err(error) = measured else {
                        panic!("a remaining length of {remaining} bytes has no packet");
                    };
                    assert_eq!(
                        error.current_context(),
                        &MqttRecordError::BeyondProtocol { payload_bytes }
                    );
                    return;
                }
                let Ok(packet_bytes) = measured else {
                    panic!("a remaining length of {remaining} bytes is a packet");
                };
                assert_eq!(
                    packet_bytes,
                    1 + variable_integer_bytes(remaining) + remaining
                );
                if payload_bytes <= 2_200_000 {
                    let mut publish =
                        rumqttc::Publish::new(name.as_str(), qos, vec![0_u8; payload_bytes], None);
                    if qos != rumqttc::QoS::AtMostOnce {
                        publish.pkid = 1;
                    }
                    let mut written = bytes::BytesMut::new();
                    let length = publish
                        .write(&mut written)
                        .assured("the client writes every packet within the protocol's length");
                    assert_eq!(written.len(), length);
                    assert_eq!(packet_bytes, written.len(), "{payload_bytes}-byte payload");
                }

                let limit = MqttBrokerPacketLimit::default();
                limit.declare(&connack(case.broker_maximum));
                let maximum = match case.broker_maximum {
                    Some(maximum) => NonZeroU32::new(maximum),
                    None => None,
                };
                let admitted = limit.admit(packet_bytes);
                match maximum {
                    Some(maximum)
                        if packet_bytes
                            > usize::try_from(maximum.get()).assured("u32 fits usize") =>
                    {
                        let Err(refused) = admitted else {
                            panic!("a {packet_bytes}-byte packet passed a {maximum}-byte maximum");
                        };
                        assert_eq!(
                            refused.current_context(),
                            &MqttRecordError::PacketTooLarge {
                                packet_bytes,
                                maximum,
                            }
                        );
                    }
                    Some(_) | None => assert!(admitted.is_ok()),
                }
            });
    }

    /// A host that ignores what the emitter reports, for tests that only read its publish outcomes.
    struct SilentHost;

    impl nervix_connector::EmitterTransientErrorStatus for SilentHost {
        fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}

        fn clear_transient_error(&self) {}
    }

    impl nervix_connector::EmitterEventReporter for SilentHost {
        fn report_error(&self, _message: String) {}
    }

    impl nervix_connector::EmitterStagingDirectory for SilentHost {
        fn staging_directory(&self) -> std::path::PathBuf {
            std::env::temp_dir()
        }
    }

    impl nervix_connector::EmitterBoundedExecution for SilentHost {
        fn executor(&self) -> nervix_execution::Executor {
            nervix_execution::Executor::default()
        }
    }

    impl nervix_connector::EmitterGeneralErrorHandler for SilentHost {
        fn handle_general_error(
            &self,
            _acks: &nervix_connector::EmitterRecordAcknowledgements,
            _reason: String,
        ) {
        }
    }

    /// A host that keeps the latest transient error the emitter records, for tests that read it.
    struct RecordingHost {
        transient_error: watch::Sender<Option<String>>,
    }

    impl nervix_connector::EmitterTransientErrorStatus for RecordingHost {
        fn record_transient_error(&self, reason: String, _retry_after: Duration) {
            self.transient_error.send_replace(Some(reason));
        }

        fn clear_transient_error(&self) {}
    }

    impl nervix_connector::EmitterEventReporter for RecordingHost {
        fn report_error(&self, _message: String) {}
    }

    impl nervix_connector::EmitterStagingDirectory for RecordingHost {
        fn staging_directory(&self) -> std::path::PathBuf {
            std::env::temp_dir()
        }
    }

    impl nervix_connector::EmitterBoundedExecution for RecordingHost {
        fn executor(&self) -> nervix_execution::Executor {
            nervix_execution::Executor::default()
        }
    }

    impl nervix_connector::EmitterGeneralErrorHandler for RecordingHost {
        fn handle_general_error(
            &self,
            _acks: &nervix_connector::EmitterRecordAcknowledgements,
            _reason: String,
        ) {
        }
    }

    /// The configuration of a QoS 0 emitter on `topic` whose client connects to `addr` through `dns`,
    /// retrying slowly enough that a test never sees a second attempt.
    fn qos0_config(addr: &str, topic_name: &str, dns: DnsResolver) -> MqttTopicEmitterConfig {
        MqttTopicEmitterConfig {
            config: vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.to_string(),
            }],
            topic: topic(topic_name),
            mode: MqttPublishingMode::Qos0,
            retry_policy: ParsedRetryPolicy {
                backoff: Duration::from_secs(60),
                max_backoff: Duration::from_secs(60),
            },
            default_client_id: "nervix-mqtt-emitter-test".to_string(),
            dns,
        }
    }

    fn qos0_emitter(addr: &str, topic_name: &str, dns: DnsResolver) -> MqttTopicEmitter {
        MqttTopicEmitter::new(
            qos0_config(addr, topic_name, dns),
            EmitterHost::new(SilentHost),
        )
        .expect("the test emitter config is valid")
    }

    fn record(index: usize, payload_bytes: usize) -> EmitterRecord {
        EmitterRecord::new(
            EmitterRecordId::new(index),
            None,
            vec![b'x'; payload_bytes],
            Vec::new(),
            Timestamp::from_unix_nanos(0),
        )
    }

    #[nervix_primitives::test]
    async fn a_record_the_broker_would_refuse_is_rejected_before_the_client_sees_it() {
        // Nothing listens on the discard port, so the client never connects and the accepted
        // record waits in its request queue.
        let fixture = DnsFixture::start().await;
        let mut connector = qos0_emitter("mqtt://127.0.0.1:9", "limited", fixture.dns.clone());
        connector.broker_limit.declare(&connack(Some(64)));

        // The topic `limited` takes 12 bytes around a QoS 0 payload: the fixed header byte, one
        // remaining-length byte, the topic with its length prefix, and the property length.
        let outcome = connector
            .publish(vec![record(0, 52), record(1, 53)])
            .await
            .into_parts();

        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        let [rejected] = outcome.rejected.as_slice() else {
            panic!(
                "exactly the oversize record is rejected: {:?}",
                outcome.rejected
            );
        };
        assert_eq!(rejected.id, EmitterRecordId::new(1));
        assert_eq!(
            rejected.error.message,
            "mqtt rejected record: MQTT PUBLISH packet of 65 bytes exceeds the broker's maximum \
             packet size of 64 bytes"
        );
        assert!(outcome.infrastructure_error.is_none());
    }

    #[nervix_primitives::test]
    async fn the_event_loop_records_the_maximum_packet_size_the_broker_declares() {
        let broker = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port is available");
        let addr = broker
            .local_addr()
            .expect("a bound listener has an address");
        let fixture = DnsFixture::start().await;
        let connector = qos0_emitter(&format!("mqtt://{addr}"), "declared", fixture.dns.clone());

        let (mut connection, _) = broker
            .accept()
            .await
            .expect("the emitter's client connects to the test broker");
        let mut connect = [0_u8; 256];
        let read = tokio::io::AsyncReadExt::read(&mut connection, &mut connect)
            .await
            .expect("the client sends its CONNECT packet");
        assert!(read > 0, "the client sends its CONNECT packet");
        // CONNACK: success, no session, and one property, a Maximum Packet Size of 1024 bytes.
        let connack = [0x20, 0x08, 0x00, 0x00, 0x05, 0x27, 0x00, 0x00, 0x04, 0x00];
        tokio::io::AsyncWriteExt::write_all(&mut connection, &connack)
            .await
            .expect("the test broker answers the CONNECT");

        let declared = declared_packet_limit(&connector).await;
        assert_eq!(declared.get(), 1024);
    }

    /// The maximum packet size the broker declared to `emitter`, once its event loop has recorded the
    /// CONNACK that declared it.
    async fn declared_packet_limit(connector: &MqttTopicEmitter) -> NonZeroU32 {
        nervix_primitives::time::timeout(Duration::from_secs(30), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                if let Some(maximum) = connector.broker_limit.declared() {
                    return maximum;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the event loop records the CONNACK it received")
    }

    /// The first transient error the emitter's event loop records with its host.
    async fn first_transient_error(recorded: &mut watch::Receiver<Option<String>>) -> String {
        nervix_primitives::time::timeout(Duration::from_secs(30), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                if let Some(reason) = recorded.borrow_and_update().clone() {
                    return reason;
                }
                recorded
                    .changed()
                    .await
                    .expect("the emitter's host keeps the sender");
            }
        })
        .await
        .expect("the event loop reports its first failed connection")
    }

    #[nervix_primitives::test]
    async fn the_event_loop_reports_a_broker_name_that_does_not_resolve() {
        let fixture = DnsFixture::start().await;
        fixture.deny("missing.nervix.test");
        let (transient_error, mut recorded) = watch::channel(None);
        let config = qos0_config(
            "mqtt://missing.nervix.test:1883",
            "unresolved",
            fixture.dns.clone(),
        );
        let _emitter =
            MqttTopicEmitter::new(config, EmitterHost::new(RecordingHost { transient_error }))
                .expect("the test emitter config is valid");

        let reason = first_transient_error(&mut recorded).await;

        assert_eq!(
            reason,
            "resolving MQTT host 'missing.nervix.test' failed: the name does not exist"
        );
        assert!(fixture.authority.questions_for("missing.nervix.test") > 0);
    }

    /// The first printable attachment of `failure`: the description an emitter shows as its
    /// transient error.
    fn printable_description(failure: &Report<EmitterWriteError>) -> Option<String> {
        for frame in failure.frames() {
            if let FrameKind::Attachment(AttachmentKind::Printable(attachment)) = frame.kind() {
                return Some(attachment.to_string());
            }
        }
        None
    }

    /// The configuration of a QoS 1 emitter with `confirmation` on `topic` whose client connects to
    /// `addr` through `dns`, retrying slowly enough that a test never sees a second attempt.
    fn qos1_config(
        addr: &str,
        topic_name: &str,
        dns: DnsResolver,
        confirmation: AckConfirmation,
    ) -> MqttTopicEmitterConfig {
        MqttTopicEmitterConfig {
            mode: MqttPublishingMode::Qos1(confirmation),
            ..qos0_config(addr, topic_name, dns)
        }
    }

    fn sorted_delivered(outcome: &PerRecordOutcomeParts<EmitterRecordId>) -> Vec<EmitterRecordId> {
        let mut delivered = outcome.delivered.clone();
        delivered.sort();
        delivered
    }

    #[nervix_primitives::test]
    async fn a_failed_answer_settles_the_answers_the_broker_gave_before_it() {
        let broker = ScriptedBroker::start().await;
        let fixture = DnsFixture::start().await;
        let confirmation = AckConfirmation {
            max_in_flight: NonZeroUsize::new(4).expect("four is non-zero"),
            timeout: Duration::from_secs(30),
        };
        let mut connector = MqttTopicEmitter::new(
            qos1_config(&broker.addr(), "window", fixture.dns.clone(), confirmation),
            EmitterHost::new(SilentHost),
        )
        .expect("the test emitter config is valid");
        let script = async {
            let mut connection = broker.accept().await;
            let mut publishes = Vec::new();
            for _ in 0..3 {
                publishes.push(connection.read_publish().await);
            }
            // The broker confirms the younger records, then fails the oldest's answer for a reason
            // that is not about the record.
            connection.puback(publishes[1].packet_id, 0x00).await;
            connection.puback(publishes[2].packet_id, 0x00).await;
            connection.puback(publishes[0].packet_id, 0x97).await;
            connection
        };

        let (outcome, _connection) = tokio::join!(
            connector.publish(vec![record(0, 1), record(1, 1), record(2, 1)]),
            script
        );
        let outcome = outcome.into_parts();

        assert_eq!(
            sorted_delivered(&outcome),
            vec![EmitterRecordId::new(1), EmitterRecordId::new(2)],
            "the records the broker confirmed before the failure are delivered"
        );
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .expect("the oldest record's failed answer fails the publish");
        assert_eq!(
            failure.current_context(),
            &EmitterWriteError::Publish { connector: MQTT }
        );
        assert_eq!(
            failure.downcast_ref::<PublishNoticeError>(),
            Some(&PublishNoticeError::V5PubAck(
                MqttPubAckReason::QuotaExceeded
            ))
        );
        assert_eq!(
            printable_description(&failure).as_deref(),
            Some("mqtt publish failed: v5 puback returned non-success reason: QuotaExceeded")
        );
    }

    #[nervix_primitives::test]
    async fn a_confirmed_record_the_broker_would_refuse_is_rejected_before_the_client_sees_it() {
        let broker = ScriptedBroker::start().await;
        let fixture = DnsFixture::start().await;
        let confirmation = AckConfirmation {
            max_in_flight: NonZeroUsize::new(4).expect("four is non-zero"),
            timeout: Duration::from_secs(30),
        };
        let mut connector = MqttTopicEmitter::new(
            qos1_config(&broker.addr(), "limited", fixture.dns.clone(), confirmation),
            EmitterHost::new(SilentHost),
        )
        .expect("the test emitter config is valid");
        let mut connection = broker.accept_declaring_packet_limit(64).await;
        assert_eq!(declared_packet_limit(&connector).await.get(), 64);
        let script = async {
            let first = connection.read_publish().await;
            let second = connection.read_publish().await;
            connection.puback(first.packet_id, 0x00).await;
            connection.puback(second.packet_id, 0x00).await;
            [first.payload.len(), second.payload.len()]
        };

        // The topic `limited` takes 14 bytes around a QoS 1 payload: the fixed header byte, one
        // remaining-length byte, the topic with its length prefix, the packet identifier, and the
        // property length.
        let (outcome, received) = tokio::join!(
            connector.publish(vec![record(0, 50), record(1, 51), record(2, 50)]),
            script
        );
        let outcome = outcome.into_parts();

        assert_eq!(
            received,
            [50, 50],
            "the broker receives only the records that fit"
        );
        assert_eq!(
            sorted_delivered(&outcome),
            vec![EmitterRecordId::new(0), EmitterRecordId::new(2)]
        );
        let [rejected] = outcome.rejected.as_slice() else {
            panic!(
                "exactly the oversize record is rejected: {:?}",
                outcome.rejected
            );
        };
        assert_eq!(rejected.id, EmitterRecordId::new(1));
        assert_eq!(
            rejected.error.message,
            "mqtt rejected record: MQTT PUBLISH packet of 65 bytes exceeds the broker's maximum \
             packet size of 64 bytes"
        );
        assert!(outcome.infrastructure_error.is_none());
    }

    #[nervix_primitives::test]
    async fn a_reason_code_about_one_record_rejects_only_that_record() {
        let broker = ScriptedBroker::start().await;
        let fixture = DnsFixture::start().await;
        let confirmation = AckConfirmation {
            max_in_flight: NonZeroUsize::new(2).expect("two is non-zero"),
            timeout: Duration::from_secs(30),
        };
        let mut connector = MqttTopicEmitter::new(
            qos1_config(&broker.addr(), "window", fixture.dns.clone(), confirmation),
            EmitterHost::new(SilentHost),
        )
        .expect("the test emitter config is valid");
        let script = async {
            let mut connection = broker.accept().await;
            let refused = connection.read_publish().await;
            let confirmed = connection.read_publish().await;
            assert_eq!(refused.payload, b"x");
            connection.puback(refused.packet_id, 0x87).await;
            connection.puback(confirmed.packet_id, 0x00).await;
            connection
        };

        let (outcome, _connection) =
            tokio::join!(connector.publish(vec![record(0, 1), record(1, 1)]), script);
        let outcome = outcome.into_parts();

        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(1)]);
        let [rejected] = outcome.rejected.as_slice() else {
            panic!(
                "exactly the refused record is rejected: {:?}",
                outcome.rejected
            );
        };
        assert_eq!(rejected.id, EmitterRecordId::new(0));
        assert_eq!(
            rejected.error.message,
            "mqtt rejected record: v5 puback returned non-success reason: NotAuthorized"
        );
        assert!(outcome.infrastructure_error.is_none());
    }

    #[nervix_primitives::test]
    async fn finishing_a_qos0_emitter_that_published_nothing_has_nothing_to_send() {
        let fixture = DnsFixture::start().await;
        let mut connector = qos0_emitter("mqtt://127.0.0.1:9", "idle", fixture.dns.clone());

        let finished = connector
            .finish(Instant::now())
            .await
            .expect("a QoS 0 emitter that took no record has nothing to send");

        assert_eq!(finished, EmitterFinish::NothingHeld);
    }

    #[nervix_primitives::test]
    async fn finishing_a_qos0_emitter_sends_the_publish_its_client_still_holds() {
        // The scripted broker reads the client's CONNECT and holds its answer, so the client takes
        // the record but cannot send it until the broker answers.
        let broker = ScriptedBroker::start().await;
        let fixture = DnsFixture::start().await;
        let mut connector = qos0_emitter(&broker.addr(), "held", fixture.dns.clone());
        let mut connection = broker.accept_without_reply().await;
        let outcome = connector.publish(vec![record(0, 3)]).await.into_parts();
        assert_eq!(
            outcome.delivered,
            vec![EmitterRecordId::new(0)],
            "a QoS 0 record is delivered once the client takes it"
        );
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .assured("thirty seconds after now fits the monotonic clock");
        let answer = async {
            // CONNACK: no session present, success, and an empty property section.
            connection.write(&[0x20, 0x03, 0x00, 0x00, 0x00]).await;
            connection.read_publish_at_most_once().await
        };

        let (finished, payload) = tokio::join!(connector.finish(deadline), answer);

        let finished = finished.expect("the client sends the publish once the broker answers");
        assert_eq!(finished, EmitterFinish::Flushed);
        assert_eq!(
            payload, b"xxx",
            "the broker receives the publish the client held"
        );
    }

    #[nervix_primitives::test]
    async fn finishing_counts_the_qos0_publish_its_client_still_holds_at_the_deadline() {
        // The scripted broker never answers the client's CONNECT, so the client never sends the
        // record it took.
        let broker = ScriptedBroker::start().await;
        let fixture = DnsFixture::start().await;
        let mut connector = qos0_emitter(&broker.addr(), "unsent", fixture.dns.clone());
        let _connection = broker.accept_without_reply().await;
        let outcome = connector.publish(vec![record(0, 3)]).await.into_parts();
        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(200))
            .assured("two hundred milliseconds after now fits the monotonic clock");

        let finished = connector.finish(deadline).await;

        let error = finished.expect_err("the deadline passes while the client still holds it");
        assert_eq!(
            error.current_context(),
            &EmitterFinishError::Unsent {
                connector: MQTT,
                records: 1
            }
        );
    }

    #[nervix_primitives::test]
    async fn finishing_a_confirmed_emitter_has_nothing_to_send() {
        let fixture = DnsFixture::start().await;
        let confirmation = AckConfirmation {
            max_in_flight: NonZeroUsize::MIN,
            timeout: Duration::from_secs(30),
        };
        let mut connector = MqttTopicEmitter::new(
            qos1_config(
                "mqtt://127.0.0.1:9",
                "answered",
                fixture.dns.clone(),
                confirmation,
            ),
            EmitterHost::new(SilentHost),
        )
        .expect("the test emitter config is valid");

        let finished = connector
            .finish(Instant::now())
            .await
            .expect("a confirmed emitter answers its finish");

        assert_eq!(finished, EmitterFinish::NothingHeld);
    }

    /// A flush the first poll finds pending and the next finds failed, as one whose connection
    /// the event loop loses while a finish waits for it.
    struct FailsOnceWaitedFor {
        polled: bool,
    }

    impl Future for FailsOnceWaitedFor {
        type Output = Result<(), PublishNoticeError>;

        fn poll(
            mut self: Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if self.polled {
                return std::task::Poll::Ready(Err(PublishNoticeError::Qos0NotFlushed));
            }
            self.polled = true;
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }

    fn failing_flush() -> MqttFlush {
        Box::pin(FailsOnceWaitedFor { polled: false })
    }

    fn open_flush() -> MqttFlush {
        Box::pin(std::future::pending())
    }

    fn ended_flush(ended: Result<(), PublishNoticeError>) -> MqttFlush {
        Box::pin(std::future::ready(ended))
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_flush_that_fails_while_the_finish_waits_fails_the_finish_with_its_cause() {
        let mut unsent = MqttUnsentPublishes::default();
        unsent.hold(failing_flush());
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .assured("thirty seconds after now fits the monotonic clock");

        let finished = unsent.finish(deadline).await;

        let error = finished.expect_err("a flush that failed fails the finish");
        assert_eq!(
            error.current_context(),
            &EmitterFinishError::Flush { connector: MQTT }
        );
        assert_eq!(
            error.downcast_ref::<PublishNoticeError>(),
            Some(&PublishNoticeError::Qos0NotFlushed),
            "the driver's cause stays beneath the finish failure"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_flush_that_ended_before_the_finish_leaves_nothing_held() {
        let mut unsent = MqttUnsentPublishes::default();
        unsent.hold(ended_flush(Ok(())));
        unsent.hold(ended_flush(Err(PublishNoticeError::Qos0NotFlushed)));

        let finished = unsent.finish(Instant::now()).await;

        assert_eq!(
            finished.expect("the client holds no publish once its flushes ended"),
            EmitterFinish::NothingHeld
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn the_deadline_counts_every_open_flush_beside_the_failure_the_finish_saw() {
        let mut unsent = MqttUnsentPublishes::default();
        unsent.hold(failing_flush());
        unsent.hold(open_flush());
        unsent.hold(open_flush());
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .assured("thirty seconds after now fits the monotonic clock");

        let finished = unsent.finish(deadline).await;

        let error = finished.expect_err("open flushes at the deadline fail the finish");
        assert_eq!(
            error.current_context(),
            &EmitterFinishError::Unsent {
                connector: MQTT,
                records: 2
            }
        );
        assert_eq!(
            error.downcast_ref::<PublishNoticeError>(),
            Some(&PublishNoticeError::Qos0NotFlushed),
            "the failure the finish saw stays beneath the count"
        );
        assert_eq!(
            unsent.flushes.len(),
            2,
            "a later finish still answers for the open flushes"
        );
    }

    #[nervix_primitives::test]
    async fn a_record_the_client_cannot_queue_fails_the_publish() {
        // Nothing listens on the discard port. Once the client's first connection has failed, its
        // event loop waits out the minute-long backoff without reading its request queue, which
        // holds one request.
        let fixture = DnsFixture::start().await;
        let (transient_error, mut recorded) = watch::channel(None);
        let timeout = Duration::from_millis(50);
        let confirmation = AckConfirmation {
            max_in_flight: NonZeroUsize::MIN,
            timeout,
        };
        let mut connector = MqttTopicEmitter::new(
            qos1_config(
                "mqtt://127.0.0.1:9",
                "unreachable",
                fixture.dns.clone(),
                confirmation,
            ),
            EmitterHost::new(RecordingHost { transient_error }),
        )
        .expect("the test emitter config is valid");
        first_transient_error(&mut recorded).await;

        let unanswered = connector.publish(vec![record(0, 1)]).await.into_parts();
        let failure = unanswered
            .infrastructure_error
            .expect("an answer that never comes fails the publish");
        assert_eq!(
            failure.downcast_ref::<ConfirmationWindowError>(),
            Some(&ConfirmationWindowError::AnswerTimedOut { timeout })
        );
        assert_eq!(
            printable_description(&failure).as_deref(),
            Some("the broker did not answer within ACK TIMEOUT 50ms")
        );
        assert!(unanswered.delivered.is_empty());

        // The record the failed publish handed over still fills the queue.
        let refused = connector.publish(vec![record(0, 1)]).await.into_parts();
        let failure = refused
            .infrastructure_error
            .expect("a record the client cannot queue fails the publish");
        assert_eq!(
            failure.current_context(),
            &EmitterWriteError::Publish { connector: MQTT }
        );
        assert!(
            matches!(
                failure.downcast_ref::<MqttClientError>(),
                Some(MqttClientError::RequestChannelFull(_))
            ),
            "the client's typed refusal stays beneath the failure: {failure:?}"
        );
        assert_eq!(
            printable_description(&failure).as_deref(),
            Some("mqtt client refused the record: Request channel is full")
        );
        assert!(refused.delivered.is_empty());
        assert!(refused.rejected.is_empty());
    }

    #[nervix_primitives::test]
    async fn a_qos0_record_the_client_cannot_queue_fails_the_publish() {
        // As above: the event loop no longer reads its one-request queue after its first failed
        // connection, so the client takes the first record and refuses the second.
        let fixture = DnsFixture::start().await;
        let (transient_error, mut recorded) = watch::channel(None);
        let mut connector = MqttTopicEmitter::new(
            qos0_config("mqtt://127.0.0.1:9", "unreachable", fixture.dns.clone()),
            EmitterHost::new(RecordingHost { transient_error }),
        )
        .expect("the test emitter config is valid");
        first_transient_error(&mut recorded).await;

        let outcome = connector
            .publish(vec![record(0, 1), record(1, 1)])
            .await
            .into_parts();

        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .expect("a record the client cannot queue fails the publish");
        assert!(matches!(
            failure.downcast_ref::<MqttClientError>(),
            Some(MqttClientError::RequestChannelFull(_))
        ));
        assert_eq!(
            printable_description(&failure).as_deref(),
            Some("mqtt client refused the record: Request channel is full")
        );
    }

    #[test]
    fn any_other_ending_leaves_the_record_unknown_and_keeps_its_cause() {
        let endings = [
            (
                PublishNoticeError::V5PubAck(MqttPubAckReason::QuotaExceeded),
                "mqtt publish failed: v5 puback returned non-success reason: QuotaExceeded",
            ),
            (
                PublishNoticeError::V5PubAck(MqttPubAckReason::UnspecifiedError),
                "mqtt publish failed: v5 puback returned non-success reason: UnspecifiedError",
            ),
            (
                PublishNoticeError::V5PubRec(MqttPubRecReason::QuotaExceeded),
                "mqtt publish failed: v5 pubrec returned non-success reason: QuotaExceeded",
            ),
            (
                PublishNoticeError::V5PubComp(rumqttc::PubCompReason::PacketIdentifierNotFound),
                "mqtt publish failed: v5 pubcomp returned non-success reason: \
                 PacketIdentifierNotFound",
            ),
            (
                PublishNoticeError::SessionReset,
                "mqtt publish failed: message dropped due to session reset",
            ),
            (
                PublishNoticeError::Recv,
                "mqtt publish failed: event loop dropped notice sender",
            ),
        ];
        for (ending, description) in endings {
            let failure = MqttTopicEmitter::broker_answer(Err(ending.clone()))
                .expect_err("an ending that is not about the record leaves its outcome unknown");

            assert_eq!(
                failure.current_context(),
                &EmitterWriteError::Publish { connector: MQTT }
            );
            assert_eq!(failure.downcast_ref::<PublishNoticeError>(), Some(&ending));
            assert_eq!(
                printable_description(&failure).as_deref(),
                Some(description)
            );
        }
    }

    #[test]
    fn only_an_invalid_request_is_the_records_own_refusal() {
        let publish = || {
            rumqttc::Request::Publish(rumqttc::Publish::new(
                "refused",
                rumqttc::QoS::AtLeastOnce,
                Vec::new(),
                None,
            ))
        };
        assert!(MqttTopicEmitter::is_record_client_rejection(
            &MqttClientError::InvalidRequest(Box::new(publish()))
        ));
        let refusals = [
            (
                MqttClientError::RequestChannelFull(Box::new(publish())),
                "mqtt client refused the record: Request channel is full",
            ),
            (
                MqttClientError::RequestChannelDisconnected(Box::new(publish())),
                "mqtt client refused the record: Request channel is disconnected",
            ),
            (
                MqttClientError::TrackingUnavailable,
                "mqtt client refused the record: Tracked request API is unavailable for this \
                 client instance",
            ),
        ];
        for (refusal, description) in refusals {
            assert!(!MqttTopicEmitter::is_record_client_rejection(&refusal));
            let failure = MqttTopicEmitter::refused_hand_over(refusal);
            assert_eq!(
                failure.current_context(),
                &EmitterWriteError::Publish { connector: MQTT }
            );
            assert!(failure.downcast_ref::<MqttClientError>().is_some());
            assert_eq!(
                printable_description(&failure).as_deref(),
                Some(description)
            );
        }
    }
}
