//! NATS source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The NATS connection a client configures, its reconnect delay, the queue
//!   subscription a source reads through, core and JetStream publication, the bound the client's
//!   `timeout_ms` sets on a core write and the records a core client still holds unflushed, the
//!   JetStream answer each record's `PubAck` gives the confirmation window, header mapping in both
//!   directions, and per-record rejection classification.
//! - **Depends on.** The connector contract and its confirmation window, vocabulary values,
//!   `error-stack`, Tokio and `async-nats`.
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod scripted_server;
mod source;

use std::{future::Future, pin::Pin, time::Duration};

use async_nats::{
    Client as NatsClient, PublishError as NatsPublishError,
    PublishErrorKind as NatsPublishErrorKind, Subject,
    jetstream::{
        Context as NatsJetStream,
        context::{PublishAckFuture, PublishError as JetStreamPublishError, PublishErrorKind},
        publish::PublishAck,
    },
    message::OutboundMessage,
};
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::SinkExt;
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerAnswer, BrokerVerdict, ConfiguredDeadline, ConfirmationWindow,
    ParsedRetryPolicy, PerRecordOutcome, RecordSink, RejectedSinkRecord, SinkFinish,
    SinkFinishError, SinkFinishResult, SinkHost, SinkLifecycle, SinkPublishError, SinkRecord,
    SinkRecordId, SinkStartError, SinkStartResult, client_config_value, client_tls_paths,
};
use nervix_models::{ClientConfigEntry, NatsPublishSubjectName};
use nervix_primitives::{
    sync::{
        StdArc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
pub use source::{
    NatsMessageHeaders, NatsSource, NatsSourceError, NatsSourceMessage, NatsSourcePlan,
};

const NATS: &str = "nats";

/// Whether a NATS sink publishes through core NATS or waits for JetStream to store each record.
///
/// A NATS sink publishes through its own client even without an acknowledgement, so `MODE NO ACK`
/// decides [`NatsPublishingMode::Core`] rather than a broker mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatsPublishingMode {
    Core,
    JetStream(AckConfirmation),
}

/// What one NATS sink publishes through: its client entries, subject, delivery and reconnect
/// backoff.
pub struct NatsSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub subject: NatsPublishSubjectName,
    pub mode: NatsPublishingMode,
    pub retry_policy: ParsedRetryPolicy,
}

pub struct NatsSink {
    client: NatsClient,
    delivery: NatsDelivery,
    subject: Subject,
}

/// How this sink publishes, together with whatever that way of publishing needs.
///
/// A JetStream context exists only for `MODE ACK`, and the bound and the unflushed records of a
/// core write only for `MODE NO ACK`, so pairing each with the mode that requires it means the
/// publish path reads one value instead of matching a mode and then hoping a separately stored
/// value agrees with it.
enum NatsDelivery {
    Core(CoreWrites),
    JetStream {
        context: Box<NatsJetStream>,
        confirmation: AckConfirmation,
    },
}

/// JetStream's answer for one record, which resolves when the record's `PubAck` arrives or the
/// client gives the record up.
type NatsAnswer = Pin<Box<dyn Future<Output = BrokerAnswer> + Send>>;

/// The wait the client's `timeout_ms` bounds, as a write that runs out of it names the wait.
const CORE_WRITE: &str = "a core NATS publish and its flush";

impl NatsDelivery {
    /// JetStream publishing through `client`, with a context that awaits each record's `PubAck`
    /// for at most the declared `ACK TIMEOUT` and holds at most `MAX` records awaiting one.
    fn jetstream(confirmation: AckConfirmation, client: &NatsClient) -> Self {
        Self::JetStream {
            context: Box::new(
                async_nats::jetstream::ContextBuilder::new()
                    .timeout(confirmation.timeout)
                    .ack_timeout(confirmation.timeout)
                    .max_ack_inflight(confirmation.max_in_flight.get())
                    .backpressure_on_inflight(true)
                    .build(client.clone()),
            ),
            confirmation,
        }
    }
}

/// The core writes of one sink: the bound the client's `timeout_ms` sets on each, and the records
/// the client took that it has not flushed to its socket.
///
/// A core write hands its records to the client and delivers them once the client flushed them to
/// its socket; the server answers nothing. A write that runs out of `timeout_ms` before its flush
/// completes leaves its records with the client, which still sends them once its connection drains
/// or reconnects, so the sink counts them until a flush completes.
struct CoreWrites {
    /// The client's `timeout_ms`, which bounds each write, the hand-over and the flush included.
    /// Without the key a write waits for its flush.
    timeout: Option<ConfiguredDeadline>,
    /// The records the client took since its last completed flush.
    unflushed: u64,
}

impl CoreWrites {
    /// Core writes bounded by `timeout`, of a client that holds nothing yet.
    fn new(timeout: Option<ConfiguredDeadline>) -> Self {
        Self {
            timeout,
            unflushed: 0,
        }
    }

    /// Writes `records` to `subject` through `client` within the client's `timeout_ms`, and
    /// answers for each of them.
    async fn publish(
        &mut self,
        client: &NatsClient,
        subject: &Subject,
        records: Vec<SinkRecord>,
    ) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let timeout = self.timeout;
        let write = self.write(client, subject, records, &mut outcome);
        let written = match timeout {
            Some(timeout) => {
                let bounded = nervix_primitives::time::timeout(timeout.bound(), write).await;
                match bounded {
                    Ok(written) => written,
                    Err(_elapsed) => Err(Self::elapsed(timeout)),
                }
            }
            None => write.await,
        };
        match written {
            Ok(flushed) => {
                for record_id in flushed {
                    outcome.deliver(record_id);
                }
            }
            Err(failure) => outcome.fail(failure),
        }
        outcome
    }

    /// Hands `records` to `client` and flushes them to its socket, after it flushed what an
    /// earlier write left with the client. Answers with the records it flushed, having recorded in
    /// `outcome` each record NATS refuses for good.
    ///
    /// The count of unflushed records changes only once the client took a record or a flush
    /// completed, so a write the bound cuts short leaves it naming exactly what the client holds.
    async fn write(
        &mut self,
        client: &NatsClient,
        subject: &Subject,
        records: Vec<SinkRecord>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> Result<Vec<SinkRecordId>, Report<SinkPublishError>> {
        // Records handed over before what the client still holds is flushed would queue behind
        // it, and a write that retries those records would queue a second copy of each.
        if self.unflushed > 0 {
            Self::flush(client).await?;
            self.unflushed = 0;
        }
        let mut sink = client.clone();
        let mut handed_over = Vec::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let headers = if record.headers.is_empty() {
                None
            } else {
                Some(NatsSink::header_map(&record.headers))
            };
            let result = sink
                .feed(OutboundMessage {
                    subject: subject.clone(),
                    reply: None,
                    payload: record.payload.into(),
                    headers,
                })
                .await;
            match result {
                Ok(()) => {
                    handed_over.push(record_id);
                    self.unflushed = self.unflushed.checked_add(1).assured(
                        "the records one client holds are in memory, far fewer than u64 counts",
                    );
                }
                Err(error) if NatsSink::is_core_record_rejection(&error) => {
                    outcome.reject(RejectedSinkRecord::external(
                        record_id,
                        occurred_at,
                        format!("nats rejected record: {error}"),
                    ));
                }
                Err(error) => return Err(NatsSink::publish_error(error)),
            }
        }
        Self::flush(client).await?;
        self.unflushed = 0;
        Ok(handed_over)
    }

    /// Flushes `client`'s connection, which writes everything handed to the client before it to
    /// the client's socket.
    async fn flush(client: &NatsClient) -> Result<(), Report<SinkPublishError>> {
        client.flush().await.map_err(NatsSink::publish_error)
    }

    /// The failure of a write that ran out of the client's `timeout_ms`. An emitter shows the
    /// message its sink attached, so the deadline leads.
    fn elapsed(timeout: ConfiguredDeadline) -> Report<SinkPublishError> {
        let elapsed = timeout.elapsed(CORE_WRITE);
        Report::new(elapsed)
            .change_context(SinkPublishError::Publish { sink: NATS })
            .attach_printable(elapsed.to_string())
    }

    /// Flushes what the client still holds of the records writes handed it, by `deadline`.
    async fn finish(&mut self, client: &NatsClient, deadline: Instant) -> SinkFinishResult {
        if self.unflushed == 0 {
            return Ok(SinkFinish::NothingHeld);
        }
        let flushed = nervix_primitives::time::timeout_at(deadline, client.flush()).await;
        match flushed {
            Ok(Ok(())) => {
                self.unflushed = 0;
                Ok(SinkFinish::Flushed)
            }
            Ok(Err(error)) => {
                let description =
                    format!("nats client failed to flush the records it held: {error}");
                Err(Report::new(error)
                    .change_context(SinkFinishError::Flush { sink: NATS })
                    .attach_printable(description))
            }
            Err(_elapsed) => Err(Report::new(SinkFinishError::Unsent {
                sink: NATS,
                records: self.unflushed,
            })),
        }
    }
}

impl NatsSink {
    pub async fn new(config: NatsSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        // Read before the client connects, so a value that breaks the deadline rule fails the
        // start as configuration whether or not the server answers.
        let write_timeout = Self::write_timeout(&config.config)?;
        let client = Self::client_from_config(&config.config, config.retry_policy).await?;
        let delivery = match config.mode {
            NatsPublishingMode::Core => NatsDelivery::Core(CoreWrites::new(write_timeout)),
            NatsPublishingMode::JetStream(confirmation) => {
                NatsDelivery::jetstream(confirmation, &client)
            }
        };
        Ok(Self {
            client,
            delivery,
            subject: Subject::from(config.subject.as_str().to_string()),
        })
    }

    /// The bound the client's `timeout_ms` sets on each core write, where it sets one. JetStream
    /// bounds its records by `ACK TIMEOUT` instead.
    fn write_timeout(config: &[ClientConfigEntry]) -> SinkStartResult<Option<ConfiguredDeadline>> {
        ConfiguredDeadline::timeout(config)
            .change_context(SinkStartError::InvalidConfiguration { sink: NATS })
    }

    async fn client_from_config(
        config: &[ClientConfigEntry],
        retry_policy: ParsedRetryPolicy,
    ) -> SinkStartResult<NatsClient> {
        let addr = Self::config_value(config, "addr")?;
        let connected_once = StdArc::new(AtomicBool::new(false));
        let event_connected_once = connected_once.clone();
        let delay_connected_once = connected_once;
        let mut options = async_nats::ConnectOptions::new()
            .event_callback(move |event| {
                let connected_once = event_connected_once.clone();
                async move {
                    if let async_nats::Event::Connected = event {
                        connected_once.store(true, Ordering::Relaxed);
                    }
                }
            })
            .reconnect_delay_callback(move |attempts| {
                Self::connection_delay(
                    retry_policy,
                    attempts,
                    delay_connected_once.load(Ordering::Relaxed),
                )
            });
        let tls = client_tls_paths(config);
        if let Some(ca_file) = tls.ca_file.as_ref() {
            options = options.add_root_certificates(ca_file.clone());
        }
        match (&tls.cert_file, &tls.key_file) {
            (Some(cert_file), Some(key_file)) => {
                options = options.add_client_certificate(cert_file.clone(), key_file.clone());
            }
            (None, None) => {}
            _ => {
                return Err(Self::config_error(
                    "NATS TLS client authentication requires both 'tls_cert_file' and \
                     'tls_key_file'",
                ));
            }
        }
        options.connect(addr).await.map_err(Self::start_error)
    }

    fn connection_delay(
        policy: ParsedRetryPolicy,
        attempts: usize,
        connected_once: bool,
    ) -> Duration {
        // The client counts reconnect attempts from one. The first attempt after a connection
        // that never succeeded retries immediately; from the first delayed attempt onward each
        // further attempt doubles the configured backoff.
        let first_delayed_attempt = if connected_once { 1 } else { 2 };
        let Some(retries) = attempts.checked_sub(first_delayed_attempt) else {
            return Duration::ZERO;
        };
        let mut delay = policy.backoff;
        for _ in 0..retries {
            if delay >= policy.max_backoff {
                return policy.max_backoff;
            }
            // Saturation is the policy here: the backoff doubles until it reaches the configured
            // ceiling and stays there, so a doubling that leaves `Duration` clamps to it too.
            let doubled = nervix_primitives::expect_lint!(
                nervix::clamped_arithmetic,
                "doubling past Duration MAX must remain at the configured retry ceiling",
                delay.saturating_mul(2)
            );
            delay = doubled.min(policy.max_backoff);
        }
        delay
    }

    /// Publishes `records` to JetStream and awaits each record's `PubAck` through the
    /// confirmation window `settings` declare.
    ///
    /// Every way the publish can fail ends it through the window, which first settles each answer
    /// JetStream already gave, so the host's retry never resends a record the stream confirmed.
    async fn publish_jetstream(
        subject: &Subject,
        jetstream: &NatsJetStream,
        settings: AckConfirmation,
        records: Vec<SinkRecord>,
    ) -> PerRecordOutcome<SinkRecordId> {
        let mut window: ConfirmationWindow<NatsAnswer> =
            ConfirmationWindow::new(NATS, settings, records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let handed_over = if record.headers.is_empty() {
                jetstream
                    .publish(subject.clone(), record.payload.into())
                    .await
            } else {
                jetstream
                    .publish_with_headers(
                        subject.clone(),
                        Self::header_map(&record.headers),
                        record.payload.into(),
                    )
                    .await
            };
            let pending = match handed_over {
                Ok(pending) => pending,
                Err(error) if Self::is_jetstream_record_rejection(&error) => {
                    window.reject(RejectedSinkRecord::external(
                        record_id,
                        occurred_at,
                        format!("nats JetStream rejected record: {error}"),
                    ));
                    continue;
                }
                Err(error) => return window.fail(Self::refused_hand_over(error)),
            };
            let answer: NatsAnswer = Box::pin(Self::await_answer(pending));
            if let Err(failure) = window.admit(record_id, occurred_at, answer).await {
                return window.fail(failure);
            }
        }
        window.finish().await
    }

    /// Waits for the `PubAck` of the record `pending` tracks and reads JetStream's answer from it.
    async fn await_answer(pending: PublishAckFuture) -> BrokerAnswer {
        let acknowledgement = pending.await;
        Self::broker_answer(acknowledgement)
    }

    /// JetStream's answer for a record whose wait for its `PubAck` ended as `acknowledgement`.
    ///
    /// A `PubAck` confirms the record, and a message above the server's `max_payload` is refused
    /// for good. Any other ending leaves the record's outcome unknown: no stream captures the
    /// subject, the `PubAck` did not arrive, or the client or its connection failed.
    fn broker_answer(acknowledgement: Result<PublishAck, JetStreamPublishError>) -> BrokerAnswer {
        match acknowledgement {
            Ok(_ack) => Ok(BrokerVerdict::Confirmed),
            Err(error) if Self::is_jetstream_record_rejection(&error) => {
                Ok(BrokerVerdict::Rejected {
                    message: format!("nats JetStream rejected record: {error}"),
                })
            }
            Err(error) => {
                let description = format!("nats JetStream publish failed: {error}");
                Err(Report::new(error)
                    .change_context(SinkPublishError::Publish { sink: NATS })
                    .attach_printable(description))
            }
        }
    }

    /// The failure that ends a publish whose record the client did not take for a reason that is
    /// not the record's own, such as a client that could not queue it within `ACK TIMEOUT`.
    fn refused_hand_over(error: JetStreamPublishError) -> Report<SinkPublishError> {
        let description = format!("nats JetStream client did not take the record: {error}");
        Report::new(error)
            .change_context(SinkPublishError::Publish { sink: NATS })
            .attach_printable(description)
    }

    fn header_map(headers: &[(String, String)]) -> async_nats::HeaderMap {
        let mut header_map = async_nats::HeaderMap::new();
        for (name, value) in headers {
            header_map.append(name.as_str(), value.as_str());
        }
        header_map
    }

    fn is_core_record_rejection(error: &NatsPublishError) -> bool {
        matches!(error.kind(), NatsPublishErrorKind::MaxPayloadExceeded)
    }

    fn is_jetstream_record_rejection(error: &JetStreamPublishError) -> bool {
        matches!(error.kind(), PublishErrorKind::MaxPayloadExceeded)
    }

    #[cfg(test)]
    fn jetstream_error_is_missing_stream(error: &JetStreamPublishError) -> bool {
        matches!(error.kind(), PublishErrorKind::StreamNotFound)
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> SinkStartResult<String> {
        {
            let mapping_input = client_config_value(config, key, "NATS");
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let message = error.current_context().to_string();
                    error
                        .change_context(SinkStartError::InvalidConfiguration { sink: NATS })
                        .attach_printable(message)
                }),
            }
        }
    }

    fn config_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::InvalidConfiguration { sink: NATS })
            .attach_printable(error.to_string())
    }

    fn start_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::Initialize { sink: NATS }).attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "formats the typed external NATS driver failure; no internal runtime \
                      ownership is inferred"
        )
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: NATS }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for NatsSink {
    /// A core write delivers its records once the client flushed them, so the client holds
    /// records only after a write ran out of `timeout_ms`, and finishing flushes them by
    /// `deadline`. JetStream answers each record, so its client holds nothing a publish reported
    /// delivered.
    async fn finish(&mut self, deadline: Instant) -> SinkFinishResult {
        match &mut self.delivery {
            NatsDelivery::Core(writes) => writes.finish(&self.client, deadline).await,
            NatsDelivery::JetStream { .. } => Ok(SinkFinish::NothingHeld),
        }
    }

    /// A publish failure leaves the client connected: it reconnects on its own schedule, and
    /// dropping it would discard the session the broker still holds.
    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }
}

#[async_trait]
impl RecordSink for NatsSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        match &mut self.delivery {
            NatsDelivery::Core(writes) => {
                writes.publish(&self.client, &self.subject, records).await
            }
            NatsDelivery::JetStream {
                context,
                confirmation,
            } => Self::publish_jetstream(&self.subject, context, *confirmation, records).await,
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::num::NonZeroUsize;

    use async_nats::connection::State;
    use nervix_connector::{DeadlineElapsed, SinkFinishError};
    use nervix_models::{DeadlineKey, Timestamp};

    use super::*;
    use crate::scripted_server::{DownServer, ScriptedServer};

    /// How long a check waits for what the client or the scripted server must do. Generous,
    /// because each wait ends as soon as what it waits for has happened.
    const CHECK_BOUND: Duration = Duration::from_secs(30);

    /// The `timeout_ms` of the clients whose core writes the checks hold up.
    const WRITE_TIMEOUT_MS: &str = "200";

    /// `operation`, which must end within [`CHECK_BOUND`].
    async fn bounded<T>(operation: impl Future<Output = T>) -> T {
        nervix_primitives::time::timeout(CHECK_BOUND, operation)
            .await
            .expect("the operation ends well within the check's bound")
    }

    /// A deadline [`CHECK_BOUND`] from now.
    fn check_deadline() -> Instant {
        Instant::now()
            .checked_add(CHECK_BOUND)
            .expect("thirty seconds from now is within the monotonic clock's range")
    }

    /// A core sink publishing to subject `events` through the server at `addr`, with the client
    /// entries `config` adds to its address.
    async fn core_sink(addr: &str, config: &[(&str, &str)]) -> NatsSink {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
        };
        let mut entries = vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: addr.to_string(),
        }];
        for (key, value) in config {
            entries.push(ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            });
        }
        let write_timeout =
            NatsSink::write_timeout(&entries).expect("the checks configure a valid timeout_ms");
        let client = NatsSink::client_from_config(&entries, policy)
            .await
            .expect("the sink connects to the scripted server");
        NatsSink {
            delivery: NatsDelivery::Core(CoreWrites::new(write_timeout)),
            client,
            subject: Subject::from("events"),
        }
    }

    /// A core sink whose server went down after the sink connected: the server ended the
    /// connection and refuses every one its client opens to reconnect, until it comes back.
    struct StrandedSink {
        sink: NatsSink,
        server: DownServer,
    }

    /// A [`StrandedSink`] whose client sets `config`, returned once its client noticed that its
    /// server went down.
    async fn stranded_core_sink(config: &[(&str, &str)]) -> StrandedSink {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let connection = server.greet(1024 * 1024).await;
            connection.end().await;
            server.go_down()
        });
        let sink = core_sink(&addr, config).await;
        let server = script.await.expect("the scripted server ran");
        wait_for_state(&sink.client, State::Disconnected).await;
        StrandedSink { sink, server }
    }

    /// Waits until `client`'s connection is in `state`.
    async fn wait_for_state(client: &NatsClient, state: State) {
        let reached = nervix_primitives::time::timeout(CHECK_BOUND, async {
            while client.connection_state() != state {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            reached.is_ok(),
            "the client's connection did not become {state:?}"
        );
    }

    /// A JetStream sink publishing to subject `events` through the server at `addr`, with at most
    /// `max_in_flight` records awaiting their `PubAck` at once.
    async fn jetstream_sink(addr: &str, max_in_flight: usize) -> NatsSink {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        };
        let config = [ClientConfigEntry {
            key: "addr".to_string(),
            value: addr.to_string(),
        }];
        let client = NatsSink::client_from_config(&config, policy)
            .await
            .expect("the sink connects to the scripted server");
        let confirmation = AckConfirmation {
            max_in_flight: NonZeroUsize::new(max_in_flight).expect("a window holds a record"),
            timeout: Duration::from_secs(30),
        };
        NatsSink {
            delivery: NatsDelivery::jetstream(confirmation, &client),
            client,
            subject: Subject::from("events"),
        }
    }

    fn record(index: usize, payload: &[u8], headers: &[(&str, &str)]) -> SinkRecord {
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect();
        SinkRecord::new(
            SinkRecordId::new(index),
            None,
            payload.to_vec(),
            headers,
            Timestamp::from_unix_nanos(0),
        )
    }

    const CONFIRMED: &str = r#"{"stream":"events","seq":1}"#;

    #[nervix_primitives::test]
    async fn jetstream_confirms_each_record_and_refuses_one_above_max_payload() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(64).await;
            let plain = connection.expect_publish().await;
            let tagged = connection.expect_publish().await;
            connection.reply(&plain, CONFIRMED).await;
            connection.reply(&tagged, CONFIRMED).await;
            (plain.message, tagged.message, connection)
        });
        let mut sink = jetstream_sink(&addr, 8).await;

        let outcome = sink
            .publish(vec![
                record(0, b"plain", &[]),
                record(1, &[b'x'; 128], &[]),
                record(2, b"tagged", &[("trace", "a")]),
            ])
            .await
            .into_parts();

        assert_eq!(
            outcome.delivered,
            vec![SinkRecordId::new(0), SinkRecordId::new(2)]
        );
        let rejected = outcome
            .rejected
            .iter()
            .map(|rejected| rejected.id)
            .collect::<Vec<_>>();
        assert_eq!(rejected, vec![SinkRecordId::new(1)]);
        assert!(outcome.infrastructure_error.is_none());
        let (plain, tagged, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(plain, b"plain");
        assert_eq!(tagged, b"NATS/1.0\r\ntrace: a\r\n\r\ntagged");
    }

    #[nervix_primitives::test]
    async fn a_failed_pub_ack_ends_the_publish_after_settling_the_answers_already_given() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let mut publishes = Vec::new();
            for _ in 0..4 {
                publishes.push(connection.expect_publish().await);
            }
            // The fourth record's PubAck arrives before the third record's failure, so the
            // window holds a given answer when the failure ends the publish.
            for (index, reply) in [
                (0, CONFIRMED),
                (1, CONFIRMED),
                (3, CONFIRMED),
                (
                    2,
                    r#"{"error":{"code":500,"err_code":10049,"description":"stream store failed"}}"#,
                ),
            ] {
                let publish = publishes
                    .get(index)
                    .expect("the client published four records");
                connection.reply(publish, reply).await;
            }
            connection
        });
        let mut sink = jetstream_sink(&addr, 8).await;

        let outcome = sink
            .publish((0..4).map(|index| record(index, b"event", &[])).collect())
            .await
            .into_parts();

        assert_eq!(
            outcome.delivered,
            vec![
                SinkRecordId::new(0),
                SinkRecordId::new(1),
                SinkRecordId::new(3)
            ],
            "the publish settles every PubAck JetStream gave before the failure"
        );
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .expect("the failed PubAck ends the publish");
        let description = printable_description(&failure).expect("the failure describes itself");
        assert!(
            description.starts_with("nats JetStream publish failed: "),
            "{description}"
        );
        let _connection = script.await.expect("the scripted server ran");
    }

    /// A window of one waits for each record's `PubAck` before it takes the next record, so a
    /// failed `PubAck` ends the publish while the window admits its record.
    #[nervix_primitives::test]
    async fn a_window_of_one_ends_the_publish_on_a_failed_pub_ack() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(64).await;
            let publish = connection.expect_publish().await;
            connection
                .reply(
                    &publish,
                    r#"{"error":{"code":500,"err_code":10049,"description":"stream store failed"}}"#,
                )
                .await;
            connection
        });
        let mut sink = jetstream_sink(&addr, 1).await;

        let outcome = sink
            .publish(vec![record(0, b"first", &[]), record(1, b"second", &[])])
            .await
            .into_parts();

        assert!(outcome.delivered.is_empty());
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .expect("the failed PubAck ends the publish");
        let description = printable_description(&failure).expect("the failure describes itself");
        assert!(
            description.starts_with("nats JetStream publish failed: "),
            "{description}"
        );
        let _connection = script.await.expect("the scripted server ran");
    }

    /// JetStream answers every record but the oldest, then fails the oldest's `PubAck`: the
    /// publish settles every `PubAck` it was given, more of them than one cooperative budget lets
    /// a task read, so the host's retry resends none of the records the stream confirmed.
    #[nervix_primitives::test]
    async fn a_failure_settles_more_pub_acks_than_one_cooperative_budget() {
        const RECORDS: usize = 300;
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let mut publishes = Vec::with_capacity(RECORDS);
            for _ in 0..RECORDS {
                nervix_primitives::task::consume_budget().await;
                publishes.push(connection.expect_publish().await);
            }
            for publish in publishes.iter().skip(1) {
                nervix_primitives::task::consume_budget().await;
                connection.reply(publish, CONFIRMED).await;
            }
            let oldest = publishes
                .first()
                .expect("the client published every record");
            connection
                .reply(
                    oldest,
                    r#"{"error":{"code":500,"err_code":10049,"description":"stream store failed"}}"#,
                )
                .await;
            connection
        });
        let mut sink = jetstream_sink(&addr, 1024).await;

        let outcome = sink
            .publish(
                (0..RECORDS)
                    .map(|index| record(index, b"event", &[]))
                    .collect(),
            )
            .await
            .into_parts();

        let confirmed = (1..RECORDS).map(SinkRecordId::new).collect::<Vec<_>>();
        assert_eq!(
            outcome.delivered.len(),
            confirmed.len(),
            "every PubAck JetStream gave is settled"
        );
        assert_eq!(outcome.delivered, confirmed);
        assert!(outcome.rejected.is_empty());
        assert!(
            outcome.infrastructure_error.is_some(),
            "the oldest record's failed PubAck ends the publish"
        );
        let _connection = script.await.expect("the scripted server ran");
    }

    /// A core write whose client cannot flush, because its server went down and refuses every
    /// reconnection, fails within the client's `timeout_ms` and leaves its record unresolved. The
    /// failure names the deadline it ran out of, which the emitter shows as its transient error.
    #[nervix_primitives::test]
    async fn a_core_write_its_client_cannot_flush_fails_within_timeout_ms() {
        let StrandedSink {
            mut sink,
            server: _down,
        } = stranded_core_sink(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;

        let outcome = bounded(sink.publish(vec![record(0, b"held", &[])]))
            .await
            .into_parts();

        assert!(
            outcome.delivered.is_empty(),
            "a record its client did not flush is not delivered"
        );
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .expect("the flush the client could not finish fails the write");
        assert!(matches!(
            failure.current_context(),
            SinkPublishError::Publish { sink: NATS }
        ));
        let elapsed = failure
            .downcast_ref::<DeadlineElapsed>()
            .expect("the failure names the deadline the write ran out of");
        assert_eq!(elapsed.key, DeadlineKey::Timeout);
        assert_eq!(elapsed.bound, Duration::from_millis(200));
        assert_eq!(printable_description(&failure), Some(elapsed.to_string()));
    }

    /// A write after one that ran out of `timeout_ms` first waits for the client to flush what it
    /// still holds, and hands the client none of its own records while it cannot. The client
    /// therefore never queues a second copy behind the record it holds: once its server is back it
    /// sends that record once, and then the record of the write that found it flushed.
    #[nervix_primitives::test]
    async fn a_core_write_flushes_what_its_client_still_holds_before_it_hands_over_more() {
        let StrandedSink { mut sink, server } =
            stranded_core_sink(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let held = bounded(sink.publish(vec![record(0, b"held", &[])]))
            .await
            .into_parts();
        assert!(
            held.infrastructure_error.is_some(),
            "the first write runs out of timeout_ms"
        );
        let refused = bounded(sink.publish(vec![record(0, b"refused", &[])]))
            .await
            .into_parts();
        assert!(refused.delivered.is_empty());
        assert!(
            refused.infrastructure_error.is_some(),
            "the client still cannot flush the record it holds"
        );

        let script = nervix_primitives::task::spawn(async move {
            let server = server.come_back().await;
            let mut connection = server.accept(1024 * 1024).await;
            let first = connection.expect_publish().await;
            let second = connection.expect_publish().await;
            (first.message, second.message, connection)
        });
        wait_for_state(&sink.client, State::Connected).await;
        let next = bounded(sink.publish(vec![record(0, b"next", &[])]))
            .await
            .into_parts();

        assert_eq!(next.delivered, vec![SinkRecordId::new(0)]);
        assert!(next.infrastructure_error.is_none());
        let (first, second, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(first, b"held");
        assert_eq!(
            second, b"next",
            "the write that found the client holding a record handed over none of its own"
        );
    }

    /// Finishing a core sink whose write ran out of `timeout_ms` flushes what its client still
    /// holds by the finish deadline, which sends the record once the server is back.
    #[nervix_primitives::test]
    async fn finishing_a_core_sink_sends_what_a_stalled_write_left_with_its_client() {
        let StrandedSink { mut sink, server } =
            stranded_core_sink(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let held = bounded(sink.publish(vec![record(0, b"held", &[])]))
            .await
            .into_parts();
        assert!(
            held.infrastructure_error.is_some(),
            "the write runs out of timeout_ms"
        );

        let script = nervix_primitives::task::spawn(async move {
            let server = server.come_back().await;
            let mut connection = server.accept(1024 * 1024).await;
            let publish = connection.expect_publish().await;
            (publish.message, connection)
        });
        let finished = bounded(sink.finish(check_deadline())).await;

        assert!(matches!(finished, Ok(SinkFinish::Flushed)), "{finished:?}");
        let (message, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(message, b"held");
    }

    /// Finishing a core sink whose client still holds the records of a write that ran out of
    /// `timeout_ms` when the finish deadline passes counts them.
    #[nervix_primitives::test]
    async fn finishing_a_core_sink_counts_what_its_client_still_holds_at_the_deadline() {
        let StrandedSink {
            mut sink,
            server: _down,
        } = stranded_core_sink(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let held = bounded(sink.publish(vec![record(0, b"first", &[]), record(1, b"second", &[])]))
            .await
            .into_parts();
        assert!(
            held.infrastructure_error.is_some(),
            "the write runs out of timeout_ms"
        );

        let deadline = Instant::now()
            .checked_add(Duration::from_millis(200))
            .expect("a moment from now is within the monotonic clock's range");
        let finished = bounded(sink.finish(deadline)).await;

        let failure = match finished {
            Ok(finished) => {
                panic!("a client that cannot flush leaves its records unsent, not {finished:?}")
            }
            Err(failure) => failure,
        };
        assert_eq!(
            *failure.current_context(),
            SinkFinishError::Unsent {
                sink: NATS,
                records: 2
            }
        );
    }

    /// A core write delivers the records its client flushed, writes a record's headers into its
    /// message and refuses for good a record above the server's `max_payload`.
    #[nervix_primitives::test]
    async fn a_core_write_delivers_what_its_client_flushed_and_refuses_a_record_above_max_payload()
    {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(64).await;
            let plain = connection.expect_publish().await;
            let tagged = connection.expect_publish().await;
            (plain.message, tagged.message, connection)
        });
        let mut sink = core_sink(&addr, &[("timeout_ms", WRITE_TIMEOUT_MS)]).await;

        let outcome = bounded(sink.publish(vec![
            record(0, b"plain", &[]),
            record(1, &[b'x'; 128], &[]),
            record(2, b"tagged", &[("trace", "a")]),
        ]))
        .await
        .into_parts();

        assert_eq!(
            outcome.delivered,
            vec![SinkRecordId::new(0), SinkRecordId::new(2)]
        );
        let rejected = outcome
            .rejected
            .iter()
            .map(|rejected| rejected.id)
            .collect::<Vec<_>>();
        assert_eq!(rejected, vec![SinkRecordId::new(1)]);
        assert!(outcome.infrastructure_error.is_none());
        let (plain, tagged, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(plain, b"plain");
        assert_eq!(tagged, b"NATS/1.0\r\ntrace: a\r\n\r\ntagged");
    }

    /// A client without `timeout_ms` bounds no core write: a write whose client cannot flush
    /// waits until its server is back, and then delivers its record.
    #[nervix_primitives::test]
    async fn a_core_write_without_timeout_ms_waits_until_its_server_is_back() {
        let StrandedSink { mut sink, server } = stranded_core_sink(&[]).await;
        let script = async move {
            // The outage outlasts the bound the other checks configure.
            nervix_primitives::time::sleep(Duration::from_millis(500)).await;
            let server = server.come_back().await;
            let mut connection = server.accept(1024 * 1024).await;
            let publish = connection.expect_publish().await;
            (publish.message, connection)
        };

        let (outcome, (message, _connection)) = bounded(async {
            futures_util::join!(sink.publish(vec![record(0, b"waited", &[])]), script)
        })
        .await;

        let outcome = outcome.into_parts();
        assert_eq!(outcome.delivered, vec![SinkRecordId::new(0)]);
        assert!(outcome.infrastructure_error.is_none());
        assert_eq!(message, b"waited");
    }

    /// Finishing a core sink whose client closed while it still held a record reports the
    /// client's failure with the driver's flush error beneath.
    #[nervix_primitives::test]
    async fn finishing_a_core_sink_whose_client_closed_reports_its_failed_flush() {
        let StrandedSink { mut sink, server } =
            stranded_core_sink(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let held = bounded(sink.publish(vec![record(0, b"held", &[])]))
            .await
            .into_parts();
        assert!(
            held.infrastructure_error.is_some(),
            "the write runs out of timeout_ms"
        );
        let script = nervix_primitives::task::spawn(async move {
            let server = server.come_back().await;
            let mut connection = server.accept(1024 * 1024).await;
            let publish = connection.expect_publish().await;
            (publish.message, connection)
        });
        let (message, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(message, b"held");
        sink.client
            .drain()
            .await
            .expect("the client takes the request to close");
        // The client closes once its connection handler takes the request; from then on a flush
        // fails.
        let closed = nervix_primitives::time::timeout(CHECK_BOUND, async {
            while sink.client.flush().await.is_ok() {
                nervix_primitives::task::consume_budget().await;
            }
        })
        .await;
        assert!(closed.is_ok(), "the drained client did not close");

        let finished = bounded(sink.finish(check_deadline())).await;

        let failure = match finished {
            Ok(finished) => panic!("a closed client cannot flush, not {finished:?}"),
            Err(failure) => failure,
        };
        assert_eq!(
            *failure.current_context(),
            SinkFinishError::Flush { sink: NATS }
        );
        assert!(
            failure
                .downcast_ref::<async_nats::client::FlushError>()
                .is_some(),
            "the failure keeps the driver's flush error: {failure:?}"
        );
    }

    /// Every record of a completed core write was flushed to the client's socket, so finishing
    /// the sink has nothing to send.
    #[nervix_primitives::test]
    async fn finishing_a_core_sink_whose_writes_flushed_has_nothing_to_send() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let publish = connection.expect_publish().await;
            (publish.message, connection)
        });
        let mut sink = core_sink(&addr, &[("timeout_ms", WRITE_TIMEOUT_MS)]).await;

        let outcome = bounded(sink.publish(vec![record(0, b"sent", &[])]))
            .await
            .into_parts();
        let finished = bounded(sink.finish(check_deadline())).await;

        assert_eq!(outcome.delivered, vec![SinkRecordId::new(0)]);
        assert!(
            matches!(finished, Ok(SinkFinish::NothingHeld)),
            "{finished:?}"
        );
        let (message, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(message, b"sent");
    }

    #[test]
    fn a_core_write_takes_the_bound_the_clients_timeout_ms_sets() {
        let configured = NatsSink::write_timeout(&[ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "1500".to_string(),
        }])
        .expect("1500 ms is a valid deadline")
        .expect("the client sets timeout_ms");
        assert_eq!(configured.key(), DeadlineKey::Timeout);
        assert_eq!(configured.bound(), Duration::from_millis(1500));

        let omitted = NatsSink::write_timeout(&[]).expect("a client may leave timeout_ms out");
        assert_eq!(
            omitted, None,
            "without the key a core write waits for its flush"
        );
    }

    #[test]
    fn a_timeout_ms_that_breaks_the_deadline_rule_fails_the_start_as_configuration() {
        for value in ["0", "-1", "1.5", "1s", ""] {
            let failure = NatsSink::write_timeout(&[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: value.to_string(),
            }])
            .expect_err("the value breaks the deadline rule");
            assert_eq!(
                *failure.current_context(),
                SinkStartError::InvalidConfiguration { sink: NATS },
                "{value:?}"
            );
            assert!(
                matches!(
                    failure.downcast_ref::<nervix_connector::ClientConfigError>(),
                    Some(nervix_connector::ClientConfigError::InvalidDeadline {
                        key: "timeout_ms"
                    })
                ),
                "{value:?}: {failure:?}"
            );
        }
    }

    #[test]
    fn only_core_max_payload_is_a_record_rejection() {
        let oversized = NatsPublishError::new(NatsPublishErrorKind::MaxPayloadExceeded);
        let invalid_subject = NatsPublishError::new(NatsPublishErrorKind::InvalidSubject);

        assert!(NatsSink::is_core_record_rejection(&oversized));
        assert!(!NatsSink::is_core_record_rejection(&invalid_subject));
    }

    #[test]
    fn missing_jetstream_is_infrastructure_not_record_rejection() {
        let error = JetStreamPublishError::new(PublishErrorKind::StreamNotFound);
        assert!(NatsSink::jetstream_error_is_missing_stream(&error));
        assert!(!NatsSink::is_jetstream_record_rejection(&error));
    }

    #[test]
    fn oversized_jetstream_payload_is_a_record_rejection() {
        let error = JetStreamPublishError::new(PublishErrorKind::MaxPayloadExceeded);
        assert!(NatsSink::is_jetstream_record_rejection(&error));
        assert!(!NatsSink::jetstream_error_is_missing_stream(&error));
    }

    /// The first printable attachment of `failure`: the description an emitter shows as its
    /// transient error.
    fn printable_description(failure: &Report<SinkPublishError>) -> Option<String> {
        for frame in failure.frames() {
            if let error_stack::FrameKind::Attachment(error_stack::AttachmentKind::Printable(
                attachment,
            )) = frame.kind()
            {
                return Some(attachment.to_string());
            }
        }
        None
    }

    #[test]
    fn a_pub_ack_confirms_the_record() {
        let ack = serde_json::from_str::<PublishAck>(r#"{"stream":"events","seq":7}"#)
            .expect("a PubAck names its stream and sequence");
        let answer = NatsSink::broker_answer(Ok(ack));
        assert!(matches!(answer, Ok(BrokerVerdict::Confirmed)));
    }

    #[test]
    fn an_oversized_message_is_refused_for_good() {
        let error = JetStreamPublishError::new(PublishErrorKind::MaxPayloadExceeded);
        let answer = NatsSink::broker_answer(Err(error));
        let Ok(BrokerVerdict::Rejected { message }) = answer else {
            panic!("a message above max_payload is refused for good");
        };
        assert_eq!(
            message,
            "nats JetStream rejected record: max payload size exceeded"
        );
    }

    #[test]
    fn a_failed_pub_ack_leaves_the_record_unresolved_with_its_typed_cause() {
        let error = JetStreamPublishError::new(PublishErrorKind::StreamNotFound);
        let Err(failure) = NatsSink::broker_answer(Err(error)) else {
            panic!("a missing stream leaves the record's outcome unknown");
        };
        assert!(matches!(
            failure.current_context(),
            SinkPublishError::Publish { sink: NATS }
        ));
        let cause = failure
            .downcast_ref::<JetStreamPublishError>()
            .expect("the failure keeps the driver's typed cause");
        assert_eq!(cause.kind(), PublishErrorKind::StreamNotFound);
        assert_eq!(
            printable_description(&failure).as_deref(),
            Some("nats JetStream publish failed: no stream found for given subject")
        );
    }

    #[test]
    fn a_refused_hand_over_describes_the_client_failure() {
        let failure =
            NatsSink::refused_hand_over(JetStreamPublishError::new(PublishErrorKind::TimedOut));
        assert!(matches!(
            failure.current_context(),
            SinkPublishError::Publish { sink: NATS }
        ));
        let cause = failure
            .downcast_ref::<JetStreamPublishError>()
            .expect("the failure keeps the driver's typed cause");
        assert_eq!(cause.kind(), PublishErrorKind::TimedOut);
        assert_eq!(
            printable_description(&failure).as_deref(),
            Some(
                "nats JetStream client did not take the record: timed out: didn't receive ack in \
                 time"
            )
        );
    }

    #[test]
    fn connection_delay_uses_declared_exponential_policy() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(125),
            max_backoff: Duration::from_secs(1),
        };

        assert_eq!(NatsSink::connection_delay(policy, 1, false), Duration::ZERO);
        assert_eq!(
            NatsSink::connection_delay(policy, 2, false),
            Duration::from_millis(125)
        );
        assert_eq!(
            NatsSink::connection_delay(policy, 1, true),
            Duration::from_millis(125)
        );
        assert_eq!(
            NatsSink::connection_delay(policy, 2, true),
            Duration::from_millis(250)
        );
        assert_eq!(
            NatsSink::connection_delay(policy, 4, true),
            Duration::from_secs(1)
        );
        assert_eq!(
            NatsSink::connection_delay(policy, 50, true),
            Duration::from_secs(1)
        );
    }
}
