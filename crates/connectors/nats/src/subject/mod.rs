//! NATS ingestor and emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The NATS connection a client configures, its reconnect delay, the queue
//!   subscription an ingestor reads through, core publication, the bound the client's
//!   `timeout_ms` sets on a core write and the records a core client still holds unflushed,
//!   header mapping in both directions, and per-record rejection classification.
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

mod ingestor;
#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
pub(crate) mod scripted_server;

use std::{num::NonZeroU64, time::Duration};

use async_nats::{
    Client as NatsClient, PublishError as NatsPublishError,
    PublishErrorKind as NatsPublishErrorKind, Subject, message::OutboundMessage,
};
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::SinkExt;
pub(crate) use ingestor::PENDING_LOSS_SIGNALS;
pub use ingestor::{
    NatsMessageHeaders, NatsSubjectIngestor, NatsSubjectIngestorError, NatsSubjectIngestorMessage,
    NatsSubjectIngestorPlan,
};
use meticulous::OptionExt as _;
use nervix_connector::{
    ConfiguredDeadline, EmitterFinish, EmitterFinishError, EmitterFinishResult, EmitterHost,
    EmitterLifecycle, EmitterOpenError, EmitterRecord, EmitterRecordId, EmitterStartResult,
    EmitterWriteError, ParsedRetryPolicy, PerRecordOutcome, RecordEmitter, RejectedEmitterMember,
    ServiceUrl, client_config_value, client_tls_paths,
};
use nervix_models::{ClientConfigEntry, ConnectorKind, NatsPublishSubjectName};
use nervix_primitives::{
    sync::{
        StdArc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

const NATS: &str = "nats";

/// What one NATS SUBJECT emitter publishes through: its client entries, subject and reconnect
/// backoff.
pub struct NatsSubjectEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub subject: NatsPublishSubjectName,
    pub retry_policy: ParsedRetryPolicy,
}

pub struct NatsSubjectEmitter {
    client: NatsClient,
    writes: CoreWrites,
    subject: Subject,
}

/// The wait the client's `timeout_ms` bounds, as a write that runs out of it names the wait.
const CORE_WRITE: &str = "a core NATS publish and its flush";

/// The core writes of one emitter: the bound the client's `timeout_ms` sets on each, and the records
/// the client took whose flush has not completed on their connection.
///
/// A core write delivers only after its connection flushes. A replacement connection cannot
/// certify the preceding connection's discarded bytes. A timed-out attempt keeps its count and
/// connection expectation until a later flush drains the client or reports the loss.
struct CoreWrites {
    /// The client's `timeout_ms`, which bounds each write, the hand-over and the flush included.
    /// Without the key a write waits for its flush.
    timeout: Option<ConfiguredDeadline>,
    pending: CoreBuffer,
}

/// The connection that must flush one write. A write queued while disconnected belongs to the
/// next successful connection; a second reconnect makes its outcome uncertain again.
#[derive(Clone, Copy)]
enum CoreConnection {
    Connected { generation: u64 },
    Reconnecting { after: u64 },
}

impl CoreConnection {
    fn capture(client: &NatsClient) -> Self {
        let generation = client.connection_generation();
        match client.connection_state() {
            async_nats::connection::State::Connected => Self::Connected { generation },
            async_nats::connection::State::Pending
            | async_nats::connection::State::Disconnected => {
                Self::Reconnecting { after: generation }
            }
        }
    }

    fn accepts(self, generation: u64) -> bool {
        match self {
            Self::Connected {
                generation: expected,
            } => generation == expected,
            Self::Reconnecting { after } => after.checked_add(1) == Some(generation),
        }
    }
}

/// A pending count always carries the connection whose flush can settle it.
enum CoreBuffer {
    Empty,
    Pending {
        records: NonZeroU64,
        connection: CoreConnection,
    },
}

impl CoreBuffer {
    fn records(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Pending { records, .. } => records.get(),
        }
    }

    fn accepted(&mut self, connection: CoreConnection) {
        match self {
            Self::Empty => {
                *self = Self::Pending {
                    records: NonZeroU64::MIN,
                    connection,
                };
            }
            Self::Pending { records, .. } => {
                *records = records.checked_add(1).assured(
                    "the records one client holds are in memory, far fewer than u64 counts",
                );
            }
        }
    }

    /// A completed driver flush leaves no queued bytes, even when reconnection discarded part
    /// of this attempt. Clear that drained state, but keep the uncertain records unresolved.
    fn flushed(&mut self, generation: u64) -> Result<(), Report<CoreFlushError>> {
        let pending = std::mem::replace(self, Self::Empty);
        match pending {
            Self::Empty => Ok(()),
            Self::Pending {
                records,
                connection,
            } => {
                if connection.accepts(generation) {
                    Ok(())
                } else {
                    Err(Report::new(CoreFlushError::ConnectionChanged { records }))
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum CoreFlushError {
    #[error(
        "{} client failed to flush its pending records",
        nervix_models::ConnectorKind::Nats
    )]
    ClientFlush,
    #[error(
        "{} connection changed before {records} pending records were flushed",
        nervix_models::ConnectorKind::Nats
    )]
    ConnectionChanged { records: NonZeroU64 },
}

impl CoreWrites {
    /// Core writes bounded by `timeout`, of a client that holds nothing yet.
    fn new(timeout: Option<ConfiguredDeadline>) -> Self {
        Self {
            timeout,
            pending: CoreBuffer::Empty,
        }
    }

    /// Writes `records` to `subject` through `client` within the client's `timeout_ms`, and
    /// answers for each of them.
    async fn publish(
        &mut self,
        client: &NatsClient,
        subject: &Subject,
        records: Vec<EmitterRecord>,
    ) -> PerRecordOutcome<EmitterRecordId> {
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
    /// A cut-short write retains both its count and the connection that must complete its flush.
    async fn write(
        &mut self,
        client: &NatsClient,
        subject: &Subject,
        records: Vec<EmitterRecord>,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) -> Result<Vec<EmitterRecordId>, Report<EmitterWriteError>> {
        // Records handed over before what the client still holds is flushed would queue behind
        // it, and a write that retries those records would queue a second copy of each.
        if let CoreBuffer::Pending { .. } = self.pending {
            self.flush(client).await.map_err(Self::publish_failure)?;
        }
        let connection = CoreConnection::capture(client);
        let mut connector = client.clone();
        let mut handed_over = Vec::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let reply = match record.envelope.reply_to {
                Some(reply) => match NatsPublishSubjectName::try_from(reply) {
                    Ok(reply) => Some(Subject::from(reply.to_string())),
                    Err(cause) => {
                        let report = Report::new(cause);
                        outcome.reject(RejectedEmitterMember::external(
                            record_id,
                            occurred_at,
                            format!("{} reply subject rejected: {report:#}", ConnectorKind::Nats),
                        ));
                        continue;
                    }
                },
                None => None,
            };
            let headers = if record.headers.is_empty() {
                None
            } else {
                Some(NatsSubjectEmitter::header_map(&record.headers))
            };
            let result = connector
                .feed(OutboundMessage {
                    subject: subject.clone(),
                    reply,
                    payload: record.payload.into(),
                    headers,
                })
                .await;
            match result {
                Ok(()) => {
                    handed_over.push(record_id);
                    self.pending.accepted(connection);
                }
                Err(error) if NatsSubjectEmitter::is_core_record_rejection(&error) => {
                    outcome.reject(RejectedEmitterMember::external(
                        record_id,
                        occurred_at,
                        format!("nats rejected record: {error}"),
                    ));
                }
                Err(error) => return Err(NatsSubjectEmitter::publish_error(error)),
            }
        }
        self.flush(client).await.map_err(Self::publish_failure)?;
        Ok(handed_over)
    }

    /// Drains the client's queued bytes, then checks the connection that flushed them. The
    /// driver's completed flush orders its generation observation; its private atomics and socket
    /// remain outside Nervix's modeled execution.
    async fn flush(&mut self, client: &NatsClient) -> Result<(), Report<CoreFlushError>> {
        if let CoreBuffer::Empty = self.pending {
            return Ok(());
        }
        client
            .flush()
            .await
            .change_context(CoreFlushError::ClientFlush)?;
        self.pending.flushed(client.connection_generation())
    }

    fn publish_failure(error: Report<CoreFlushError>) -> Report<EmitterWriteError> {
        let description = format!("{error:#}");
        error
            .change_context(EmitterWriteError::Publish { connector: NATS })
            .attach_printable(description)
    }

    /// The failure of a write that ran out of the client's `timeout_ms`. An emitter shows the
    /// message its emitter attached, so the deadline leads.
    fn elapsed(timeout: ConfiguredDeadline) -> Report<EmitterWriteError> {
        let elapsed = timeout.elapsed(CORE_WRITE);
        Report::new(elapsed)
            .change_context(EmitterWriteError::Publish { connector: NATS })
            .attach_printable(elapsed.to_string())
    }

    /// Flushes what the client still holds of the records writes handed it, by `deadline`.
    async fn finish(&mut self, client: &NatsClient, deadline: Instant) -> EmitterFinishResult {
        let records = self.pending.records();
        if let CoreBuffer::Empty = self.pending {
            return Ok(EmitterFinish::NothingHeld);
        }
        let flushed = nervix_primitives::time::timeout_at(deadline, self.flush(client)).await;
        match flushed {
            Ok(Ok(())) => Ok(EmitterFinish::Flushed),
            Ok(Err(error)) => {
                let description = format!("{error:#}");
                Err(error
                    .change_context(EmitterFinishError::Flush { connector: NATS })
                    .attach_printable(description))
            }
            Err(_elapsed) => Err(Report::new(EmitterFinishError::Unsent {
                connector: NATS,
                records,
            })),
        }
    }
}

impl NatsSubjectEmitter {
    pub async fn new(
        config: NatsSubjectEmitterConfig,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        // Read before the client connects, so a value that breaks the deadline rule fails the
        // start as configuration whether or not the server answers.
        let write_timeout = Self::write_timeout(&config.config)?;
        let client = Self::client_from_config(&config.config, config.retry_policy).await?;
        Ok(Self {
            client,
            writes: CoreWrites::new(write_timeout),
            subject: Subject::from(config.subject.as_str().to_string()),
        })
    }

    /// The bound the client's `timeout_ms` sets on each core write, where it sets one.
    fn write_timeout(
        config: &[ClientConfigEntry],
    ) -> EmitterStartResult<Option<ConfiguredDeadline>> {
        ConfiguredDeadline::timeout(config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: NATS })
    }

    pub(crate) async fn client_from_config(
        config: &[ClientConfigEntry],
        retry_policy: ParsedRetryPolicy,
    ) -> EmitterStartResult<NatsClient> {
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
        if ServiceUrl::new(&addr, "NATS addr")
            .has_scheme("tls")
            .change_context(EmitterOpenError::InvalidConfiguration { connector: NATS })?
        {
            options = options.require_tls(true);
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

    pub(crate) fn header_map(headers: &[(String, String)]) -> async_nats::HeaderMap {
        let mut header_map = async_nats::HeaderMap::new();
        for (name, value) in headers {
            header_map.append(name.as_str(), value.as_str());
        }
        header_map
    }

    fn is_core_record_rejection(error: &NatsPublishError) -> bool {
        matches!(error.kind(), NatsPublishErrorKind::MaxPayloadExceeded)
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> EmitterStartResult<String> {
        {
            let mapping_input = client_config_value(config, key, "NATS");
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let message = error.current_context().to_string();
                    error
                        .change_context(EmitterOpenError::InvalidConfiguration { connector: NATS })
                        .attach_printable(message)
                }),
            }
        }
    }

    fn config_error(error: impl std::fmt::Display) -> Report<EmitterOpenError> {
        Report::new(EmitterOpenError::InvalidConfiguration { connector: NATS })
            .attach_printable(error.to_string())
    }

    fn start_error(error: impl std::fmt::Display) -> Report<EmitterOpenError> {
        Report::new(EmitterOpenError::Initialize { connector: NATS })
            .attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "formats the typed external NATS driver failure; no internal runtime \
                      ownership is inferred"
        )
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<EmitterWriteError> {
        Report::new(EmitterWriteError::Publish { connector: NATS })
            .attach_printable(error.to_string())
    }
}

impl NatsSubjectEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Nats(nervix_models::NatsEmitterEntity::Subject);
}

#[async_trait]
impl EmitterLifecycle for NatsSubjectEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// A failed or timed-out core write retains its count and connection expectation. Finishing
    /// validates that connection's flush by `deadline`; a replacement connection cannot certify
    /// discarded bytes.
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        self.writes.finish(&self.client, deadline).await
    }

    /// A publish failure leaves the client connected: it reconnects on its own schedule, and
    /// dropping it would discard the session the broker still holds.
    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }
}

#[async_trait]
impl RecordEmitter for NatsSubjectEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        self.writes
            .publish(&self.client, &self.subject, records)
            .await
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::future::Future;

    use async_nats::connection::State;
    use meticulous::ResultExt as _;
    use nervix_connector::{DeadlineElapsed, EmitterFinishError};
    use nervix_models::{DeadlineKey, Timestamp};

    use super::*;
    use crate::subject::scripted_server::{DownServer, ScriptedServer};

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

    /// A core emitter publishing to subject `events` through the server at `addr`, with the client
    /// entries `config` adds to its address.
    async fn core_emitter(addr: &str, config: &[(&str, &str)]) -> NatsSubjectEmitter {
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
        let write_timeout = NatsSubjectEmitter::write_timeout(&entries)
            .expect("the checks configure a valid timeout_ms");
        let client = NatsSubjectEmitter::client_from_config(&entries, policy)
            .await
            .expect("the emitter connects to the scripted server");
        NatsSubjectEmitter {
            writes: CoreWrites::new(write_timeout),
            client,
            subject: Subject::from("events"),
        }
    }

    /// A core emitter whose server went down after the emitter connected: the server ended the
    /// connection and refuses every one its client opens to reconnect, until it comes back.
    struct StrandedEmitter {
        connector: NatsSubjectEmitter,
        server: DownServer,
    }

    /// A [`StrandedEmitter`] whose client sets `config`, returned once its client noticed that its
    /// server went down.
    async fn stranded_core_emitter(config: &[(&str, &str)]) -> StrandedEmitter {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let connection = server.greet(1024 * 1024).await;
            connection.end().await;
            server.go_down()
        });
        let connector = core_emitter(&addr, config).await;
        let server = script.await.expect("the scripted server ran");
        wait_for_state(&connector.client, State::Disconnected).await;
        StrandedEmitter { connector, server }
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

    fn record(index: usize, payload: &[u8], headers: &[(&str, &str)]) -> EmitterRecord {
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect();
        EmitterRecord::new(
            EmitterRecordId::new(index),
            None,
            payload.to_vec(),
            headers,
            Timestamp::from_unix_nanos(0),
        )
    }

    #[nervix_primitives::test]
    async fn a_core_write_keeps_each_reply_subject_and_rejects_an_invalid_one() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let first = connection.expect_publish().await;
            let second = connection.expect_publish().await;
            (first, second, connection)
        });
        let mut connector = core_emitter(&addr, &[]).await;
        let mut first = record(0, b"first", &[]);
        first.envelope.reply_to = Some("reply.first".to_string());
        let mut second = record(1, b"second", &[("trace", "a")]);
        second.envelope.reply_to = Some("reply.second".to_string());
        let mut invalid = record(2, b"refused", &[]);
        invalid.envelope.reply_to = Some("reply.*".to_string());
        let outcome = bounded(connector.publish(vec![first, second, invalid]))
            .await
            .into_parts();
        assert_eq!(
            outcome.delivered,
            vec![EmitterRecordId::new(0), EmitterRecordId::new(1)]
        );
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].id, EmitterRecordId::new(2));
        assert!(outcome.infrastructure_error.is_none());
        let (first, second, _connection) =
            script.await.expect("both valid records reached the server");
        assert_eq!(first.reply.as_deref(), Some("reply.first"));
        assert_eq!(second.reply.as_deref(), Some("reply.second"));
        assert_eq!(first.message, b"first");
        assert_eq!(second.message, b"NATS/1.0\r\ntrace: a\r\n\r\nsecond");
    }

    /// A core write whose client cannot flush, because its server went down and refuses every
    /// reconnection, fails within the client's `timeout_ms` and leaves its record unresolved. The
    /// failure names the deadline it ran out of, which the emitter shows as its transient error.
    #[nervix_primitives::test]
    async fn a_core_write_its_client_cannot_flush_fails_within_timeout_ms() {
        let StrandedEmitter {
            mut connector,
            server: _down,
        } = stranded_core_emitter(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;

        let outcome = bounded(connector.publish(vec![record(0, b"held", &[])]))
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
            EmitterWriteError::Publish { connector: NATS }
        ));
        let elapsed = failure
            .downcast_ref::<DeadlineElapsed>()
            .expect("the failure names the deadline the write ran out of");
        assert_eq!(elapsed.key, DeadlineKey::Timeout);
        assert_eq!(elapsed.bound, Duration::from_millis(200));
        assert_eq!(printable_description(&failure), Some(elapsed.to_string()));
    }

    /// A reconnect discards the partly written record. Flushing the replacement connection
    /// cannot establish that this write sent the record it handed to the failed connection.
    #[nervix_primitives::test]
    async fn a_core_write_does_not_deliver_a_record_its_failed_connection_discarded() {
        const PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
        let server = ScriptedServer::with_receive_buffer(4_096).await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.greet(PAYLOAD_BYTES).await;
            let bytes = connection.expect_publish_prefix(b"discarded").await;
            assert_eq!(bytes, PAYLOAD_BYTES);
            connection.end().await;
            server.accept(PAYLOAD_BYTES).await
        });
        let mut connector = core_emitter(&addr, &[]).await;
        let mut payload = vec![b'x'; PAYLOAD_BYTES];
        payload[..9].copy_from_slice(b"discarded");

        let (outcome, connection) = bounded(async {
            futures_util::join!(connector.publish(vec![record(0, &payload, &[])]), script)
        })
        .await;

        let outcome = outcome.into_parts();
        assert!(
            outcome.delivered.is_empty(),
            "a replacement connection cannot flush the record the failed connection discarded"
        );
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .assured("the interrupted write reports its uncertainty");
        assert_eq!(
            failure.downcast_ref::<CoreFlushError>(),
            Some(&CoreFlushError::ConnectionChanged {
                records: NonZeroU64::MIN,
            })
        );
        let mut connection = connection.assured("the replacement connection is greeted");
        let (retried, received) = bounded(async {
            futures_util::join!(
                connector.publish(vec![record(0, b"retry", &[])]),
                connection.expect_publish()
            )
        })
        .await;
        let retried = retried.into_parts();
        assert_eq!(retried.delivered, vec![EmitterRecordId::new(0)]);
        assert!(retried.infrastructure_error.is_none());
        assert_eq!(received.message, b"retry");
    }

    /// Finishing cannot certify the outstanding record when the connection holding its
    /// unwritten suffix ends while finish is waiting for that suffix to flush.
    #[nervix_primitives::test]
    async fn a_core_finish_does_not_certify_records_its_failed_connection_discarded() {
        const PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
        let server = ScriptedServer::with_receive_buffer(4_096).await;
        let addr = server.addr();
        let (end, ended) = nervix_primitives::sync::oneshot::channel();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.greet(PAYLOAD_BYTES).await;
            let bytes = connection.expect_publish_prefix(b"discarded").await;
            assert_eq!(bytes, PAYLOAD_BYTES);
            ended
                .await
                .assured("the check ends the original connection");
            connection.end().await;
            server.accept(PAYLOAD_BYTES).await
        });
        let mut connector = core_emitter(&addr, &[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let mut payload = vec![b'x'; PAYLOAD_BYTES];
        payload[..9].copy_from_slice(b"discarded");
        let outcome = bounded(connector.publish(vec![record(0, &payload, &[])]))
            .await
            .into_parts();
        assert!(outcome.delivered.is_empty());
        assert!(outcome.infrastructure_error.is_some());

        let finish = connector.finish(check_deadline());
        futures_util::pin_mut!(finish);
        assert!(futures_util::poll!(finish.as_mut()).is_pending());
        end.send(()).assured("the original connection waits to end");
        let (finished, connection) = bounded(async { futures_util::join!(finish, script) }).await;
        let _connection = connection.assured("the replacement connection is greeted");

        let failure = match finished {
            Err(failure) => failure,
            Ok(finished) => panic!("the interrupted connection cannot certify {finished:?}"),
        };
        assert_eq!(
            *failure.current_context(),
            EmitterFinishError::Flush { connector: NATS }
        );
        assert_eq!(
            failure.downcast_ref::<CoreFlushError>(),
            Some(&CoreFlushError::ConnectionChanged {
                records: NonZeroU64::MIN,
            })
        );
    }

    /// A write after one that ran out of `timeout_ms` first waits for the client to flush what it
    /// still holds, and hands the client none of its own records while it cannot. The client
    /// therefore never queues a second copy behind the record it holds: once its server is back it
    /// sends that record once, and then the record of the write that found it flushed.
    #[nervix_primitives::test]
    async fn a_core_write_flushes_what_its_client_still_holds_before_it_hands_over_more() {
        let StrandedEmitter {
            mut connector,
            server,
        } = stranded_core_emitter(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let held = bounded(connector.publish(vec![record(0, b"held", &[])]))
            .await
            .into_parts();
        assert!(
            held.infrastructure_error.is_some(),
            "the first write runs out of timeout_ms"
        );
        let refused = bounded(connector.publish(vec![record(0, b"refused", &[])]))
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
        wait_for_state(&connector.client, State::Connected).await;
        let next = bounded(connector.publish(vec![record(0, b"next", &[])]))
            .await
            .into_parts();

        assert_eq!(next.delivered, vec![EmitterRecordId::new(0)]);
        assert!(next.infrastructure_error.is_none());
        let (first, second, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(first, b"held");
        assert_eq!(
            second, b"next",
            "the write that found the client holding a record handed over none of its own"
        );
    }

    /// Finishing a core emitter whose write ran out of `timeout_ms` flushes what its client still
    /// holds by the finish deadline, which sends the record once the server is back.
    #[nervix_primitives::test]
    async fn finishing_a_core_emitter_sends_what_a_stalled_write_left_with_its_client() {
        let StrandedEmitter {
            mut connector,
            server,
        } = stranded_core_emitter(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let held = bounded(connector.publish(vec![record(0, b"held", &[])]))
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
        let finished = bounded(connector.finish(check_deadline())).await;

        assert!(
            matches!(finished, Ok(EmitterFinish::Flushed)),
            "{finished:?}"
        );
        let (message, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(message, b"held");
    }

    /// Finishing a core emitter whose client still holds the records of a write that ran out of
    /// `timeout_ms` when the finish deadline passes counts them.
    #[nervix_primitives::test]
    async fn finishing_a_core_emitter_counts_what_its_client_still_holds_at_the_deadline() {
        let StrandedEmitter {
            mut connector,
            server: _down,
        } = stranded_core_emitter(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let held =
            bounded(connector.publish(vec![record(0, b"first", &[]), record(1, b"second", &[])]))
                .await
                .into_parts();
        assert!(
            held.infrastructure_error.is_some(),
            "the write runs out of timeout_ms"
        );

        let deadline = Instant::now()
            .checked_add(Duration::from_millis(200))
            .expect("a moment from now is within the monotonic clock's range");
        let finished = bounded(connector.finish(deadline)).await;

        let failure = match finished {
            Ok(finished) => {
                panic!("a client that cannot flush leaves its records unsent, not {finished:?}")
            }
            Err(failure) => failure,
        };
        assert_eq!(
            *failure.current_context(),
            EmitterFinishError::Unsent {
                connector: NATS,
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
        let mut connector = core_emitter(&addr, &[("timeout_ms", WRITE_TIMEOUT_MS)]).await;

        let outcome = bounded(connector.publish(vec![
            record(0, b"plain", &[]),
            record(1, &[b'x'; 128], &[]),
            record(2, b"tagged", &[("trace", "a")]),
        ]))
        .await
        .into_parts();

        assert_eq!(
            outcome.delivered,
            vec![EmitterRecordId::new(0), EmitterRecordId::new(2)]
        );
        let rejected = outcome
            .rejected
            .iter()
            .map(|rejected| rejected.id)
            .collect::<Vec<_>>();
        assert_eq!(rejected, vec![EmitterRecordId::new(1)]);
        assert!(outcome.infrastructure_error.is_none());
        let (plain, tagged, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(plain, b"plain");
        assert_eq!(tagged, b"NATS/1.0\r\ntrace: a\r\n\r\ntagged");
    }

    /// A client without `timeout_ms` bounds no core write: a write whose client cannot flush
    /// waits until its server is back, and then delivers its record.
    #[nervix_primitives::test]
    async fn a_core_write_without_timeout_ms_waits_until_its_server_is_back() {
        let StrandedEmitter {
            mut connector,
            server,
        } = stranded_core_emitter(&[]).await;
        let script = async move {
            // The outage outlasts the bound the other checks configure.
            nervix_primitives::time::sleep(Duration::from_millis(500)).await;
            let server = server.come_back().await;
            let mut connection = server.accept(1024 * 1024).await;
            let publish = connection.expect_publish().await;
            (publish.message, connection)
        };

        let (outcome, (message, _connection)) = bounded(async {
            futures_util::join!(connector.publish(vec![record(0, b"waited", &[])]), script)
        })
        .await;

        let outcome = outcome.into_parts();
        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        assert!(outcome.infrastructure_error.is_none());
        assert_eq!(message, b"waited");
    }

    /// Finishing a core emitter whose client closed while it still held a record reports the
    /// client's failure with the driver's flush error beneath.
    #[nervix_primitives::test]
    async fn finishing_a_core_emitter_whose_client_closed_reports_its_failed_flush() {
        let StrandedEmitter {
            mut connector,
            server,
        } = stranded_core_emitter(&[("timeout_ms", WRITE_TIMEOUT_MS)]).await;
        let held = bounded(connector.publish(vec![record(0, b"held", &[])]))
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
        connector
            .client
            .drain()
            .await
            .expect("the client takes the request to close");
        // The client closes once its connection handler takes the request; from then on a flush
        // fails.
        let closed = nervix_primitives::time::timeout(CHECK_BOUND, async {
            while connector.client.flush().await.is_ok() {
                nervix_primitives::task::consume_budget().await;
            }
        })
        .await;
        assert!(closed.is_ok(), "the drained client did not close");

        let finished = bounded(connector.finish(check_deadline())).await;

        let failure = match finished {
            Ok(finished) => panic!("a closed client cannot flush, not {finished:?}"),
            Err(failure) => failure,
        };
        assert_eq!(
            *failure.current_context(),
            EmitterFinishError::Flush { connector: NATS }
        );
        assert!(
            failure
                .downcast_ref::<async_nats::client::FlushError>()
                .is_some(),
            "the failure keeps the driver's flush error: {failure:?}"
        );
    }

    /// Every record of a completed core write was flushed to the client's socket, so finishing
    /// the emitter has nothing to send.
    #[nervix_primitives::test]
    async fn finishing_a_core_emitter_whose_writes_flushed_has_nothing_to_send() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let publish = connection.expect_publish().await;
            (publish.message, connection)
        });
        let mut connector = core_emitter(&addr, &[("timeout_ms", WRITE_TIMEOUT_MS)]).await;

        let outcome = bounded(connector.publish(vec![record(0, b"sent", &[])]))
            .await
            .into_parts();
        let finished = bounded(connector.finish(check_deadline())).await;

        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        assert!(
            matches!(finished, Ok(EmitterFinish::NothingHeld)),
            "{finished:?}"
        );
        let (message, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(message, b"sent");
    }

    #[test]
    fn a_core_write_takes_the_bound_the_clients_timeout_ms_sets() {
        let configured = NatsSubjectEmitter::write_timeout(&[ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "1500".to_string(),
        }])
        .expect("1500 ms is a valid deadline")
        .expect("the client sets timeout_ms");
        assert_eq!(configured.key(), DeadlineKey::Timeout);
        assert_eq!(configured.bound(), Duration::from_millis(1500));

        let omitted =
            NatsSubjectEmitter::write_timeout(&[]).expect("a client may leave timeout_ms out");
        assert_eq!(
            omitted, None,
            "without the key a core write waits for its flush"
        );
    }

    #[test]
    fn a_timeout_ms_that_breaks_the_deadline_rule_fails_the_start_as_configuration() {
        for value in ["0", "-1", "1.5", "1s", ""] {
            let failure = NatsSubjectEmitter::write_timeout(&[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: value.to_string(),
            }])
            .expect_err("the value breaks the deadline rule");
            assert_eq!(
                *failure.current_context(),
                EmitterOpenError::InvalidConfiguration { connector: NATS },
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

        assert!(NatsSubjectEmitter::is_core_record_rejection(&oversized));
        assert!(!NatsSubjectEmitter::is_core_record_rejection(
            &invalid_subject
        ));
    }

    /// The first printable attachment of `failure`: the description an emitter shows as its
    /// transient error.
    fn printable_description(failure: &Report<EmitterWriteError>) -> Option<String> {
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
    fn connection_delay_uses_declared_exponential_policy() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(125),
            max_backoff: Duration::from_secs(1),
        };

        assert_eq!(
            NatsSubjectEmitter::connection_delay(policy, 1, false),
            Duration::ZERO
        );
        assert_eq!(
            NatsSubjectEmitter::connection_delay(policy, 2, false),
            Duration::from_millis(125)
        );
        assert_eq!(
            NatsSubjectEmitter::connection_delay(policy, 1, true),
            Duration::from_millis(125)
        );
        assert_eq!(
            NatsSubjectEmitter::connection_delay(policy, 2, true),
            Duration::from_millis(250)
        );
        assert_eq!(
            NatsSubjectEmitter::connection_delay(policy, 4, true),
            Duration::from_secs(1)
        );
        assert_eq!(
            NatsSubjectEmitter::connection_delay(policy, 50, true),
            Duration::from_secs(1)
        );
    }
}
