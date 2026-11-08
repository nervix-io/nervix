//! JetStream publication through one named stream.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Stream lookup, expected-stream publication, message IDs, and PubAck outcomes.
//! - **Depends on.** The NATS client, connector confirmation window, vocabulary and async-nats.
//! - **Must not know.** Runtime batches, relays, branches, schedules or registry state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the engines boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

use std::{error::Error as _, future::Future, pin::Pin};

use async_nats::{
    Client as NatsClient, Subject,
    jetstream::{
        Context as NatsJetStream, Error as NatsJetStreamError, ErrorCode,
        context::{
            GetStreamErrorKind, PublishAckFuture, PublishError as JetStreamPublishError,
            PublishErrorKind,
        },
        message::PublishMessage,
        publish::PublishAck,
    },
};
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerAnswer, BrokerVerdict, ConfirmationWindow, EmitterFinish,
    EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterOpenError, EmitterRecord,
    EmitterRecordId, EmitterStartResult, EmitterWriteError, ParsedRetryPolicy, PerRecordOutcome,
    RecordEmitter, RejectedEmitterMember,
};
use nervix_models::{ClientConfigEntry, NatsPublishSubjectName, NatsStreamName};
use nervix_primitives::time::Instant;

use crate::subject::NatsSubjectEmitter;

const NATS: &str = "nats";

/// A stream emitter's client configuration, destination and bounded confirmation policy.
pub struct NatsStreamEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub stream: NatsStreamName,
    pub subject: NatsPublishSubjectName,
    pub confirmation: AckConfirmation,
    pub retry_policy: ParsedRetryPolicy,
}

/// One open JetStream publisher, with a client kept alive for its connection.
pub struct NatsStreamEmitter {
    client: Option<NatsClient>,
    context: Option<NatsJetStream>,
    prepared: Option<crate::client::NatsClientOptions>,
    stream: NatsStreamName,
    subject: Subject,
    confirmation: AckConfirmation,
}

/// JetStream's answer for one record, resolved by its PubAck or a publish failure.
type NatsAnswer = Pin<Box<dyn Future<Output = BrokerAnswer> + Send>>;

impl NatsStreamEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Nats(nervix_models::NatsEmitterEntity::Stream);

    async fn prepare(
        config: NatsStreamEmitterConfig,
        host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let prepared = NatsSubjectEmitter::prepare_client(
            &host.executor(),
            &config.config,
            config.retry_policy,
        )
        .await?;
        Ok(Self {
            client: None,
            context: None,
            prepared: Some(prepared),
            stream: config.stream,
            subject: Subject::from(config.subject.as_str().to_owned()),
            confirmation: config.confirmation,
        })
    }

    fn context(client: &NatsClient, confirmation: AckConfirmation) -> NatsJetStream {
        async_nats::jetstream::ContextBuilder::new()
            .timeout(confirmation.timeout)
            .ack_timeout(confirmation.timeout)
            .max_ack_inflight(confirmation.max_in_flight.get())
            .backpressure_on_inflight(true)
            .build(client.clone())
    }

    async fn await_answer(pending: PublishAckFuture) -> BrokerAnswer {
        Self::broker_answer(pending.await)
    }

    fn broker_answer(acknowledgement: Result<PublishAck, JetStreamPublishError>) -> BrokerAnswer {
        match acknowledgement {
            Ok(_ack) => Ok(BrokerVerdict::Confirmed),
            Err(error) if Self::is_record_rejection(&error) => Ok(BrokerVerdict::Rejected {
                message: format!("nats JetStream rejected record: {error}"),
            }),
            Err(error) => {
                let description = format!("nats JetStream publish failed: {error}");
                Err(Report::new(error)
                    .change_context(EmitterWriteError::Publish { connector: NATS })
                    .attach_printable(description))
            }
        }
    }

    fn refused_hand_over(error: JetStreamPublishError) -> Report<EmitterWriteError> {
        let description = format!("nats JetStream client did not take the record: {error}");
        Report::new(error)
            .change_context(EmitterWriteError::Publish { connector: NATS })
            .attach_printable(description)
    }

    fn is_record_rejection(error: &JetStreamPublishError) -> bool {
        if matches!(
            error.kind(),
            PublishErrorKind::MaxPayloadExceeded | PublishErrorKind::StreamNotFound
        ) {
            return true;
        }
        error
            .source()
            .and_then(|source| source.downcast_ref::<NatsJetStreamError>())
            .is_some_and(|cause| cause.error_code() == ErrorCode::STREAM_NOT_MATCH)
    }
}

#[async_trait]
impl EmitterLifecycle for NatsStreamEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    fn needs_resume(&mut self) -> bool {
        self.context.is_none()
    }

    fn resume(
        &mut self,
    ) -> nervix_connector::EmitterLifecycleStep<'_, nervix_connector::EmitterResume> {
        if !self.needs_resume() {
            return nervix_connector::EmitterLifecycleStep::ready(Ok(
                nervix_connector::EmitterResume::Ready,
            ));
        }
        nervix_connector::EmitterLifecycleStep::run(async move {
            let resuming = nervix_connector::EmitterLifecycleError::Resume { connector: NATS };
            if self.client.is_none() {
                let prepared = self
                    .prepared
                    .as_ref()
                    .verified("an unconnected emitter retains its prepared options");
                self.client = Some(
                    NatsSubjectEmitter::connect_client(prepared)
                        .await
                        .change_context(resuming)?,
                );
                self.prepared = None;
            }
            if self.context.is_none() {
                let context = Self::context(
                    self.client
                        .as_ref()
                        .verified("resumption acquired the client"),
                    self.confirmation,
                );
                if let Err(error) = context.get_stream(self.stream.as_str()).await {
                    let missing = matches!(error.kind(), GetStreamErrorKind::JetStream(ref cause)
                    if cause.error_code() == ErrorCode::STREAM_NOT_FOUND);
                    let report = if missing {
                        let missing = EmitterOpenError::MissingExternalEntity {
                            connector: NATS,
                            kind: "NATS stream",
                            name: self.stream.as_str().to_owned(),
                        };
                        let description = missing.to_string();
                        Report::new(error)
                            .change_context(missing)
                            .attach_printable(description)
                    } else {
                        Report::new(error)
                            .change_context(EmitterOpenError::Initialize { connector: NATS })
                    };
                    return Err(report.change_context(resuming));
                }
                self.context = Some(context);
            }
            Ok(nervix_connector::EmitterResume::Ready)
        })
    }

    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RecordEmitter for NatsStreamEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        let Some(context) = self.context.as_ref() else {
            let mut outcome = PerRecordOutcome::with_capacity(records.len());
            outcome.fail(Report::new(EmitterWriteError::NotInitialized {
                connector: NATS,
            }));
            return outcome;
        };
        let mut window: ConfirmationWindow<NatsAnswer> =
            ConfirmationWindow::new(NATS, self.confirmation, records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let headers = async_nats::HeaderMap::from_ordered_with_stream(
                record.headers,
                record.envelope.message_id,
                self.stream.as_str().to_owned(),
            );
            let message = PublishMessage::build()
                .payload(record.payload.into())
                .headers(headers);
            let handed_over = context.send_publish(self.subject.clone(), message).await;
            let pending = match handed_over {
                Ok(pending) => pending,
                Err(error) if Self::is_record_rejection(&error) => {
                    window.reject(RejectedEmitterMember::external(
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
}

impl nervix_connector::EmitterConnector for NatsStreamEmitter {
    const KIND: nervix_models::EmitterEntityKind = Self::KIND;
    type Plan = NatsStreamEmitterConfig;
    async fn open(plan: Self::Plan, host: EmitterHost) -> EmitterStartResult<Self> {
        Self::prepare(plan, host).await
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use meticulous::ResultExt as _;
    use nervix_models::Timestamp;

    use super::*;
    use crate::subject::scripted_server::ScriptedServer;

    const CONFIRMED: &str = r#"{"stream":"orders","seq":1}"#;

    async fn emitter(addr: &str, max_in_flight: usize) -> NatsStreamEmitter {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
        };
        let config = [ClientConfigEntry {
            key: "addr".to_owned(),
            value: addr.to_owned(),
        }];
        let prepared = NatsSubjectEmitter::prepare_client(
            &nervix_execution::Executor::default(),
            &config,
            policy,
        )
        .await
        .expect("the emitter connects to the scripted server");
        let client = NatsSubjectEmitter::connect_client(&prepared)
            .await
            .assured("the fixture connects");
        let confirmation = AckConfirmation {
            max_in_flight: NonZeroUsize::new(max_in_flight).expect("the window holds a record"),
            timeout: Duration::from_secs(5),
        };
        NatsStreamEmitter {
            context: Some(NatsStreamEmitter::context(&client, confirmation)),
            client: Some(client),
            prepared: None,
            stream: NatsStreamName::try_from("orders").expect("valid stream"),
            subject: Subject::from("orders.created"),
            confirmation,
        }
    }

    fn record(index: usize, payload: &[u8], headers: &[(&str, &str)]) -> EmitterRecord {
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
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
    async fn publishes_expected_stream_and_message_id_before_accepting_pub_ack() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let publish = connection.expect_publish().await;
            connection
                .reply(&publish, r#"{"stream":"orders","seq":1}"#)
                .await;
            (publish, connection)
        });
        let mut emitter = emitter(&addr, 1).await;
        let mut record = EmitterRecord::new(
            EmitterRecordId::new(0),
            None,
            b"event".to_vec(),
            vec![("trace".to_owned(), "a".to_owned())],
            Timestamp::from_unix_nanos(0),
        );
        record.envelope.message_id = Some("record-42".to_owned());
        let outcome = emitter.publish(vec![record]).await.into_parts();
        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        assert!(outcome.rejected.is_empty());
        assert!(outcome.infrastructure_error.is_none());
        let (publish, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(publish.subject, "orders.created");
        let message =
            String::from_utf8(publish.message).expect("NATS headers and payload are text");
        assert!(
            message.contains("Nats-Expected-Stream: orders"),
            "{message}"
        );
        assert!(message.contains("Nats-Msg-Id: record-42"), "{message}");
        assert!(message.contains("trace: a"), "{message}");
        assert!(message.ends_with("\r\n\r\nevent"), "{message}");
    }

    #[nervix_primitives::test]
    async fn settles_confirmed_records_and_rejects_an_oversized_payload() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(128).await;
            let plain = connection.expect_publish().await;
            let tagged = connection.expect_publish().await;
            connection.reply(&plain, CONFIRMED).await;
            connection.reply(&tagged, CONFIRMED).await;
            connection
        });
        let mut emitter = emitter(&addr, 8).await;
        let outcome = emitter
            .publish(vec![
                record(0, b"plain", &[]),
                record(1, &[b'x'; 256], &[]),
                record(2, b"tagged", &[("trace", "a")]),
            ])
            .await
            .into_parts();
        assert_eq!(
            outcome.delivered,
            vec![EmitterRecordId::new(0), EmitterRecordId::new(2)]
        );
        assert_eq!(
            outcome
                .rejected
                .iter()
                .map(|member| member.id)
                .collect::<Vec<_>>(),
            vec![EmitterRecordId::new(1)]
        );
        assert!(outcome.infrastructure_error.is_none());
        let _connection = script.await.expect("the scripted server ran");
    }

    #[nervix_primitives::test]
    async fn a_failed_pub_ack_preserves_every_other_answer_in_the_window() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let mut publishes = Vec::new();
            for _ in 0..4 {
                publishes.push(connection.expect_publish().await);
            }
            for (index, reply) in [
                (0, CONFIRMED),
                (1, CONFIRMED),
                (3, CONFIRMED),
                (
                    2,
                    r#"{"error":{"code":500,"err_code":10049,"description":"stream store failed"}}"#,
                ),
            ] {
                connection.reply(&publishes[index], reply).await;
            }
            connection
        });
        let mut emitter = emitter(&addr, 8).await;
        let outcome = emitter
            .publish((0..4).map(|index| record(index, b"event", &[])).collect())
            .await
            .into_parts();
        assert_eq!(
            outcome.delivered,
            vec![
                EmitterRecordId::new(0),
                EmitterRecordId::new(1),
                EmitterRecordId::new(3)
            ]
        );
        assert!(outcome.rejected.is_empty());
        assert!(outcome.infrastructure_error.is_some());
        let _connection = script.await.expect("the scripted server ran");
    }

    #[nervix_primitives::test]
    async fn a_sequential_window_stops_on_a_failed_pub_ack() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let publish = connection.expect_publish().await;
            connection.reply(&publish, r#"{"error":{"code":500,"err_code":10049,"description":"stream store failed"}}"#).await;
            connection
        });
        let mut emitter = emitter(&addr, 1).await;
        let outcome = emitter
            .publish(vec![record(0, b"first", &[]), record(1, b"second", &[])])
            .await
            .into_parts();
        assert!(outcome.delivered.is_empty());
        assert!(outcome.rejected.is_empty());
        assert!(outcome.infrastructure_error.is_some());
        let _connection = script.await.expect("the scripted server ran");
    }

    #[test]
    fn a_stream_without_a_responder_is_a_definitive_record_rejection() {
        let error = JetStreamPublishError::new(PublishErrorKind::StreamNotFound);
        assert!(NatsStreamEmitter::is_record_rejection(&error));
        let Ok(BrokerVerdict::Rejected { message }) = NatsStreamEmitter::broker_answer(Err(error))
        else {
            panic!("the named stream did not capture this subject");
        };
        assert!(message.contains("no stream found for given subject"));
    }

    #[test]
    fn an_expected_stream_mismatch_is_a_definitive_record_rejection() {
        let cause: NatsJetStreamError = serde_json::from_str(
            r#"{"code":400,"err_code":10060,"description":"expected stream does not match"}"#,
        )
        .expect("the server's typed mismatch error");
        let error = JetStreamPublishError::with_source(PublishErrorKind::Other, cause);
        assert!(NatsStreamEmitter::is_record_rejection(&error));
        let other = JetStreamPublishError::new(PublishErrorKind::TimedOut);
        assert!(!NatsStreamEmitter::is_record_rejection(&other));
    }
}
