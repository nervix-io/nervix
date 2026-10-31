//! NATS ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The core NATS connection and queue subscription each ingestor instance reads
//!   through, its TLS options, the end of that subscription when the connection is lost, and
//!   message headers as ingest headers.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   the boundary's channels and `async-nats`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or Plexus.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use async_nats::{Client as NatsClient, Event as NatsEvent, Message as NatsMessage, Subscriber};
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use nervix_connector::{
    BrokerIngestorConnector, IngestMessageHeaders, IngestMetadataRow, IngestorBatch,
    IngestorBatchRequest, IngestorConnector, IngestorError, IngestorMessage,
    IngestorReadCancellation, IngestorResult, IngestorResume, ServiceUrl, client_config_value,
    client_tls_paths,
};
use nervix_models::{ClientConfigEntry, NatsQueueGroupName, NatsSubjectName};
use nervix_primitives::sync::mpsc::{self, error::TrySendError};
use thiserror::Error;

const NATS: &str = "nats";

/// Why a NATS ingestor could not connect or subscribe.
#[derive(Debug, Error)]
pub enum NatsSubjectIngestorError {
    #[error("invalid NATS client configuration")]
    ClientConfig,
    #[error("NATS TLS client authentication requires both 'tls_cert_file' and 'tls_key_file'")]
    IncompleteTlsIdentity,
    #[error("failed to connect NATS client")]
    Connect,
    #[error("failed to subscribe to NATS subject '{subject}'")]
    Subscribe { subject: String },
    #[error("failed to flush the NATS subscription")]
    Flush,
    /// The client lost its connection to the server, which ends the subscription that connection
    /// carried.
    #[error("NATS connection lost")]
    ConnectionLost,
}

type NatsSubjectIngestorResult<T> = Result<T, Report<NatsSubjectIngestorError>>;

/// What one NATS ingestor subscribes to: its resolved client entries, the subject, and the queue
/// group every instance joins.
#[derive(Clone)]
pub struct NatsSubjectIngestorPlan {
    config: Vec<ClientConfigEntry>,
    subject: NatsSubjectName,
    queue_group: NatsQueueGroupName,
}

/// One instance's connection and queue subscription, kept together because the subscription
/// lives only as long as its client, with the signal the client raises when it loses the
/// connection.
struct NatsSubscription {
    _client: NatsClient,
    subscriber: Subscriber,
    /// Receives a signal when the client loses its connection to the server, and ends once the
    /// client's connection task has ended.
    connection_lost: mpsc::Receiver<()>,
}

/// What a subscription's wait for its next message ended with.
enum NatsReceipt {
    Message(NatsMessage),
    /// The subscription ended: the client lost its connection, or the subscription's stream ended
    /// with the client's connection task.
    Ended,
}

impl NatsSubscription {
    /// Waits for the next message the subscription delivers, or for the subscription to end.
    ///
    /// A message the client already received is taken before a lost connection ends the
    /// subscription, so a loss never discards what reached the client before it.
    async fn next_message(&mut self) -> NatsReceipt {
        nervix_primitives::select! {
            biased;
            message = self.subscriber.next() => match message {
                Some(message) => NatsReceipt::Message(message),
                // The client ends a subscription's stream only when its connection task ended,
                // which a lost connection the client did not restore does.
                None => NatsReceipt::Ended,
            },
            _ = self.connection_lost.recv() => NatsReceipt::Ended,
        }
    }
}

impl NatsSubjectIngestorPlan {
    pub fn new(
        config: Vec<ClientConfigEntry>,
        subject: NatsSubjectName,
        queue_group: NatsQueueGroupName,
    ) -> Self {
        Self {
            config,
            subject,
            queue_group,
        }
    }

    async fn subscribe(&self) -> NatsSubjectIngestorResult<NatsSubscription> {
        // One pending signal is enough: the instance ends its subscription on the first.
        let (lost, connection_lost) = mpsc::channel(1);
        let client = Self::client_from_config(&self.config, lost).await?;
        let subscriber = client
            .queue_subscribe(
                self.subject.as_str().to_string(),
                self.queue_group.as_str().to_string(),
            )
            .await
            .map_err(|source| {
                Report::new(source).change_context(NatsSubjectIngestorError::Subscribe {
                    subject: self.subject.as_str().to_string(),
                })
            })?;
        client.flush().await.map_err(|source| {
            Report::new(source).change_context(NatsSubjectIngestorError::Flush)
        })?;
        Ok(NatsSubscription {
            _client: client,
            subscriber,
            connection_lost,
        })
    }

    /// Connects a client that signals `lost` when it loses its connection to the server.
    ///
    /// The host reopens a lost connection on its own reconnect cadence, so the client reports the
    /// loss instead of reconnecting in the background. The client cannot be told to make no
    /// attempt of its own, so it makes one before its connection task ends; the instance has
    /// ended its subscription by then, and that attempt only lets the task end.
    async fn client_from_config(
        config: &[ClientConfigEntry],
        lost: mpsc::Sender<()>,
    ) -> NatsSubjectIngestorResult<NatsClient> {
        let addr = client_config_value(config, "addr", "NATS")
            .change_context(NatsSubjectIngestorError::ClientConfig)?;
        let mut options = async_nats::ConnectOptions::new()
            .max_reconnects(1)
            .event_callback(move |event| {
                let lost = lost.clone();
                async move {
                    if let NatsEvent::Disconnected = event {
                        Self::signal_connection_lost(&lost);
                    }
                }
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
                return Err(Report::new(NatsSubjectIngestorError::IncompleteTlsIdentity));
            }
        }
        if ServiceUrl::new(&addr, "NATS addr")
            .has_scheme("tls")
            .change_context(NatsSubjectIngestorError::ClientConfig)?
        {
            options = options.require_tls(true);
        }
        options
            .connect(addr)
            .await
            .map_err(|source| Report::new(source).change_context(NatsSubjectIngestorError::Connect))
    }

    /// Tells the instance whose client lost its connection that its subscription ended.
    fn signal_connection_lost(lost: &mpsc::Sender<()>) {
        match lost.try_send(()) {
            Ok(()) => {}
            // A loss is already pending, and the instance ends its subscription on the first one.
            Err(TrySendError::Full(())) => {}
            // The instance already ended this subscription and no longer listens for its end.
            Err(TrySendError::Closed(())) => {}
        }
    }
}

/// One received NATS message, whose headers are its ingest headers, each value visited under its
/// own name.
pub struct NatsMessageHeaders {
    message: NatsMessage,
}

impl IngestMessageHeaders for NatsMessageHeaders {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        let Some(headers) = self.message.headers.as_ref() else {
            return;
        };
        for (name, values) in headers.iter() {
            for value in values {
                visit(name.as_ref(), value.as_str());
            }
        }
    }
}

pub struct NatsSubjectIngestorMessage {
    headers: NatsMessageHeaders,
    position: (),
}

impl IngestorMessage for NatsSubjectIngestorMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        &self.headers.message.payload
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::headers(&self.headers)
    }
}

/// One NATS ingestor instance: its queue subscription, absent while suspended or reconnecting.
pub struct NatsSubjectIngestor {
    plan: NatsSubjectIngestorPlan,
    subscription: Option<NatsSubscription>,
}

#[async_trait]
impl IngestorConnector for NatsSubjectIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Nats(nervix_models::NatsIngestorEntity::Subject);

    type Plan = NatsSubjectIngestorPlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            subscription: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.subscription.is_none()
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        self.subscription = None;
        Ok(())
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if self.subscription.is_none() {
            let subscription = self
                .plan
                .subscribe()
                .await
                .change_context(IngestorError::Resume { connector: NATS })?;
            self.subscription = Some(subscription);
        }
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.subscription = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for NatsSubjectIngestor {
    type Message = NatsSubjectIngestorMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let Some(subscription) = self.subscription.as_mut() else {
            return Ok(IngestorBatch::ResumeRequired);
        };
        let Some(receipt) = cancellation
            .unless_cancelled(subscription.next_message())
            .await
        else {
            return Ok(IngestorBatch::Interrupted {
                gathered: Vec::new(),
            });
        };
        let NatsReceipt::Message(message) = receipt else {
            // The subscription ended with its connection. The host shows the loss as a transient
            // status and resumes the instance on its reconnect cadence, which connects again.
            self.subscription = None;
            return Err(Report::new(NatsSubjectIngestorError::ConnectionLost)
                .change_context(IngestorError::Read { connector: NATS }));
        };
        Ok(IngestorBatch::Messages(vec![NatsSubjectIngestorMessage {
            headers: NatsMessageHeaders { message },
            position: (),
        }]))
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> IngestorResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> IngestorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use nervix_primitives::{sync::oneshot, time::timeout};

    use super::*;
    use crate::subject::scripted_server::ScriptedServer;

    /// How long a check waits for the ingestor to observe what the stand-in server did. It bounds a
    /// wait for an event that follows at once on a loopback connection, so only an ingestor that
    /// never observes it reaches the bound.
    const OBSERVATION_BOUND: Duration = Duration::from_secs(30);

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[nervix_primitives::test]
    async fn a_cancelled_read_takes_nothing_and_leaves_the_next_message_to_the_next_read() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let (allow_delivery, delivery_allowed) = oneshot::channel::<()>();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let subscription = connection.expect_subscription().await;
            connection.answer_ping().await;
            delivery_allowed
                .await
                .expect("the test allows the delivery once its read was cancelled");
            connection
                .deliver(&subscription, b"delivered after the cancelled read")
                .await;
            // The connection stays open until the test has read the delivery.
            connection
        });
        let plan = NatsSubjectIngestorPlan::new(
            vec![entry("addr", &addr)],
            NatsSubjectName::try_from("events".to_string()).expect("a valid subject"),
            NatsQueueGroupName::try_from("members".to_string()).expect("a valid queue group"),
        );
        let request = IngestorBatchRequest {
            max_messages: NonZeroUsize::MIN,
            batch_timeout: None,
        };
        let mut source = NatsSubjectIngestor::open(&plan, 0)
            .await
            .expect("opening a NATS ingestor does not connect");
        source
            .resume()
            .await
            .expect("the ingestor subscribes through the stand-in server");

        let cancellation = IngestorReadCancellation::new();
        cancellation.cancel();
        let interrupted = timeout(OBSERVATION_BOUND, source.next_batch(request, &cancellation))
            .await
            .expect("a cancelled read ends at once")
            .expect("a cancelled read is not a failure");
        let IngestorBatch::Interrupted { gathered } = interrupted else {
            panic!("a read the host cancelled ends as interrupted");
        };
        assert!(gathered.is_empty(), "nothing was delivered to take");

        allow_delivery
            .send(())
            .expect("the scripted server waits for the delivery to be allowed");
        let batch = timeout(
            OBSERVATION_BOUND,
            source.next_batch(request, &IngestorReadCancellation::new()),
        )
        .await
        .expect("the delivered message reaches the ingestor")
        .expect("the delivered message is read");
        let IngestorBatch::Messages(messages) = batch else {
            panic!("the ingestor reads the delivered message as a batch");
        };
        let payloads = messages
            .iter()
            .map(|message| message.payload().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            payloads,
            vec![b"delivered after the cancelled read".to_vec()]
        );
        let _connection = script.await.expect("the scripted server ran its script");
    }

    #[nervix_primitives::test]
    async fn a_lost_connection_ends_the_subscription_after_what_it_delivered() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let subscription = connection.expect_subscription().await;
            // The ingestor flushes once it subscribed, so the subscription exists once that ping is
            // answered.
            connection.answer_ping().await;
            connection
                .deliver(&subscription, b"delivered before the loss")
                .await;
            // The server goes down: it ends the connection and refuses the client's own attempt
            // to reconnect.
            connection.end().await;
        });
        let plan = NatsSubjectIngestorPlan::new(
            vec![entry("addr", &addr)],
            NatsSubjectName::try_from("events".to_string()).expect("a valid subject"),
            NatsQueueGroupName::try_from("members".to_string()).expect("a valid queue group"),
        );
        let request = IngestorBatchRequest {
            max_messages: NonZeroUsize::MIN,
            batch_timeout: None,
        };
        let mut source = NatsSubjectIngestor::open(&plan, 0)
            .await
            .expect("opening a NATS ingestor does not connect");
        assert!(
            source.needs_resume(),
            "an opened ingestor has not subscribed yet"
        );
        let resumed = source
            .resume()
            .await
            .expect("the ingestor subscribes through the stand-in server");
        assert_eq!(resumed, IngestorResume::Ready);

        let batch = timeout(
            OBSERVATION_BOUND,
            source.next_batch(request, &IngestorReadCancellation::new()),
        )
        .await
        .expect("the delivered message reaches the ingestor")
        .expect("the delivered message is read");
        let IngestorBatch::Messages(messages) = batch else {
            panic!("the ingestor reads the delivered message as a batch");
        };
        let payloads = messages
            .iter()
            .map(|message| message.payload().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(payloads, vec![b"delivered before the loss".to_vec()]);
        script.await.expect("the scripted server ran its script");

        let ended = timeout(
            OBSERVATION_BOUND,
            source.next_batch(request, &IngestorReadCancellation::new()),
        )
        .await
        .expect("the lost connection ends the wait for the next message");
        let Err(error) = ended else {
            panic!("a lost connection ends the subscription with an error");
        };
        assert!(matches!(
            error.current_context(),
            IngestorError::Read { connector: NATS }
        ));
        assert!(matches!(
            error.downcast_ref::<NatsSubjectIngestorError>(),
            Some(NatsSubjectIngestorError::ConnectionLost)
        ));
        assert!(
            source.needs_resume(),
            "the host reconnects an ingestor whose subscription ended through its resume"
        );

        let error = source
            .resume()
            .await
            .expect_err("the server is down, so the next resume cannot connect");
        assert!(matches!(
            error.downcast_ref::<NatsSubjectIngestorError>(),
            Some(NatsSubjectIngestorError::Connect)
        ));
        assert!(error.downcast_ref::<async_nats::ConnectError>().is_some());
    }

    /// The instance ends its subscription on the first loss, so a loss signalled while another is
    /// pending, or after the instance stopped listening, is dropped.
    #[test]
    fn a_lost_connection_is_signalled_once_and_not_after_the_instance_stopped_listening() {
        let (lost, mut connection_lost) = mpsc::channel(1);
        NatsSubjectIngestorPlan::signal_connection_lost(&lost);
        NatsSubjectIngestorPlan::signal_connection_lost(&lost);
        assert!(
            connection_lost.try_recv().is_ok(),
            "the first loss is signalled"
        );
        assert!(
            connection_lost.try_recv().is_err(),
            "a loss signalled while one is pending is not signalled again"
        );
        drop(connection_lost);
        NatsSubjectIngestorPlan::signal_connection_lost(&lost);
        assert!(
            lost.is_closed(),
            "the instance no longer listens for its end"
        );
    }

    #[nervix_primitives::test]
    async fn client_configuration_failures_keep_typed_context() {
        let (lost, _connection_lost) = mpsc::channel(1);
        let error = NatsSubjectIngestorPlan::client_from_config(&[], lost.clone())
            .await
            .expect_err("a missing NATS address must fail");
        assert!(matches!(
            error.current_context(),
            NatsSubjectIngestorError::ClientConfig
        ));

        let error = NatsSubjectIngestorPlan::client_from_config(
            &[
                entry("addr", "tls://127.0.0.1:1"),
                entry("tls_cert_file", "client.pem"),
            ],
            lost,
        )
        .await
        .expect_err("a client certificate without a key must fail");
        assert!(matches!(
            error.current_context(),
            NatsSubjectIngestorError::IncompleteTlsIdentity
        ));
    }

    #[test]
    fn every_header_value_is_visited_under_its_name() {
        let mut header_map = async_nats::HeaderMap::new();
        header_map.append("trace", "a");
        header_map.append("trace", "b");
        let headers = NatsMessageHeaders {
            message: NatsMessage {
                subject: "events".into(),
                reply: None,
                payload: bytes::Bytes::new(),
                headers: Some(header_map),
                status: None,
                description: None,
                length: 0,
            },
        };
        let mut visited = Vec::new();
        headers.visit(&mut |name, value| visited.push(format!("{name}={value}")));
        assert_eq!(visited, vec!["trace=a", "trace=b"]);
    }

    /// Both NATS entities declare repeats by name with exact names: the header map an emitter writes
    /// keeps every value of a name together under it in write order, so a reader of the published
    /// message finds each name's values together and in that order, while names that differ in
    /// letter case stay two names.
    #[test]
    fn written_headers_read_back_with_every_value_of_a_name_together_in_write_order() {
        let mut written = Vec::new();
        for (name, value) in [
            ("trace", "a"),
            ("Tenant", "x"),
            ("trace", "b"),
            ("tenant", "y"),
            ("trace", "c"),
        ] {
            written.push((name.to_string(), value.to_string()));
        }
        let headers = NatsMessageHeaders {
            message: NatsMessage {
                subject: "events".into(),
                reply: None,
                payload: bytes::Bytes::new(),
                headers: Some(crate::subject::NatsSubjectEmitter::header_map(&written)),
                status: None,
                description: None,
                length: 0,
            },
        };
        let mut visited: Vec<(String, String)> = Vec::new();
        headers.visit(&mut |name, value| visited.push((name.to_string(), value.to_string())));

        let mut finished = std::collections::BTreeSet::new();
        let mut current: Option<&str> = None;
        let mut values: std::collections::BTreeMap<&str, Vec<&str>> =
            std::collections::BTreeMap::new();
        for (name, value) in &visited {
            if current != Some(name.as_str()) {
                if let Some(previous) = current {
                    finished.insert(previous);
                }
                assert!(
                    !finished.contains(name.as_str()),
                    "every value of {name} is visited together: {visited:?}"
                );
                current = Some(name);
            }
            values
                .entry(name.as_str())
                .or_default()
                .push(value.as_str());
        }
        let expected = std::collections::BTreeMap::from([
            ("Tenant", vec!["x"]),
            ("tenant", vec!["y"]),
            ("trace", vec!["a", "b", "c"]),
        ]);
        assert_eq!(values, expected);
    }
}
