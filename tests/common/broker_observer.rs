//! Messages and bounded reads a scenario observes at a broker destination.
//!
//! Layer: test harness.
//! - **Owns.** Broker messages, one destination's ordered delivery stream, bounded sequence and
//!   header assertions, cancellation of a forwarding task, and client receipt diagnostics.
//! - **Depends on.** The NATS observer, channels, runtime tasks and monotonic timers.
//! - **Must not know.** Cluster lifecycle, connector execution or how a scenario produces output.

use std::{future::Future, io, time::Duration};

use arch_into::ArchInto as _;
use async_nats::Message as NatsMessage;
use nervix_primitives::{
    sync::mpsc,
    task::{AbortOnDropHandle, JoinHandle},
    time::{Instant, timeout},
};

use super::nats_observer::NatsObserver;

pub(crate) const BROKER_TIMEOUT: Duration = Duration::from_secs(10);

/// When a broker says a record it delivered was made, for a broker whose records carry a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BrokerRecordTime {
    /// The time the record's producer wrote on it, in Unix milliseconds: a Kafka `CreateTime`.
    Created { unix_millis: i64 },
    /// The time the broker appended the record to its log, in Unix milliseconds: a Kafka
    /// `LogAppendTime`.
    Appended { unix_millis: i64 },
}

#[derive(Debug)]
pub(crate) struct BrokerMessage {
    /// The payload as text, with any byte that is not valid UTF-8 replaced.
    pub(crate) payload: String,
    /// The payload exactly as the broker delivered it.
    pub(crate) bytes: Vec<u8>,
    /// The headers the broker delivered with the payload: Kafka and NATS headers in their written
    /// order, and Pulsar properties, RabbitMQ headers and SQS message attributes sorted by name,
    /// because those brokers keep no order of their own.
    pub(crate) headers: Vec<(String, String)>,
    /// The key the broker delivered with the payload: a Kafka record key or a Pulsar partition key.
    pub(crate) key: Option<String>,
    /// The ordering group the broker delivered the payload in: an SQS FIFO message group.
    pub(crate) group: Option<String>,
    /// When the broker says the record was made: a Kafka record's create or log-append time.
    pub(crate) time: Option<BrokerRecordTime>,
    /// The headers a broker delivered with a type of their own, sorted by name: SQS message
    /// attributes with their data types. Empty for a broker whose headers carry no type.
    pub(crate) typed_headers: Vec<TypedBrokerHeader>,
}

/// One header a broker delivered with a type of its own, such as an SQS message attribute.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TypedBrokerHeader {
    pub(crate) name: String,
    /// The broker's name for the value's type, such as SQS's `Number`.
    pub(crate) data_type: String,
    /// The value as text, and a binary value as its bytes in lowercase hex.
    pub(crate) value: String,
}

impl BrokerMessage {
    pub(crate) fn from_bytes(bytes: &[u8], headers: Vec<(String, String)>) -> Self {
        Self {
            payload: String::from_utf8_lossy(bytes).to_string(),
            bytes: bytes.to_vec(),
            headers,
            key: None,
            group: None,
            time: None,
            typed_headers: Vec::new(),
        }
    }

    pub(crate) fn with_typed_headers(mut self, typed_headers: Vec<TypedBrokerHeader>) -> Self {
        self.typed_headers = typed_headers;
        self
    }

    pub(crate) fn with_key(mut self, key: Option<String>) -> Self {
        self.key = key;
        self
    }

    pub(crate) fn with_time(mut self, time: Option<BrokerRecordTime>) -> Self {
        self.time = time;
        self
    }

    pub(crate) fn with_group(mut self, group: Option<String>) -> Self {
        self.group = group;
        self
    }

    /// A message a core NATS subscription delivered, with every header value under its name in the
    /// order the message carries them.
    fn from_nats(message: &NatsMessage) -> Self {
        let mut headers = Vec::new();
        if let Some(delivered) = message.headers.as_ref() {
            for (name, values) in delivered.iter() {
                for value in values {
                    headers.push((name.to_string(), value.as_str().to_string()));
                }
            }
        }
        Self::from_bytes(message.payload.as_ref(), headers)
    }
}

/// What a scenario observes on one broker destination, in the order the broker delivered it.
#[derive(Debug)]
pub(crate) struct BrokerObserver {
    deliveries: ObservedDeliveries,
}

/// Where an observer takes the messages its broker delivered from.
#[derive(Debug)]
enum ObservedDeliveries {
    /// A task that reads the broker's client hands each message over this channel, and is aborted
    /// with the observer.
    Forwarded {
        messages: mpsc::Receiver<BrokerMessage>,
        _reader: AbortOnDropHandle<()>,
    },
    /// A core NATS subscription. Its client buffers what the server delivers on a task of its own,
    /// so a step takes each message straight from that buffer.
    Nats(NatsObserver),
}

/// The observer and final message retained after a complete sequence assertion.
pub(crate) struct SequenceObservation {
    pub(crate) observer: BrokerObserver,
    pub(crate) last_message: Option<BrokerMessage>,
}

impl BrokerObserver {
    pub(crate) fn forwarded(
        messages: mpsc::Receiver<BrokerMessage>,
        reader: JoinHandle<()>,
    ) -> Self {
        Self {
            deliveries: ObservedDeliveries::Forwarded {
                messages,
                _reader: AbortOnDropHandle::new(reader),
            },
        }
    }

    pub(crate) fn nats(observer: NatsObserver) -> Self {
        Self {
            deliveries: ObservedDeliveries::Nats(observer),
        }
    }

    /// Starts checking every sequence number and header on a runtime task, with the deadline
    /// captured by the step before dispatch. A delayed caller learns the result later without
    /// stopping the drain. Dropping the returned future aborts its observation.
    pub(crate) fn observe_sequence(
        mut self,
        deadline: Instant,
        count: usize,
        field: String,
        expected_headers: Vec<(String, String)>,
    ) -> impl Future<Output = SequenceObservation> + Send {
        super::runner_observation::observe_off_the_runner(async move {
            let mut last_message = None;
            for expected_sequence in 1..=count {
                nervix_primitives::task::consume_budget().await;
                let now = Instant::now();
                assert!(
                    now < deadline,
                    "timed out after receiving {} of {count} sequential broker messages; {}",
                    expected_sequence - 1,
                    self.describe_client_receipts()
                );
                let next = self
                    .try_next_message(deadline.saturating_duration_since(now))
                    .await
                    .unwrap_or_else(|error| {
                        panic!("failed while waiting for sequential broker message: {error}")
                    });
                let Some(message) = next else {
                    panic!(
                        "timed out waiting for broker message {expected_sequence} of {count}; {}",
                        self.describe_client_receipts()
                    );
                };
                let payload = serde_json::from_str::<serde_json::Value>(&message.payload)
                    .unwrap_or_else(|error| panic!("broker payload is not valid JSON: {error}"));
                let Some(actual_sequence) = payload.get(&field).and_then(serde_json::Value::as_u64)
                else {
                    panic!(
                        "broker payload field '{field}' is not an unsigned integer: {}",
                        message.payload
                    );
                };
                let expected_u64 = expected_sequence.arch_into();
                assert_eq!(
                    actual_sequence,
                    expected_u64,
                    "broker messages were duplicated, lost, or reordered: expected sequence \
                     {expected_u64} but observed {actual_sequence} ({} messages behind, {} of {} \
                     consumed so far); observed headers: {:?}; {}",
                    actual_sequence.abs_diff(expected_u64),
                    expected_sequence,
                    count,
                    message.headers,
                    self.describe_client_receipts()
                );
                assert_eq!(
                    message.headers, expected_headers,
                    "broker message {expected_sequence} headers changed"
                );
                last_message = Some(message);
            }

            let duplicate = self
                .try_next_message(Duration::from_millis(500))
                .await
                .unwrap_or_else(|error| {
                    panic!("failed while checking for a duplicate broker message: {error}")
                });
            assert!(
                duplicate.is_none(),
                "observed a duplicate broker message after the expected sequence: {duplicate:?}"
            );
            SequenceObservation {
                observer: self,
                last_message,
            }
        })
    }

    pub(crate) async fn next_message(&mut self) -> io::Result<BrokerMessage> {
        timeout(BROKER_TIMEOUT, self.deliveries.next())
            .await
            .map_err(|_| io::Error::other("timed out waiting for broker payload"))?
            .ok_or_else(|| io::Error::other("broker observer closed before delivering payload"))
    }

    pub(crate) async fn try_next_payload(
        &mut self,
        duration: Duration,
    ) -> io::Result<Option<String>> {
        self.try_next_message(duration)
            .await
            .map(|message| message.map(|message| message.payload))
    }

    pub(crate) async fn try_next_message(
        &mut self,
        duration: Duration,
    ) -> io::Result<Option<BrokerMessage>> {
        match timeout(duration, self.deliveries.next()).await {
            Ok(Some(message)) => Ok(Some(message)),
            Ok(None) => Err(io::Error::other(
                "broker observer closed before delivering payload",
            )),
            Err(_) => Ok(None),
        }
    }

    /// What the observing client had received from its broker, for a failure message: the count of
    /// a client that keeps one, so a message that never reached the observer reads differently from
    /// one a step had not yet taken.
    pub(crate) fn describe_client_receipts(&self) -> String {
        match &self.deliveries {
            ObservedDeliveries::Forwarded { .. } => {
                "the observing client keeps no count of what its broker delivered".to_string()
            }
            ObservedDeliveries::Nats(observer) => observer.describe_receipts(),
        }
    }
}

impl ObservedDeliveries {
    /// The next message the broker delivered, or `None` once the observer can deliver no more.
    async fn next(&mut self) -> Option<BrokerMessage> {
        match self {
            Self::Forwarded { messages, .. } => messages.recv().await,
            Self::Nats(observer) => {
                let message = observer.next().await?;
                Some(BrokerMessage::from_nats(&message))
            }
        }
    }
}
