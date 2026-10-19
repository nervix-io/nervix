//! SQS source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The SQS client a source configuration declares, the queue-URL lookup each instance
//!   makes when it resumes, long polling and how a receive in flight completes when the host
//!   cancels a read, message attributes as ingest headers, and deleting a message once it is
//!   acknowledged.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio, the node resolver, and the AWS SQS SDK.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

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

use std::{borrow::Cow, fmt::Debug, future::Future, pin::pin, time::Duration};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::{
    Client as SqsClient,
    error::SdkError,
    operation::get_queue_url::GetQueueUrlError,
    types::{Message as SqsMessage, MessageAttributeValue},
};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceReadCancellation,
    SourceResult, SourceResume, client_config_value, client_tls_paths,
    optional_client_config_value, read_tls_file,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, SqsQueueName};
use nervix_primitives::time::timeout;
use thiserror::Error;

use crate::connection::{FailedRequest, SqsTrust};

const SQS: &str = "sqs";
/// How long one receive request waits for a message before the service answers empty, in the whole
/// seconds the request carries.
const LONG_POLL_SECONDS: i32 = 1;
/// The long poll as a duration.
const LONG_POLL: Duration =
    match Duration::from_secs(1).checked_mul(LONG_POLL_SECONDS.unsigned_abs()) {
        Some(long_poll) => long_poll,
        None => panic!("a long poll of a few seconds fits a duration"),
    };
const _: () = assert!(LONG_POLL_SECONDS > 0, "every receive long-polls");
/// How long the service may take beyond the long poll to answer a receive: the request's and the
/// answer's round trip on a loaded service.
const RECEIVE_ANSWER_ALLOWANCE: Duration = Duration::from_secs(9);
/// How long a read the host cancelled still waits for its receive in flight: the rest of the
/// receive's long poll and its answer. A service that has not answered by then is not answering.
const CANCELLED_RECEIVE_BUDGET: Duration = match LONG_POLL.checked_add(RECEIVE_ANSWER_ALLOWANCE) {
    Some(budget) => budget,
    None => panic!("a long poll and its answer allowance of a few seconds fit a duration"),
};
/// The JSON protocol's error type for a queue the service does not have.
const QUEUE_DOES_NOT_EXIST: &str = "QueueDoesNotExist";

/// Why an SQS source could not build its client, look its queue up, receive from it, or delete a
/// message.
#[derive(Debug, Error)]
pub enum SqsSourceError {
    #[error("invalid SQS client configuration")]
    ClientConfig,
    #[error("failed to build SQS TLS context")]
    BuildTlsContext,
    #[error("failed to resolve SQS queue '{queue}'")]
    ResolveQueue { queue: String },
    #[error("SQS queue '{queue}' does not exist")]
    MissingQueue { queue: String },
    #[error("SQS queue '{queue}' has no URL")]
    MissingQueueUrl { queue: String },
    #[error("failed to receive SQS messages")]
    Receive,
    #[error(
        "the SQS receive in flight did not answer within {budget:?} of its read's cancellation"
    )]
    CancelledReceiveUnanswered { budget: Duration },
    #[error("failed to delete an acknowledged SQS message")]
    Delete,
}

type SqsSourceResult<T> = Result<T, Report<SqsSourceError>>;

/// One SQS source's client and the queue every instance polls.
///
/// The client is built once, from configuration and TLS material alone, and each instance polls
/// through a clone. Building the plan sends no request: each instance looks the queue up when it
/// resumes.
#[derive(Clone)]
pub struct SqsSourcePlan {
    client: SqsClient,
    queue: SqsQueueName,
}

impl SqsSourcePlan {
    /// The client `config` declares, whose every connection resolves the endpoint host through
    /// `dns`, polling `queue`.
    pub async fn new(
        config: &[ClientConfigEntry],
        queue: SqsQueueName,
        dns: DnsResolver,
    ) -> SqsSourceResult<Self> {
        let client = Self::client_from_config(config, dns).await?;
        Ok(Self { client, queue })
    }

    async fn client_from_config(
        config: &[ClientConfigEntry],
        dns: DnsResolver,
    ) -> SqsSourceResult<SqsClient> {
        let endpoint = client_config_value(config, "endpoint", "SQS")
            .change_context(SqsSourceError::ClientConfig)?;
        let region = optional_client_config_value(config, "region")
            .unwrap_or("us-east-1")
            .to_string();
        let access_key_id = optional_client_config_value(config, "access_key_id")
            .unwrap_or("x")
            .to_string();
        let secret_access_key = optional_client_config_value(config, "secret_access_key")
            .unwrap_or("x")
            .to_string();

        let trust = match client_tls_paths(config).ca_file.as_ref() {
            Some(ca_file) => {
                let ca_pem = read_tls_file(ca_file, "TLS CA certificate")
                    .change_context(SqsSourceError::ClientConfig)?;
                SqsTrust::ca(ca_pem).change_context(SqsSourceError::BuildTlsContext)?
            }
            None => SqsTrust::Platform,
        };
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_sqs::config::Region::new(region))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "nervix-sqs",
            ))
            .http_client(trust.http_client(dns))
            .load()
            .await;
        Ok(SqsClient::new(&sdk_config))
    }
}

/// The receipt handle that deletes one received message once it is acknowledged.
///
/// A message the service returned without one cannot be deleted and reappears after its
/// visibility timeout.
#[derive(Debug, Clone)]
pub struct SqsSourcePosition {
    receipt_handle: Option<String>,
}

/// One received SQS message, whose message attributes are its ingest headers.
///
/// Reading them visits the attributes in place, so a message without attributes costs nothing and
/// a value only allocates when its type is not already a string.
pub struct SqsMessageAttributes {
    message: SqsMessage,
}

impl SqsMessageAttributes {
    fn attribute_value(value: &MessageAttributeValue) -> Cow<'_, str> {
        if let Some(value) = value.string_value() {
            return Cow::Borrowed(value);
        }
        if let Some(value) = value.binary_value() {
            return String::from_utf8_lossy(value.as_ref());
        }
        if !value.string_list_values().is_empty() {
            return Cow::Owned(value.string_list_values().join(","));
        }
        if !value.binary_list_values().is_empty() {
            return Cow::Owned(
                value
                    .binary_list_values()
                    .iter()
                    .map(|value| String::from_utf8_lossy(value.as_ref()).to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        Cow::Borrowed("")
    }
}

impl IngestMessageHeaders for SqsMessageAttributes {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        let Some(attributes) = self.message.message_attributes() else {
            return;
        };
        for (name, value) in attributes {
            visit(name, Self::attribute_value(value).as_ref());
        }
    }
}

pub struct SqsSourceMessage {
    attributes: SqsMessageAttributes,
    position: SqsSourcePosition,
}

impl SqsSourceMessage {
    fn new(message: SqsMessage) -> Self {
        let position = SqsSourcePosition {
            receipt_handle: message.receipt_handle().map(ToOwned::to_owned),
        };
        Self {
            attributes: SqsMessageAttributes { message },
            position,
        }
    }
}

impl SourceMessage for SqsSourceMessage {
    type Position = SqsSourcePosition;

    fn payload(&self) -> &[u8] {
        self.attributes
            .message
            .body()
            .unwrap_or_default()
            .as_bytes()
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.attributes
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Headers {
            headers: &self.attributes,
        }
    }
}

/// One SQS source instance polling the shared client's queue.
pub struct SqsSource {
    client: SqsClient,
    queue: SqsQueueName,
    /// The URL every receive and deletion addresses: absent until `resume` looks the queue up, and
    /// kept from then on, because a named queue keeps its URL.
    queue_url: Option<String>,
}

impl SqsSource {
    /// The URL of `queue`, which must already exist.
    async fn queue_url(client: &SqsClient, queue: &str) -> SqsSourceResult<String> {
        let response = match client.get_queue_url().queue_name(queue).send().await {
            Ok(response) => response,
            Err(error) => return Err(Self::queue_lookup_error(queue, &error)),
        };
        Self::require_queue_url(queue, response.queue_url().map(ToOwned::to_owned))
    }

    fn queue_lookup_error<R: Debug + 'static>(
        queue: &str,
        error: &SdkError<GetQueueUrlError, R>,
    ) -> Report<SqsSourceError> {
        let failure = FailedRequest::new(error);
        let missing = error
            .as_service_error()
            .is_some_and(Self::names_a_missing_queue);
        if missing {
            return Report::new(SqsSourceError::MissingQueue {
                queue: queue.to_string(),
            })
            .attach_printable(failure.description());
        }
        failure.report(
            SqsSourceError::ResolveQueue {
                queue: queue.to_string(),
            },
            "SQS GetQueueUrl failed",
        )
    }

    /// Whether the service answered a lookup that its queue does not exist. The SDK recognises the
    /// query-compatible code AWS sends beside the answer; an SQS-compatible service that sends only
    /// the JSON protocol's error type gives the same answer.
    fn names_a_missing_queue(error: &GetQueueUrlError) -> bool {
        if error.is_queue_does_not_exist() {
            return true;
        }
        error.meta().code() == Some(QUEUE_DOES_NOT_EXIST)
    }

    fn require_queue_url(queue: &str, queue_url: Option<String>) -> SqsSourceResult<String> {
        match queue_url {
            Some(queue_url) => Ok(queue_url),
            None => Err(Report::new(SqsSourceError::MissingQueueUrl {
                queue: queue.to_string(),
            })),
        }
    }

    /// Long-polls `queue` until the service returns at least one message, or the host cancels the
    /// read. The cancellation is observed between receives, so a cancelled read sends no further
    /// receive, and a receive already in flight still completes and hands over what it brought.
    async fn read_batch<Q: QueueReceive>(
        queue: &Q,
        cancellation: &SourceReadCancellation,
    ) -> SourceResult<SourceBatch<SqsSourceMessage>> {
        loop {
            nervix_primitives::task::consume_budget().await;
            if cancellation.is_cancelled() {
                return Ok(SourceBatch::Interrupted {
                    gathered: Vec::new(),
                });
            }
            let received = Self::complete_receive(queue.receive(), cancellation)
                .await
                .change_context(SourceError::Read { connector: SQS })?;
            if received.is_empty() {
                continue;
            }
            let mut messages = Vec::with_capacity(received.len());
            for message in received {
                messages.push(SqsSourceMessage::new(message));
            }
            return Ok(SourceBatch::Messages(messages));
        }
    }

    /// Waits for `receive`, which the host's cancellation does not abandon: the service takes the
    /// messages it answers with off the queue, so an abandoned answer would leave them invisible
    /// until their visibility timeout. A receive the host cancels while it waits is awaited for at
    /// most `CANCELLED_RECEIVE_BUDGET` more, the rest of its long poll and the answer's round trip;
    /// a service that has not answered by then has stopped answering, and the receive is abandoned
    /// with a typed failure.
    async fn complete_receive<F>(
        receive: F,
        cancellation: &SourceReadCancellation,
    ) -> SqsSourceResult<Vec<SqsMessage>>
    where
        F: Future<Output = SqsSourceResult<Vec<SqsMessage>>>,
    {
        let mut receive = pin!(receive);
        if let Some(received) = cancellation.unless_cancelled(&mut receive).await {
            return received;
        }
        match timeout(CANCELLED_RECEIVE_BUDGET, receive).await {
            Ok(received) => received,
            Err(elapsed) => Err(Report::new(elapsed).change_context(
                SqsSourceError::CancelledReceiveUnanswered {
                    budget: CANCELLED_RECEIVE_BUDGET,
                },
            )),
        }
    }
}

/// One long-polling receive from a queue: the request a source instance sends through its SDK
/// client, and a fake service in the checks of how a read meets the host's cancellation.
trait QueueReceive: Sync {
    /// Asks the queue for one message with all of its attributes, waiting up to the long poll for
    /// one to arrive, and returns the messages the service answered with, possibly none.
    fn receive(&self) -> impl Future<Output = SqsSourceResult<Vec<SqsMessage>>> + Send;
}

/// The queue a source instance receives from through its SDK client.
struct SdkQueue<'a> {
    client: &'a SqsClient,
    url: &'a str,
}

impl QueueReceive for SdkQueue<'_> {
    fn receive(&self) -> impl Future<Output = SqsSourceResult<Vec<SqsMessage>>> + Send {
        let request = self
            .client
            .receive_message()
            .queue_url(self.url)
            .max_number_of_messages(1)
            .message_attribute_names("All")
            .wait_time_seconds(LONG_POLL_SECONDS)
            .send();
        async move {
            match request.await {
                Ok(response) => Ok(response.messages.unwrap_or_default()),
                Err(error) => Err(FailedRequest::new(&error)
                    .report(SqsSourceError::Receive, "SQS ReceiveMessage failed")),
            }
        }
    }
}

#[async_trait]
impl SourceConnector for SqsSource {
    type Plan = SqsSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        Ok(Self {
            client: plan.client.clone(),
            queue: plan.queue.clone(),
            queue_url: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.queue_url.is_none()
    }

    /// Looks the queue up the first time the instance resumes. A service that cannot be reached
    /// and a queue the service does not have both fail the resume, which the host retries on the
    /// source's policy; neither creates the queue.
    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.queue_url.is_some() {
            return Ok(SourceResume::Ready);
        }
        let queue_url = Self::queue_url(&self.client, self.queue.as_str())
            .await
            .change_context(SourceError::Resume { connector: SQS })?;
        self.queue_url = Some(queue_url);
        Ok(SourceResume::Ready)
    }
}

#[async_trait]
impl BrokerSourceConnector for SqsSource {
    type Message = SqsSourceMessage;
    type Position = SqsSourcePosition;

    /// Long-polls until the service returns at least one message. Every request asks for one
    /// message with all of its attributes.
    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
        cancellation: &SourceReadCancellation,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let Some(url) = self.queue_url.as_deref() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        let queue = SdkQueue {
            client: &self.client,
            url,
        };
        Self::read_batch(&queue, cancellation).await
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        for position in positions {
            nervix_primitives::task::consume_budget().await;
            let Some(receipt_handle) = position.receipt_handle.as_deref() else {
                continue;
            };
            let queue_url = self.queue_url.as_deref().assured(
                "a position comes from a receive, which addressed the URL `resume` looked up, and \
                 nothing clears it",
            );
            let deleted = self
                .client
                .delete_message()
                .queue_url(queue_url)
                .receipt_handle(receipt_handle)
                .send()
                .await;
            if let Err(error) = deleted {
                let failure = FailedRequest::new(&error)
                    .report(SqsSourceError::Delete, "SQS DeleteMessage failed");
                return Err(failure.change_context(SourceError::Acknowledge { connector: SQS }));
            }
        }
        Ok(())
    }

    /// A rejected message is left in the queue: it stays invisible until its visibility timeout
    /// and is then delivered again.
    async fn reject(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use aws_sdk_sqs::{
        error::{ConnectorError, ErrorMetadata},
        types::error::QueueDoesNotExist,
    };

    use super::*;

    #[test]
    fn queue_lookup_failures_are_distinct_and_keep_their_source_message() {
        let missing = SdkError::<GetQueueUrlError, ()>::service_error(
            GetQueueUrlError::QueueDoesNotExist(QueueDoesNotExist::builder().build()),
            (),
        );
        let missing = SqsSource::queue_lookup_error("missing-queue", &missing);
        assert!(matches!(
            missing.current_context(),
            SqsSourceError::MissingQueue { queue } if queue == "missing-queue"
        ));
        assert!(format!("{missing:?}").contains("service error"));

        let missing_by_type = SdkError::<GetQueueUrlError, ()>::service_error(
            GetQueueUrlError::generic(
                ErrorMetadata::builder()
                    .code("QueueDoesNotExist")
                    .message("The specified queue does not exist.")
                    .build(),
            ),
            (),
        );
        let missing_by_type = SqsSource::queue_lookup_error("missing-queue", &missing_by_type);
        assert!(matches!(
            missing_by_type.current_context(),
            SqsSourceError::MissingQueue { queue } if queue == "missing-queue"
        ));
        let denied = SdkError::<GetQueueUrlError, ()>::service_error(
            GetQueueUrlError::generic(ErrorMetadata::builder().code("AccessDenied").build()),
            (),
        );
        let denied = SqsSource::queue_lookup_error("orders", &denied);
        assert!(matches!(
            denied.current_context(),
            SqsSourceError::ResolveQueue { queue } if queue == "orders"
        ));

        let refused = SdkError::<GetQueueUrlError, ()>::dispatch_failure(ConnectorError::io(
            "connection refused".into(),
        ));
        let connection = SqsSource::queue_lookup_error("orders", &refused);
        assert!(matches!(
            connection.current_context(),
            SqsSourceError::ResolveQueue { queue } if queue == "orders"
        ));
        assert!(format!("{connection:?}").contains("connection refused"));

        let lookup = nervix_dns::DnsLookupError::new(
            "sqs.nervix.test",
            nervix_dns::DnsLookupFailure::Timeout,
        );
        // The Smithy hook hands the SDK the lookup's whole report, as `DnsLookupReport`.
        let carried = nervix_dns::DnsLookupReport::from(Report::new(lookup));
        let unresolved = SdkError::<GetQueueUrlError, ()>::dispatch_failure(ConnectorError::io(
            Box::new(carried),
        ));
        let unresolved = SqsSource::queue_lookup_error("orders", &unresolved);
        assert!(matches!(
            unresolved.current_context(),
            SqsSourceError::ResolveQueue { queue } if queue == "orders"
        ));
        assert_eq!(
            format!("{unresolved:#}"),
            "failed to resolve SQS queue 'orders': resolving 'sqs.nervix.test' failed: no answer \
             arrived in time"
        );

        assert_eq!(
            SqsSource::require_queue_url("orders", Some("queue-url".to_string()))
                .expect("a returned queue URL should be accepted"),
            "queue-url"
        );
        let no_url = SqsSource::require_queue_url("orders", None)
            .expect_err("a successful response without a queue URL must fail");
        assert!(matches!(
            no_url.current_context(),
            SqsSourceError::MissingQueueUrl { queue } if queue == "orders"
        ));
    }

    #[test]
    fn attribute_values_render_strings_binaries_and_lists() {
        let string = MessageAttributeValue::builder()
            .data_type("String")
            .string_value("value")
            .build()
            .expect("a string attribute builds");
        assert_eq!(SqsMessageAttributes::attribute_value(&string), "value");

        let binary = MessageAttributeValue::builder()
            .data_type("Binary")
            .binary_value(aws_sdk_sqs::primitives::Blob::new(b"bytes".to_vec()))
            .build()
            .expect("a binary attribute builds");
        assert_eq!(SqsMessageAttributes::attribute_value(&binary), "bytes");

        let list = MessageAttributeValue::builder()
            .data_type("String")
            .string_list_values("a")
            .string_list_values("b")
            .build()
            .expect("a string list attribute builds");
        assert_eq!(SqsMessageAttributes::attribute_value(&list), "a,b");
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "source_read_tests.rs"]
mod read_tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "source_shuttle_tests.rs"]
mod shuttle_tests;
