//! SQS source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The SQS client a source configuration declares, queue-URL lookup, long polling,
//!   message attributes as ingest headers, and deleting a message once it is acknowledged.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio, the node resolver, and the AWS SQS SDK.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use std::{borrow::Cow, fmt::Debug};

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
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult,
    client_config_value, client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, QueueName};
use thiserror::Error;

use crate::connection::{FailedRequest, SqsTrust};

const SQS: &str = "sqs";
/// How long one receive request waits for a message before the service answers empty.
const LONG_POLL_SECONDS: i32 = 1;

/// Why an SQS source could not open its queue, receive from it, or delete a message.
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
    #[error("failed to delete an acknowledged SQS message")]
    Delete,
}

type SqsSourceResult<T> = Result<T, Report<SqsSourceError>>;

/// One SQS source's client and the URL of the queue every instance polls.
///
/// The client is built and the queue resolved once; each instance polls through a clone.
#[derive(Clone)]
pub struct SqsSourcePlan {
    client: SqsClient,
    queue_url: String,
}

impl SqsSourcePlan {
    /// The client `config` declares, whose every connection resolves the endpoint host through
    /// `dns`, and the URL of `queue`, which must already exist.
    pub async fn connect(
        config: &[ClientConfigEntry],
        queue: &QueueName,
        dns: DnsResolver,
    ) -> SqsSourceResult<Self> {
        let client = Self::client_from_config(config, dns).await?;
        let queue_url = Self::queue_url(&client, queue.as_str()).await?;
        Ok(Self { client, queue_url })
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
            .is_some_and(GetQueueUrlError::is_queue_does_not_exist);
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

    fn require_queue_url(queue: &str, queue_url: Option<String>) -> SqsSourceResult<String> {
        match queue_url {
            Some(queue_url) => Ok(queue_url),
            None => Err(Report::new(SqsSourceError::MissingQueueUrl {
                queue: queue.to_string(),
            })),
        }
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
    queue_url: String,
}

#[async_trait]
impl SourceConnector for SqsSource {
    type Plan = SqsSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        Ok(Self {
            client: plan.client.clone(),
            queue_url: plan.queue_url.clone(),
        })
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
    ) -> SourceResult<SourceBatch<Self::Message>> {
        loop {
            tokio::task::consume_budget().await;
            let received = self
                .client
                .receive_message()
                .queue_url(self.queue_url.clone())
                .max_number_of_messages(1)
                .message_attribute_names("All")
                .wait_time_seconds(LONG_POLL_SECONDS)
                .send()
                .await;
            let response = match received {
                Ok(response) => response,
                Err(error) => {
                    let failure = FailedRequest::new(&error)
                        .report(SqsSourceError::Receive, "SQS ReceiveMessage failed");
                    return Err(failure.change_context(SourceError::Read { connector: SQS }));
                }
            };
            let received = response.messages.unwrap_or_default();
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

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        for position in positions {
            tokio::task::consume_budget().await;
            let Some(receipt_handle) = position.receipt_handle.as_deref() else {
                continue;
            };
            let deleted = self
                .client
                .delete_message()
                .queue_url(self.queue_url.clone())
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
mod tests {
    use aws_sdk_sqs::{error::ConnectorError, types::error::QueueDoesNotExist};

    use super::*;

    #[test]
    fn queue_lookup_failures_are_distinct_and_keep_their_source_message() {
        let missing = SdkError::<GetQueueUrlError, ()>::service_error(
            GetQueueUrlError::QueueDoesNotExist(QueueDoesNotExist::builder().build()),
            (),
        );
        let missing = SqsSourcePlan::queue_lookup_error("missing-queue", &missing);
        assert!(matches!(
            missing.current_context(),
            SqsSourceError::MissingQueue { queue } if queue == "missing-queue"
        ));
        assert!(format!("{missing:?}").contains("service error"));

        let refused = SdkError::<GetQueueUrlError, ()>::dispatch_failure(ConnectorError::io(
            "connection refused".into(),
        ));
        let connection = SqsSourcePlan::queue_lookup_error("orders", &refused);
        assert!(matches!(
            connection.current_context(),
            SqsSourceError::ResolveQueue { queue } if queue == "orders"
        ));
        assert!(format!("{connection:?}").contains("connection refused"));

        let lookup = nervix_dns::DnsLookupError::new(
            "sqs.nervix.test",
            nervix_dns::DnsLookupFailure::Timeout,
        );
        let unresolved = SdkError::<GetQueueUrlError, ()>::dispatch_failure(ConnectorError::io(
            Box::new(lookup),
        ));
        let unresolved = SqsSourcePlan::queue_lookup_error("orders", &unresolved);
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
            SqsSourcePlan::require_queue_url("orders", Some("queue-url".to_string()))
                .expect("a returned queue URL should be accepted"),
            "queue-url"
        );
        let no_url = SqsSourcePlan::require_queue_url("orders", None)
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
