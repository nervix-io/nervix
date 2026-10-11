//! SQS source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The SQS client a configuration declares and the connections it makes, queue-URL
//!   lookup, long polling and message deletion on the source side, protocol validation of each
//!   message body, attribute and FIFO group, request batching, and per-entry response
//!   classification.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, the node
//!   resolver, the kernel crate's XML character check, and the AWS SQS SDK with its Smithy HTTP
//!   client.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation. A FIFO message group arrives already evaluated for its record, so
//!   the expression behind it stays with the host.

mod connection;
mod source;

use std::{fmt::Debug, time::Duration};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::{
    Client as SqsClient,
    error::SdkError,
    operation::get_queue_url::GetQueueUrlError,
    types::{MessageAttributeValue, SendMessageBatchRequestEntry},
};
use connection::{FailedRequest, SqsTrust};
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_connector::{
    PerRecordOutcome, RecordSink, RejectedSinkRecord, SinkHost, SinkLifecycle, SinkPublishError,
    SinkRecord, SinkRecordId, SinkStartError, SinkStartResult, client_config_value,
    client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, Timestamp};
use nervix_simd_kernels::{XmlChars, XmlCharsError};
pub use source::{
    SqsMessageAttributes, SqsSource, SqsSourceError, SqsSourceMessage, SqsSourcePlan,
    SqsSourcePosition,
};
use thiserror::Error;

const SQS: &str = "sqs";
const SQS_MAX_BATCH_ENTRIES: usize = 10;
const SQS_MAX_REQUEST_BYTES: usize = 256 * 1024;

/// Whether an SQS sink sends one message per request or batches them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqsPublishingMode {
    Single,
    Batch,
}

/// What one SQS sink sends to: its client entries, queue name and request shape.
pub struct SqsSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub queue: String,
    pub mode: SqsPublishingMode,
    /// The node resolver every connection of the client resolves the endpoint host through.
    pub dns: DnsResolver,
}

pub struct SqsSink {
    client: SqsClient,
    queue_url: String,
    mode: SqsPublishingMode,
}

/// Why one record is not a message this sink can send.
#[derive(Debug, PartialEq, Eq, Error)]
enum SqsRecordError {
    #[error("SQS message body is not valid UTF-8")]
    InvalidBodyEncoding,
    #[error("SQS message body contains a character the service forbids")]
    ForbiddenBodyCharacter,
    #[error("SQS message has {count} attributes; the service permits at most {maximum}")]
    AttributeCount { count: usize, maximum: usize },
    #[error("SQS message attribute names must contain 1 to 256 bytes")]
    AttributeNameLength,
    #[error("SQS message attribute name '{name}' uses a reserved prefix")]
    ReservedAttributePrefix { name: String },
    #[error("invalid SQS message attribute name '{name}'")]
    InvalidAttributeName { name: String },
    #[error("SQS message attribute '{name}' contains a forbidden character")]
    ForbiddenAttributeCharacter { name: String },
    #[error("SQS message attribute '{name}' has an empty value, which SQS does not accept")]
    EmptyAttributeValue { name: String },
    #[error("invalid SQS message attribute '{name}'")]
    BuildAttribute { name: String },
    #[error("SQS FIFO message group must contain 1 to 128 characters")]
    MessageGroupLength,
    #[error("SQS FIFO message group contains an unsupported character")]
    MessageGroupCharacter,
    #[error(
        "SQS message is {bytes} bytes counting its attributes and message group; SQS accepts at \
         most {maximum} bytes"
    )]
    RequestSize { bytes: usize, maximum: usize },
}

type SqsRecordResult<T> = Result<T, Report<SqsRecordError>>;

#[derive(Debug)]
struct PreparedSqsRecord {
    record: SinkRecordId,
    occurred_at: Timestamp,
    body: String,
    attributes: HashMap<String, MessageAttributeValue>,
    group_id: Option<String>,
    encoded_bytes: usize,
}

impl PreparedSqsRecord {
    fn new(record: SinkRecord) -> SqsRecordResult<Self> {
        const ENCODED_IN_MEMORY: &str =
            "every term counts bytes of a record this node already holds in memory";

        let SinkRecord {
            id: record,
            key: _,
            payload,
            headers,
            message_group: group_id,
            occurred_at,
        } = record;
        let body = match XmlChars::into_string(payload) {
            Ok(body) => body,
            Err(report) => {
                let issue = match report.current_context() {
                    XmlCharsError::InvalidUtf8 => SqsRecordError::InvalidBodyEncoding,
                    XmlCharsError::ExcludedCharacter => SqsRecordError::ForbiddenBodyCharacter,
                };
                return Err(report.change_context(issue));
            }
        };
        if headers.len() > 10 {
            return Err(Report::new(SqsRecordError::AttributeCount {
                count: headers.len(),
                maximum: 10,
            }));
        }
        let mut attributes = HashMap::with_capacity(headers.len());
        for (name, value) in headers {
            SqsSink::validate_attribute(&name, &value)?;
            let attribute = MessageAttributeValue::builder()
                .data_type("String")
                .string_value(&value)
                .build()
                .map_err(|error| {
                    Report::new(SqsRecordError::BuildAttribute { name: name.clone() })
                        .attach_printable(error)
                })?;
            attributes.insert(name, attribute);
        }
        if let Some(group_id) = group_id.as_deref() {
            SqsSink::validate_group_id(group_id)?;
        }
        let body_bytes = body.len();
        let mut attribute_bytes = 0_usize;
        for (name, attribute) in &attributes {
            // Read the value first to retain the original contribution-evaluation order.
            let value_bytes = match attribute.string_value() {
                Some(value) => value.len(),
                None => 0,
            };
            let name_bytes = name.len();
            let data_type_bytes = attribute.data_type().len();
            let name_and_type_bytes = name_bytes
                .checked_add(data_type_bytes)
                .assured(ENCODED_IN_MEMORY);
            let attribute_total_bytes = name_and_type_bytes
                .checked_add(value_bytes)
                .assured(ENCODED_IN_MEMORY);
            attribute_bytes = attribute_bytes
                .checked_add(attribute_total_bytes)
                .assured(ENCODED_IN_MEMORY);
        }
        let group_bytes = match group_id.as_ref() {
            Some(group_id) => group_id.len(),
            None => 0,
        };
        let body_and_attribute_bytes = body_bytes
            .checked_add(attribute_bytes)
            .assured(ENCODED_IN_MEMORY);
        let encoded_bytes = body_and_attribute_bytes
            .checked_add(group_bytes)
            .assured(ENCODED_IN_MEMORY);
        if encoded_bytes > SQS_MAX_REQUEST_BYTES {
            return Err(Report::new(SqsRecordError::RequestSize {
                bytes: encoded_bytes,
                maximum: SQS_MAX_REQUEST_BYTES,
            }));
        }
        Ok(Self {
            record,
            occurred_at,
            body,
            attributes,
            group_id,
            encoded_bytes,
        })
    }

    fn rejected(&self, reason: String) -> RejectedSinkRecord<SinkRecordId> {
        RejectedSinkRecord::external(self.record, self.occurred_at, reason)
    }
}

impl SqsSink {
    pub async fn new(config: SqsSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let SqsSinkConfig {
            config,
            queue,
            mode,
            dns,
        } = config;
        let client = Self::client_from_config(&config, dns).await?;
        let queue_url = Self::queue_url(&client, &queue).await?;
        Ok(Self {
            client,
            queue_url,
            mode,
        })
    }

    async fn client_from_config(
        config: &[ClientConfigEntry],
        dns: DnsResolver,
    ) -> SinkStartResult<SqsClient> {
        let endpoint = Self::config_value(config, "endpoint")?;
        let region = optional_client_config_value(config, "region")
            .unwrap_or("us-east-1")
            .to_string();
        let access_key_id = optional_client_config_value(config, "access_key_id")
            .unwrap_or("x")
            .to_string();
        let secret_access_key = optional_client_config_value(config, "secret_access_key")
            .unwrap_or("x")
            .to_string();

        let request_timeout = optional_client_config_value(config, "timeout_ms")
            .map(|timeout_ms| {
                timeout_ms
                    .parse::<u64>()
                    .map(Duration::from_millis)
                    .map_err(|_| {
                        Self::config_error(format!("invalid SQS timeout_ms '{timeout_ms}'"))
                    })
            })
            .transpose()?;

        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_sqs::config::Region::new(region))
            .endpoint_url(endpoint)
            .retry_config(aws_config::retry::RetryConfig::disabled())
            .credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "nervix-sqs",
            ));
        if let Some(request_timeout) = request_timeout {
            loader = loader.timeout_config(
                aws_config::timeout::TimeoutConfig::builder()
                    .operation_timeout(request_timeout)
                    .operation_attempt_timeout(request_timeout)
                    .build(),
            );
        }
        let trust = match client_tls_paths(config).ca_file.as_ref() {
            Some(ca_file) => {
                let ca_pem = read_tls_file(ca_file, "TLS CA certificate").map_err(|error| {
                    let message = error.current_context().to_string();
                    error
                        .change_context(SinkStartError::InvalidConfiguration { sink: SQS })
                        .attach_printable(message)
                })?;
                SqsTrust::ca(ca_pem).map_err(Self::start_error)?
            }
            None => SqsTrust::Platform,
        };
        let sdk_config = loader.http_client(trust.http_client(dns)).load().await;
        Ok(SqsClient::new(&sdk_config))
    }

    async fn queue_url(client: &SqsClient, queue: &str) -> SinkStartResult<String> {
        let response = match client.get_queue_url().queue_name(queue).send().await {
            Ok(response) => response,
            Err(error) => return Err(Self::queue_lookup_error(queue, &error)),
        };
        Self::require_queue_url(queue, response.queue_url().map(ToOwned::to_owned))
    }

    fn queue_lookup_error<R: Debug + 'static>(
        queue: &str,
        error: &SdkError<GetQueueUrlError, R>,
    ) -> Report<SinkStartError> {
        let failure = FailedRequest::new(error);
        let missing = error
            .as_service_error()
            .is_some_and(GetQueueUrlError::is_queue_does_not_exist);
        if missing {
            return Report::new(SinkStartError::MissingExternalEntity {
                sink: SQS,
                kind: "SQS queue",
                name: queue.to_string(),
            })
            .attach_printable(failure.description());
        }
        failure.report(
            SinkStartError::Initialize { sink: SQS },
            "SQS GetQueueUrl failed",
        )
    }

    fn require_queue_url(queue: &str, queue_url: Option<String>) -> SinkStartResult<String> {
        match queue_url {
            Some(queue_url) => Ok(queue_url),
            None => Err(Self::start_error(format!("SQS queue '{queue}' has no URL"))),
        }
    }

    async fn publish_single(
        &self,
        records: Vec<PreparedSqsRecord>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let mut request = self
                .client
                .send_message()
                .queue_url(&self.queue_url)
                .message_body(&record.body)
                .set_message_attributes(
                    (!record.attributes.is_empty())
                        .then(|| record.attributes.clone().into_iter().collect()),
                );
            if let Some(group_id) = record.group_id.as_deref() {
                request = request.message_group_id(group_id);
            }
            match request.send().await {
                Ok(_) => outcome.deliver(record.record),
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_invalid_message_contents()) =>
                {
                    outcome.reject(record.rejected(format!("SQS rejected the record: {error}")));
                }
                Err(error) => {
                    outcome.fail(FailedRequest::new(&error).report(
                        SinkPublishError::Publish { sink: SQS },
                        "SQS SendMessage failed",
                    ));
                    return;
                }
            }
        }
    }

    async fn publish_batches(
        &self,
        records: Vec<PreparedSqsRecord>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        for records in Self::batch_chunks(records) {
            nervix_primitives::task::consume_budget().await;
            let mut request = self.client.send_message_batch().queue_url(&self.queue_url);
            for (index, record) in records.iter().enumerate() {
                let mut entry = SendMessageBatchRequestEntry::builder()
                    .id(Self::batch_entry_id(index))
                    .message_body(&record.body)
                    .set_message_attributes(
                        (!record.attributes.is_empty())
                            .then(|| record.attributes.clone().into_iter().collect()),
                    );
                if let Some(group_id) = record.group_id.as_deref() {
                    entry = entry.message_group_id(group_id);
                }
                let entry = match entry.build() {
                    Ok(entry) => entry,
                    Err(error) => {
                        outcome.fail(Self::publish_error(format!(
                            "failed to build SQS batch request entry: {error}"
                        )));
                        return;
                    }
                };
                request = request.entries(entry);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    outcome.fail(FailedRequest::new(&error).report(
                        SinkPublishError::Publish { sink: SQS },
                        "SQS SendMessageBatch failed",
                    ));
                    return;
                }
            };
            if let Some(reason) = Self::apply_batch_response(&records, &response, outcome) {
                outcome.fail(Self::publish_error(reason));
                return;
            }
        }
    }

    fn batch_chunks(records: Vec<PreparedSqsRecord>) -> Vec<Vec<PreparedSqsRecord>> {
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0_usize;
        let mut current_fifo_groups = HashSet::new();
        for record in records {
            let would_exceed_count = current.len() == SQS_MAX_BATCH_ENTRIES;
            let would_exceed_bytes = !current.is_empty()
                && record.encoded_bytes
                    > SQS_MAX_REQUEST_BYTES.checked_sub(current_bytes).verified(
                        "PreparedSqsRecord::new rejects a record above the request limit, so a \
                         chunk's running total never passes it",
                    );
            let would_repeat_fifo_group = record
                .group_id
                .as_ref()
                .is_some_and(|group| current_fifo_groups.contains(group));
            if would_exceed_count || would_exceed_bytes || would_repeat_fifo_group {
                chunks.push(std::mem::take(&mut current));
                current_bytes = 0;
                current_fifo_groups.clear();
            }
            current_bytes = current_bytes
                .checked_add(record.encoded_bytes)
                .assured("both counts total bytes of records this node already holds in memory");
            if let Some(group) = record.group_id.as_ref() {
                current_fifo_groups.insert(group.clone());
            }
            current.push(record);
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        chunks
    }

    fn apply_batch_response(
        records: &[PreparedSqsRecord],
        response: &aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> Option<String> {
        let mut accounted = vec![false; records.len()];
        let mut infrastructure_reasons = Vec::new();
        for success in response.successful() {
            match Self::batch_entry_index(success.id(), records.len()) {
                Some(index) if !accounted[index] => {
                    accounted[index] = true;
                    outcome.deliver(records[index].record);
                }
                Some(index) => infrastructure_reasons.push(format!(
                    "SQS returned duplicate result for batch entry {}",
                    Self::batch_entry_id(index)
                )),
                None => infrastructure_reasons.push(format!(
                    "SQS returned an unknown successful batch entry id '{}'",
                    success.id()
                )),
            }
        }
        for failure in response.failed() {
            match Self::batch_entry_index(failure.id(), records.len()) {
                Some(index) if !accounted[index] => {
                    accounted[index] = true;
                    let reason = format!("SQS batch entry failed with {}", failure.code());
                    if Self::is_record_failure(
                        failure.sender_fault(),
                        failure.code(),
                        failure.message(),
                    ) {
                        outcome.reject(records[index].rejected(reason));
                    } else {
                        infrastructure_reasons.push(reason);
                    }
                }
                Some(index) => infrastructure_reasons.push(format!(
                    "SQS returned duplicate result for batch entry {}",
                    Self::batch_entry_id(index)
                )),
                None => infrastructure_reasons.push(format!(
                    "SQS returned an unknown failed batch entry id '{}'",
                    failure.id()
                )),
            }
        }
        for (index, accounted) in accounted.into_iter().enumerate() {
            if !accounted {
                infrastructure_reasons.push(format!(
                    "SQS omitted a result for batch entry {}",
                    Self::batch_entry_id(index)
                ));
            }
        }
        (!infrastructure_reasons.is_empty()).then(|| infrastructure_reasons.join("; "))
    }

    fn batch_entry_id(index: usize) -> String {
        format!("m{index}")
    }

    fn batch_entry_index(id: &str, record_count: usize) -> Option<usize> {
        let index = id.strip_prefix('m')?;
        let Ok(index) = index.parse::<usize>() else {
            return None;
        };
        if index < record_count {
            Some(index)
        } else {
            None
        }
    }

    fn is_record_failure(sender_fault: bool, code: &str, message: Option<&str>) -> bool {
        if !sender_fault {
            return false;
        }
        match code {
            "InvalidMessageContents" | "MessageTooLong" => true,
            "InvalidParameterValue" => {
                let message = message.unwrap_or_default().to_ascii_lowercase();
                !message.contains("deduplication")
                    && (message.contains("message body")
                        || message.contains("message attribute")
                        || message.contains("messagegroupid")
                        || message.contains("message group id"))
            }
            _ => false,
        }
    }

    fn validate_attribute(name: &str, value: &str) -> SqsRecordResult<()> {
        let normalized = name.to_ascii_lowercase();
        if name.is_empty() || name.len() > 256 {
            return Err(Report::new(SqsRecordError::AttributeNameLength));
        }
        if normalized.starts_with("aws.") || normalized.starts_with("amazon.") {
            return Err(Report::new(SqsRecordError::ReservedAttributePrefix {
                name: name.to_string(),
            }));
        }
        if name.starts_with('.')
            || name.ends_with('.')
            || name.contains("..")
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(Report::new(SqsRecordError::InvalidAttributeName {
                name: name.to_string(),
            }));
        }
        if value.is_empty() {
            return Err(Report::new(SqsRecordError::EmptyAttributeValue {
                name: name.to_string(),
            }));
        }
        if !XmlChars::admits(value) {
            return Err(Report::new(SqsRecordError::ForbiddenAttributeCharacter {
                name: name.to_string(),
            }));
        }
        Ok(())
    }

    fn validate_group_id(group_id: &str) -> SqsRecordResult<()> {
        if group_id.is_empty() || group_id.chars().count() > 128 {
            return Err(Report::new(SqsRecordError::MessageGroupLength));
        }
        if !group_id.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(
                    character,
                    '!' | '"'
                        | '#'
                        | '$'
                        | '%'
                        | '&'
                        | '\''
                        | '('
                        | ')'
                        | '*'
                        | '+'
                        | ','
                        | '-'
                        | '.'
                        | '/'
                        | ':'
                        | ';'
                        | '<'
                        | '='
                        | '>'
                        | '?'
                        | '@'
                        | '['
                        | '\\'
                        | ']'
                        | '^'
                        | '_'
                        | '`'
                        | '{'
                        | '|'
                        | '}'
                        | '~'
                )
        }) {
            return Err(Report::new(SqsRecordError::MessageGroupCharacter));
        }
        Ok(())
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> SinkStartResult<String> {
        client_config_value(config, key, "SQS").map_err(|error| {
            let message = error.current_context().to_string();
            error
                .change_context(SinkStartError::InvalidConfiguration { sink: SQS })
                .attach_printable(message)
        })
    }

    fn config_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::InvalidConfiguration { sink: SQS })
            .attach_printable(error.to_string())
    }

    fn start_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::Initialize { sink: SQS }).attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external SQS SDK failure; no internal \
                                   runtime ownership is inferred")
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: SQS }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for SqsSink {}

#[async_trait]
impl RecordSink for SqsSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let mut prepared = Vec::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            match PreparedSqsRecord::new(record) {
                Ok(record) => prepared.push(record),
                Err(error) => outcome.reject(RejectedSinkRecord::external(
                    record_id,
                    occurred_at,
                    error.to_string(),
                )),
            }
        }
        match self.mode {
            SqsPublishingMode::Single => self.publish_single(prepared, &mut outcome).await,
            SqsPublishingMode::Batch => self.publish_batches(prepared, &mut outcome).await,
        }
        outcome
    }
}

#[cfg(test)]
mod connection_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use aws_sdk_sqs::{error::ConnectorError, types::error::QueueDoesNotExist};
    use meticulous::ResultExt as _;

    use super::*;
    use crate::connection_tests::Fixture;

    fn client_config(timeout_ms: &str) -> Vec<ClientConfigEntry> {
        vec![
            ClientConfigEntry {
                key: "endpoint".to_string(),
                value: "http://127.0.0.1:9324".to_string(),
            },
            ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: timeout_ms.to_string(),
            },
        ]
    }

    fn record(index: usize, payload: Vec<u8>) -> SinkRecord {
        SinkRecord::new(
            SinkRecordId::new(index),
            None,
            payload,
            Vec::new(),
            Timestamp::from_unix_nanos(0),
        )
    }

    fn prepared(payload_bytes: usize) -> PreparedSqsRecord {
        PreparedSqsRecord::new(record(0, vec![b'x'; payload_bytes]))
            .expect("test SQS record should be valid")
    }

    fn prepared_as(index: usize, payload_bytes: usize) -> PreparedSqsRecord {
        PreparedSqsRecord::new(record(index, vec![b'x'; payload_bytes]))
            .expect("test SQS record should be valid")
    }

    fn prepared_in_group(index: usize, group: &str) -> PreparedSqsRecord {
        PreparedSqsRecord::new(record(index, vec![b'x']).with_message_group(group.to_string()))
            .expect("test SQS FIFO record should be valid")
    }

    #[test]
    fn rejects_a_record_larger_than_the_declared_sqs_protocol_limit() {
        let error = PreparedSqsRecord::new(record(0, vec![b'x'; SQS_MAX_REQUEST_BYTES + 1]))
            .expect_err("oversized SQS record should be rejected");

        assert_eq!(
            *error.current_context(),
            SqsRecordError::RequestSize {
                bytes: SQS_MAX_REQUEST_BYTES + 1,
                maximum: SQS_MAX_REQUEST_BYTES,
            }
        );
    }

    #[test]
    fn rejects_invalid_sqs_body_and_attribute_shapes_with_typed_errors() {
        let build = |payload: Vec<u8>, headers: Vec<(String, String)>| {
            let mut record = record(0, payload);
            record.headers = headers;
            PreparedSqsRecord::new(record)
        };

        let body =
            build(vec![0], Vec::new()).expect_err("SQS must reject a forbidden body character");
        assert_eq!(
            body.current_context(),
            &SqsRecordError::ForbiddenBodyCharacter
        );

        let too_many = (0..11)
            .map(|index| (format!("attribute_{index}"), "value".to_string()))
            .collect();
        let count =
            build(vec![b'x'], too_many).expect_err("SQS must reject more than ten attributes");
        assert_eq!(
            count.current_context(),
            &SqsRecordError::AttributeCount {
                count: 11,
                maximum: 10,
            }
        );

        let empty = build(vec![b'x'], vec![(String::new(), "value".to_string())])
            .expect_err("SQS attribute names cannot be empty");
        assert_eq!(
            empty.current_context(),
            &SqsRecordError::AttributeNameLength
        );

        let reserved = build(
            vec![b'x'],
            vec![("AWS.trace".to_string(), "value".to_string())],
        )
        .expect_err("SQS attribute names cannot use reserved prefixes");
        assert!(matches!(
            reserved.current_context(),
            SqsRecordError::ReservedAttributePrefix { .. }
        ));

        let name = build(
            vec![b'x'],
            vec![("bad..name".to_string(), "value".to_string())],
        )
        .expect_err("SQS attribute names cannot contain repeated dots");
        assert!(matches!(
            name.current_context(),
            SqsRecordError::InvalidAttributeName { .. }
        ));

        let value = build(vec![b'x'], vec![("valid".to_string(), "\0".to_string())])
            .expect_err("SQS attribute values cannot contain forbidden characters");
        assert!(matches!(
            value.current_context(),
            SqsRecordError::ForbiddenAttributeCharacter { .. }
        ));

        let empty_value = build(vec![b'x'], vec![("tenant".to_string(), String::new())])
            .expect_err("SQS refuses a message attribute without a value");
        assert_eq!(
            empty_value.current_context(),
            &SqsRecordError::EmptyAttributeValue {
                name: "tenant".to_string()
            }
        );
    }

    #[test]
    fn bodies_admit_xml_characters_and_reject_noncharacters_and_invalid_utf8() {
        let prepare = |body: &[u8]| PreparedSqsRecord::new(record(0, body.to_vec()));
        for admitted in [
            "{\"note\":\"tab\tline\nreturn\r\"}",
            "\u{7F}\u{9F}\u{D7FF}\u{E000}\u{FDD0}\u{FFFD}",
            "\u{1_0000}\u{1F600}\u{1_FFFE}\u{10_FFFF}",
            "",
        ] {
            let prepared = prepare(admitted.as_bytes()).expect("SQS admits every character");
            assert_eq!(prepared.body, admitted);
        }
        for excluded in [
            "\u{FFFE}",
            "before \u{FFFF} after",
            "\u{0}",
            "\u{B}",
            "\u{1F}",
        ] {
            let error = prepare(excluded.as_bytes())
                .expect_err("SQS forbids noncharacters U+FFFE and U+FFFF and C0 controls");
            assert_eq!(
                error.current_context(),
                &SqsRecordError::ForbiddenBodyCharacter
            );
        }
        let mut forbidden_then_invalid = "\u{FFFF}".as_bytes().to_vec();
        forbidden_then_invalid.push(0xFF);
        let mut invalid_then_forbidden = vec![0xED, 0xA0, 0x80];
        invalid_then_forbidden.extend_from_slice("\u{0}".as_bytes());
        for invalid in [forbidden_then_invalid, invalid_then_forbidden] {
            let error = prepare(&invalid).expect_err("the body is not UTF-8");
            assert_eq!(
                error.current_context(),
                &SqsRecordError::InvalidBodyEncoding
            );
        }

        let mut noncharacter_value = record(0, vec![b'x']);
        noncharacter_value.headers = vec![("tenant".to_string(), "\u{FFFF}".to_string())];
        let error = PreparedSqsRecord::new(noncharacter_value)
            .expect_err("SQS forbids a noncharacter in an attribute value");
        assert_eq!(
            error.current_context(),
            &SqsRecordError::ForbiddenAttributeCharacter {
                name: "tenant".to_string()
            }
        );
    }

    #[test]
    fn sqs_queue_lookup_distinguishes_missing_entities_from_connection_failures() {
        let missing = SdkError::<GetQueueUrlError, ()>::service_error(
            GetQueueUrlError::QueueDoesNotExist(QueueDoesNotExist::builder().build()),
            (),
        );
        let missing = SqsSink::queue_lookup_error("missing-queue", &missing);
        assert!(matches!(
            missing.current_context(),
            SinkStartError::MissingExternalEntity {
                sink: SQS,
                kind: "SQS queue",
                name,
            } if name == "missing-queue"
        ));

        let refused = SdkError::<GetQueueUrlError, ()>::dispatch_failure(ConnectorError::io(
            "connection refused".into(),
        ));
        let connection = SqsSink::queue_lookup_error("orders", &refused);
        assert_eq!(
            connection.current_context(),
            &SinkStartError::Initialize { sink: SQS }
        );
        let message = connection
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            message.map(String::as_str),
            Some("SQS GetQueueUrl failed: dispatch failure: io error: connection refused")
        );

        assert_eq!(
            SqsSink::require_queue_url("orders", Some("queue-url".to_string()))
                .expect("a returned queue URL should be accepted"),
            "queue-url"
        );
        let no_url = SqsSink::require_queue_url("orders", None)
            .expect_err("a successful response without a queue URL must fail");
        assert_eq!(
            no_url.current_context(),
            &SinkStartError::Initialize { sink: SQS }
        );
    }

    #[test]
    fn request_size_counts_attribute_names_types_values_and_the_fifo_group() {
        const ATTRIBUTE_NAME: &str = "tenant";
        const ATTRIBUTE_VALUE: &str = "acme";
        const GROUP_ID: &str = "orders";

        // Every attribute the sink builds declares the "String" data type, so those bytes count
        // against the protocol limit alongside the attribute name and value.
        let attribute_bytes = ATTRIBUTE_NAME.len() + "String".len() + ATTRIBUTE_VALUE.len();
        let body_bytes = SQS_MAX_REQUEST_BYTES - attribute_bytes - GROUP_ID.len();
        let headers = vec![(ATTRIBUTE_NAME.to_string(), ATTRIBUTE_VALUE.to_string())];
        let fitting = |payload_bytes: usize| {
            let mut record =
                record(0, vec![b'x'; payload_bytes]).with_message_group(GROUP_ID.to_string());
            record.headers = headers.clone();
            PreparedSqsRecord::new(record)
        };

        let record = fitting(body_bytes)
            .expect("a record that exactly fills the protocol limit should be accepted");

        assert_eq!(record.encoded_bytes, SQS_MAX_REQUEST_BYTES);

        let error = fitting(body_bytes + 1)
            .expect_err("one byte past the protocol limit should be rejected");

        assert_eq!(
            *error.current_context(),
            SqsRecordError::RequestSize {
                bytes: SQS_MAX_REQUEST_BYTES + 1,
                maximum: SQS_MAX_REQUEST_BYTES,
            }
        );
    }

    #[test]
    fn batch_chunks_respect_entry_and_byte_limits_without_reordering() {
        let mut records = (0..11)
            .map(|index| prepared_as(index, 1))
            .collect::<Vec<_>>();
        records.push(prepared(SQS_MAX_REQUEST_BYTES));
        records.push(prepared(2));

        let chunks = SqsSink::batch_chunks(records);
        let records = chunks
            .iter()
            .flatten()
            .map(|record| record.record)
            .collect::<Vec<_>>();

        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            [10, 1, 1, 1]
        );
        assert_eq!(records[0], SinkRecordId::new(0));
        assert_eq!(records[10], SinkRecordId::new(10));
    }

    #[test]
    fn batch_chunks_never_send_two_records_from_one_fifo_group_together() {
        let records = vec![
            prepared_in_group(0, "acme"),
            prepared_in_group(1, "beta"),
            prepared_in_group(2, "acme"),
            prepared_in_group(3, "beta"),
        ];

        let chunks = SqsSink::batch_chunks(records);
        let records = chunks
            .iter()
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|record| record.record.index())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(records, [vec![0, 1], vec![2, 3]]);
    }

    /// The attribute names a generated record draws from.
    const ATTRIBUTE_NAMES: [&str; 5] = ["tenant", "trace-id", "a.b", "x_1", "Region"];

    /// The FIFO message groups a generated record draws from.
    const MESSAGE_GROUPS: [&str; 4] = ["orders", "b", "group-with-a-longer-name", "#1"];

    /// What a generated body repeats: characters of one, two, three and four UTF-8 bytes, and the
    /// whitespace XML admits.
    const BODY_PIECES: [&str; 6] = ["x", "é", "日", "😀", "\t", "\n"];

    /// A generated body length: small, or within a few bytes either side of what fills the
    /// request limit beside the record's attributes and message group.
    #[derive(Debug, bolero::TypeGenerator)]
    enum BodyLength {
        Small { pieces: u16, piece: u8 },
        NearLimit(i16),
    }

    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedAttribute {
        name: u8,
        value_bytes: u8,
    }

    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedRecord {
        body: BodyLength,
        #[generator(bolero::generator::produce_with::<Vec<GeneratedAttribute>>().len(0_usize..=4))]
        attributes: Vec<GeneratedAttribute>,
        group: Option<u8>,
    }

    impl GeneratedRecord {
        /// The record's attributes, each name once, the last value written for a name kept.
        fn headers(&self) -> Vec<(String, String)> {
            let mut values = BTreeMap::new();
            for attribute in &self.attributes {
                let name = ATTRIBUTE_NAMES[usize::from(attribute.name) % ATTRIBUTE_NAMES.len()];
                let value = "v".repeat(usize::from(attribute.value_bytes % 64) + 1);
                values.insert(name, value);
            }
            let mut headers = Vec::with_capacity(values.len());
            for (name, value) in values {
                headers.push((name.to_string(), value));
            }
            headers
        }

        fn group(&self) -> Option<String> {
            self.group
                .map(|group| MESSAGE_GROUPS[usize::from(group) % MESSAGE_GROUPS.len()].to_string())
        }

        /// The bytes SQS counts for everything but the body: each attribute's name, its `String`
        /// data type and its value, and the message group.
        fn envelope_bytes(&self) -> usize {
            let mut bytes = 0;
            for (name, value) in self.headers() {
                bytes += name.len() + "String".len() + value.len();
            }
            if let Some(group) = self.group() {
                bytes += group.len();
            }
            bytes
        }

        fn body(&self) -> String {
            match self.body {
                BodyLength::Small { pieces, piece } => {
                    let piece = BODY_PIECES[usize::from(piece) % BODY_PIECES.len()];
                    piece.repeat(usize::from(pieces % 2048))
                }
                BodyLength::NearLimit(offset) => {
                    let target = i64::try_from(SQS_MAX_REQUEST_BYTES - self.envelope_bytes())
                        .assured("the request limit fits i64");
                    let length = (target + i64::from(offset % 64)).max(0);
                    "x".repeat(usize::try_from(length).assured("the length is not negative"))
                }
            }
        }

        fn record(&self, index: usize) -> SinkRecord {
            let mut record = record(index, self.body().into_bytes());
            record.headers = self.headers();
            if let Some(group) = self.group() {
                record = record.with_message_group(group);
            }
            record
        }
    }

    /// A record's measured size is its body, every attribute's name, data type and value, and its
    /// message group, and a record above 256 KiB is refused with that size. Batch requests take the
    /// accepted records in order, never more than ten, never more than 256 KiB together and never
    /// two of one message group, and a request ends only where the next record would break one of
    /// those limits.
    #[test]
    fn bolero_batch_requests_hold_their_records_within_every_sqs_limit() {
        /// What one batch request held, which decides whether the request after it began early.
        struct HeldRequest {
            records: usize,
            bytes: usize,
            groups: HashSet<String>,
        }

        // Up to 24 records fill at least two batch requests while every case fits its input bytes.
        let records = bolero::generator::produce_with::<Vec<GeneratedRecord>>().len(0_usize..=24);
        bolero::check!()
            .with_iterations(256)
            .with_max_len(1024)
            .with_generator(records)
            .for_each(|generated| {
                let mut accepted = Vec::new();
                for (index, generated) in generated.iter().enumerate() {
                    let size = generated.body().len() + generated.envelope_bytes();
                    match PreparedSqsRecord::new(generated.record(index)) {
                        Ok(prepared) => {
                            assert!(
                                size <= SQS_MAX_REQUEST_BYTES,
                                "record {index} of {size} bytes"
                            );
                            assert_eq!(prepared.encoded_bytes, size, "record {index}");
                            accepted.push(prepared);
                        }
                        Err(report) => {
                            assert_eq!(
                                report.current_context(),
                                &SqsRecordError::RequestSize {
                                    bytes: size,
                                    maximum: SQS_MAX_REQUEST_BYTES,
                                },
                                "record {index}"
                            );
                        }
                    }
                }
                let order = accepted
                    .iter()
                    .map(|record| record.record)
                    .collect::<Vec<_>>();
                let chunks = SqsSink::batch_chunks(accepted);
                let mut chunked = Vec::new();
                let mut previous: Option<HeldRequest> = None;
                for chunk in &chunks {
                    assert!(!chunk.is_empty() && chunk.len() <= SQS_MAX_BATCH_ENTRIES);
                    let mut held = HeldRequest {
                        records: chunk.len(),
                        bytes: 0,
                        groups: HashSet::new(),
                    };
                    for record in chunk {
                        held.bytes += record.encoded_bytes;
                        if let Some(group) = &record.group_id {
                            assert!(held.groups.insert(group.clone()), "two records of {group}");
                        }
                        chunked.push(record.record);
                    }
                    assert!(held.bytes <= SQS_MAX_REQUEST_BYTES);
                    if let Some(previous) = &previous {
                        let next = &chunk[0];
                        let full = previous.records == SQS_MAX_BATCH_ENTRIES;
                        let too_large = previous.bytes + next.encoded_bytes > SQS_MAX_REQUEST_BYTES;
                        let repeats = match &next.group_id {
                            Some(group) => previous.groups.contains(group),
                            None => false,
                        };
                        assert!(full || too_large || repeats, "a request ended early");
                    }
                    previous = Some(held);
                }
                assert_eq!(chunked, order);
            });
    }

    fn delivered_entry(id: &str) -> aws_sdk_sqs::types::SendMessageBatchResultEntry {
        aws_sdk_sqs::types::SendMessageBatchResultEntry::builder()
            .id(id)
            .message_id(format!("message-{id}"))
            .md5_of_message_body("unchecked")
            .build()
            .expect("the test entry names every required field")
    }

    fn failed_entry(
        id: &str,
        sender_fault: bool,
        code: &str,
    ) -> aws_sdk_sqs::types::BatchResultErrorEntry {
        aws_sdk_sqs::types::BatchResultErrorEntry::builder()
            .id(id)
            .sender_fault(sender_fault)
            .code(code)
            .build()
            .expect("the test entry names every required field")
    }

    /// Each entry of a batch request is one record, which with the emitter's `BATCH` clause is one
    /// batch payload, so the service's answer for an entry is the answer for the whole payload.
    #[test]
    fn a_partial_batch_response_answers_each_entry_and_leaves_the_rest_to_a_retry() {
        let records = (0..4)
            .map(|index| prepared_as(index, 16))
            .collect::<Vec<_>>();
        let response =
            aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput::builder()
                .successful(delivered_entry("m0"))
                .failed(failed_entry("m1", true, "InvalidMessageContents"))
                .failed(failed_entry("m2", false, "ServiceUnavailable"))
                .build()
                .expect("the test response names every required field");
        let mut outcome = PerRecordOutcome::empty();

        let infrastructure = SqsSink::apply_batch_response(&records, &response, &mut outcome)
            .expect("an entry the service failed on its side and an omitted entry are retried");
        let outcome = outcome.into_parts();

        assert_eq!(outcome.delivered, vec![SinkRecordId::new(0)]);
        assert_eq!(
            outcome
                .rejected
                .iter()
                .map(|rejected| rejected.id)
                .collect::<Vec<_>>(),
            vec![SinkRecordId::new(1)]
        );
        assert!(infrastructure.contains("SQS batch entry failed with ServiceUnavailable"));
        assert!(infrastructure.contains("SQS omitted a result for batch entry m3"));
    }

    #[test]
    fn only_definitive_per_entry_failures_are_record_rejections() {
        assert!(SqsSink::is_record_failure(
            true,
            "InvalidMessageContents",
            Some("message contains an invalid character")
        ));
        assert!(SqsSink::is_record_failure(
            true,
            "InvalidParameterValue",
            Some("MessageGroupId contains an invalid character")
        ));
        assert!(!SqsSink::is_record_failure(
            true,
            "InvalidParameterValue",
            Some("ContentBasedDeduplication is not enabled")
        ));
        assert!(!SqsSink::is_record_failure(
            false,
            "ThrottlingException",
            None
        ));
        assert!(!SqsSink::is_record_failure(
            true,
            "UnknownSenderFault",
            None
        ));
    }

    #[test]
    fn fifo_group_ids_accept_branch_json_and_reject_invalid_values() {
        assert!(SqsSink::validate_group_id(r#"{"tenant":"acme"}"#).is_ok());
        assert!(SqsSink::validate_group_id("").is_err());
        assert!(SqsSink::validate_group_id(&"x".repeat(129)).is_err());
        assert!(SqsSink::validate_group_id("contains space").is_err());
    }

    #[nervix_primitives::test]
    async fn client_timeout_bounds_each_request_while_sdk_retries_stay_disabled() {
        let fixture = Fixture::start().await;
        let client = SqsSink::client_from_config(&client_config("275"), fixture.dns())
            .await
            .expect("SQS client config should be valid");
        let timeout = client
            .config()
            .timeout_config()
            .expect("SQS client should have a request timeout");

        assert_eq!(
            timeout.operation_timeout(),
            Some(Duration::from_millis(275))
        );
        assert_eq!(
            timeout.operation_attempt_timeout(),
            Some(Duration::from_millis(275))
        );
        assert_eq!(
            client
                .config()
                .retry_config()
                .expect("SQS retry config should be explicit")
                .max_attempts(),
            1
        );
    }

    #[nervix_primitives::test]
    async fn client_rejects_an_invalid_request_timeout() {
        let fixture = Fixture::start().await;
        let error = SqsSink::client_from_config(&client_config("later"), fixture.dns())
            .await
            .expect_err("invalid SQS timeout should fail client initialization");

        assert!(
            format!("{error:?}").contains("invalid SQS timeout_ms 'later'"),
            "unexpected error: {error:?}"
        );
    }
}
