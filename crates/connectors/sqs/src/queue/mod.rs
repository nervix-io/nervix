//! SQS ingestor and emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The SQS client a configuration declares and the connections it makes, queue-URL
//!   lookup, long polling and message deletion on the ingestor side, protocol validation of each
//!   message body, typed attribute, FIFO group, deduplication id and delay, request batching, and
//!   per-entry response classification.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, the node
//!   resolver, the kernel crate's XML character check, and the AWS SQS SDK with its Smithy HTTP
//!   client.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation. A FIFO message group, a deduplication id and every attribute value
//!   arrive already evaluated for their record, so the expressions behind them stay with the host.

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

mod connection;
mod ingestor;

use std::fmt::Debug;

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
use error_stack::{Report, ResultExt as _};
pub use ingestor::{
    SqsMessageAttributes, SqsQueueIngestor, SqsQueueIngestorError, SqsQueueIngestorMessage,
    SqsQueueIngestorPlan, SqsQueueIngestorPosition,
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    ConfiguredDeadline, EmitterFinish, EmitterFinishResult, EmitterHost, EmitterLifecycle,
    EmitterOpenError, EmitterRecord, EmitterRecordId, EmitterStartResult, EmitterWriteError,
    PerRecordOutcome, PropertyValue, RecordEmitter, RejectedEmitterMember, TypedRecordHeader,
    client_config_value, client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_dns::DnsResolver;
use nervix_models::{
    ClientConfigEntry, PropertyType, SQS_ATTRIBUTE_VALUES, SQS_DELAY_MAXIMUM,
    SQS_MESSAGE_SIZE_MAXIMUM, SqsSend, Timestamp,
};
use nervix_primitives::time::Instant;
use nervix_simd_kernels::{XmlChars, XmlCharsError};
use thiserror::Error;

const SQS: &str = "sqs";
const SQS_MAX_BATCH_ENTRIES: usize = 10;
/// The longest message group or deduplication id SQS accepts, in characters.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "SQS limits a FIFO message group and a deduplication id to 128 characters"
    )
)]
const SQS_FIFO_IDENTIFIER_MAX_CHARACTERS: usize = 128;

/// The most bytes one SQS message, and one batch request, carries, as the vocabulary declares it.
fn max_request_bytes() -> usize {
    usize::try_from(SQS_MESSAGE_SIZE_MAXIMUM.bytes().get())
        .assured("1 MiB fits the address space of every target Nervix builds for")
}

/// What one SQS emitter sends to: its client entries, queue name and request shape.
pub struct SqsQueueEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub queue: nervix_models::SqsQueueName,
    pub send: SqsSend,
    /// The node resolver every connection of the client resolves the endpoint host through.
    pub dns: DnsResolver,
}

pub struct SqsQueueEmitter {
    client: SqsClient,
    queue_url: String,
    send: SqsSend,
}

/// Why one record is not a message this emitter can send.
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
    #[error("SQS message attribute '{name}' has a {ty} value, which SQS QUEUE does not declare")]
    UndeclaredAttributeType { name: String, ty: PropertyType },
    #[error("SQS deduplication id must contain 1 to 128 characters")]
    DeduplicationIdLength,
    #[error(
        "SQS deduplication id contains a character other than an ASCII letter, digit or \
         punctuation mark"
    )]
    DeduplicationIdCharacter,
    #[error("SQS DELAY {delay:?} is not a whole number of seconds from 0 to 900")]
    Delay { delay: std::time::Duration },
    #[error(
        "SQS message is {bytes} bytes counting its attributes and message group; SQS accepts at \
         most {maximum} bytes"
    )]
    RequestSize { bytes: usize, maximum: usize },
}

type SqsRecordResult<T> = Result<T, Report<SqsRecordError>>;

#[derive(Debug)]
struct PreparedSqsRecord {
    record: EmitterRecordId,
    occurred_at: Timestamp,
    body: String,
    attributes: HashMap<String, MessageAttributeValue>,
    group_id: Option<String>,
    deduplication_id: Option<String>,
    delay_seconds: Option<i32>,
    encoded_bytes: usize,
}

impl PreparedSqsRecord {
    fn new(record: EmitterRecord) -> SqsRecordResult<Self> {
        const ENCODED_IN_MEMORY: &str =
            "every term counts bytes of a record this node already holds in memory";

        // SQS QUEUE declares typed attribute values, so the host hands a record's writes over in
        // `typed_headers` and leaves its text `headers` empty. Of the envelope it declares only the
        // delay and the deduplication id.
        let EmitterRecord {
            id: record,
            key: _,
            payload,
            headers: _,
            typed_headers,
            message_group: group_id,
            envelope,
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
        // A later write of a name replaces the earlier value, so the message carries one attribute
        // per name and SQS counts those, after the writes have collapsed.
        let mut attributes = HashMap::with_capacity(typed_headers.len());
        for header in typed_headers {
            let (name, attribute) = SqsQueueEmitter::attribute(header)?;
            attributes.insert(name, attribute);
        }
        if attributes.len() > 10 {
            return Err(Report::new(SqsRecordError::AttributeCount {
                count: attributes.len(),
                maximum: 10,
            }));
        }
        if let Some(group_id) = group_id.as_deref() {
            SqsQueueEmitter::validate_group_id(group_id)?;
        }
        let deduplication_id = envelope.deduplication_id;
        if let Some(deduplication_id) = deduplication_id.as_deref() {
            SqsQueueEmitter::validate_deduplication_id(deduplication_id)?;
        }
        let delay_seconds = match envelope.delay {
            Some(delay) => Some(SqsQueueEmitter::delay_seconds(delay)?),
            None => None,
        };
        let body_bytes = body.len();
        let mut attribute_bytes = 0_usize;
        for (name, attribute) in &attributes {
            // Read the value first to retain the original contribution-evaluation order.
            let value_bytes = SqsQueueEmitter::attribute_value_bytes(attribute);
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
        let maximum = max_request_bytes();
        if encoded_bytes > maximum {
            return Err(Report::new(SqsRecordError::RequestSize {
                bytes: encoded_bytes,
                maximum,
            }));
        }
        Ok(Self {
            record,
            occurred_at,
            body,
            attributes,
            group_id,
            deduplication_id,
            delay_seconds,
            encoded_bytes,
        })
    }

    fn rejected(&self, reason: String) -> RejectedEmitterMember<EmitterRecordId> {
        RejectedEmitterMember::external(self.record, self.occurred_at, reason)
    }
}

impl SqsQueueEmitter {
    pub async fn new(
        config: SqsQueueEmitterConfig,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let SqsQueueEmitterConfig {
            config,
            queue,
            send,
            dns,
        } = config;
        let client = Self::client_from_config(&config, dns).await?;
        let queue_url = Self::queue_url(&client, queue.as_str()).await?;
        Ok(Self {
            client,
            queue_url,
            send,
        })
    }

    async fn client_from_config(
        config: &[ClientConfigEntry],
        dns: DnsResolver,
    ) -> EmitterStartResult<SqsClient> {
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

        let request_timeout = ConfiguredDeadline::timeout(config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: SQS })?
            .map(ConfiguredDeadline::bound);

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
                let ca_pem = {
                    let mapping_input = read_tls_file(ca_file, "TLS CA certificate");
                    match mapping_input {
                        Ok(mapped_value) => Ok(mapped_value),
                        Err(error) => Err({
                            let message = error.current_context().to_string();
                            error
                                .change_context(EmitterOpenError::InvalidConfiguration {
                                    connector: SQS,
                                })
                                .attach_printable(message)
                        }),
                    }
                }?;
                SqsTrust::ca(ca_pem).map_err(Self::start_error)?
            }
            None => SqsTrust::Platform,
        };
        let sdk_config = loader.http_client(trust.http_client(dns)).load().await;
        Ok(SqsClient::new(&sdk_config))
    }

    async fn queue_url(client: &SqsClient, queue: &str) -> EmitterStartResult<String> {
        let response = match client.get_queue_url().queue_name(queue).send().await {
            Ok(response) => response,
            Err(error) => return Err(Self::queue_lookup_error(queue, &error)),
        };
        Self::require_queue_url(queue, response.queue_url().map(ToOwned::to_owned))
    }

    fn queue_lookup_error<R: Debug + 'static>(
        queue: &str,
        error: &SdkError<GetQueueUrlError, R>,
    ) -> Report<EmitterOpenError> {
        let failure = FailedRequest::new(error);
        let missing = error
            .as_service_error()
            .is_some_and(GetQueueUrlError::is_queue_does_not_exist);
        if missing {
            return Report::new(EmitterOpenError::MissingExternalEntity {
                connector: SQS,
                kind: "SQS queue",
                name: queue.to_string(),
            })
            .attach_printable(failure.description());
        }
        failure.report(
            EmitterOpenError::Initialize { connector: SQS },
            "SQS GetQueueUrl failed",
        )
    }

    fn require_queue_url(queue: &str, queue_url: Option<String>) -> EmitterStartResult<String> {
        match queue_url {
            Some(queue_url) => Ok(queue_url),
            None => Err(Self::start_error(format!("SQS queue '{queue}' has no URL"))),
        }
    }

    async fn publish_single(
        &self,
        records: Vec<PreparedSqsRecord>,
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
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
            if let Some(deduplication_id) = record.deduplication_id.as_deref() {
                request = request.message_deduplication_id(deduplication_id);
            }
            if let Some(delay_seconds) = record.delay_seconds {
                request = request.delay_seconds(delay_seconds);
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
                        EmitterWriteError::Publish { connector: SQS },
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
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
    ) {
        for records in Self::batch_chunks(records) {
            nervix_primitives::task::consume_budget().await;
            let mut request = self.client.send_message_batch().queue_url(&self.queue_url);
            for (index, record) in records.iter().enumerate() {
                let entry = match Self::batch_entry(index, record) {
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
                        EmitterWriteError::Publish { connector: SQS },
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
                    > max_request_bytes().checked_sub(current_bytes).verified(
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
        outcome: &mut PerRecordOutcome<EmitterRecordId>,
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

    /// The entry of a batch request that sends `record` as the request's entry `index`, with its
    /// attributes, FIFO group, deduplication id and delay.
    fn batch_entry(
        index: usize,
        record: &PreparedSqsRecord,
    ) -> Result<SendMessageBatchRequestEntry, aws_sdk_sqs::error::BuildError> {
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
        if let Some(deduplication_id) = record.deduplication_id.as_deref() {
            entry = entry.message_deduplication_id(deduplication_id);
        }
        if let Some(delay_seconds) = record.delay_seconds {
            entry = entry.delay_seconds(delay_seconds);
        }
        entry.build()
    }

    /// The SQS message attribute one typed header write becomes: a `String`, `Number` or `Binary`
    /// value under the data type the vocabulary declares for the write's exact type, an integer
    /// carried as its decimal digits.
    fn attribute(header: TypedRecordHeader) -> SqsRecordResult<(String, MessageAttributeValue)> {
        let TypedRecordHeader { name, value } = header;
        Self::validate_attribute_name(&name)?;
        let ty = value.ty();
        let Some(data_type) = Self::declared_data_type(ty) else {
            return Err(Report::new(SqsRecordError::UndeclaredAttributeType {
                name,
                ty,
            }));
        };
        let builder = MessageAttributeValue::builder().data_type(data_type);
        let builder = match value {
            PropertyValue::String(text) => {
                Self::validate_attribute_text(&name, &text)?;
                builder.string_value(text)
            }
            PropertyValue::U8(number) => builder.string_value(number.to_string()),
            PropertyValue::I8(number) => builder.string_value(number.to_string()),
            PropertyValue::U16(number) => builder.string_value(number.to_string()),
            PropertyValue::I16(number) => builder.string_value(number.to_string()),
            PropertyValue::U32(number) => builder.string_value(number.to_string()),
            PropertyValue::I32(number) => builder.string_value(number.to_string()),
            PropertyValue::U64(number) => builder.string_value(number.to_string()),
            PropertyValue::I64(number) => builder.string_value(number.to_string()),
            PropertyValue::Bytes(bytes) => {
                if bytes.is_empty() {
                    return Err(Report::new(SqsRecordError::EmptyAttributeValue { name }));
                }
                builder.binary_value(aws_sdk_sqs::primitives::Blob::new(bytes))
            }
            PropertyValue::Bool(_) | PropertyValue::Datetime(_) => {
                return Err(Report::new(SqsRecordError::UndeclaredAttributeType {
                    name,
                    ty,
                }));
            }
        };
        let attribute = builder.build().map_err(|error| {
            Report::new(SqsRecordError::BuildAttribute { name: name.clone() })
                .attach_printable(error)
        })?;
        Ok((name, attribute))
    }

    /// The SQS data type the vocabulary declares for an attribute value of exact type `ty`.
    ///
    /// The declaration lists the ten types an attribute carries, so scanning it is bounded by the
    /// declaration rather than by any record.
    fn declared_data_type(ty: PropertyType) -> Option<&'static str> {
        for declared in SQS_ATTRIBUTE_VALUES {
            if declared.ty == ty {
                return Some(declared.carried_as);
            }
        }
        None
    }

    /// The bytes SQS counts for an attribute's value: its text, or its binary value.
    fn attribute_value_bytes(attribute: &MessageAttributeValue) -> usize {
        if let Some(value) = attribute.string_value() {
            return value.len();
        }
        match attribute.binary_value() {
            Some(value) => value.as_ref().len(),
            None => 0,
        }
    }

    /// The `DelaySeconds` a record's `DELAY` becomes: whole seconds, at most SQS's maximum.
    fn delay_seconds(delay: std::time::Duration) -> SqsRecordResult<i32> {
        if delay.subsec_nanos() != 0 || delay > SQS_DELAY_MAXIMUM {
            return Err(Report::new(SqsRecordError::Delay { delay }));
        }
        let seconds = i32::try_from(delay.as_secs())
            .verified("the check above holds the delay to SQS's 900-second maximum");
        Ok(seconds)
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

    fn validate_attribute_name(name: &str) -> SqsRecordResult<()> {
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
        Ok(())
    }

    /// Checks a `String` attribute value: SQS refuses an empty one and the characters XML forbids.
    fn validate_attribute_text(name: &str, value: &str) -> SqsRecordResult<()> {
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
        if group_id.is_empty() || group_id.chars().count() > SQS_FIFO_IDENTIFIER_MAX_CHARACTERS {
            return Err(Report::new(SqsRecordError::MessageGroupLength));
        }
        if !group_id.chars().all(Self::is_fifo_identifier_character) {
            return Err(Report::new(SqsRecordError::MessageGroupCharacter));
        }
        Ok(())
    }

    fn validate_deduplication_id(deduplication_id: &str) -> SqsRecordResult<()> {
        if deduplication_id.is_empty()
            || deduplication_id.chars().count() > SQS_FIFO_IDENTIFIER_MAX_CHARACTERS
        {
            return Err(Report::new(SqsRecordError::DeduplicationIdLength));
        }
        if !deduplication_id
            .chars()
            .all(Self::is_fifo_identifier_character)
        {
            return Err(Report::new(SqsRecordError::DeduplicationIdCharacter));
        }
        Ok(())
    }

    /// Whether SQS accepts `character` in a FIFO message group or deduplication id: an ASCII
    /// letter, digit or punctuation mark.
    fn is_fifo_identifier_character(character: char) -> bool {
        character.is_ascii_alphanumeric() || character.is_ascii_punctuation()
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> EmitterStartResult<String> {
        {
            let mapping_input = client_config_value(config, key, "SQS");
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let message = error.current_context().to_string();
                    error
                        .change_context(EmitterOpenError::InvalidConfiguration { connector: SQS })
                        .attach_printable(message)
                }),
            }
        }
    }

    fn start_error(error: impl std::fmt::Display) -> Report<EmitterOpenError> {
        Report::new(EmitterOpenError::Initialize { connector: SQS })
            .attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external SQS SDK failure; no internal \
                                   runtime ownership is inferred")
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<EmitterWriteError> {
        Report::new(EmitterWriteError::Publish { connector: SQS })
            .attach_printable(error.to_string())
    }
}

impl SqsQueueEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Sqs(nervix_models::SqsEmitterEntity::Queue);
}

#[async_trait]
impl EmitterLifecycle for SqsQueueEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each send is answered before the publish returns, so the client holds nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RecordEmitter for SqsQueueEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let mut prepared = Vec::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            match PreparedSqsRecord::new(record) {
                Ok(record) => prepared.push(record),
                Err(error) => outcome.reject(RejectedEmitterMember::external(
                    record_id,
                    occurred_at,
                    error.to_string(),
                )),
            }
        }
        match self.send {
            SqsSend::Single => self.publish_single(prepared, &mut outcome).await,
            SqsSend::Batch => self.publish_batches(prepared, &mut outcome).await,
        }
        outcome
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod connection_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use aws_sdk_sqs::{error::ConnectorError, types::error::QueueDoesNotExist};
    use nervix_connector::RecordEnvelope;

    use super::*;
    use crate::queue::connection_tests::Fixture;

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

    fn record(index: usize, payload: Vec<u8>) -> EmitterRecord {
        EmitterRecord::new(
            EmitterRecordId::new(index),
            None,
            payload,
            Vec::new(),
            Timestamp::from_unix_nanos(0),
        )
    }

    /// Header writes of `STRING` values, as the host hands an SQS emitter its typed writes.
    fn string_headers(pairs: Vec<(String, String)>) -> Vec<TypedRecordHeader> {
        let mut headers = Vec::with_capacity(pairs.len());
        for (name, value) in pairs {
            headers.push(TypedRecordHeader {
                name,
                value: PropertyValue::String(value),
            });
        }
        headers
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
        let error = PreparedSqsRecord::new(record(0, vec![b'x'; max_request_bytes() + 1]))
            .expect_err("oversized SQS record should be rejected");

        assert_eq!(
            *error.current_context(),
            SqsRecordError::RequestSize {
                bytes: max_request_bytes() + 1,
                maximum: max_request_bytes(),
            }
        );
    }

    #[test]
    fn rejects_invalid_sqs_body_and_attribute_shapes_with_typed_errors() {
        let build = |payload: Vec<u8>, headers: Vec<(String, String)>| {
            let mut record = record(0, payload);
            record.typed_headers = string_headers(headers);
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

    /// SQS keeps one attribute per name, the last one written, so ten names written in eleven
    /// writes make a message of ten attributes, which SQS accepts, and the count refuses only
    /// eleven distinct names.
    #[test]
    fn attributes_are_counted_after_a_repeated_name_keeps_its_last_value() {
        let mut headers = Vec::new();
        for index in 0..10 {
            headers.push((format!("attribute_{index}"), format!("value_{index}")));
        }
        headers.push(("attribute_3".to_string(), "replaced".to_string()));
        let mut repeated = record(0, vec![b'x']);
        repeated.typed_headers = string_headers(headers.clone());
        let prepared = PreparedSqsRecord::new(repeated)
            .expect("ten distinct names are within the attribute limit");
        assert_eq!(prepared.attributes.len(), 10);
        let replaced = prepared
            .attributes
            .get("attribute_3")
            .and_then(MessageAttributeValue::string_value);
        assert_eq!(replaced, Some("replaced"));

        headers.push(("attribute_10".to_string(), "value_10".to_string()));
        let mut eleven = record(0, vec![b'x']);
        eleven.typed_headers = string_headers(headers);
        let refused =
            PreparedSqsRecord::new(eleven).expect_err("eleven distinct names exceed the limit");
        assert_eq!(
            refused.current_context(),
            &SqsRecordError::AttributeCount {
                count: 11,
                maximum: 10,
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
        noncharacter_value.typed_headers =
            string_headers(vec![("tenant".to_string(), "\u{FFFF}".to_string())]);
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
        let missing = SqsQueueEmitter::queue_lookup_error("missing-queue", &missing);
        assert!(matches!(
            missing.current_context(),
            EmitterOpenError::MissingExternalEntity {
                connector: SQS,
                kind: "SQS queue",
                name,
            } if name == "missing-queue"
        ));

        let refused = SdkError::<GetQueueUrlError, ()>::dispatch_failure(ConnectorError::io(
            "connection refused".into(),
        ));
        let connection = SqsQueueEmitter::queue_lookup_error("orders", &refused);
        assert_eq!(
            connection.current_context(),
            &EmitterOpenError::Initialize { connector: SQS }
        );
        let message = connection
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            message.map(String::as_str),
            Some("SQS GetQueueUrl failed: dispatch failure: io error: connection refused")
        );

        assert_eq!(
            SqsQueueEmitter::require_queue_url("orders", Some("queue-url".to_string()))
                .expect("a returned queue URL should be accepted"),
            "queue-url"
        );
        let no_url = SqsQueueEmitter::require_queue_url("orders", None)
            .expect_err("a successful response without a queue URL must fail");
        assert_eq!(
            no_url.current_context(),
            &EmitterOpenError::Initialize { connector: SQS }
        );
    }

    #[test]
    fn request_size_counts_attribute_names_types_values_and_the_fifo_group() {
        const ATTRIBUTE_NAME: &str = "tenant";
        const ATTRIBUTE_VALUE: &str = "acme";
        const GROUP_ID: &str = "orders";

        // Every attribute the emitter builds declares the "String" data type, so those bytes count
        // against the protocol limit alongside the attribute name and value.
        let attribute_bytes = ATTRIBUTE_NAME.len() + "String".len() + ATTRIBUTE_VALUE.len();
        let body_bytes = max_request_bytes() - attribute_bytes - GROUP_ID.len();
        let headers = vec![(ATTRIBUTE_NAME.to_string(), ATTRIBUTE_VALUE.to_string())];
        let fitting = |payload_bytes: usize| {
            let mut record =
                record(0, vec![b'x'; payload_bytes]).with_message_group(GROUP_ID.to_string());
            record.typed_headers = string_headers(headers.clone());
            PreparedSqsRecord::new(record)
        };

        let record = fitting(body_bytes)
            .expect("a record that exactly fills the protocol limit should be accepted");

        assert_eq!(record.encoded_bytes, max_request_bytes());

        let error = fitting(body_bytes + 1)
            .expect_err("one byte past the protocol limit should be rejected");

        assert_eq!(
            *error.current_context(),
            SqsRecordError::RequestSize {
                bytes: max_request_bytes() + 1,
                maximum: max_request_bytes(),
            }
        );
    }

    /// A record carrying `headers` as its typed writes and `envelope` beside its payload.
    fn typed_record(headers: Vec<TypedRecordHeader>, envelope: RecordEnvelope) -> EmitterRecord {
        let mut record = record(0, vec![b'x']).with_envelope(envelope);
        record.typed_headers = headers;
        record
    }

    fn header(name: &str, value: PropertyValue) -> TypedRecordHeader {
        TypedRecordHeader {
            name: name.to_string(),
            value,
        }
    }

    #[test]
    fn typed_writes_become_attributes_of_the_data_type_their_exact_type_declares() {
        let prepared = PreparedSqsRecord::new(typed_record(
            vec![
                header("tenant", PropertyValue::String("acme".to_string())),
                header("attempt", PropertyValue::U8(3)),
                header("offset", PropertyValue::I64(-42)),
                header("total", PropertyValue::U64(u64::MAX)),
                header("digest", PropertyValue::Bytes(vec![0x00, 0xFF, 0x10])),
            ],
            RecordEnvelope::default(),
        ))
        .expect("strings, integers and bytes are the attribute types SQS QUEUE declares");

        let attribute = |name: &str| {
            let Some(attribute) = prepared.attributes.get(name) else {
                panic!("attribute {name} is written");
            };
            attribute.clone()
        };
        assert_eq!(attribute("tenant").data_type(), "String");
        assert_eq!(attribute("tenant").string_value(), Some("acme"));
        assert_eq!(attribute("attempt").data_type(), "Number");
        assert_eq!(attribute("attempt").string_value(), Some("3"));
        assert_eq!(attribute("offset").string_value(), Some("-42"));
        assert_eq!(
            attribute("total").string_value(),
            Some("18446744073709551615")
        );
        assert_eq!(attribute("digest").data_type(), "Binary");
        assert_eq!(
            attribute("digest")
                .binary_value()
                .map(|value| value.as_ref().to_vec()),
            Some(vec![0x00, 0xFF, 0x10])
        );
        // Each attribute counts its name, its data type and its value: the digits of a number
        // and the bytes of a binary value.
        let attributes = ("tenant".len() + "String".len() + "acme".len())
            + ("attempt".len() + "Number".len() + "3".len())
            + ("offset".len() + "Number".len() + "-42".len())
            + ("total".len() + "Number".len() + "18446744073709551615".len())
            + ("digest".len() + "Binary".len() + 3);
        assert_eq!(prepared.encoded_bytes, 1 + attributes);
    }

    #[test]
    fn every_integer_width_is_a_number_attribute_of_its_decimal_digits() {
        let widths = [
            (PropertyValue::I8(i8::MIN), "-128"),
            (PropertyValue::U16(u16::MAX), "65535"),
            (PropertyValue::I16(i16::MIN), "-32768"),
            (PropertyValue::U32(u32::MAX), "4294967295"),
            (PropertyValue::I32(i32::MIN), "-2147483648"),
        ];
        for (value, digits) in widths {
            let prepared = PreparedSqsRecord::new(typed_record(
                vec![header("width", value.clone())],
                RecordEnvelope::default(),
            ))
            .expect("every integer width is an attribute type SQS QUEUE declares");
            let Some(attribute) = prepared.attributes.get("width") else {
                panic!("the attribute is written for {value:?}");
            };
            assert_eq!(attribute.data_type(), "Number", "{value:?}");
            assert_eq!(attribute.string_value(), Some(digits), "{value:?}");
        }
    }

    #[test]
    fn refuses_an_empty_binary_value_and_a_type_sqs_queue_does_not_declare() {
        let empty = PreparedSqsRecord::new(typed_record(
            vec![header("digest", PropertyValue::Bytes(Vec::new()))],
            RecordEnvelope::default(),
        ))
        .expect_err("SQS refuses an empty attribute value");
        assert_eq!(
            empty.current_context(),
            &SqsRecordError::EmptyAttributeValue {
                name: "digest".to_string()
            }
        );

        let flag = PreparedSqsRecord::new(typed_record(
            vec![header("flag", PropertyValue::Bool(true))],
            RecordEnvelope::default(),
        ))
        .expect_err("no SQS data type carries a BOOL exactly");
        assert_eq!(
            flag.current_context(),
            &SqsRecordError::UndeclaredAttributeType {
                name: "flag".to_string(),
                ty: PropertyType::Bool,
            }
        );
    }

    #[test]
    fn a_batch_entry_carries_the_records_deduplication_id_delay_and_attributes() {
        let deduplicated = PreparedSqsRecord::new(
            typed_record(
                vec![header("attempt", PropertyValue::I32(2))],
                RecordEnvelope {
                    deduplication_id: Some("order-1".to_string()),
                    ..RecordEnvelope::default()
                },
            )
            .with_message_group("orders".to_string()),
        )
        .expect("a deduplication id of ASCII letters, digits and punctuation is valid");
        let entry = SqsQueueEmitter::batch_entry(3, &deduplicated)
            .expect("a prepared record builds its batch entry");
        assert_eq!(entry.id(), "m3");
        assert_eq!(entry.message_group_id(), Some("orders"));
        assert_eq!(entry.message_deduplication_id(), Some("order-1"));
        assert_eq!(entry.delay_seconds(), None);
        let attributes = entry
            .message_attributes()
            .expect("the record's attribute is sent");
        assert_eq!(attributes["attempt"].data_type(), "Number");

        let delayed = PreparedSqsRecord::new(typed_record(
            Vec::new(),
            RecordEnvelope {
                delay: Some(Duration::from_secs(900)),
                ..RecordEnvelope::default()
            },
        ))
        .expect("SQS delays a message by up to 900 seconds");
        let entry = SqsQueueEmitter::batch_entry(0, &delayed)
            .expect("a prepared record builds its batch entry");
        assert_eq!(entry.delay_seconds(), Some(900));
        assert_eq!(entry.message_deduplication_id(), None);
        assert_eq!(entry.message_attributes(), None);
    }

    #[test]
    fn refuses_a_deduplication_id_or_a_delay_sqs_does_not_accept() {
        let prepare =
            |envelope: RecordEnvelope| PreparedSqsRecord::new(typed_record(Vec::new(), envelope));
        let deduplication = |id: &str| RecordEnvelope {
            deduplication_id: Some(id.to_string()),
            ..RecordEnvelope::default()
        };
        let delay = |delay: Duration| RecordEnvelope {
            delay: Some(delay),
            ..RecordEnvelope::default()
        };

        prepare(deduplication(&"d".repeat(128)))
            .expect("a deduplication id of 128 characters is valid");
        for (id, expected) in [
            (String::new(), SqsRecordError::DeduplicationIdLength),
            ("d".repeat(129), SqsRecordError::DeduplicationIdLength),
            (
                "order 1".to_string(),
                SqsRecordError::DeduplicationIdCharacter,
            ),
            (
                "заказ".to_string(),
                SqsRecordError::DeduplicationIdCharacter,
            ),
        ] {
            let error = prepare(deduplication(&id)).expect_err("SQS refuses the deduplication id");
            assert_eq!(error.current_context(), &expected, "{id:?}");
        }

        prepare(delay(Duration::ZERO)).expect("a zero delay is valid");
        for refused in [Duration::from_millis(1500), Duration::from_secs(901)] {
            let error = prepare(delay(refused)).expect_err("SQS refuses the delay");
            assert_eq!(
                error.current_context(),
                &SqsRecordError::Delay { delay: refused }
            );
        }
    }

    #[test]
    fn batch_chunks_respect_entry_and_byte_limits_without_reordering() {
        let mut records = (0..11)
            .map(|index| prepared_as(index, 1))
            .collect::<Vec<_>>();
        records.push(prepared(max_request_bytes()));
        records.push(prepared(2));

        let chunks = SqsQueueEmitter::batch_chunks(records);
        let records = chunks
            .iter()
            .flatten()
            .map(|record| record.record)
            .collect::<Vec<_>>();

        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            [10, 1, 1, 1]
        );
        assert_eq!(records[0], EmitterRecordId::new(0));
        assert_eq!(records[10], EmitterRecordId::new(10));
    }

    #[test]
    fn batch_chunks_never_send_two_records_from_one_fifo_group_together() {
        let records = vec![
            prepared_in_group(0, "acme"),
            prepared_in_group(1, "beta"),
            prepared_in_group(2, "acme"),
            prepared_in_group(3, "beta"),
        ];

        let chunks = SqsQueueEmitter::batch_chunks(records);
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

    /// The first selector value that draws a near-limit body: one value in eight.
    const NEAR_LIMIT_FROM: u8 = 224;

    /// A generated body length: small, or, for one body in eight, within a few bytes either side
    /// of what fills the request limit beside the record's attributes and message group. A
    /// near-limit body holds about a mebibyte, so drawing it rarely keeps one case within a few
    /// mebibytes while most cases still carry one.
    #[derive(Debug, bolero::TypeGenerator)]
    struct BodyLength {
        pieces: u16,
        piece: u8,
        near_limit: u8,
        offset: i16,
    }

    /// A generated attribute value of each type SQS QUEUE declares: text, an integer SQS carries
    /// as its decimal digits, and bytes.
    #[derive(Debug, bolero::TypeGenerator)]
    enum GeneratedValue {
        Text { bytes: u8 },
        Number(i64),
        Binary { bytes: u8 },
    }

    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedAttribute {
        name: u8,
        value: GeneratedValue,
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
        fn headers(&self) -> Vec<TypedRecordHeader> {
            let mut values = BTreeMap::new();
            for attribute in &self.attributes {
                let name = ATTRIBUTE_NAMES[usize::from(attribute.name) % ATTRIBUTE_NAMES.len()];
                let value = match attribute.value {
                    GeneratedValue::Text { bytes } => {
                        PropertyValue::String("v".repeat(usize::from(bytes % 64) + 1))
                    }
                    GeneratedValue::Number(number) => PropertyValue::I64(number),
                    GeneratedValue::Binary { bytes } => {
                        PropertyValue::Bytes(vec![0xAB; usize::from(bytes % 64) + 1])
                    }
                };
                values.insert(name, value);
            }
            let mut headers = Vec::with_capacity(values.len());
            for (name, value) in values {
                headers.push(TypedRecordHeader {
                    name: name.to_string(),
                    value,
                });
            }
            headers
        }

        fn group(&self) -> Option<String> {
            self.group
                .map(|group| MESSAGE_GROUPS[usize::from(group) % MESSAGE_GROUPS.len()].to_string())
        }

        /// The bytes SQS counts for everything but the body: each attribute's name, its data type
        /// and its value, and the message group.
        fn envelope_bytes(&self) -> usize {
            let mut bytes = 0;
            for header in self.headers() {
                let (data_type, value) = match &header.value {
                    PropertyValue::String(text) => ("String", text.len()),
                    PropertyValue::I64(number) => ("Number", number.to_string().len()),
                    PropertyValue::Bytes(bytes) => ("Binary", bytes.len()),
                    other => panic!("the generator writes no {other:?}"),
                };
                bytes += header.name.len() + data_type.len() + value;
            }
            if let Some(group) = self.group() {
                bytes += group.len();
            }
            bytes
        }

        /// The body as the piece it repeats and how many times it repeats it, so the property
        /// measures a near-limit body without building it twice.
        fn body_pieces(&self, envelope_bytes: usize) -> (&'static str, usize) {
            let BodyLength {
                pieces,
                piece,
                near_limit,
                offset,
            } = self.body;
            // The top eighth of the selector's values draws a near-limit body, so an input the
            // generator has run out of, which reads as zero, draws small ones.
            if near_limit < NEAR_LIMIT_FROM {
                let piece = BODY_PIECES[usize::from(piece) % BODY_PIECES.len()];
                return (piece, usize::from(pieces % 2048));
            }
            let target = i64::try_from(max_request_bytes() - envelope_bytes)
                .assured("the request limit fits i64");
            let length = (target + i64::from(offset % 64)).max(0);
            (
                "x",
                usize::try_from(length).assured("the length is not negative"),
            )
        }

        fn record(&self, index: usize, body: String) -> EmitterRecord {
            let mut record = record(index, body.into_bytes());
            record.typed_headers = self.headers();
            if let Some(group) = self.group() {
                record = record.with_message_group(group);
            }
            record
        }
    }

    /// A record's measured size is its body, every attribute's name, data type and value, and its
    /// message group, and a record above 1 MiB is refused with that size. Batch requests take the
    /// accepted records in order, never more than ten, never more than 1 MiB together and never two
    /// of one message group, and a request ends only where the next record would break one of
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
                    let envelope_bytes = generated.envelope_bytes();
                    let (piece, pieces) = generated.body_pieces(envelope_bytes);
                    let size = piece.len() * pieces + envelope_bytes;
                    let body = piece.repeat(pieces);
                    match PreparedSqsRecord::new(generated.record(index, body)) {
                        Ok(prepared) => {
                            assert!(
                                size <= max_request_bytes(),
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
                                    maximum: max_request_bytes(),
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
                let chunks = SqsQueueEmitter::batch_chunks(accepted);
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
                    assert!(held.bytes <= max_request_bytes());
                    if let Some(previous) = &previous {
                        let next = &chunk[0];
                        let full = previous.records == SQS_MAX_BATCH_ENTRIES;
                        let too_large = previous.bytes + next.encoded_bytes > max_request_bytes();
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

        let infrastructure =
            SqsQueueEmitter::apply_batch_response(&records, &response, &mut outcome)
                .expect("an entry the service failed on its side and an omitted entry are retried");
        let outcome = outcome.into_parts();

        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        assert_eq!(
            outcome
                .rejected
                .iter()
                .map(|rejected| rejected.id)
                .collect::<Vec<_>>(),
            vec![EmitterRecordId::new(1)]
        );
        assert!(infrastructure.contains("SQS batch entry failed with ServiceUnavailable"));
        assert!(infrastructure.contains("SQS omitted a result for batch entry m3"));
    }

    #[test]
    fn only_definitive_per_entry_failures_are_record_rejections() {
        assert!(SqsQueueEmitter::is_record_failure(
            true,
            "InvalidMessageContents",
            Some("message contains an invalid character")
        ));
        assert!(SqsQueueEmitter::is_record_failure(
            true,
            "InvalidParameterValue",
            Some("MessageGroupId contains an invalid character")
        ));
        assert!(!SqsQueueEmitter::is_record_failure(
            true,
            "InvalidParameterValue",
            Some("ContentBasedDeduplication is not enabled")
        ));
        assert!(!SqsQueueEmitter::is_record_failure(
            false,
            "ThrottlingException",
            None
        ));
        assert!(!SqsQueueEmitter::is_record_failure(
            true,
            "UnknownSenderFault",
            None
        ));
    }

    #[test]
    fn fifo_group_ids_accept_branch_json_and_reject_invalid_values() {
        assert!(SqsQueueEmitter::validate_group_id(r#"{"tenant":"acme"}"#).is_ok());
        assert!(SqsQueueEmitter::validate_group_id("").is_err());
        assert!(SqsQueueEmitter::validate_group_id(&"x".repeat(129)).is_err());
        assert!(SqsQueueEmitter::validate_group_id("contains space").is_err());
    }

    #[nervix_primitives::test]
    async fn client_timeout_bounds_each_request_while_sdk_retries_stay_disabled() {
        let fixture = Fixture::start().await;
        let client = SqsQueueEmitter::client_from_config(&client_config("275"), fixture.dns())
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
        for value in ["later", "0"] {
            let error = SqsQueueEmitter::client_from_config(&client_config(value), fixture.dns())
                .await
                .expect_err("invalid SQS timeout should fail client initialization");

            assert!(
                error.frames().any(|frame| matches!(
                    frame.downcast_ref::<nervix_connector::ClientConfigError>(),
                    Some(nervix_connector::ClientConfigError::InvalidDeadline {
                        key: "timeout_ms"
                    })
                )),
                "{value}: unexpected error: {error:?}"
            );
        }
    }
}
