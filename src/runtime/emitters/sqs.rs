use std::collections::{HashMap as StdHashMap, HashSet as StdHashSet};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::{
    Client as SqsClient,
    types::{MessageAttributeValue, SendMessageBatchRequestEntry},
};

use super::*;

const SQS_MAX_BATCH_ENTRIES: usize = 10;
const SQS_MAX_REQUEST_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SqsPublishingMode {
    Single,
    Batch,
}

pub(in crate::runtime) struct SqsEmitter {
    client: SqsClient,
    queue_url: String,
    mode: SqsPublishingMode,
}

#[derive(Debug)]
struct PreparedSqsRecord {
    position: BrokerRecordPosition,
    body: String,
    attributes: StdHashMap<String, MessageAttributeValue>,
    group_id: Option<String>,
    encoded_bytes: usize,
    acks: AckSet,
}

impl PreparedSqsRecord {
    fn new(
        position: BrokerRecordPosition,
        payload: Vec<u8>,
        headers: EmitterHeaders,
        group_id: Result<Option<String>, String>,
        acks: AckSet,
    ) -> Result<Self, String> {
        let body = String::from_utf8(payload)
            .map_err(|_| "SQS message body is not valid UTF-8".to_string())?;
        if !SqsEmitter::has_valid_message_characters(&body) {
            return Err("SQS message body contains a character the service forbids".to_string());
        }
        if headers.len() > 10 {
            return Err(format!(
                "SQS message has {} attributes; the service permits at most 10",
                headers.len()
            ));
        }
        let mut attributes = StdHashMap::with_capacity(headers.len());
        for (name, value) in headers {
            SqsEmitter::validate_attribute(&name, &value)?;
            let attribute = MessageAttributeValue::builder()
                .data_type("String")
                .string_value(&value)
                .build()
                .map_err(|error| format!("invalid SQS message attribute '{name}': {error}"))?;
            attributes.insert(name, attribute);
        }
        let group_id = group_id?;
        if let Some(group_id) = group_id.as_deref() {
            SqsEmitter::validate_group_id(group_id)?;
        }
        let encoded_bytes = body
            .len()
            .saturating_add(
                attributes
                    .iter()
                    .map(|(name, value)| {
                        name.len()
                            .saturating_add(value.data_type().len())
                            .saturating_add(value.string_value().map(str::len).unwrap_or_default())
                    })
                    .fold(0_usize, usize::saturating_add),
            )
            .saturating_add(group_id.as_ref().map_or(0, String::len));
        if encoded_bytes > SQS_MAX_REQUEST_BYTES {
            return Err(format!(
                "SQS record is {encoded_bytes} bytes; the protocol limit is 256 KiB"
            ));
        }
        Ok(Self {
            position,
            body,
            attributes,
            group_id,
            encoded_bytes,
            acks,
        })
    }
}

impl SqsEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientSqs,
        resolved: Option<&ResolvedClientConfig>,
        queue: &str,
        mode: SqsPublishingMode,
    ) -> EmitterRuntimeResult<Self> {
        let client = Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await?;
        let queue_url = Self::queue_url(&client, queue).await?;
        Ok(Self {
            client,
            queue_url,
            mode,
        })
    }

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<SqsClient> {
        let endpoint = emitter_config_value(config, "endpoint", || {
            "missing SQS client config key 'endpoint'".to_string()
        })?;
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
                        emitter_config_error(format!("invalid SQS timeout_ms '{timeout_ms}'"))
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
        if let Some(ca_file) = client_tls_paths(config).ca_file.as_ref() {
            let ca_pem = emitter_read_tls_file(ca_file, "TLS CA certificate")?;
            let tls_context = aws_smithy_http_client::tls::TlsContext::builder()
                .with_trust_store(
                    aws_smithy_http_client::tls::TrustStore::empty().with_pem_certificate(ca_pem),
                )
                .build()
                .map_err(emitter_init_error)?;
            let http_client = aws_smithy_http_client::Builder::new()
                .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                    aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
                ))
                .tls_context(tls_context)
                .build_https();
            loader = loader.http_client(http_client);
        }
        let sdk_config = loader.load().await;
        Ok(SqsClient::new(&sdk_config))
    }

    async fn queue_url(client: &SqsClient, queue: &str) -> EmitterRuntimeResult<String> {
        client
            .get_queue_url()
            .queue_name(queue)
            .send()
            .await
            .map_err(emitter_publish_error)?
            .queue_url()
            .map(ToOwned::to_owned)
            .ok_or_else(|| emitter_publish_error(format!("SQS queue '{queue}' has no URL")))
    }

    pub(super) async fn publish(
        &mut self,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        let mut prepared = Vec::with_capacity(records.len());
        for record in records {
            tokio::task::consume_budget().await;
            match PreparedSqsRecord::new(
                (record.batch_index, record.row_index),
                record.payload,
                record.headers,
                record.sqs_message_group,
                record.acks,
            ) {
                Ok(record) => prepared.push(record),
                Err(reason) => outcome.reject((record.batch_index, record.row_index), reason),
            }
        }
        match self.mode {
            SqsPublishingMode::Single => self.publish_single(prepared, &mut outcome).await,
            SqsPublishingMode::Batch => self.publish_batches(prepared, &mut outcome).await,
        }
        outcome
    }

    async fn publish_single(
        &self,
        records: Vec<PreparedSqsRecord>,
        outcome: &mut PerRecordPublishOutcome,
    ) {
        for record in records {
            tokio::task::consume_budget().await;
            let mut request = self
                .client
                .send_message()
                .queue_url(&self.queue_url)
                .message_body(&record.body)
                .set_message_attributes(
                    (!record.attributes.is_empty()).then(|| record.attributes.clone()),
                );
            if let Some(group_id) = record.group_id.as_deref() {
                request = request.message_group_id(group_id);
            }
            match await_emitter_confirmation(&record.acks, request.send()).await {
                Ok(_) => outcome.deliver(record.position),
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_invalid_message_contents()) =>
                {
                    outcome.reject(record.position, format!("SQS rejected the record: {error}"));
                }
                Err(error) => {
                    outcome.fail(emitter_publish_error(format!(
                        "SQS SendMessage failed: {error}"
                    )));
                    return;
                }
            }
        }
    }

    async fn publish_batches(
        &self,
        records: Vec<PreparedSqsRecord>,
        outcome: &mut PerRecordPublishOutcome,
    ) {
        for records in Self::batch_chunks(records) {
            tokio::task::consume_budget().await;
            let mut request = self.client.send_message_batch().queue_url(&self.queue_url);
            for (index, record) in records.iter().enumerate() {
                let mut entry = SendMessageBatchRequestEntry::builder()
                    .id(Self::batch_entry_id(index))
                    .message_body(&record.body)
                    .set_message_attributes(
                        (!record.attributes.is_empty()).then(|| record.attributes.clone()),
                    );
                if let Some(group_id) = record.group_id.as_deref() {
                    entry = entry.message_group_id(group_id);
                }
                let entry = match entry.build() {
                    Ok(entry) => entry,
                    Err(error) => {
                        outcome.fail(emitter_publish_error(format!(
                            "failed to build SQS batch request entry: {error}"
                        )));
                        return;
                    }
                };
                request = request.entries(entry);
            }
            let acks = AckSet::merged(records.iter().map(|record| record.acks.clone()));
            let response = match await_emitter_confirmation(&acks, request.send()).await {
                Ok(response) => response,
                Err(error) => {
                    outcome.fail(emitter_publish_error(format!(
                        "SQS SendMessageBatch failed: {error}"
                    )));
                    return;
                }
            };
            if let Some(reason) = Self::apply_batch_response(&records, &response, outcome) {
                outcome.fail(emitter_publish_error(reason));
                return;
            }
        }
    }

    fn batch_chunks(records: Vec<PreparedSqsRecord>) -> Vec<Vec<PreparedSqsRecord>> {
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0_usize;
        let mut current_fifo_groups = StdHashSet::new();
        for record in records {
            let would_exceed_count = current.len() == SQS_MAX_BATCH_ENTRIES;
            let would_exceed_bytes = !current.is_empty()
                && current_bytes.saturating_add(record.encoded_bytes) > SQS_MAX_REQUEST_BYTES;
            let would_repeat_fifo_group = record
                .group_id
                .as_ref()
                .is_some_and(|group| current_fifo_groups.contains(group));
            if would_exceed_count || would_exceed_bytes || would_repeat_fifo_group {
                chunks.push(std::mem::take(&mut current));
                current_bytes = 0;
                current_fifo_groups.clear();
            }
            current_bytes = current_bytes.saturating_add(record.encoded_bytes);
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
        outcome: &mut PerRecordPublishOutcome,
    ) -> Option<String> {
        let mut accounted = vec![false; records.len()];
        let mut infrastructure_reasons = Vec::new();
        for success in response.successful() {
            match Self::batch_entry_index(success.id(), records.len()) {
                Some(index) if !accounted[index] => {
                    accounted[index] = true;
                    outcome.deliver(records[index].position);
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
                        outcome.reject(records[index].position, reason);
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
        id.strip_prefix('m')
            .and_then(|index| index.parse::<usize>().ok())
            .filter(|index| *index < record_count)
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

    fn has_valid_message_characters(value: &str) -> bool {
        value.chars().all(|character| {
            matches!(character, '\u{0009}' | '\u{000A}' | '\u{000D}')
                || matches!(character as u32, 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF)
        })
    }

    fn validate_attribute(name: &str, value: &str) -> Result<(), String> {
        let normalized = name.to_ascii_lowercase();
        if name.is_empty() || name.len() > 256 {
            return Err("SQS message attribute names must contain 1 to 256 bytes".to_string());
        }
        if normalized.starts_with("aws.") || normalized.starts_with("amazon.") {
            return Err(format!(
                "SQS message attribute name '{name}' uses a reserved prefix"
            ));
        }
        if name.starts_with('.')
            || name.ends_with('.')
            || name.contains("..")
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(format!("invalid SQS message attribute name '{name}'"));
        }
        if !Self::has_valid_message_characters(value) {
            return Err(format!(
                "SQS message attribute '{name}' contains a forbidden character"
            ));
        }
        Ok(())
    }

    fn validate_group_id(group_id: &str) -> Result<(), String> {
        if group_id.is_empty() || group_id.chars().count() > 128 {
            return Err("SQS FIFO message group must contain 1 to 128 characters".to_string());
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
            return Err("SQS FIFO message group contains an unsupported character".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_config(timeout_ms: &str) -> Vec<nervix_models::ClientConfigEntry> {
        vec![
            nervix_models::ClientConfigEntry {
                key: "endpoint".to_string(),
                value: "http://127.0.0.1:9324".to_string(),
            },
            nervix_models::ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: timeout_ms.to_string(),
            },
        ]
    }

    fn prepared(payload_bytes: usize) -> PreparedSqsRecord {
        PreparedSqsRecord::new(
            (0, 0),
            vec![b'x'; payload_bytes],
            Vec::new(),
            Ok(None),
            AckSet::empty(),
        )
        .expect("test SQS record should be valid")
    }

    fn prepared_in_group(row: usize, group: &str) -> PreparedSqsRecord {
        let mut record = PreparedSqsRecord::new(
            (0, row),
            vec![b'x'],
            Vec::new(),
            Ok(Some(group.to_string())),
            AckSet::empty(),
        )
        .expect("test SQS FIFO record should be valid");
        record.position = (0, row);
        record
    }

    #[test]
    fn rejects_a_record_larger_than_the_declared_sqs_protocol_limit() {
        let error = PreparedSqsRecord::new(
            (0, 0),
            vec![b'x'; SQS_MAX_REQUEST_BYTES + 1],
            Vec::new(),
            Ok(None),
            AckSet::empty(),
        )
        .expect_err("oversized SQS record should be rejected");

        assert!(error.contains("256 KiB"), "unexpected error: {error}");
    }

    #[test]
    fn batch_chunks_respect_entry_and_byte_limits_without_reordering() {
        let mut records = (0..11)
            .map(|row| {
                let mut record = prepared(1);
                record.position = (0, row);
                record
            })
            .collect::<Vec<_>>();
        records.push(prepared(SQS_MAX_REQUEST_BYTES));
        records.push(prepared(2));

        let chunks = SqsEmitter::batch_chunks(records);
        let positions = chunks
            .iter()
            .flatten()
            .map(|record| record.position)
            .collect::<Vec<_>>();

        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            [10, 1, 1, 1]
        );
        assert_eq!(positions[0], (0, 0));
        assert_eq!(positions[10], (0, 10));
    }

    #[test]
    fn batch_chunks_never_send_two_records_from_one_fifo_group_together() {
        let records = vec![
            prepared_in_group(0, "acme"),
            prepared_in_group(1, "beta"),
            prepared_in_group(2, "acme"),
            prepared_in_group(3, "beta"),
        ];

        let chunks = SqsEmitter::batch_chunks(records);
        let positions = chunks
            .iter()
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|record| record.position)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(positions, [vec![(0, 0), (0, 1)], vec![(0, 2), (0, 3)]]);
    }

    #[test]
    fn only_definitive_per_entry_failures_are_record_rejections() {
        assert!(SqsEmitter::is_record_failure(
            true,
            "InvalidMessageContents",
            Some("message contains an invalid character")
        ));
        assert!(SqsEmitter::is_record_failure(
            true,
            "InvalidParameterValue",
            Some("MessageGroupId contains an invalid character")
        ));
        assert!(!SqsEmitter::is_record_failure(
            true,
            "InvalidParameterValue",
            Some("ContentBasedDeduplication is not enabled")
        ));
        assert!(!SqsEmitter::is_record_failure(
            false,
            "ThrottlingException",
            None
        ));
        assert!(!SqsEmitter::is_record_failure(
            true,
            "UnknownSenderFault",
            None
        ));
    }

    #[test]
    fn fifo_group_ids_accept_branch_json_and_reject_invalid_values() {
        assert!(SqsEmitter::validate_group_id(r#"{"tenant":"acme"}"#).is_ok());
        assert!(SqsEmitter::validate_group_id("").is_err());
        assert!(SqsEmitter::validate_group_id(&"x".repeat(129)).is_err());
        assert!(SqsEmitter::validate_group_id("contains space").is_err());
    }

    #[tokio::test]
    async fn client_timeout_bounds_each_request_while_sdk_retries_stay_disabled() {
        let client = SqsEmitter::client_from_config(&client_config("275"))
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

    #[tokio::test]
    async fn client_rejects_an_invalid_request_timeout() {
        let error = SqsEmitter::client_from_config(&client_config("later"))
            .await
            .expect_err("invalid SQS timeout should fail client initialization");

        assert!(
            format!("{error:?}").contains("invalid SQS timeout_ms 'later'"),
            "unexpected error: {error:?}"
        );
    }
}
