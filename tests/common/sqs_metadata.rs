//! SQS metadata expectations observed through its real service before Nervix consumes a message.
//!
//! Layer: test harness.
//! - **Owns.** Publishing a FIFO message, receiving it once without deleting it, and exposing its
//!   service-generated identity and times as the oracle for the next delivery.
//! - **Depends on.** The SQS SDK, the scenario's external service and its shared SQS client.
//! - **Must not know.** Connector implementations, ingestion metadata projection or runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the external SQS metadata oracle")
)]

use std::{io, time::Duration};

use aws_sdk_sqs::types::{Message, MessageSystemAttributeName as Attribute};
use chrono::{DateTime, Utc};
use nervix_primitives::time::timeout;

use super::{
    cluster::{sqs_client, sqs_queue_url},
    dependencies::DependencyEndpoints,
};

pub(crate) struct SqsMetadataFixture {
    pub(crate) message_id: String,
    pub(crate) sent_at: String,
    pub(crate) first_received_at: String,
    pub(crate) sequence_number: String,
    pub(crate) sender_id: String,
}

impl SqsMetadataFixture {
    /// Leaves one real, previously received message visible for the ingestor's next receive.
    pub(crate) async fn prepare(
        dependencies: &DependencyEndpoints,
        queue: &str,
        group: &str,
        deduplication_id: &str,
        payload: &str,
    ) -> io::Result<Self> {
        let client = sqs_client(dependencies).await?;
        let url = sqs_queue_url(&client, queue).await?;
        let sent = client
            .send_message()
            .queue_url(&url)
            .message_body(payload)
            .message_group_id(group)
            .message_deduplication_id(deduplication_id)
            .send()
            .await
            .map_err(io::Error::other)?;
        let receive = async {
            loop {
                nervix_primitives::task::consume_budget().await;
                let answer = client
                    .receive_message()
                    .queue_url(&url)
                    .max_number_of_messages(1)
                    .wait_time_seconds(1)
                    .message_system_attribute_names(Attribute::All)
                    .send()
                    .await
                    .map_err(io::Error::other)?;
                if let Some(message) = answer.messages.into_iter().flatten().next() {
                    return Ok::<_, io::Error>(message);
                }
            }
        };
        let message = timeout(Duration::from_secs(60), receive)
            .await
            .map_err(io::Error::other)??;
        if message.message_id() != sent.message_id() {
            return Err(io::Error::other(
                "the receive must return the message this fixture sent",
            ));
        }
        if Self::attribute(&message, Attribute::ApproximateReceiveCount)? != "1" {
            return Err(io::Error::other(
                "the fixture must be this message's first receiver",
            ));
        }
        let observed = Self {
            message_id: message
                .message_id()
                .ok_or_else(|| io::Error::other("SQS returned no message id"))?
                .to_string(),
            sent_at: Self::timestamp(&message, Attribute::SentTimestamp)?,
            first_received_at: Self::timestamp(
                &message,
                Attribute::ApproximateFirstReceiveTimestamp,
            )?,
            sequence_number: Self::attribute(&message, Attribute::SequenceNumber)?.to_string(),
            sender_id: Self::attribute(&message, Attribute::SenderId)?.to_string(),
        };
        let handle = message
            .receipt_handle()
            .ok_or_else(|| io::Error::other("SQS returned no receipt handle"))?;
        client
            .change_message_visibility()
            .queue_url(&url)
            .receipt_handle(handle)
            .visibility_timeout(0)
            .send()
            .await
            .map_err(io::Error::other)?;
        Ok(observed)
    }

    fn attribute(message: &Message, attribute: Attribute) -> io::Result<&str> {
        let value = message
            .attributes()
            .and_then(|attributes| attributes.get(&attribute));
        match value {
            Some(value) => Ok(value),
            None => Err(io::Error::other(format!("SQS did not return {attribute}"))),
        }
    }

    fn timestamp(message: &Message, attribute: Attribute) -> io::Result<String> {
        let value = Self::attribute(message, attribute)?;
        let milliseconds = value.parse::<i64>().map_err(io::Error::other)?;
        let timestamp = DateTime::<Utc>::from_timestamp_millis(milliseconds)
            .ok_or_else(|| io::Error::other("SQS timestamp is outside the test oracle's range"))?;
        Ok(timestamp.to_rfc3339())
    }
}
