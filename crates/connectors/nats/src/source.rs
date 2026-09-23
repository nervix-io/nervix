//! NATS source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The core NATS connection and queue subscription each source instance reads
//!   through, its TLS options, and message headers as ingest headers.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio and `async-nats`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use async_nats::{Client as NatsClient, Message as NatsMessage, Subscriber};
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, ServiceUrl, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume,
    client_config_value, client_tls_paths,
};
use nervix_models::{ClientConfigEntry, QueueGroupName, SubjectName};
use thiserror::Error;

const NATS: &str = "nats";

/// Why a NATS source could not connect or subscribe.
#[derive(Debug, Error)]
pub enum NatsSourceError {
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
    #[error("NATS subscription closed")]
    SubscriptionClosed,
}

type NatsSourceResult<T> = Result<T, Report<NatsSourceError>>;

/// What one NATS source subscribes to: its resolved client entries, the subject, and the queue
/// group every instance joins.
#[derive(Clone)]
pub struct NatsSourcePlan {
    config: Vec<ClientConfigEntry>,
    subject: SubjectName,
    queue_group: QueueGroupName,
}

/// One instance's connection and queue subscription, kept together because the subscription
/// lives only as long as its client.
struct NatsSubscription {
    _client: NatsClient,
    subscriber: Subscriber,
}

impl NatsSourcePlan {
    pub fn new(
        config: Vec<ClientConfigEntry>,
        subject: SubjectName,
        queue_group: QueueGroupName,
    ) -> Self {
        Self {
            config,
            subject,
            queue_group,
        }
    }

    async fn subscribe(&self) -> NatsSourceResult<NatsSubscription> {
        let client = Self::client_from_config(&self.config).await?;
        let subscriber = client
            .queue_subscribe(
                self.subject.as_str().to_string(),
                self.queue_group.as_str().to_string(),
            )
            .await
            .map_err(|source| {
                Report::new(NatsSourceError::Subscribe {
                    subject: self.subject.as_str().to_string(),
                })
                .attach_printable(source.to_string())
            })?;
        client.flush().await.map_err(|source| {
            Report::new(NatsSourceError::Flush).attach_printable(source.to_string())
        })?;
        Ok(NatsSubscription {
            _client: client,
            subscriber,
        })
    }

    async fn client_from_config(config: &[ClientConfigEntry]) -> NatsSourceResult<NatsClient> {
        let addr = client_config_value(config, "addr", "NATS")
            .change_context(NatsSourceError::ClientConfig)?;
        let mut options = async_nats::ConnectOptions::new();
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
                return Err(Report::new(NatsSourceError::IncompleteTlsIdentity));
            }
        }
        if ServiceUrl::new(&addr, "NATS addr")
            .has_scheme("tls")
            .change_context(NatsSourceError::ClientConfig)?
        {
            options = options.require_tls(true);
        }
        options.connect(addr).await.map_err(|source| {
            Report::new(NatsSourceError::Connect).attach_printable(source.to_string())
        })
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

pub struct NatsSourceMessage {
    headers: NatsMessageHeaders,
    position: (),
}

impl SourceMessage for NatsSourceMessage {
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
        IngestMetadataRow::Headers {
            headers: &self.headers,
        }
    }
}

/// One NATS source instance: its queue subscription, absent while suspended or reconnecting.
pub struct NatsSource {
    plan: NatsSourcePlan,
    subscription: Option<NatsSubscription>,
}

#[async_trait]
impl SourceConnector for NatsSource {
    type Plan = NatsSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            subscription: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.subscription.is_none()
    }

    async fn suspend(&mut self) -> SourceResult<()> {
        self.subscription = None;
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.subscription.is_none() {
            let subscription = self
                .plan
                .subscribe()
                .await
                .change_context(SourceError::Resume { connector: NATS })?;
            self.subscription = Some(subscription);
        }
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.subscription = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for NatsSource {
    type Message = NatsSourceMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let Some(subscription) = self.subscription.as_mut() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        let Some(message) = subscription.subscriber.next().await else {
            self.subscription = None;
            return Err(Report::new(NatsSourceError::SubscriptionClosed)
                .change_context(SourceError::Read { connector: NATS }));
        };
        Ok(SourceBatch::Messages(vec![NatsSourceMessage {
            headers: NatsMessageHeaders { message },
            position: (),
        }]))
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[tokio::test]
    async fn client_configuration_failures_keep_typed_context() {
        let error = NatsSourcePlan::client_from_config(&[])
            .await
            .expect_err("a missing NATS address must fail");
        assert!(matches!(
            error.current_context(),
            NatsSourceError::ClientConfig
        ));

        let error = NatsSourcePlan::client_from_config(&[
            entry("addr", "tls://127.0.0.1:1"),
            entry("tls_cert_file", "client.pem"),
        ])
        .await
        .expect_err("a client certificate without a key must fail");
        assert!(matches!(
            error.current_context(),
            NatsSourceError::IncompleteTlsIdentity
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
}
