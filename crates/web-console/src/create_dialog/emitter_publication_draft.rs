//! Explicit publication choices until an emitter form has a complete policy.
//!
//! Layer: edges.
//! - **Owns.** Incomplete acknowledgement windows, deadlines and retry fields and their conversion.
//! - **Depends on.** Entity-owned publication enums and the emitter draft's field errors.
//! - **Must not know.** Driver retries, confirmation tracking or command dispatch.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns browser publication drafts")
)]

use error_stack::{Report, ResultExt as _};
#[cfg(test)]
use nervix_models::EmitterPublishing;
use nervix_models::{AckWindow, EmitterPublishingModeKind, RetryPolicy};

use super::emitter_draft::{EmitterDraftError, required};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum WindowDraft {
    #[default]
    Unselected,
    Sequential,
    Parallel(String),
}

impl WindowDraft {
    fn build(&self) -> error_stack::Result<AckWindow, EmitterDraftError> {
        match self {
            Self::Unselected => Err(Report::new(EmitterDraftError::Missing(
                "acknowledgement window",
            ))),
            Self::Sequential => Ok(AckWindow::Sequential),
            Self::Parallel(max) => Ok(AckWindow::Parallel {
                max: max
                    .parse::<std::num::NonZeroU64>()
                    .change_context(EmitterDraftError::Invalid("ACK maximum"))?,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct PublicationDraft {
    pub(super) kind: Option<EmitterPublishingModeKind>,
    pub(super) window: WindowDraft,
    pub(super) ack_timeout: String,
    pub(super) retry_backoff: String,
    pub(super) retry_max: String,
}

impl PublicationDraft {
    fn retry(&self) -> error_stack::Result<RetryPolicy, EmitterDraftError> {
        Ok(RetryPolicy {
            backoff: required(&self.retry_backoff, "retry backoff")?,
            max_backoff: required(&self.retry_max, "maximum retry backoff")?,
        })
    }

    #[cfg(test)]
    pub(super) fn from_publication(publication: EmitterPublishing<'_>) -> Self {
        let retry = publication.retry_policy();
        let window = match publication {
            EmitterPublishing::ClientAck { window, .. }
            | EmitterPublishing::BrokerAck { window, .. } => match window {
                AckWindow::Sequential => WindowDraft::Sequential,
                AckWindow::Parallel { max } => WindowDraft::Parallel(max.to_string()),
            },
            EmitterPublishing::NoAck { .. } | EmitterPublishing::RequestAck { .. } => {
                WindowDraft::Unselected
            }
        };
        Self {
            kind: Some(publication.kind()),
            window,
            ack_timeout: publication.ack_timeout().unwrap_or_default().to_string(),
            retry_backoff: retry.backoff.clone(),
            retry_max: retry.max_backoff.clone(),
        }
    }
}

// These conversions construct each entity's closed enum. Capability selection stays in the
// declaration; this macro shares only the policy fields each representable variant carries.
macro_rules! publication_conversions {
    (request { $($request:ident => $Request:ident),+ } no_ack { $($no_ack:ident => $NoAck:ident),+ }
     broker { $($broker:ident => $Broker:ident),+ } window { $($window:ident => $Window:ident : $kind:ident),+ }) => {
        impl PublicationDraft {
            $(pub(super) fn $request(&self) -> error_stack::Result<nervix_models::$Request, EmitterDraftError> {
                Ok(nervix_models::$Request::Ack { retry_policy: self.retry()? })
            })+
            $(pub(super) fn $no_ack(&self) -> error_stack::Result<nervix_models::$NoAck, EmitterDraftError> {
                Ok(nervix_models::$NoAck::NoAck { retry_policy: self.retry()? })
            })+
            $(pub(super) fn $broker(&self) -> error_stack::Result<nervix_models::$Broker, EmitterDraftError> {
                match self.kind {
                    Some(EmitterPublishingModeKind::NoAck) => Ok(nervix_models::$Broker::NoAck { retry_policy: self.retry()? }),
                    Some(EmitterPublishingModeKind::BrokerAck) => Ok(nervix_models::$Broker::Ack {
                        window: self.window.build()?,
                        ack_timeout: required(&self.ack_timeout, "acknowledgement timeout")?,
                        retry_policy: self.retry()?,
                    }),
                    _ => Err(Report::new(EmitterDraftError::Invalid("publishing mode"))),
                }
            })+
            $(pub(super) fn $window(&self) -> error_stack::Result<nervix_models::$Window, EmitterDraftError> {
                if self.kind != Some(EmitterPublishingModeKind::$kind) {
                    return Err(Report::new(EmitterDraftError::Invalid("publishing mode")));
                }
                Ok(nervix_models::$Window::Ack {
                    window: self.window.build()?,
                    ack_timeout: required(&self.ack_timeout, "acknowledgement timeout")?,
                    retry_policy: self.retry()?,
                })
            })+
        }
    };
}

publication_conversions! {
    request {
        http => HttpRequestPublishingMode, sqs => SqsQueuePublishingMode,
        sentry => SentryEventPublishingMode, otel_logs => OtelLogsPublishingMode,
        otel_traces => OtelTracesPublishingMode, otel_metrics => OtelMetricsPublishingMode,
        clickhouse => ClickHouseTablePublishingMode, postgres => PostgresTablePublishingMode,
        mysql => MySqlTablePublishingMode, mongodb => MongoDbCollectionPublishingMode,
        iceberg => IcebergTablePublishingMode
    }
    no_ack {
        redis => RedisPubSubPublishingMode, nats_subject => NatsSubjectPublishingMode,
        zeromq => ZeroMqPushPublishingMode, syslog => SyslogCollectorPublishingMode
    }
    broker {
        kafka => KafkaTopicPublishingMode, pulsar => PulsarTopicPublishingMode,
        rabbitmq_queue => RabbitMqQueuePublishingMode, rabbitmq_exchange => RabbitMqExchangePublishingMode,
        mqtt => MqttTopicPublishingMode
    }
    window {
        client => ClientConsumerPublishingMode: ClientAck,
        nats_stream => NatsStreamPublishingMode: BrokerAck
    }
}
