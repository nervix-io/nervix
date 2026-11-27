//! Enumeration of a Pulsar topic's external partition coordinates.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Bounded driver metadata lookup and its typed outcomes.
//! - **Depends on.** The shared Pulsar client, the host handle through which the inspector takes
//!   its node's outward TLS policy, and vocabulary coordinates.
//! - **Must not know.** Branches, schedules, placement, leadership or runtime records.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns bounded broker partition enumeration")
)]
#![cfg_attr(
    nervix_lint,
    nervix::host_services(
        tls_policy,
        reason = "the Pulsar partition inspector connects its client under its node's outward TLS \
                  policy"
    )
)]

use std::{num::NonZeroU32, time::Duration};

use error_stack::{Report, ResultExt as _};
use nervix_connector::PartitionInspectorHost;
use nervix_models::{ClientConfigEntry, ExternalPartitionSet, PulsarTopicName};
use nervix_primitives::time::timeout;
use pulsar::{OperationRetryOptions, Pulsar, TokioExecutor};
use thiserror::Error;

use super::connection::PulsarClientConnection;

#[derive(Debug, Error)]
pub enum PulsarPartitionObservationError {
    #[error("{connector} partition observer could not connect", connector = nervix_models::ConnectorKind::Pulsar)]
    Connect,
    #[error("{connector} partition metadata lookup failed", connector = nervix_models::ConnectorKind::Pulsar)]
    Lookup,
    #[error("{connector} partition observation exceeded its {budget:?} operation deadline", connector = nervix_models::ConnectorKind::Pulsar)]
    Deadline { budget: Duration },
}

pub struct PulsarTopicPartitionInspector {
    client: Pulsar<TokioExecutor>,
    topic: String,
    budget: Duration,
}

impl PulsarTopicPartitionInspector {
    /// Connects the inspector's client for `topic`, with its handshakes under the outward TLS
    /// policy `host` hands over.
    pub async fn connect(
        config: &[ClientConfigEntry],
        topic: &PulsarTopicName,
        host: &PartitionInspectorHost,
    ) -> Result<Self, Report<PulsarPartitionObservationError>> {
        let budget = OperationRetryOptions::default().operation_timeout;
        let client = timeout(
            budget,
            PulsarClientConnection::new(config, None, host.tls_policy()).open(),
        )
        .await
        .change_context(PulsarPartitionObservationError::Deadline { budget })?
        .change_context(PulsarPartitionObservationError::Connect)?;
        Ok(Self {
            client,
            topic: super::qualified_topic(config, topic),
            budget,
        })
    }

    pub async fn partitions(
        &self,
    ) -> Result<ExternalPartitionSet, Report<PulsarPartitionObservationError>> {
        let count = timeout(
            self.budget,
            self.client
                .lookup_partitioned_topic_number(self.topic.clone()),
        )
        .await
        .change_context(PulsarPartitionObservationError::Deadline {
            budget: self.budget,
        })?
        .change_context(PulsarPartitionObservationError::Lookup)?;
        Ok(match NonZeroU32::new(count) {
            Some(count) => ExternalPartitionSet::Partitioned(count),
            None => ExternalPartitionSet::Unpartitioned,
        })
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "partition_observation_tests.rs"]
mod tests;
