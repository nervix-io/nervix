//! Node-local connector-host policy and its typed connector projections.
//!
//! Layer: data plane.
//! - **Owns.** The partition-watch cadence and the host's resolved connector capabilities.
//! - **Depends on.** Vocabulary setting classes and connector capability definitions.
//! - **Must not know.** Command-line parsing, driver configuration or graph validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns local connector host settings")
)]

use std::time::Duration;

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};

#[derive(Debug, thiserror::Error)]
pub enum ConnectorHostSettingsError {
    #[error("invalid connector retry capability")]
    Retry,
    #[error("invalid Kafka assignment capability")]
    Kafka,
    #[error("invalid Syslog intake capability")]
    Syslog,
    #[error("kafka-partition-watch-interval must be positive and no longer than u32::MAX seconds")]
    PartitionWatch,
    #[error(
        "partition-topology-poll-interval must be positive and no longer than u32::MAX seconds"
    )]
    TopologyPoll,
}

pub struct KafkaPartitionWatchInterval;
impl SettingDefinition for KafkaPartitionWatchInterval {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = ConnectorHostSettingsError;
    const NAME: &'static str = "kafka-partition-watch-interval";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(1)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() || value.as_secs() > u64::from(u32::MAX) {
            return Err(Report::new(ConnectorHostSettingsError::PartitionWatch));
        }
        Ok(())
    }
}

pub struct PartitionTopologyPollInterval;
impl SettingDefinition for PartitionTopologyPollInterval {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = ConnectorHostSettingsError;
    const NAME: &'static str = "partition-topology-poll-interval";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(1)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() || value.as_secs() > u64::from(u32::MAX) {
            return Err(Report::new(ConnectorHostSettingsError::TopologyPoll));
        }
        Ok(())
    }
}

/// Capabilities resolved once before opening any connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectorHostSettings {
    pub retry: nervix_connector::RetryBackoffSettings,
    pub kafka: nervix_connector_kafka::KafkaTopicIngestorSettings,
    pub syslog: nervix_connector_syslog::SyslogListenerSettings,
    partition_watch: Setting<KafkaPartitionWatchInterval>,
    topology_poll: Setting<PartitionTopologyPollInterval>,
}

#[derive(Default)]
pub struct ConnectorHostSettingOverrides {
    pub retry_multiplier: Option<std::num::NonZeroU32>,
    pub retry_jitter_percent: Option<u8>,
    pub kafka_assignment_retry: Option<Duration>,
    pub kafka_partition_watch_interval: Option<Duration>,
    pub partition_topology_poll_interval: Option<Duration>,
    pub syslog_queue_capacity: Option<std::num::NonZeroUsize>,
    pub syslog_read_chunk_bytes: Option<u64>,
}

impl ConnectorHostSettings {
    pub fn resolve(
        values: ConnectorHostSettingOverrides,
    ) -> Result<Self, Report<ConnectorHostSettingsError>> {
        Ok(Self {
            retry: nervix_connector::RetryBackoffSettings::resolve(
                values.retry_multiplier,
                values.retry_jitter_percent,
            )
            .change_context(ConnectorHostSettingsError::Retry)?,
            kafka: nervix_connector_kafka::KafkaTopicIngestorSettings::resolve(
                values.kafka_assignment_retry,
            )
            .change_context(ConnectorHostSettingsError::Kafka)?,
            syslog: nervix_connector_syslog::SyslogListenerSettings::resolve(
                values.syslog_queue_capacity,
                values.syslog_read_chunk_bytes,
            )
            .change_context(ConnectorHostSettingsError::Syslog)?,
            partition_watch: Setting::resolve(values.kafka_partition_watch_interval, &())?,
            topology_poll: Setting::resolve(values.partition_topology_poll_interval, &())?,
        })
    }
    pub fn capabilities(self) -> nervix_models::ConnectorHostCapabilities {
        let positive = |value: usize| {
            std::num::NonZeroU64::new(value.arch_into())
                .assured("the connector host settings validate positive native bounds")
        };
        nervix_models::ConnectorHostCapabilities {
            retry_multiplier: self.retry.multiplier(),
            retry_jitter_percent: self.retry.jitter_percent(),
            kafka_domain_assignment_retry: self.kafka.assignment_retry(),
            kafka_partition_watch_interval: self.kafka_partition_watch_interval(),
            partition_topology_poll_interval: self.partition_topology_poll_interval(),
            syslog_intake_queue_capacity: positive(self.syslog.queue_capacity()),
            syslog_read_chunk_bytes: positive(self.syslog.read_chunk_bytes()),
        }
    }

    pub fn kafka_partition_watch_interval(self) -> Duration {
        *self.partition_watch.value()
    }

    pub fn partition_topology_poll_interval(self) -> Duration {
        *self.topology_poll.value()
    }
}
impl Default for ConnectorHostSettings {
    fn default() -> Self {
        Self::resolve(ConnectorHostSettingOverrides::default())
            .assured("the connector host defaults validate")
    }
}

impl super::Runtime {
    pub(crate) fn partition_topology_poll_interval(&self) -> Duration {
        self.inner.connector_host.partition_topology_poll_interval()
    }

    /// The node's configured cadence for leader-owned Kafka topology observation.
    pub(crate) fn kafka_partition_watch_interval(&self) -> Duration {
        self.inner.connector_host.kafka_partition_watch_interval()
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the typed connector-host setting boundary")
)]
mod tests {
    use super::*;

    #[test]
    fn topology_polling_retains_its_resolved_override_and_rejects_invalid_bounds() {
        let interval = Duration::from_millis(250);
        let settings = ConnectorHostSettings::resolve(ConnectorHostSettingOverrides {
            partition_topology_poll_interval: Some(interval),
            ..Default::default()
        })
        .assured("a positive topology cadence is valid");
        assert_eq!(settings.partition_topology_poll_interval(), interval);
        assert_eq!(
            settings.capabilities().partition_topology_poll_interval,
            interval
        );
        assert_eq!(
            settings.kafka_partition_watch_interval(),
            KafkaPartitionWatchInterval::default_value(&())
        );
        for value in [Duration::ZERO, Duration::from_secs(u64::from(u32::MAX) + 1)] {
            let error = ConnectorHostSettings::resolve(ConnectorHostSettingOverrides {
                partition_topology_poll_interval: Some(value),
                ..Default::default()
            })
            .expect_err("zero and unrepresentable topology cadences are refused");
            assert!(matches!(
                error.current_context(),
                ConnectorHostSettingsError::TopologyPoll
            ));
        }
    }
}
