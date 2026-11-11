//! Connector-host capability arguments.
//!
//! Layer: edges.
//! - **Owns.** Local connector arguments and their conversion into validated host settings.
//! - **Depends on.** The owning connector definitions and the host's settings composition.
//! - **Must not know.** Drivers, retry loops or graph validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns connector capability arguments")
)]

use std::num::{NonZeroU32, NonZeroUsize};

use arch_into::ArchInto as _;
use nervix_connector::{ConnectorRetryJitterPercent, ConnectorRetryMultiplier};
use nervix_connector_kafka::KafkaDomainAssignmentRetry;
use nervix_connector_syslog::{SyslogIntakeQueueCapacity, SyslogReadChunkBytes};
use nervix_models::SettingDefinition as _;

use super::settings::CommandLine;
use crate::{
    application::cli_values::{HumanBytes, HumanDuration},
    runtime::{
        ConnectorHostSettingOverrides, ConnectorHostSettings, ConnectorHostSettingsError,
        KafkaPartitionWatchInterval, PartitionTopologyPollInterval,
    },
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Connector host")]
pub struct ConnectorHostArgs {
    #[arg(long = ConnectorRetryMultiplier::NAME, env = "NERVIX_CONNECTOR_RETRY_MULTIPLIER", help = format!("Multiplier for connector reconnect delays. Default: {}", ConnectorRetryMultiplier::default_value(&())))]
    pub connector_retry_multiplier: Option<NonZeroU32>,
    #[arg(long = ConnectorRetryJitterPercent::NAME, env = "NERVIX_CONNECTOR_RETRY_JITTER_PERCENT", help = format!("Additional random retry percentage, capped by the declared ceiling (0..=100). Default: {}", ConnectorRetryJitterPercent::default_value(&())))]
    pub connector_retry_jitter_percent: Option<u8>,
    #[arg(long = KafkaDomainAssignmentRetry::NAME, env = "NERVIX_KAFKA_DOMAIN_ASSIGNMENT_RETRY", help = format!("Retry cadence while a Kafka domain assignment is unavailable. Default: {}", humantime::format_duration(KafkaDomainAssignmentRetry::default_value(&()))))]
    pub kafka_domain_assignment_retry: Option<HumanDuration>,
    #[arg(long = KafkaPartitionWatchInterval::NAME, env = "NERVIX_KAFKA_PARTITION_WATCH_INTERVAL", help = format!("Kafka partition topology inspection cadence. Default: {}", humantime::format_duration(KafkaPartitionWatchInterval::default_value(&()))))]
    pub kafka_partition_watch_interval: Option<HumanDuration>,
    #[arg(long = PartitionTopologyPollInterval::NAME, env = "NERVIX_PARTITION_TOPOLOGY_POLL_INTERVAL", help = format!("Pulsar partition inspection and leader-owned topology observer reconciliation cadence. Kafka inspection uses its own interval. Default: {}", humantime::format_duration(PartitionTopologyPollInterval::default_value(&()))))]
    pub partition_topology_poll_interval: Option<HumanDuration>,
    #[arg(long = SyslogIntakeQueueCapacity::NAME, env = "NERVIX_SYSLOG_INTAKE_QUEUE_CAPACITY", help = format!("Frames queued between stream connections and the Syslog ingestor. Default: {}", SyslogIntakeQueueCapacity::default_value(&())))]
    pub syslog_intake_queue_capacity: Option<NonZeroUsize>,
    #[arg(long = SyslogReadChunkBytes::NAME, env = "NERVIX_SYSLOG_READ_CHUNK_BYTES", help = format!("Bytes appended by one Syslog stream read, also bounded by the frame limit. Default: {}", SyslogReadChunkBytes::default_value(&())))]
    pub syslog_read_chunk_bytes: Option<HumanBytes>,
}
impl ConnectorHostArgs {
    pub(super) fn settings(
        &self,
    ) -> error_stack::Result<ConnectorHostSettings, ConnectorHostSettingsError> {
        ConnectorHostSettings::resolve(ConnectorHostSettingOverrides {
            retry_multiplier: self.connector_retry_multiplier,
            retry_jitter_percent: self.connector_retry_jitter_percent,
            kafka_assignment_retry: self.kafka_domain_assignment_retry.map(|value| value.0),
            kafka_partition_watch_interval: self
                .kafka_partition_watch_interval
                .map(|value| value.0),
            partition_topology_poll_interval: self
                .partition_topology_poll_interval
                .map(|value| value.0),
            syslog_queue_capacity: self.syslog_intake_queue_capacity,
            syslog_read_chunk_bytes: self.syslog_read_chunk_bytes.map(|value| value.0.as_u64()),
        })
    }
    pub(super) fn from_settings(settings: &ConnectorHostSettings) -> Self {
        Self {
            connector_retry_multiplier: Some(settings.retry.multiplier()),
            connector_retry_jitter_percent: Some(settings.retry.jitter_percent()),
            kafka_domain_assignment_retry: Some(HumanDuration(settings.kafka.assignment_retry())),
            kafka_partition_watch_interval: Some(HumanDuration(
                settings.kafka_partition_watch_interval(),
            )),
            partition_topology_poll_interval: Some(HumanDuration(
                settings.partition_topology_poll_interval(),
            )),
            syslog_intake_queue_capacity: NonZeroUsize::new(settings.syslog.queue_capacity()),
            syslog_read_chunk_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                settings.syslog.read_chunk_bytes().arch_into(),
            ))),
        }
    }
    pub(super) fn render(&self, command: &mut CommandLine) {
        command.optional(
            ConnectorRetryMultiplier::NAME,
            self.connector_retry_multiplier,
        );
        command.optional(
            ConnectorRetryJitterPercent::NAME,
            self.connector_retry_jitter_percent,
        );
        command.optional(
            KafkaDomainAssignmentRetry::NAME,
            self.kafka_domain_assignment_retry,
        );
        command.optional(
            KafkaPartitionWatchInterval::NAME,
            self.kafka_partition_watch_interval,
        );
        command.optional(
            PartitionTopologyPollInterval::NAME,
            self.partition_topology_poll_interval,
        );
        command.optional(
            SyslogIntakeQueueCapacity::NAME,
            self.syslog_intake_queue_capacity,
        );
        command.optional(SyslogReadChunkBytes::NAME, self.syslog_read_chunk_bytes);
    }
}
