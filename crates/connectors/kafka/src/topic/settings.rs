//! The Kafka topic ingestor's local assignment polling capability.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The assignment retry setting and its validated projection into a topic ingestor.
//! - **Depends on.** Vocabulary setting classes and durations.
//! - **Must not know.** Graphs, placement or command-line parsing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns topic ingestor assignment polling")
)]

use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};

#[derive(Debug, thiserror::Error)]
pub enum KafkaTopicSettingsError {
    #[error("kafka-domain-assignment-retry must be positive and no longer than u32::MAX seconds")]
    AssignmentRetry,
}
pub struct KafkaDomainAssignmentRetry;
impl SettingDefinition for KafkaDomainAssignmentRetry {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = KafkaTopicSettingsError;
    const NAME: &'static str = "kafka-domain-assignment-retry";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_millis(100)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() || value.as_secs() > u64::from(u32::MAX) {
            return Err(Report::new(KafkaTopicSettingsError::AssignmentRetry));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KafkaTopicIngestorSettings {
    assignment_retry: Setting<KafkaDomainAssignmentRetry>,
}
impl KafkaTopicIngestorSettings {
    pub fn resolve(value: Option<Duration>) -> Result<Self, Report<KafkaTopicSettingsError>> {
        Ok(Self {
            assignment_retry: Setting::resolve(value, &())?,
        })
    }
    pub fn assignment_retry(self) -> Duration {
        *self.assignment_retry.value()
    }
}
impl Default for KafkaTopicIngestorSettings {
    fn default() -> Self {
        Self::resolve(None).assured("the assignment cadence default validates")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn assignment_retry_is_positive_and_projects_the_selected_cadence() {
        assert!(KafkaTopicIngestorSettings::resolve(Some(Duration::ZERO)).is_err());
        let settings = KafkaTopicIngestorSettings::resolve(Some(Duration::from_millis(37)))
            .assured("a positive polling cadence validates");
        assert_eq!(settings.assignment_retry(), Duration::from_millis(37));
    }
}
