//! The MQTT topic ingestor's local read-ahead capability.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The read-ahead headroom setting, its single default and native channel-bound check,
//!   and the validated group the host hands every topic ingestor it starts.
//! - **Depends on.** Vocabulary setting classes and the native primitive channel bound.
//! - **Must not know.** Graphs, placement, external client keys or command-line parsing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the MQTT topic ingestor's read-ahead capability"
    )
)]

use std::num::NonZeroUsize;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use nervix_primitives::sync::Semaphore;

/// Why a read-ahead headroom cannot run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MqttReadAheadSettingsError {
    #[error(
        "{setting} ({value}) exceeds the {maximum} publishes a read-ahead holds beside MQTT's \
         largest Receive Maximum",
        setting = MqttReadAheadHeadroom::NAME
    )]
    Headroom { value: usize, maximum: usize },
}

/// How many publishes an ingestor connection reads ahead beyond what its broker holds back: the
/// Receive Maximum every `ACK`-mode `CONNECT` advertises beyond the acknowledgement window, up to
/// MQTT's 65,535, the QoS 0 outcomes an `ACK`-mode read-ahead holds beside the acknowledged ones,
/// and the whole read-ahead under `NO ACK`.
pub struct MqttReadAheadHeadroom;

impl MqttReadAheadHeadroom {
    /// The largest headroom whose read-ahead, MQTT's widest Receive Maximum and the headroom
    /// together, still fits one native channel.
    fn maximum() -> usize {
        Semaphore::MAX_PERMITS
            .checked_sub(usize::from(u16::MAX))
            .assured("the native channel bound exceeds MQTT's sixteen-bit Receive Maximum")
    }
}

impl SettingDefinition for MqttReadAheadHeadroom {
    type Class = NodeCapabilities;
    type Value = NonZeroUsize;
    type Context = ();
    type Error = MqttReadAheadSettingsError;

    const NAME: &'static str = "mqtt-read-ahead-headroom";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> NonZeroUsize {
        NonZeroUsize::new(64).assured("the read-ahead headroom default is positive")
    }

    fn validate(value: &NonZeroUsize, _: &()) -> Result<(), Report<Self::Error>> {
        let maximum = Self::maximum();
        if value.get() > maximum {
            return Err(Report::new(MqttReadAheadSettingsError::Headroom {
                value: value.get(),
                maximum,
            }));
        }
        Ok(())
    }
}

/// The read-ahead capability the host resolves once and hands every MQTT topic ingestor it starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MqttReadAheadSettings {
    headroom: Setting<MqttReadAheadHeadroom>,
}

impl MqttReadAheadSettings {
    /// Resolves the headroom when it is not given through its definition's default, then
    /// validates it.
    pub fn resolve(
        headroom: Option<NonZeroUsize>,
    ) -> Result<Self, Report<MqttReadAheadSettingsError>> {
        Ok(Self {
            headroom: Setting::resolve(headroom, &())?,
        })
    }

    pub fn headroom(self) -> NonZeroUsize {
        *self.headroom.value()
    }
}

impl Default for MqttReadAheadSettings {
    fn default() -> Self {
        Self::resolve(None).assured("the read-ahead headroom default fits the native channel")
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the MQTT read-ahead capability boundary")
)]
mod tests {
    use super::*;

    #[test]
    fn the_default_headroom_is_the_sixty_four_publishes_it_ran_with() {
        assert_eq!(MqttReadAheadSettings::default().headroom().get(), 64);
        assert_eq!(MqttReadAheadHeadroom::NAME, "mqtt-read-ahead-headroom");
    }

    #[test]
    fn a_headroom_up_to_the_channel_bound_beside_the_widest_receive_maximum_is_kept() {
        let maximum = Semaphore::MAX_PERMITS - usize::from(u16::MAX);
        for headroom in [1, 3, maximum] {
            let settings = MqttReadAheadSettings::resolve(NonZeroUsize::new(headroom))
                .assured("a headroom within the channel bound validates");
            assert_eq!(settings.headroom().get(), headroom);
        }
    }

    #[test]
    fn a_headroom_whose_read_ahead_would_not_fit_a_channel_is_refused_naming_it() {
        let maximum = Semaphore::MAX_PERMITS - usize::from(u16::MAX);
        let refused = MqttReadAheadSettings::resolve(NonZeroUsize::new(maximum + 1))
            .err()
            .assured("one publish more than the channel holds beside 65,535 is refused");
        assert_eq!(
            refused.current_context(),
            &MqttReadAheadSettingsError::Headroom {
                value: maximum + 1,
                maximum,
            }
        );
        assert!(
            format!("{refused:#}").starts_with(&format!(
                "mqtt-read-ahead-headroom ({}) exceeds the {maximum} publishes",
                maximum + 1
            )),
            "{refused:#}"
        );
    }
}
