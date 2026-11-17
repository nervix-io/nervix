//! The deadlines a node stops within, declared as settings.
//!
//! Layer: control plane.
//!
//! - **Owns.** The setting definitions of the shutdown and drain timeouts and cooperative task
//!   grace, their single defaults and the group that holds them.
//! - **Depends on.** The vocabulary's setting classes and the shutdown lifecycle's defaults.
//! - **Must not know.** Command-line parsing or the services a shutdown stops.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::{convert::Infallible, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use thiserror::Error;

use super::{DEFAULT_DRAIN_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT};

/// Why the shutdown deadlines a node is given cannot hold together. The variant names both
/// settings by their option names, with the values given.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ShutdownSettingsError {
    #[error(
        "{drain} ({drain_timeout:?}) must not exceed {shutdown} ({shutdown_timeout:?})",
        drain = DrainTimeout::NAME,
        shutdown = ShutdownTimeout::NAME
    )]
    DrainExceedsShutdown {
        drain_timeout: Duration,
        shutdown_timeout: Duration,
    },
}

/// How long a shutdown may take from its first stop request until the process exits.
pub struct ShutdownTimeout;

impl SettingDefinition for ShutdownTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "shutdown-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        DEFAULT_SHUTDOWN_TIMEOUT
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// How long a stopping node waits for its admitted work to drain before it continues. It never
/// exceeds the shutdown timeout, which bounds every drain step: a drain timeout equal to it lets
/// the drain use the whole deadline, and the defaults leave time for terminal teardown after it.
pub struct DrainTimeout;

impl SettingDefinition for DrainTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    /// The shutdown timeout the drain must fit within.
    type Context = Duration;
    type Error = ShutdownSettingsError;

    const NAME: &'static str = "drain-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &Duration) -> Duration {
        DEFAULT_DRAIN_TIMEOUT
    }

    fn validate(
        drain_timeout: &Duration,
        shutdown_timeout: &Duration,
    ) -> Result<(), Report<ShutdownSettingsError>> {
        if drain_timeout <= shutdown_timeout {
            return Ok(());
        }
        Err(Report::new(ShutdownSettingsError::DrainExceedsShutdown {
            drain_timeout: *drain_timeout,
            shutdown_timeout: *shutdown_timeout,
        }))
    }
}

/// How long a listener or background task has to stop cooperatively after shutdown asks it to.
/// The remaining shutdown deadline caps the actual wait. This is independent of the drain
/// timeout because a drain may validly consume its whole configured deadline.
pub struct TaskGracePeriod;

impl SettingDefinition for TaskGracePeriod {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "task-grace-period";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(2)
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The deadlines one node stops within.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownSettings {
    pub shutdown_timeout: Setting<ShutdownTimeout>,
    pub drain_timeout: Setting<DrainTimeout>,
    pub task_grace_period: Setting<TaskGracePeriod>,
}

impl ShutdownSettings {
    /// Resolves each timeout that is not given through its definition's default, then validates
    /// the drain timeout against the shutdown timeout.
    pub fn resolve(
        shutdown_timeout: Option<Duration>,
        drain_timeout: Option<Duration>,
        task_grace_period: Option<Duration>,
    ) -> Result<Self, Report<ShutdownSettingsError>> {
        let shutdown_timeout = Setting::<ShutdownTimeout>::resolve(shutdown_timeout, &())
            .assured("the shutdown timeout declares no validation of its own");
        let drain_timeout = Setting::resolve(drain_timeout, shutdown_timeout.value())?;
        Ok(Self {
            shutdown_timeout,
            drain_timeout,
            task_grace_period: Setting::<TaskGracePeriod>::resolve(task_grace_period, &())
                .assured("task grace declares no validation of its own"),
        })
    }

    pub fn shutdown_timeout(&self) -> Duration {
        *self.shutdown_timeout.value()
    }

    /// The cooperative stop interval. A caller also caps it by the remaining shutdown deadline.
    pub fn task_grace_period(&self) -> Duration {
        *self.task_grace_period.value()
    }

    pub fn drain_timeout(&self) -> Duration {
        *self.drain_timeout.value()
    }
}

impl Default for ShutdownSettings {
    fn default() -> Self {
        Self::resolve(None, None, None)
            .assured("a const assertion keeps the default drain shorter than the default shutdown")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ShutdownSettings, ShutdownSettingsError};

    #[test]
    fn a_drain_within_the_shutdown_resolves_and_the_defaults_are_ordered() {
        for drain in [Duration::from_secs(4), Duration::from_secs(5)] {
            let resolved =
                ShutdownSettings::resolve(Some(Duration::from_secs(5)), Some(drain), None)
                    .expect("a drain that does not exceed the shutdown is valid");
            assert_eq!(resolved.shutdown_timeout(), Duration::from_secs(5));
            assert_eq!(resolved.drain_timeout(), drain);
        }

        let defaults = ShutdownSettings::default();
        assert_eq!(defaults.shutdown_timeout(), Duration::from_secs(50));
        assert_eq!(defaults.drain_timeout(), Duration::from_secs(30));
        assert_eq!(defaults.task_grace_period(), Duration::from_secs(2));
    }

    #[test]
    fn a_full_deadline_drain_preserves_the_connection_and_task_grace() {
        let settings = ShutdownSettings::resolve(
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(30)),
            None,
        )
        .expect("a drain may use the whole shutdown deadline");
        assert_eq!(settings.task_grace_period(), Duration::from_secs(2));

        let configured = ShutdownSettings::resolve(
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(4)),
        )
        .expect("a cooperative grace does not depend on a post-drain reserve");
        assert_eq!(configured.task_grace_period(), Duration::from_secs(4));
    }

    #[test]
    fn a_drain_above_the_shutdown_names_both() {
        for drain in [Duration::new(50, 1), Duration::from_secs(60)] {
            let refused = ShutdownSettings::resolve(None, Some(drain), None)
                .expect_err("the drain exceeds the deadline that bounds it");
            assert_eq!(
                refused.current_context(),
                &ShutdownSettingsError::DrainExceedsShutdown {
                    drain_timeout: drain,
                    shutdown_timeout: Duration::from_secs(50),
                }
            );
        }
        let refused = ShutdownSettings::resolve(None, Some(Duration::from_secs(60)), None)
            .expect_err("the drain is longer than the shutdown");
        assert_eq!(
            format!("{refused:#}"),
            "drain-timeout (60s) must not exceed shutdown-timeout (50s)"
        );
    }
}
