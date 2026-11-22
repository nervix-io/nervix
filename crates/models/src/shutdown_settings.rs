//! The deadlines a node stops within, declared as settings.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The setting definitions of the shutdown timeout and cooperative task grace, their
//!   single defaults and the group that holds them.
//! - **Depends on.** Setting classes and duration arithmetic.
//! - **Must not know.** Command-line parsing or the services a shutdown stops.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns node lifecycle setting vocabulary")
)]

use std::{convert::Infallible, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;

use crate::{NodeCapabilities, Setting, SettingDefinition};

/// How long a stopping node's work may take to move to other nodes once the node has saved it. A
/// normal shutdown ends as soon as its moves finish, so the timeout is large; a node that reaches
/// it reports a cluster disruption.
pub struct ShutdownTimeout;

impl SettingDefinition for ShutdownTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "shutdown-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// How long a listener or background task has to stop cooperatively after shutdown asks it to.
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
    pub task_grace_period: Setting<TaskGracePeriod>,
}

impl ShutdownSettings {
    /// Resolves each timeout that is not given through its definition's default.
    pub fn resolve(
        shutdown_timeout: Option<Duration>,
        task_grace_period: Option<Duration>,
    ) -> Self {
        Self {
            shutdown_timeout: Setting::<ShutdownTimeout>::resolve(shutdown_timeout, &())
                .assured("the shutdown timeout declares no validation of its own"),
            task_grace_period: Setting::<TaskGracePeriod>::resolve(task_grace_period, &())
                .assured("task grace declares no validation of its own"),
        }
    }

    pub fn shutdown_timeout(&self) -> Duration {
        *self.shutdown_timeout.value()
    }

    /// The cooperative stop interval each listener or background task is given.
    pub fn task_grace_period(&self) -> Duration {
        *self.task_grace_period.value()
    }
}

impl Default for ShutdownSettings {
    fn default() -> Self {
        Self::resolve(None, None)
    }
}

impl ShutdownTimeout {
    pub const DEFAULT: Duration = Duration::from_secs(10 * 60);
}
impl ShutdownSettings {
    pub fn capabilities(self) -> crate::ShutdownCapabilities {
        crate::ShutdownCapabilities {
            shutdown_timeout: self.shutdown_timeout(),
            task_grace_period: self.task_grace_period(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ShutdownSettings;

    #[test]
    fn a_node_gives_its_moves_ten_minutes_and_its_tasks_two_seconds_by_default() {
        let defaults = ShutdownSettings::default();
        assert_eq!(defaults.shutdown_timeout(), Duration::from_secs(600));
        assert_eq!(defaults.task_grace_period(), Duration::from_secs(2));
        assert_eq!(
            defaults.capabilities().shutdown_timeout,
            Duration::from_secs(600)
        );
    }

    #[test]
    fn given_timeouts_replace_the_defaults() {
        let configured =
            ShutdownSettings::resolve(Some(Duration::from_secs(30)), Some(Duration::from_secs(4)));
        assert_eq!(configured.shutdown_timeout(), Duration::from_secs(30));
        assert_eq!(configured.task_grace_period(), Duration::from_secs(4));
        assert_eq!(
            configured.capabilities().task_grace_period,
            Duration::from_secs(4)
        );
    }
}
