//! The deadlines a node stops within, declared as settings.
//!
//! Layer: control plane.
//!
//! - **Owns.** The setting definitions of the shutdown and drain timeouts, their single defaults
//!   and the group that holds them.
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

use super::{DEFAULT_DRAIN_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT};

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

/// How long a stopping node waits for its admitted work to drain before it continues.
pub struct DrainTimeout;

impl SettingDefinition for DrainTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "drain-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        DEFAULT_DRAIN_TIMEOUT
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
}

impl ShutdownSettings {
    /// Resolves each timeout that is not given through its definition's default.
    pub fn resolve(
        shutdown_timeout: Option<Duration>,
        drain_timeout: Option<Duration>,
    ) -> Result<Self, Report<Infallible>> {
        let shutdown_timeout = Setting::resolve(shutdown_timeout, &())?;
        let drain_timeout = Setting::resolve(drain_timeout, &())?;
        Ok(Self {
            shutdown_timeout,
            drain_timeout,
        })
    }

    pub fn shutdown_timeout(&self) -> Duration {
        *self.shutdown_timeout.value()
    }

    pub fn drain_timeout(&self) -> Duration {
        *self.drain_timeout.value()
    }
}

impl Default for ShutdownSettings {
    fn default() -> Self {
        Self::resolve(None, None).assured("the shutdown defaults declare no validation")
    }
}
