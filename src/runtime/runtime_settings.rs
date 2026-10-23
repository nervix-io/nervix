//! What a runtime is built from: the node services it uses and its settings.
//!
//! Layer: data plane.
//!
//! - **Owns.** The setting definitions of the runtime state snapshot interval and the restore
//!   staging quota, their single defaults, the group that holds them, and the named environment a
//!   runtime is constructed in.
//! - **Depends on.** The vocabulary's setting classes and the state store's and state
//!   replication's defaults.
//! - **Must not know.** Command-line parsing or the control plane that configures a node.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{convert::Infallible, path::PathBuf, time::Duration};

use error_stack::Report;
use fjall::Database;
use meticulous::ResultExt as _;
use nervix_dns::DnsResolver;
use nervix_execution::Executor;
use nervix_models::{ClusterParameters, NodeCapabilities, Setting, SettingDefinition};
use ubyte::ByteUnit;

use super::{DEFAULT_RESTORE_STAGING_MAX_BYTES, DEFAULT_STATE_SNAPSHOT_INTERVAL};
use crate::ConfiguredFaultInjection;

/// How often the owner of a piece of runtime state publishes a snapshot of it to its replicas.
pub struct StateSnapshotInterval;

impl SettingDefinition for StateSnapshotInterval {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "state-snapshot-interval";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        DEFAULT_STATE_SNAPSHOT_INTERVAL
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The most unpublished restore checkpoint key and value bytes this node stages.
pub struct RestoreStagingMaxBytes;

impl SettingDefinition for RestoreStagingMaxBytes {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "restore-staging-max-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Byte(DEFAULT_RESTORE_STAGING_MAX_BYTES)
    }

    fn validate(_: &ByteUnit, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The settings one runtime is constructed with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSettings {
    pub state_snapshot_interval: Setting<StateSnapshotInterval>,
    pub restore_staging_max_bytes: Setting<RestoreStagingMaxBytes>,
}

impl RuntimeSettings {
    /// Resolves each value that is not given through its definition's default.
    pub fn resolve(
        state_snapshot_interval: Option<Duration>,
        restore_staging_max_bytes: Option<ByteUnit>,
    ) -> Result<Self, Report<Infallible>> {
        Ok(Self {
            state_snapshot_interval: Setting::resolve(state_snapshot_interval, &())?,
            restore_staging_max_bytes: Setting::resolve(restore_staging_max_bytes, &())?,
        })
    }

    pub fn state_snapshot_interval(&self) -> Duration {
        *self.state_snapshot_interval.value()
    }

    pub fn restore_staging_max_bytes(&self) -> u64 {
        self.restore_staging_max_bytes.value().as_u64()
    }
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self::resolve(None, None).assured("the runtime defaults declare no validation")
    }
}

/// The node services and local paths a runtime is constructed in.
pub(crate) struct RuntimeEnvironment {
    /// The node's bounded execution and transient-memory admission.
    pub(crate) executor: Executor,
    /// The node's resolver, which connectors resolve host names through.
    pub(crate) dns: Option<DnsResolver>,
    /// The node database the runtime persists its state in, absent for a runtime that keeps none.
    pub(crate) db: Option<Database>,
    pub(crate) fault_injection: ConfiguredFaultInjection,
    /// Where the runtime stages local temporary files.
    pub(crate) temp_dir: PathBuf,
}

impl RuntimeEnvironment {
    /// An environment with no resolver, no persistence and the default temporary directory, whose
    /// admitted work goes through `executor`.
    pub(crate) fn transient(executor: Executor) -> Self {
        Self {
            executor,
            dns: None,
            db: None,
            fault_injection: ConfiguredFaultInjection::default(),
            temp_dir: PathBuf::from(super::DEFAULT_TEMP_DIR),
        }
    }
}
