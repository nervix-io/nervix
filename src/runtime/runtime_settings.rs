//! What a runtime is built from: the node services it uses and its settings.
//!
//! Layer: data plane.
//!
//! - **Owns.** The setting definitions of the runtime state snapshot interval and the restore
//!   staging quota, their single defaults, the derivation of the quota from the volume that holds
//!   the node database, the group that holds them, and the named environment a runtime is
//!   constructed in.
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
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use ubyte::ByteUnit;

use super::DEFAULT_RESTORE_STAGING_MAX_BYTES;
use crate::ConfiguredFaultInjection;

/// The most unpublished restore checkpoint key and value bytes this node stages. Without a value
/// the node derives it from the volume that holds its database.
pub struct RestoreStagingMaxBytes;

impl SettingDefinition for RestoreStagingMaxBytes {
    type Class = NodeCapabilities;
    type Value = Option<ByteUnit>;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "restore-staging-max-bytes";
    const DERIVATION: Option<&'static str> =
        Some("the smaller of 128 GiB and the capacity of the volume that holds the node database");

    fn default_value(_: &()) -> Option<ByteUnit> {
        None
    }

    fn validate(_: &Option<ByteUnit>, _: &()) -> Result<(), Report<Infallible>> {
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
    ) -> Result<Self, Report<nervix_models::ClusterParameterError>> {
        Ok(Self {
            state_snapshot_interval: Setting::resolve(state_snapshot_interval, &())?,
            restore_staging_max_bytes: Setting::new(restore_staging_max_bytes, &())
                .assured("the local restore capability accepts every optional byte bound"),
        })
    }

    pub fn state_snapshot_interval(&self) -> Duration {
        *self.state_snapshot_interval.value()
    }

    /// The restore staging quota the operator gave, if any.
    pub fn restore_staging_max_bytes(&self) -> Option<ByteUnit> {
        *self.restore_staging_max_bytes.value()
    }

    /// The restore staging this node holds when its database lives on a volume of
    /// `database_volume`: the operator's quota, or else the smaller of the default quota and the
    /// volume, which a quota beyond it could never be staged on.
    pub fn restore_staging_quota(&self, database_volume: ByteUnit) -> ByteUnit {
        match self.restore_staging_max_bytes() {
            Some(configured) => configured,
            None => std::cmp::min(
                ByteUnit::Byte(DEFAULT_RESTORE_STAGING_MAX_BYTES),
                database_volume,
            ),
        }
    }
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self::resolve(None, None).assured("the runtime defaults have valid bounds")
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
    /// The unpublished restore checkpoint bytes the runtime's state store may stage in that
    /// database, as the node resolved them against the database's volume.
    pub(crate) restore_staging_max_bytes: ByteUnit,
    pub(crate) fault_injection: ConfiguredFaultInjection,
    /// Where the runtime stages local temporary files.
    pub(crate) temp_dir: PathBuf,
}

impl RuntimeEnvironment {
    /// An environment with no resolver, no persistence and the default temporary directory, whose
    /// admitted work goes through `executor`. A database a caller adds to it stages up to the
    /// default restore quota, since no volume was measured for it.
    pub(crate) fn transient(executor: Executor) -> Self {
        Self {
            executor,
            dns: None,
            db: None,
            restore_staging_max_bytes: ByteUnit::Byte(DEFAULT_RESTORE_STAGING_MAX_BYTES),
            fault_injection: ConfiguredFaultInjection::default(),
            temp_dir: PathBuf::from(super::DEFAULT_TEMP_DIR),
        }
    }
}

// Cluster policy definitions have one vocabulary owner. These facades hold bootstrap inputs.
pub use nervix_models::StateSnapshotInterval;
