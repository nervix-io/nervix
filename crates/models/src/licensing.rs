//! Licensing statement values and incarnation-bound connectivity observations.
//!
//! Layer: vocabulary.
//! - **Owns.** Licensing statement bodies, credential redaction and local connectivity/tenure bindings.
//! - **Depends on.** Licensing vocabulary, node identities, timestamps and serialization.
//! - **Must not know.** Parser state, transport requests, policy decisions or runtime enforcement.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{ClusterNodeIdentity, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LicensingConnectivity {
    pub last_success: Option<Timestamp>,
    pub last_failure: Option<Timestamp>,
    pub state: LicensingConnectionState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LicensingConnectivityPublication {
    pub leader: ClusterNodeIdentity,
    pub term: u64,
    pub observation: LicensingConnectivity,
}

/// Only the current observation travels through membership-scoped gossip; it grants no rights.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum LicensingConnectionState {
    #[default]
    Unknown,
    Reachable,
    Unreachable {
        since: Timestamp,
        last_failure: LicensingTransportFailure,
    },
    Refusing {
        since: Timestamp,
        last_refusal: nervix_licensing_vocabulary::ProblemCode,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LicensingTransportFailure {
    Transport,
    Timeout,
}

/// An activation credential is transient input; durable execution retains only verified output.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct ActivateLicense {
    pub kind: nervix_licensing_vocabulary::ActivationKind,
    pub license_key: String,
}

impl std::fmt::Debug for ActivateLicense {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActivateLicense")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RefreshLicense;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ReleaseLicense;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RotateLicenseClusterKey;

impl Drop for ActivateLicense {
    fn drop(&mut self) {
        zeroize::Zeroize::zeroize(&mut self.license_key);
    }
}
