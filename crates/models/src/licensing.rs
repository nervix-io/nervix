//! Incarnation-bound licensing connectivity observations.
//!
//! Layer: vocabulary.
//! - **Owns.** Typed local connectivity and a reporting leader's tenure binding.
//! - **Depends on.** Node identities, timestamps and serialization.
//! - **Must not know.** Decisions, grants, service requests, gossip or policy evaluation.

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
