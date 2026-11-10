//! The condition catalog, resolution and canonical condition identity.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Condition values, scopes, completeness, overrides and the free usage policy.
//! - **Depends on.** Typed JSON, bounded serialization and the cryptography boundary.
//! - **Must not know.** Graphs, usage admission, databases, clocks or network clients.

use std::{collections::BTreeMap, num::NonZeroUsize};

use chrono::{DateTime, Utc};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
pub use nervix_licensing_vocabulary::Quota;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{document::encode_json, documents::DecisionStatus};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationKind {
    Persistent,
    Ephemeral,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum Capability {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConditionDuration {
    Bounded { seconds: u64 },
    Unbounded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OfflineLapsePolicy {
    Freeze,
}

/// Unknown condition names intentionally retain their complete external value, including a
/// value grammar introduced after this receiver. A known name never accepts an extension value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConditionValue<T> {
    Known(T),
    Extension(Value),
}

impl<T> ConditionValue<T> {
    pub fn known(&self) -> Option<&T> {
        match self {
            Self::Known(value) => Some(value),
            Self::Extension(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageConditions {
    pub capabilities: BTreeMap<String, ConditionValue<Capability>>,
    pub quotas: BTreeMap<String, ConditionValue<Quota>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatingConditions {
    pub quotas: BTreeMap<String, ConditionValue<Quota>>,
    pub durations: BTreeMap<String, ConditionValue<ConditionDuration>>,
    pub policies: BTreeMap<String, ConditionValue<OfflineLapsePolicy>>,
    pub sets: BTreeMap<String, ConditionValue<Vec<ActivationKind>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Conditions {
    pub usage: UsageConditions,
    pub operating: OperatingConditions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Cluster,
    Organization,
    License,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConditionKind {
    Capability,
    Quota,
    Duration,
    Policy,
    Set,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConditionShape {
    Quota {
        scope: Scope,
        affects_existing: bool,
    },
    Duration {
        allows_unbounded: bool,
    },
    Policy,
    ActivationKinds,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConditionDefinition {
    pub name: &'static str,
    pub shape: ConditionShape,
    pub owner: &'static str,
}

impl ConditionDefinition {
    pub const fn kind(self) -> ConditionKind {
        match self.shape {
            ConditionShape::Quota { .. } => ConditionKind::Quota,
            ConditionShape::Duration { .. } => ConditionKind::Duration,
            ConditionShape::Policy => ConditionKind::Policy,
            ConditionShape::ActivationKinds => ConditionKind::Set,
        }
    }

    pub const fn scope(self) -> Scope {
        match self.shape {
            ConditionShape::Quota { scope, .. } => scope,
            _ => Scope::Organization,
        }
    }
}

pub const CLUSTER_NODES: &str = nervix_licensing_vocabulary::QuotaName::ClusterNodes.as_str();
pub const CLUSTER_DOMAINS: &str = nervix_licensing_vocabulary::QuotaName::ClusterDomains.as_str();
pub const PERSISTENT_ACTIVATIONS: &str = "license.activations.persistent";
pub const EPHEMERAL_ACTIVATIONS: &str = "license.activations.ephemeral";
pub const ACTIVATION_KINDS: &str = "activation.kinds";
pub const EPHEMERAL_LIFETIME: &str = "ephemeral.max_lifetime";
pub const REFRESH_INTERVAL: &str = "refresh.interval";
pub const REFRESH_OVERDUE: &str = "refresh.overdue_after";
pub const OFFLINE_ALLOWANCE: &str = "offline.allowance";
pub const OFFLINE_LAPSE: &str = "offline.on_lapse";
pub const CAPABILITY_GRACE: &str = "transition.capability_removal_grace";
pub const QUOTA_GRACE: &str = "transition.quota_reduction_grace";

/// The current catalog is finite and read by every resolver; a new name lands with its owner.
pub const CATALOG: &[ConditionDefinition] = &[
    ConditionDefinition {
        name: CLUSTER_NODES,
        shape: ConditionShape::Quota {
            scope: Scope::Cluster,
            affects_existing: true,
        },
        owner: "membership admission; committed voters and learners",
    },
    ConditionDefinition {
        name: CLUSTER_DOMAINS,
        shape: ConditionShape::Quota {
            scope: Scope::Cluster,
            affects_existing: true,
        },
        owner: "domain admission; committed domain records",
    },
    ConditionDefinition {
        name: PERSISTENT_ACTIVATIONS,
        shape: ConditionShape::Quota {
            scope: Scope::License,
            affects_existing: false,
        },
        owner: "service activation capacity",
    },
    ConditionDefinition {
        name: EPHEMERAL_ACTIVATIONS,
        shape: ConditionShape::Quota {
            scope: Scope::License,
            affects_existing: false,
        },
        owner: "service activation capacity",
    },
    ConditionDefinition {
        name: ACTIVATION_KINDS,
        shape: ConditionShape::ActivationKinds,
        owner: "service activation admission",
    },
    ConditionDefinition {
        name: EPHEMERAL_LIFETIME,
        shape: ConditionShape::Duration {
            allows_unbounded: false,
        },
        owner: "service activation creation",
    },
    ConditionDefinition {
        name: REFRESH_INTERVAL,
        shape: ConditionShape::Duration {
            allows_unbounded: false,
        },
        owner: "service decision timing",
    },
    ConditionDefinition {
        name: REFRESH_OVERDUE,
        shape: ConditionShape::Duration {
            allows_unbounded: false,
        },
        owner: "service decision timing",
    },
    ConditionDefinition {
        name: OFFLINE_ALLOWANCE,
        shape: ConditionShape::Duration {
            allows_unbounded: true,
        },
        owner: "service decision timing",
    },
    ConditionDefinition {
        name: OFFLINE_LAPSE,
        shape: ConditionShape::Policy,
        owner: "cluster admission freeze",
    },
    ConditionDefinition {
        name: CAPABILITY_GRACE,
        shape: ConditionShape::Duration {
            allows_unbounded: false,
        },
        owner: "cluster transition arithmetic",
    },
    ConditionDefinition {
        name: QUOTA_GRACE,
        shape: ConditionShape::Duration {
            allows_unbounded: false,
        },
        owner: "cluster transition arithmetic",
    },
];

#[derive(Debug, Error)]
pub enum ConditionError {
    #[error("condition {name} is missing")]
    Missing { name: String },
    #[error("condition {name} has an invalid value or section")]
    Invalid { name: String },
    #[error("condition {name} is not in the service's catalog")]
    Unknown { name: String },
    #[error("failed to encode the resolved conditions")]
    Encoding,
    #[error("the license term or activation timing cannot produce a current decision")]
    Timing,
}

pub struct Catalog;

impl Catalog {
    pub fn definition(name: &str) -> Option<ConditionDefinition> {
        // Exactly twelve fixed protocol declarations, independent of graph/organization size.
        CATALOG.iter().find(|entry| entry.name == name).copied()
    }

    fn validate_map<T>(
        values: &BTreeMap<String, ConditionValue<T>>,
        kind: ConditionKind,
        cluster: bool,
        allow_unknown: bool,
    ) -> Result<usize, Report<ConditionError>> {
        let mut unknown = 0usize;
        for (name, value) in values {
            let Some(definition) = Self::definition(name) else {
                if !allow_unknown {
                    return Err(Report::new(ConditionError::Unknown { name: name.clone() }));
                }
                unknown = unknown
                    .checked_add(1)
                    .assured("unknown entries are bounded by map length");
                continue;
            };
            if definition.kind() != kind
                || (definition.scope() == Scope::Cluster) != cluster
                || value.known().is_none()
            {
                return Err(Report::new(ConditionError::Invalid { name: name.clone() }));
            }
        }
        Ok(unknown)
    }

    pub fn validate(
        conditions: &Conditions,
        complete: bool,
        allow_unknown: bool,
        stopped: bool,
    ) -> Result<usize, Report<ConditionError>> {
        let usage = &conditions.usage;
        let operating = &conditions.operating;
        Self::validate_parts(usage, operating, complete, allow_unknown, stopped)
    }

    pub fn validate_parts(
        usage: &UsageConditions,
        operating: &OperatingConditions,
        complete: bool,
        allow_unknown: bool,
        stopped: bool,
    ) -> Result<usize, Report<ConditionError>> {
        let counts = [
            Self::validate_map(
                &usage.capabilities,
                ConditionKind::Capability,
                true,
                allow_unknown,
            )?,
            Self::validate_map(&usage.quotas, ConditionKind::Quota, true, allow_unknown)?,
            Self::validate_map(
                &operating.quotas,
                ConditionKind::Quota,
                false,
                allow_unknown,
            )?,
            Self::validate_map(
                &operating.durations,
                ConditionKind::Duration,
                false,
                allow_unknown,
            )?,
            Self::validate_map(
                &operating.policies,
                ConditionKind::Policy,
                false,
                allow_unknown,
            )?,
            Self::validate_map(&operating.sets, ConditionKind::Set, false, allow_unknown)?,
        ];
        if stopped && (!usage.quotas.is_empty() || !usage.capabilities.is_empty()) {
            return Err(Report::new(ConditionError::Invalid {
                name: "usage".to_owned(),
            }));
        }
        for definition in CATALOG {
            if stopped && definition.scope() == Scope::Cluster {
                continue;
            }
            let present = match definition.shape {
                ConditionShape::Quota {
                    scope: Scope::Cluster,
                    ..
                } => usage.quotas.contains_key(definition.name),
                ConditionShape::Quota { .. } => operating.quotas.contains_key(definition.name),
                ConditionShape::Duration { allows_unbounded } => {
                    if let Some(ConditionValue::Known(ConditionDuration::Unbounded)) =
                        operating.durations.get(definition.name)
                        && !allows_unbounded
                    {
                        return Err(Report::new(ConditionError::Invalid {
                            name: definition.name.to_owned(),
                        }));
                    }
                    operating.durations.contains_key(definition.name)
                }
                ConditionShape::Policy => operating.policies.contains_key(definition.name),
                ConditionShape::ActivationKinds => {
                    if let Some(ConditionValue::Known(kinds)) = operating.sets.get(definition.name)
                    {
                        let unique: std::collections::BTreeSet<_> = kinds.iter().collect();
                        if unique.len() != kinds.len() {
                            return Err(Report::new(ConditionError::Invalid {
                                name: definition.name.to_owned(),
                            }));
                        }
                    }
                    operating.sets.contains_key(definition.name)
                }
            };
            if complete && !present {
                return Err(Report::new(ConditionError::Missing {
                    name: definition.name.to_owned(),
                }));
            }
        }
        let mut total = 0usize;
        for count in counts {
            total = total
                .checked_add(count)
                .assured("counts partition allocated entries");
        }
        Ok(total)
    }
}

impl UsageConditions {
    pub fn quota(&self, name: &str) -> Result<Quota, Report<ConditionError>> {
        match self.quotas.get(name) {
            Some(ConditionValue::Known(value)) => Ok(*value),
            Some(ConditionValue::Extension(_)) => Err(Report::new(ConditionError::Invalid {
                name: name.to_owned(),
            })),
            None => Err(Report::new(ConditionError::Missing {
                name: name.to_owned(),
            })),
        }
    }
}

impl OperatingConditions {
    pub fn duration(&self, name: &str) -> Result<ConditionDuration, Report<ConditionError>> {
        match self.durations.get(name) {
            Some(ConditionValue::Known(value)) => Ok(*value),
            Some(ConditionValue::Extension(_)) => Err(Report::new(ConditionError::Invalid {
                name: name.to_owned(),
            })),
            None => Err(Report::new(ConditionError::Missing {
                name: name.to_owned(),
            })),
        }
    }
}

/// A free cluster has no operating half and grants every current cluster quota without a limit.
pub const FREE_POLICY: FreePolicy = FreePolicy;

#[derive(Clone, Copy, Debug)]
pub struct FreePolicy;

impl FreePolicy {
    pub fn usage(self) -> UsageConditions {
        let mut quotas = BTreeMap::new();
        for definition in CATALOG {
            if let ConditionShape::Quota {
                scope: Scope::Cluster,
                ..
            } = definition.shape
            {
                quotas.insert(
                    definition.name.to_owned(),
                    ConditionValue::Known(Quota::Unlimited),
                );
            }
        }
        UsageConditions {
            capabilities: BTreeMap::new(),
            quotas,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LicenseStatus {
    Active,
    Suspended,
    Revoked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrganizationStatus {
    Active,
    Suspended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationStatus {
    Active,
    Released,
    Revoked,
    Transferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationLifetime {
    Persistent,
    Ephemeral { expires_at: DateTime<Utc> },
}

#[derive(Clone, Copy, Debug)]
pub struct ResolutionFacts {
    pub organization_status: OrganizationStatus,
    pub license_status: LicenseStatus,
    pub term_starts_at: DateTime<Utc>,
    pub term_ends_at: DateTime<Utc>,
    pub activation_status: ActivationStatus,
    pub created_at: DateTime<Utc>,
    pub lifetime: ActivationLifetime,
    pub at: DateTime<Utc>,
}

impl ResolutionFacts {
    pub fn status(self) -> Result<DecisionStatus, Report<ConditionError>> {
        if self.term_starts_at > self.term_ends_at || self.at < self.created_at {
            return Err(Report::new(ConditionError::Timing));
        }
        if let ActivationLifetime::Ephemeral { expires_at } = self.lifetime {
            if expires_at < self.created_at {
                return Err(Report::new(ConditionError::Timing));
            }
            if self.at >= expires_at {
                return Ok(DecisionStatus::Expired);
            }
        }
        match self.activation_status {
            ActivationStatus::Released => return Ok(DecisionStatus::Released),
            ActivationStatus::Revoked => return Ok(DecisionStatus::Revoked),
            ActivationStatus::Transferred => return Ok(DecisionStatus::Transferred),
            ActivationStatus::Active => {}
        }
        match self.license_status {
            LicenseStatus::Suspended => return Ok(DecisionStatus::Suspended),
            LicenseStatus::Revoked => return Ok(DecisionStatus::Revoked),
            LicenseStatus::Active => {}
        }
        if self.organization_status == OrganizationStatus::Suspended {
            return Ok(DecisionStatus::Suspended);
        }
        if self.at < self.term_starts_at {
            return Err(Report::new(ConditionError::Timing));
        }
        if self.at >= self.term_ends_at {
            return Ok(DecisionStatus::TermEnded);
        }
        Ok(DecisionStatus::Active)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedConditions {
    status: DecisionStatus,
    values: Conditions,
    conditions_digest: [u8; 32],
}

impl ResolvedConditions {
    pub const fn status(&self) -> DecisionStatus {
        self.status
    }
    pub fn values(&self) -> &Conditions {
        &self.values
    }
    pub const fn conditions_digest(&self) -> &[u8; 32] {
        &self.conditions_digest
    }
}

impl Conditions {
    pub fn resolve(
        &self,
        overrides: &Self,
        facts: ResolutionFacts,
        limit: NonZeroUsize,
    ) -> Result<ResolvedConditions, Report<ConditionError>> {
        Catalog::validate(self, true, false, false)?;
        Catalog::validate(overrides, false, false, false)?;
        let mut result = self.clone();
        result
            .usage
            .capabilities
            .extend(overrides.usage.capabilities.clone());
        result.usage.quotas.extend(overrides.usage.quotas.clone());
        result
            .operating
            .quotas
            .extend(overrides.operating.quotas.clone());
        result
            .operating
            .durations
            .extend(overrides.operating.durations.clone());
        result
            .operating
            .policies
            .extend(overrides.operating.policies.clone());
        result
            .operating
            .sets
            .extend(overrides.operating.sets.clone());
        result.operating.validate_timing()?;
        let status = facts.status()?;
        match status {
            DecisionStatus::Active => {}
            DecisionStatus::Expired => {
                result.usage = UsageConditions {
                    capabilities: BTreeMap::new(),
                    quotas: BTreeMap::new(),
                }
            }
            _ => result.usage = FREE_POLICY.usage(),
        }
        let conditions_digest = result.digest(limit)?;
        Ok(ResolvedConditions {
            status,
            values: result,
            conditions_digest,
        })
    }

    pub fn digest(&self, limit: NonZeroUsize) -> Result<[u8; 32], Report<ConditionError>> {
        let bytes = encode_json(self, limit).change_context(ConditionError::Encoding)?;
        Ok(nervix_crypto::digest::sha256(&bytes))
    }
}

impl OperatingConditions {
    fn validate_timing(&self) -> Result<(), Report<ConditionError>> {
        let ConditionDuration::Bounded { seconds: refresh } = self.duration(REFRESH_INTERVAL)?
        else {
            return Err(Report::new(ConditionError::Timing));
        };
        let ConditionDuration::Bounded { seconds: overdue } = self.duration(REFRESH_OVERDUE)?
        else {
            return Err(Report::new(ConditionError::Timing));
        };
        let Some(overdue_after_issue) = refresh.checked_add(overdue) else {
            return Err(Report::new(ConditionError::Timing));
        };
        if let ConditionDuration::Bounded { seconds: offline } = self.duration(OFFLINE_ALLOWANCE)?
            && offline < overdue_after_issue
        {
            return Err(Report::new(ConditionError::Timing));
        }
        Ok(())
    }
}
