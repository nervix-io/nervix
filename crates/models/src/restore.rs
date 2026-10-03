//! The restore statements, the steps a restore applies, and the report it returns.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The `RESTORE` Model, the policy for users that already exist, the steps a restore
//!   applies in their fixed order, and the report a restore answers and its outcome records.
//! - **Depends on.** Vocabulary names, timestamps, archive digests, and the transaction impact
//!   report a dry run carries.
//! - **Must not know.** The archive's encoding, how an archive travels to the leader or is staged
//!   there, or how a step reaches consensus.

use std::{fmt, num::NonZeroU64};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use crate::{ArchiveDigest, DomainName, Timestamp, TransactionImpactReport};

/// `RESTORE CLUSTER FROM '<file>' [ON EXISTING USER FAIL | SKIP | REPLACE] [DRY RUN]` or
/// `RESTORE DOMAIN <name> [AS <new_name>] FROM '<file>' [DRY RUN]`.
///
/// The client that sent the statement reads the archive from `source` and streams it to the
/// leader, which never reads the path.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct Restore {
    pub scope: RestoreScope,
    /// The local archive file the client reads.
    pub source: String,
    pub mode: RestoreMode,
}

/// What a restore recreates.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RestoreScope {
    /// Every user and every domain of a cluster archive.
    Cluster { existing_users: ExistingUserPolicy },
    /// One domain of an archive, restored under its archived name or under `target`.
    Domain {
        domain: DomainName,
        /// `AS <new_name>`. Absent restores the domain under its archived name.
        target: Option<DomainName>,
    },
}

impl RestoreScope {
    /// The name a domain named `archived` in the archive is restored under.
    pub fn target_of<'name>(&'name self, archived: &'name DomainName) -> &'name DomainName {
        match self {
            Self::Cluster { .. } => archived,
            Self::Domain {
                target: Some(target),
                ..
            } => target,
            Self::Domain { target: None, .. } => archived,
        }
    }
}

/// What a cluster restore does with an archived user whose name the cluster already has.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum ExistingUserPolicy {
    /// Refuses the whole restore before it writes anything.
    #[default]
    Fail,
    /// Keeps the existing user and its password.
    Skip,
    /// Replaces the existing user's password hash with the archived one.
    Replace,
}

/// Whether a restore applies what it plans.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum RestoreMode {
    Apply,
    /// `DRY RUN`: verify and plan, and change nothing.
    DryRun,
}

/// The archive a restore reads, as its client declared it before sending a byte.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct RestoreArchive {
    /// The archive file's exact size.
    pub total_bytes: NonZeroU64,
    /// The BLAKE3 digest of the whole archive file.
    pub digest: ArchiveDigest,
}

/// One step of a restore that changes the cluster. A restore applies its steps in a fixed order:
/// the users, then for each domain its creation, its resource versions, and its models.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord))]
pub enum RestoreStep {
    /// Importing the archived users under the restore's policy.
    Users,
    /// Creating the domain, stopped, with its declared resources.
    CreateDomain(DomainName),
    /// Importing every completed resource version of the domain under its archived number.
    ImportResources(DomainName),
    /// Applying the domain's models.
    ApplyModels(DomainName),
}

impl fmt::Display for RestoreStep {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Users => formatter.write_str("import users"),
            Self::CreateDomain(domain) => write!(formatter, "create domain '{domain}'"),
            Self::ImportResources(domain) => {
                write!(formatter, "import resource versions of domain '{domain}'")
            }
            Self::ApplyModels(domain) => write!(formatter, "apply models of domain '{domain}'"),
        }
    }
}

/// What became of one step of a restore.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum RestoreStepOutcome {
    /// The step's effects are in place.
    Applied,
    /// A dry run planned the step and applied nothing.
    Planned,
    /// The step failed. The steps before it stay applied, and none after it was attempted.
    Failed,
    /// An earlier step failed before this one was reached.
    NotAttempted,
}

/// One step of a restore and what became of it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RestoreStepReport {
    pub step: RestoreStep,
    pub outcome: RestoreStepOutcome,
}

/// The users a cluster restore imports, by what it does with each.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct RestoredUsers {
    /// Archived users the cluster did not have.
    pub created: u64,
    /// Existing users `ON EXISTING USER SKIP` kept.
    pub skipped: u64,
    /// Existing users `ON EXISTING USER REPLACE` gave the archived password hash.
    pub replaced: u64,
}

/// One domain a restore recreates.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RestoredDomain {
    /// The domain's name in the archive.
    pub source: DomainName,
    /// The name the domain is restored under.
    pub domain: DomainName,
    /// The completed resource versions the restore imports, each under its archived number.
    pub resource_versions: u64,
    /// The models the restore applies.
    pub models: u64,
    /// For a dry run, the transaction planner's report on the domain's model run. Absent when the
    /// restore applies, and when the domain holds no models.
    pub planned_models: Option<TransactionImpactReport>,
}

/// What a restore did, or for a dry run what it would do.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RestoreReport {
    pub mode: RestoreMode,
    pub archive: RestoreArchive,
    /// When the backup that wrote the archive read its contents.
    pub captured_at: Timestamp,
    /// The users a cluster restore imports. Absent for a domain restore, which imports none.
    pub users: Option<RestoredUsers>,
    /// Every domain the restore recreates, in archive order.
    pub domains: Vec<RestoredDomain>,
    /// Every step, in the order the restore applies them.
    pub steps: Vec<RestoreStepReport>,
}

impl RestoreReport {
    /// The step that failed, when one did.
    pub fn failed_step(&self) -> Option<&RestoreStep> {
        // A report holds one entry per step of its restore, three for each domain and one for the
        // users, which is what bounds this walk.
        for report in &self.steps {
            if report.outcome == RestoreStepOutcome::Failed {
                return Some(&report.step);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain is an identifier-shaped literal")
    }

    #[test]
    fn a_domain_restores_under_its_target_name_when_one_is_given() {
        let archived = domain("prod");
        let renamed = RestoreScope::Domain {
            domain: archived.clone(),
            target: Some(domain("prod_copy")),
        };
        assert_eq!(renamed.target_of(&archived).as_str(), "prod_copy");
        let same = RestoreScope::Domain {
            domain: archived.clone(),
            target: None,
        };
        assert_eq!(same.target_of(&archived).as_str(), "prod");
        let cluster = RestoreScope::Cluster {
            existing_users: ExistingUserPolicy::Fail,
        };
        assert_eq!(cluster.target_of(&archived).as_str(), "prod");
    }

    #[test]
    fn steps_name_what_they_change() {
        assert_eq!(RestoreStep::Users.to_string(), "import users");
        assert_eq!(
            RestoreStep::CreateDomain(domain("prod")).to_string(),
            "create domain 'prod'"
        );
        assert_eq!(
            RestoreStep::ImportResources(domain("prod")).to_string(),
            "import resource versions of domain 'prod'"
        );
        assert_eq!(
            RestoreStep::ApplyModels(domain("prod")).to_string(),
            "apply models of domain 'prod'"
        );
    }

    #[test]
    fn a_report_names_its_failed_step() {
        let prod = domain("prod");
        let report = RestoreReport {
            mode: RestoreMode::Apply,
            archive: RestoreArchive {
                total_bytes: NonZeroU64::MIN,
                digest: ArchiveDigest::from_bytes([7; 32]),
            },
            captured_at: Timestamp::from_unix_nanos(0),
            users: None,
            domains: Vec::new(),
            steps: vec![
                RestoreStepReport {
                    step: RestoreStep::CreateDomain(prod.clone()),
                    outcome: RestoreStepOutcome::Applied,
                },
                RestoreStepReport {
                    step: RestoreStep::ImportResources(prod.clone()),
                    outcome: RestoreStepOutcome::Failed,
                },
                RestoreStepReport {
                    step: RestoreStep::ApplyModels(prod.clone()),
                    outcome: RestoreStepOutcome::NotAttempted,
                },
            ],
        };
        assert_eq!(
            report.failed_step(),
            Some(&RestoreStep::ImportResources(prod))
        );
    }
}
