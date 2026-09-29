//! Restoring a backup into the replicated state, one recorded step at a time.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The durable progress of an applying restore, the effect of each restore step and
//!   the rule that applies a step at most once, and importing a resource version under the number
//!   an archive recorded for it.
//! - **Depends on.** The command execution records, the domain mutation leases, the resource
//!   catalog records, and vocabulary Models.
//! - **Must not know.** Archives, sessions, how a leader plans a restore, or how it installs the
//!   bytes of a resource version.
//!
//! A step's effect and the record that it happened are one replicated command. A step already
//! recorded applies nothing, so a leader that resumes a restore after an earlier leader's proposal
//! of the same step committed, or after its own reply was lost, never repeats the step's effect.

use std::{collections::BTreeSet, num::NonZeroU64};

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::{
    CommandExecutionReference, DomainName, DomainState, ExistingUserPolicy, ResourceName,
    ResourceNodeStatus, ResourceUploadKey, ResourceVersion, Restore, RestoreArchive, RestoreScope,
    RestoreStep, RestoredUsers, UserName,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{StateMachineChanges, StateMachineData, UserCredentials, validate_domain_mutation};

/// A restore as its command execution records it while it applies.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RestoreExecution {
    pub restore: Restore,
    /// The archive the restore was admitted with. A leader that resumes the restore reads only an
    /// archive of this size and digest.
    pub archive: RestoreArchive,
    /// Every step whose effects are applied.
    completed: BTreeSet<RestoreStep>,
    /// What the users step did. Absent until it applies, and for a domain restore.
    users: Option<RestoredUsers>,
}

impl RestoreExecution {
    pub fn new(restore: Restore, archive: RestoreArchive) -> Self {
        Self {
            restore,
            archive,
            completed: BTreeSet::new(),
            users: None,
        }
    }

    /// Whether `step`'s effects are applied.
    pub fn is_recorded(&self, step: &RestoreStep) -> bool {
        self.completed.contains(step)
    }

    /// What the users step did, once it applied.
    pub fn restored_users(&self) -> Option<RestoredUsers> {
        self.users
    }

    /// Every step whose effects are applied.
    pub fn completed_steps(&self) -> &BTreeSet<RestoreStep> {
        &self.completed
    }

    /// The step that must be recorded before `step` may apply, when it is not recorded yet. A
    /// cluster restore imports its users before it creates any domain, and every domain is
    /// created, then given its resource versions, then its models.
    fn missing_prerequisite(&self, step: &RestoreStep) -> Option<RestoreStep> {
        let prerequisite = match step {
            RestoreStep::Users => return None,
            RestoreStep::CreateDomain(_) => match &self.restore.scope {
                RestoreScope::Cluster { .. } => RestoreStep::Users,
                RestoreScope::Domain { .. } => return None,
            },
            RestoreStep::ImportResources(domain) => RestoreStep::CreateDomain(domain.clone()),
            RestoreStep::ApplyModels(domain) => RestoreStep::ImportResources(domain.clone()),
        };
        if self.completed.contains(&prerequisite) {
            return None;
        }
        Some(prerequisite)
    }

    pub(crate) fn record(&mut self, step: RestoreStep, users: Option<RestoredUsers>) {
        if let Some(users) = users {
            self.users = Some(users);
        }
        self.completed.insert(step);
    }
}

/// What one restore step changes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RestoreStepEffect {
    /// Imports the archived users under the restore's policy, with their password hashes as the
    /// archive holds them.
    Users {
        users: Vec<UserCredentials>,
        policy: ExistingUserPolicy,
    },
    /// Creates the domain and declares each of its resources.
    Domain {
        state: Box<DomainState>,
        resources: Vec<RestoredResource>,
    },
    /// Records a step whose effects commands of their own applied: the domain's resource versions,
    /// or its models.
    Completion,
}

/// A resource a restored domain declares, with the version its next upload receives. Every
/// version below it is either restored under its archived number or stays assigned to an upload
/// the archive records as failed.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RestoredResource {
    pub resource: ResourceName,
    pub next_version: NonZeroU64,
}

/// Why the replicated state refused a restore step or an imported resource version.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RestoreStepConflict {
    #[error("command execution '{reference}' is not an applying restore")]
    NotARestore {
        reference: CommandExecutionReference,
    },
    #[error("the restore step to {step} carries the effect of another step")]
    EffectMismatch { step: RestoreStep },
    #[error("the restore step to {step} cannot apply before the step to {missing}")]
    OutOfOrder {
        step: RestoreStep,
        missing: RestoreStep,
    },
    #[error("user '{user}' already exists")]
    UserExists { user: UserName },
    #[error("domain '{domain}' already exists")]
    DomainExists { domain: DomainName },
    #[error("resource '{resource}' is already declared in domain '{domain}'")]
    CatalogExists {
        domain: DomainName,
        resource: ResourceName,
    },
    #[error("the restore holds no mutation lease for domain '{domain}'")]
    MissingLease { domain: DomainName },
    #[error("the restore's mutation lease for domain '{domain}' is not authoritative")]
    Lease { domain: DomainName },
    #[error("version {version} of resource '{resource}' in domain '{domain}' cannot be imported")]
    Import {
        domain: DomainName,
        resource: ResourceName,
        version: u64,
    },
    #[error("the restore already recorded every resource version of domain '{domain}'")]
    ResourcesRecorded { domain: DomainName },
}

/// Applies `step` of the restore `reference` names and records it, unless it is already recorded,
/// in which case nothing changes.
pub(crate) fn apply_restore_step(
    state: &mut StateMachineData,
    reference: &CommandExecutionReference,
    step: &RestoreStep,
    effect: &RestoreStepEffect,
    changes: &mut StateMachineChanges,
) -> Result<(), Report<RestoreStepConflict>> {
    let execution = applying_restore(state, reference)?;
    if execution.is_recorded(step) {
        return Ok(());
    }
    if let Some(missing) = execution.missing_prerequisite(step) {
        return Err(Report::new(RestoreStepConflict::OutOfOrder {
            step: step.clone(),
            missing,
        }));
    }
    let users = match (step, effect) {
        (RestoreStep::Users, RestoreStepEffect::Users { users, policy }) => {
            Some(import_users(state, users, *policy)?)
        }
        (
            RestoreStep::CreateDomain(domain),
            RestoreStepEffect::Domain {
                state: domain_state,
                resources,
            },
        ) if domain_state.id == *domain => {
            create_domain(state, reference, domain_state, resources, changes)?;
            None
        }
        (
            RestoreStep::ImportResources(_) | RestoreStep::ApplyModels(_),
            RestoreStepEffect::Completion,
        ) => None,
        (
            RestoreStep::Users
            | RestoreStep::CreateDomain(_)
            | RestoreStep::ImportResources(_)
            | RestoreStep::ApplyModels(_),
            RestoreStepEffect::Users { .. }
            | RestoreStepEffect::Domain { .. }
            | RestoreStepEffect::Completion,
        ) => {
            return Err(Report::new(RestoreStepConflict::EffectMismatch {
                step: step.clone(),
            }));
        }
    };
    let mut recorded = state
        .command_executions
        .get(reference)
        .cloned()
        .ok_or_else(|| {
            Report::new(RestoreStepConflict::NotARestore {
                reference: reference.clone(),
            })
        })?;
    recorded.record_restore_step(step.clone(), users);
    state.command_executions.replace(recorded);
    Ok(())
}

/// Records `resource` as version `resource.id.version` of an applying restore's domain, published
/// with the leader's ready copy `replica`. Importing the same version under the same key again
/// changes nothing.
pub(crate) fn import_resource_version(
    state: &mut StateMachineData,
    reference: &CommandExecutionReference,
    key: &ResourceUploadKey,
    resource: &ResourceVersion,
    replica: &ResourceNodeStatus,
    changes: &mut StateMachineChanges,
) -> Result<(), Report<RestoreStepConflict>> {
    let execution = applying_restore(state, reference)?;
    let created = RestoreStep::CreateDomain(key.domain.clone());
    if !execution.is_recorded(&created) {
        return Err(Report::new(RestoreStepConflict::OutOfOrder {
            step: RestoreStep::ImportResources(key.domain.clone()),
            missing: created,
        }));
    }
    let imported = RestoreStep::ImportResources(key.domain.clone());
    if execution.is_recorded(&imported) && state.resources.uploads.get(key).is_none() {
        return Err(Report::new(RestoreStepConflict::ResourcesRecorded {
            domain: key.domain.clone(),
        }));
    }
    let import = state.resources.import_version(key, resource, replica);
    if let Err(error) = import {
        return Err(error.change_context(RestoreStepConflict::Import {
            domain: key.domain.clone(),
            resource: key.identifier.clone(),
            version: resource.id.version,
        }));
    }
    changes.resources_changed = true;
    Ok(())
}

/// The restore an applying command execution records under `reference`.
fn applying_restore<'state>(
    state: &'state StateMachineData,
    reference: &CommandExecutionReference,
) -> Result<&'state RestoreExecution, Report<RestoreStepConflict>> {
    let restore = match state.command_executions.get(reference) {
        Some(execution) => execution.restore_execution(),
        None => None,
    };
    let Some(restore) = restore else {
        return Err(Report::new(RestoreStepConflict::NotARestore {
            reference: reference.clone(),
        }));
    };
    Ok(restore)
}

/// Imports `users` under `policy`. Every user is checked before any is written, so a refusal
/// leaves every user as it was.
fn import_users(
    state: &mut StateMachineData,
    users: &[UserCredentials],
    policy: ExistingUserPolicy,
) -> Result<RestoredUsers, Report<RestoreStepConflict>> {
    if policy == ExistingUserPolicy::Fail {
        for user in users {
            if state.users.contains_key(&user.name) {
                return Err(Report::new(RestoreStepConflict::UserExists {
                    user: user.name.clone(),
                }));
            }
        }
    }
    let mut restored = RestoredUsers::default();
    for user in users {
        let exists = state.users.contains_key(&user.name);
        match (exists, policy) {
            (false, _) => {
                restored.created = restored.created.checked_add(1).assured(USER_COUNT_BOUND);
                state.users.insert(user.name.clone(), user.clone());
            }
            (true, ExistingUserPolicy::Skip) => {
                restored.skipped = restored.skipped.checked_add(1).assured(USER_COUNT_BOUND);
            }
            (true, ExistingUserPolicy::Replace) => {
                restored.replaced = restored.replaced.checked_add(1).assured(USER_COUNT_BOUND);
                state.users.insert(user.name.clone(), user.clone());
            }
            (true, ExistingUserPolicy::Fail) => {
                return Err(Report::new(RestoreStepConflict::UserExists {
                    user: user.name.clone(),
                }));
            }
        }
    }
    Ok(restored)
}

/// Why a count of imported users cannot overflow.
const USER_COUNT_BOUND: &str =
    "each count is at most the number of archived users, and a slice holds fewer than u64 counts";

/// Creates `domain_state` under the restore's lease for it, and declares each of its resources.
fn create_domain(
    state: &mut StateMachineData,
    reference: &CommandExecutionReference,
    domain_state: &DomainState,
    resources: &[RestoredResource],
    changes: &mut StateMachineChanges,
) -> Result<(), Report<RestoreStepConflict>> {
    let domain = &domain_state.id;
    let lease = match state.command_executions.get(reference) {
        Some(execution) => execution.domain_mutation(domain).cloned(),
        None => None,
    };
    let Some(lease) = lease else {
        return Err(Report::new(RestoreStepConflict::MissingLease {
            domain: domain.clone(),
        }));
    };
    if let Err(error) = validate_domain_mutation(state, domain, Some(&lease)) {
        return Err(error.change_context(RestoreStepConflict::Lease {
            domain: domain.clone(),
        }));
    }
    if state.domains.contains_key(domain) {
        return Err(Report::new(RestoreStepConflict::DomainExists {
            domain: domain.clone(),
        }));
    }
    for declared in resources {
        if state.resources.declares(domain, &declared.resource) {
            return Err(Report::new(RestoreStepConflict::CatalogExists {
                domain: domain.clone(),
                resource: declared.resource.clone(),
            }));
        }
    }
    state.domains.insert(domain.clone(), domain_state.clone());
    changes.domains_changed = true;
    for declared in resources {
        state
            .resources
            .restore_catalog(domain, &declared.resource, declared.next_version);
        changes.resources_changed = true;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use meticulous::ResultExt as _;
    use nervix_models::{
        ArchiveDigest, ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, DomainConfig,
        DomainPace, DomainStartPoint, DomainStatus, PlacementPolicy, ResourceId, ResourceNodeState,
        ResourceReplicaKey, ResourceUploadIdentity, ResourceUploadState, RestoreMode, Timestamp,
    };

    use super::*;
    use crate::{
        CommandExecution, CommandExecutionAdmissionPolicy, CommandExecutionEffect,
        ConsensusCommand, ConsensusResponse, apply_consensus_command,
    };

    fn domain(raw: &str) -> DomainName {
        DomainName::parse(raw).assured("the test domain is an identifier-shaped literal")
    }

    fn user(raw: &str) -> UserName {
        UserName::parse(raw).assured("the test user is an identifier-shaped literal")
    }

    fn resource(raw: &str) -> ResourceName {
        ResourceName::parse(raw).assured("the test resource is an identifier-shaped literal")
    }

    fn reference_at(index: u64) -> CommandExecutionReference {
        CommandExecutionReference::parse(format!("018bcfe5-6800-7000-8000-{index:012x}"))
            .assured("the test command reference is a UUIDv7 value")
    }

    fn credentials(name: &str, hash: &str) -> UserCredentials {
        UserCredentials {
            name: user(name),
            password_hash: hash.to_string(),
        }
    }

    fn restore_of(scope: RestoreScope) -> RestoreExecution {
        RestoreExecution::new(
            Restore {
                scope,
                source: "archive.nvxb".to_string(),
                mode: RestoreMode::Apply,
                state: nervix_models::RestoreState::All,
            },
            RestoreArchive {
                total_bytes: NonZeroU64::MIN,
                digest: ArchiveDigest::from_bytes([3; 32]),
            },
        )
    }

    fn cluster_restore(policy: ExistingUserPolicy) -> RestoreExecution {
        restore_of(RestoreScope::Cluster {
            existing_users: policy,
        })
    }

    /// Admits `restore` under `reference`, holding the mutation lease of every domain in `leases`.
    fn admit(
        state: &mut StateMachineData,
        reference: &CommandExecutionReference,
        restore: RestoreExecution,
        leases: &[&str],
    ) {
        let execution = CommandExecution::applying(
            reference.clone(),
            user("operator"),
            None,
            [9; 32],
            Timestamp::from_unix_nanos(1_700_000_010_000_000_000),
            CommandExecutionEffect::Restore(Box::new(restore)),
        );
        let response = apply_consensus_command(
            state,
            &ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(execution),
                mutation_domains: leases.iter().map(|name| domain(name)).collect(),
                policy: CommandExecutionAdmissionPolicy::at(
                    Timestamp::from_unix_nanos(1_700_000_010_000_000_000),
                    Duration::from_secs(60),
                    10,
                ),
            },
        );
        assert_eq!(response.response, ConsensusResponse::Applied);
    }

    fn step(
        state: &mut StateMachineData,
        reference: &CommandExecutionReference,
        step: RestoreStep,
        effect: RestoreStepEffect,
    ) -> ConsensusResponse {
        apply_consensus_command(
            state,
            &ConsensusCommand::ApplyRestoreStep {
                reference: reference.clone(),
                step,
                effect: Box::new(effect),
            },
        )
        .response
    }

    fn conflict_text(response: &ConsensusResponse) -> String {
        match response {
            ConsensusResponse::Conflict(conflict) => conflict.to_string(),
            other => panic!("the step must be refused, not {other:?}"),
        }
    }

    fn stopped(name: &str) -> DomainState {
        DomainState {
            id: domain(name),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                placement: PlacementPolicy::Neutral,
            },
            status: DomainStatus::Stopped,
            start_version: 3,
            last_start: DomainStartPoint::Resume,
            clock: None,
        }
    }

    fn domain_effect(name: &str, next_version: u64) -> RestoreStepEffect {
        RestoreStepEffect::Domain {
            state: Box::new(stopped(name)),
            resources: vec![RestoredResource {
                resource: resource("model"),
                next_version: NonZeroU64::new(next_version).assured("a positive test version"),
            }],
        }
    }

    fn users_effect(policy: ExistingUserPolicy) -> RestoreStepEffect {
        RestoreStepEffect::Users {
            users: vec![
                credentials("alice", "$archived-alice"),
                credentials("bob", "$bob"),
            ],
            policy,
        }
    }

    fn progress(
        state: &StateMachineData,
        reference: &CommandExecutionReference,
    ) -> RestoreExecution {
        state
            .command_executions
            .get(reference)
            .and_then(CommandExecution::restore_execution)
            .cloned()
            .assured("the test restore is applying")
    }

    #[test]
    fn a_restore_applies_its_steps_in_order_and_each_once() {
        let mut state = StateMachineData::default();
        let reference = reference_at(1);
        admit(
            &mut state,
            &reference,
            cluster_restore(ExistingUserPolicy::Fail),
            &["prod"],
        );

        let early = step(
            &mut state,
            &reference,
            RestoreStep::CreateDomain(domain("prod")),
            domain_effect("prod", 4),
        );
        assert!(
            conflict_text(&early).contains("cannot apply before the step to import users"),
            "{early:?}"
        );
        assert!(!state.domains.contains_key(&domain("prod")));

        let users = step(
            &mut state,
            &reference,
            RestoreStep::Users,
            users_effect(ExistingUserPolicy::Fail),
        );
        assert_eq!(users, ConsensusResponse::Applied);
        assert_eq!(
            progress(&state, &reference).restored_users(),
            Some(RestoredUsers {
                created: 2,
                skipped: 0,
                replaced: 0,
            })
        );

        // A step recorded before applies nothing, whatever the repeated proposal carries.
        let repeated = step(
            &mut state,
            &reference,
            RestoreStep::Users,
            RestoreStepEffect::Users {
                users: vec![credentials("carol", "$carol")],
                policy: ExistingUserPolicy::Replace,
            },
        );
        assert_eq!(repeated, ConsensusResponse::Applied);
        assert!(!state.users.contains_key(&user("carol")));

        let models_first = step(
            &mut state,
            &reference,
            RestoreStep::ApplyModels(domain("prod")),
            RestoreStepEffect::Completion,
        );
        assert!(conflict_text(&models_first).contains("import resource versions of domain"));

        let created = step(
            &mut state,
            &reference,
            RestoreStep::CreateDomain(domain("prod")),
            domain_effect("prod", 4),
        );
        assert_eq!(created, ConsensusResponse::Applied);
        assert_eq!(state.domains.get(&domain("prod")), Some(&stopped("prod")));
        assert!(
            state
                .resources
                .declares(&domain("prod"), &resource("model"))
        );

        for completed in [
            RestoreStep::ImportResources(domain("prod")),
            RestoreStep::ApplyModels(domain("prod")),
        ] {
            let response = step(
                &mut state,
                &reference,
                completed.clone(),
                RestoreStepEffect::Completion,
            );
            assert_eq!(response, ConsensusResponse::Applied);
            assert!(progress(&state, &reference).is_recorded(&completed));
        }
    }

    #[test]
    fn a_step_must_carry_its_own_effect() {
        let mut state = StateMachineData::default();
        let reference = reference_at(2);
        admit(
            &mut state,
            &reference,
            cluster_restore(ExistingUserPolicy::Fail),
            &["prod"],
        );
        let mismatched = step(
            &mut state,
            &reference,
            RestoreStep::Users,
            RestoreStepEffect::Completion,
        );
        assert!(conflict_text(&mismatched).contains("carries the effect of another step"));
        let users = step(
            &mut state,
            &reference,
            RestoreStep::Users,
            users_effect(ExistingUserPolicy::Fail),
        );
        assert_eq!(users, ConsensusResponse::Applied);
        let misnamed = step(
            &mut state,
            &reference,
            RestoreStep::CreateDomain(domain("staging")),
            domain_effect("prod", 2),
        );
        assert!(conflict_text(&misnamed).contains("carries the effect of another step"));
    }

    #[test]
    fn the_users_step_follows_its_policy() {
        for (policy, alice_hash, expected) in [
            (
                ExistingUserPolicy::Skip,
                "$current-alice",
                RestoredUsers {
                    created: 1,
                    skipped: 1,
                    replaced: 0,
                },
            ),
            (
                ExistingUserPolicy::Replace,
                "$archived-alice",
                RestoredUsers {
                    created: 1,
                    skipped: 0,
                    replaced: 1,
                },
            ),
        ] {
            let mut state = StateMachineData::default();
            state
                .users
                .insert(user("alice"), credentials("alice", "$current-alice"));
            let reference = reference_at(3);
            admit(&mut state, &reference, cluster_restore(policy), &[]);
            let response = step(
                &mut state,
                &reference,
                RestoreStep::Users,
                users_effect(policy),
            );
            assert_eq!(response, ConsensusResponse::Applied, "{policy:?}");
            assert_eq!(
                state
                    .users
                    .get(&user("alice"))
                    .map(|credentials| credentials.password_hash.as_str()),
                Some(alice_hash),
                "{policy:?}"
            );
            assert!(state.users.contains_key(&user("bob")), "{policy:?}");
            assert_eq!(
                progress(&state, &reference).restored_users(),
                Some(expected),
                "{policy:?}"
            );
        }
    }

    #[test]
    fn an_existing_user_refuses_the_users_step_under_fail_and_writes_nothing() {
        let mut state = StateMachineData::default();
        state
            .users
            .insert(user("alice"), credentials("alice", "$current-alice"));
        let reference = reference_at(4);
        admit(
            &mut state,
            &reference,
            cluster_restore(ExistingUserPolicy::Fail),
            &[],
        );
        let response = step(
            &mut state,
            &reference,
            RestoreStep::Users,
            users_effect(ExistingUserPolicy::Fail),
        );
        assert!(conflict_text(&response).contains("user 'alice' already exists"));
        assert!(!state.users.contains_key(&user("bob")));
        assert!(!progress(&state, &reference).is_recorded(&RestoreStep::Users));
    }

    #[test]
    fn a_domain_is_created_only_where_none_exists_and_only_under_the_restores_lease() {
        let domain_restore = || {
            restore_of(RestoreScope::Domain {
                domain: domain("prod"),
                target: None,
            })
        };
        let mut state = StateMachineData::default();
        let reference = reference_at(5);
        admit(&mut state, &reference, domain_restore(), &[]);
        let without_lease = step(
            &mut state,
            &reference,
            RestoreStep::CreateDomain(domain("prod")),
            domain_effect("prod", 2),
        );
        assert!(conflict_text(&without_lease).contains("holds no mutation lease"));

        let mut state = StateMachineData::default();
        state.domains.insert(domain("prod"), stopped("prod"));
        let reference = reference_at(6);
        admit(&mut state, &reference, domain_restore(), &["prod"]);
        let existing = step(
            &mut state,
            &reference,
            RestoreStep::CreateDomain(domain("prod")),
            domain_effect("prod", 2),
        );
        assert!(conflict_text(&existing).contains("domain 'prod' already exists"));
        assert!(
            !state
                .resources
                .declares(&domain("prod"), &resource("model"))
        );
    }

    #[test]
    fn a_step_of_anything_but_an_applying_restore_is_refused() {
        let mut state = StateMachineData::default();
        let response = step(
            &mut state,
            &reference_at(7),
            RestoreStep::Users,
            users_effect(ExistingUserPolicy::Skip),
        );
        assert!(conflict_text(&response).contains("is not an applying restore"));
    }

    fn node() -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-1").assured("the test node is valid"),
            ClusterNodeIncarnation::new(7),
        )
    }

    fn imported(version: u64, checksum: &str) -> (ResourceVersion, ResourceNodeStatus) {
        let published = ResourceVersion {
            id: ResourceId::new(domain("prod"), resource("model"), version),
            root_checksum: checksum.to_string(),
            manifest_checksum: format!("manifest-{version}"),
            file_count: 1,
            total_bytes: 5,
            archive_bytes: 2048,
            created_at: Timestamp::from_unix_nanos(11),
            created_by_node: ClusterNodeName::parse("source-node").assured("valid node"),
        };
        let replica = ResourceNodeStatus {
            key: ResourceReplicaKey::new(domain("prod"), resource("model"), version, node()),
            state: ResourceNodeState::Ready,
            root_checksum: Some(checksum.to_string()),
            last_verified_at: Some(Timestamp::from_unix_nanos(12)),
            source_node: Some(node()),
            error: None,
        };
        (published, replica)
    }

    fn upload_key(identity: &str) -> ResourceUploadKey {
        ResourceUploadKey::new(
            user("operator"),
            domain("prod"),
            resource("model"),
            ResourceUploadIdentity::parse(identity.to_string()).assured("a valid test identity"),
        )
    }

    fn import(
        state: &mut StateMachineData,
        reference: &CommandExecutionReference,
        key: &ResourceUploadKey,
        version: u64,
        checksum: &str,
    ) -> ConsensusResponse {
        let (published, replica) = imported(version, checksum);
        apply_consensus_command(
            state,
            &ConsensusCommand::ImportResourceVersion {
                reference: reference.clone(),
                key: Box::new(key.clone()),
                resource: Box::new(published),
                replica: Box::new(replica),
            },
        )
        .response
    }

    #[test]
    fn an_imported_version_keeps_its_archived_number_and_is_published_at_once() {
        let mut state = StateMachineData::default();
        let reference = reference_at(8);
        admit(
            &mut state,
            &reference,
            restore_of(RestoreScope::Domain {
                domain: domain("prod"),
                target: None,
            }),
            &["prod"],
        );
        let key = upload_key("restore-v3");
        let before_domain = import(&mut state, &reference, &key, 3, "root-3");
        assert!(conflict_text(&before_domain).contains("before the step to create domain"));

        let created = step(
            &mut state,
            &reference,
            RestoreStep::CreateDomain(domain("prod")),
            domain_effect("prod", 5),
        );
        assert_eq!(created, ConsensusResponse::Applied);

        for _ in 0..2 {
            assert_eq!(
                import(&mut state, &reference, &key, 3, "root-3"),
                ConsensusResponse::Applied
            );
        }
        let status = nervix_models::ResourceVersionStatus::from(&state.resources);
        let upload = status.upload(&key).assured("the import is recorded");
        assert_eq!(upload.version, 3);
        assert!(matches!(upload.state, ResourceUploadState::Applying { .. }));
        let id = ResourceId::new(domain("prod"), resource("model"), 3);
        assert!(status.version(&id).is_some(), "the version is published");
        assert_eq!(
            status.next_version(&domain("prod"), &resource("model")),
            Some(5),
            "an import never moves the sequence the restore declared"
        );

        let changed = import(&mut state, &reference, &key, 3, "root-other");
        assert!(
            conflict_text(&changed).contains("cannot be imported"),
            "{changed:?}"
        );
        let taken = import(&mut state, &reference, &upload_key("other"), 3, "root-3");
        assert!(
            conflict_text(&taken).contains("cannot be imported"),
            "{taken:?}"
        );
        let beyond = import(
            &mut state,
            &reference,
            &upload_key("restore-v5"),
            5,
            "root-5",
        );
        assert!(
            conflict_text(&beyond).contains("cannot be imported"),
            "{beyond:?}"
        );

        let recorded = step(
            &mut state,
            &reference,
            RestoreStep::ImportResources(domain("prod")),
            RestoreStepEffect::Completion,
        );
        assert_eq!(recorded, ConsensusResponse::Applied);
        let late = import(
            &mut state,
            &reference,
            &upload_key("restore-v4"),
            4,
            "root-4",
        );
        assert!(conflict_text(&late).contains("already recorded every resource version"));
        assert_eq!(
            import(&mut state, &reference, &key, 3, "root-3"),
            ConsensusResponse::Applied,
            "a repeated import of a recorded version stays accepted"
        );
    }

    #[test]
    fn progress_survives_as_part_of_the_applying_record() {
        let mut restore = cluster_restore(ExistingUserPolicy::Skip);
        assert!(!restore.is_recorded(&RestoreStep::Users));
        restore.record(RestoreStep::Users, Some(RestoredUsers::default()));
        restore.record(RestoreStep::CreateDomain(domain("prod")), None);
        assert!(restore.is_recorded(&RestoreStep::Users));
        assert_eq!(restore.restored_users(), Some(RestoredUsers::default()));
        assert_eq!(
            restore.missing_prerequisite(&RestoreStep::ApplyModels(domain("prod"))),
            Some(RestoreStep::ImportResources(domain("prod")))
        );
        assert_eq!(
            restore.missing_prerequisite(&RestoreStep::ImportResources(domain("prod"))),
            None
        );
        let expected: BTreeSet<RestoreStep> = [
            RestoreStep::Users,
            RestoreStep::CreateDomain(domain("prod")),
        ]
        .into_iter()
        .collect();
        assert_eq!(restore.completed, expected);
    }
}
