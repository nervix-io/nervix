//! What a restore recreates, decided from a verified archive before anything changes.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving what a `RESTORE` recreates: which archived domains under which names,
//!   what each archived user becomes under the restore's policy, each domain's stopped state and
//!   declared resources, the completed resource versions imported under their archived numbers,
//!   the models pinned to exactly those versions, the fixed order of the steps that apply them,
//!   and every refusal that must happen before the first of those steps.
//! - **Depends on.** The archive format's verified description, the replicated records a restore
//!   step carries, and the vocabulary.
//! - **Must not know.** How an archive was read or staged, how its NSPL was parsed, or how a step
//!   reaches consensus or the resource store.
//!
//! A plan is a pure function of the archive, the statement, the users and domains the cluster
//! already has, and the steps an earlier attempt of the same restore recorded. A recorded step is
//! never checked again: its effects are in place, so the domain it created or the users it imported
//! are the restore's own rather than a conflict.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use error_stack::Report;
use indexmap::IndexMap;
use meticulous::OptionExt as _;
use nervix_backup::{
    ArchiveDescription, ArchiveScope, DescribedDomain, DescribedResourceVersion, DescribedSection,
    ResourceVersionState, UsersRecord,
};
use nervix_consensus::{RestoredResource, UserCredentials};
use nervix_models::{
    BackupResources, DomainConfig, DomainName, DomainState, DomainStatus, ExistingUserPolicy,
    Model, ModelName, RequestedResourceVersion, ResourceId, ResourceName, ResourceVersion, Restore,
    RestoreScope, RestoreStep, RestoredUsers, UserName,
};
use thiserror::Error;

/// Why a restore was refused before it changed anything. No variant carries archive contents: an
/// archive holds secrets and password hashes, and a refusal names only where the problem is.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RestorePlanError {
    #[error(
        "RESTORE CLUSTER needs a cluster archive, and this archive holds only domain '{domain}'; \
         restore it with RESTORE DOMAIN"
    )]
    NotAClusterArchive { domain: DomainName },
    #[error("the archive holds no domain '{domain}'")]
    DomainNotInArchive { domain: DomainName },
    #[error("domain '{domain}' already exists; restore it AS another name")]
    DomainExists { domain: DomainName },
    #[error(
        "user '{user}' already exists; restore with ON EXISTING USER SKIP or ON EXISTING USER \
         REPLACE"
    )]
    UserExists { user: UserName },
    #[error(
        "the archive's resource catalog of domain '{domain}' does not declare resource \
         '{resource}'"
    )]
    UndeclaredResource {
        domain: DomainName,
        resource: ResourceName,
    },
    #[error(
        "version {version} of resource '{resource}' in domain '{domain}' lies outside the \
         sequence the archive records"
    )]
    VersionOutsideSequence {
        domain: DomainName,
        resource: ResourceName,
        version: NonZeroU64,
    },
    #[error(
        "the archive records version {version} of resource '{resource}' in domain '{domain}' as \
         completed without its checksums"
    )]
    UnpublishedVersion {
        domain: DomainName,
        resource: ResourceName,
        version: NonZeroU64,
    },
    #[error(
        "the archive was taken WITHOUT RESOURCES and holds no bytes of version {version} of \
         resource '{resource}' in domain '{domain}'"
    )]
    ResourcesOmitted {
        domain: DomainName,
        resource: ResourceName,
        version: NonZeroU64,
    },
    #[error(
        "the archive holds no bytes of completed version {version} of resource '{resource}' in \
         domain '{domain}'"
    )]
    MissingResourceArchive {
        domain: DomainName,
        resource: ResourceName,
        version: NonZeroU64,
    },
    #[error(
        "the bytes the archive holds for version {version} of resource '{resource}' in domain \
         '{domain}' do not match the version's root checksum"
    )]
    ResourceDigestMismatch {
        domain: DomainName,
        resource: ResourceName,
        version: NonZeroU64,
    },
    #[error("{kind} '{model}' of domain '{domain}' does not name its resource versions by number")]
    ModelsNotPinned {
        domain: DomainName,
        kind: &'static str,
        model: ModelName,
    },
    #[error(
        "{kind} '{model}' of domain '{domain}' binds version {version} of resource '{resource}', \
         which the archive does not restore as a completed version"
    )]
    UnrestoredBinding {
        domain: DomainName,
        kind: &'static str,
        model: ModelName,
        resource: ResourceName,
        version: u64,
    },
}

/// What a restore reads from its archive: the verified description, and each domain's models,
/// parsed from its `models.nspl` in written order and keyed by the domain's archived name.
pub(crate) struct ArchiveToRestore<'archive> {
    pub(crate) description: &'archive ArchiveDescription,
    pub(crate) models: &'archive BTreeMap<DomainName, Vec<Model<RequestedResourceVersion>>>,
}

/// What the cluster already has, as a restore plans against it.
pub(crate) struct ExistingState<'state> {
    pub(crate) users: &'state BTreeSet<UserName>,
    pub(crate) domains: &'state BTreeSet<DomainName>,
    /// The steps earlier attempts of the same restore recorded as applied. Empty for a restore not
    /// yet admitted.
    pub(crate) recorded: &'state BTreeSet<RestoreStep>,
}

impl ExistingState<'_> {
    fn is_recorded(&self, step: &RestoreStep) -> bool {
        self.recorded.contains(step)
    }
}

/// Everything a restore recreates, and the order it recreates it in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RestorePlan {
    /// The users a cluster restore imports. Absent for a domain restore.
    pub(crate) users: Option<PlannedUsers>,
    /// Every domain the restore recreates, by the name it is restored under, in archive order.
    pub(crate) domains: IndexMap<DomainName, PlannedDomain>,
}

/// The archived users and what importing them does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedUsers {
    /// Every archived user, with the password hash the archive holds, in name order.
    pub(crate) users: Vec<UserCredentials>,
    pub(crate) policy: ExistingUserPolicy,
    /// What importing them does to the users the plan was made against.
    pub(crate) outcome: RestoredUsers,
}

/// One domain a restore recreates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedDomain {
    /// The domain's name in the archive.
    pub(crate) source: DomainName,
    /// The name it is restored under.
    pub(crate) target: DomainName,
    /// The domain as the restore creates it: stopped, with its archived configuration.
    pub(crate) state: DomainState,
    /// Every resource the domain declares, with the version its next upload receives.
    pub(crate) resources: Vec<RestoredResource>,
    /// Every completed version, imported under its archived number, in archive order.
    pub(crate) versions: Vec<PlannedVersion>,
    /// The domain's models, each binding versions by number, in archive order.
    pub(crate) models: Vec<Model<u64>>,
}

/// One completed resource version a restore imports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedVersion {
    /// The version's catalog metadata, under the domain it is restored into.
    pub(crate) resource: ResourceVersion,
    /// Where the version's original archive sits in the restore's archive.
    pub(crate) archive: DescribedSection,
}

impl RestorePlan {
    /// Plans `restore` of `archive` against `existing`, or refuses it before anything changes.
    pub(crate) fn new(
        restore: &Restore,
        archive: ArchiveToRestore<'_>,
        existing: &ExistingState<'_>,
    ) -> Result<Self, Report<RestorePlanError>> {
        let description = archive.description;
        let selected = selected_domains(restore, description)?;
        let users = match (&restore.scope, &description.users) {
            (RestoreScope::Cluster { existing_users }, Some(users)) => {
                Some(plan_users(users, *existing_users, existing)?)
            }
            (RestoreScope::Cluster { .. } | RestoreScope::Domain { .. }, _) => None,
        };
        let mut domains = IndexMap::with_capacity(selected.len());
        for described in selected {
            let source = &described.record.domain;
            let target = restore.scope.target_of(source).clone();
            let created = RestoreStep::CreateDomain(target.clone());
            if existing.domains.contains(&target) && !existing.is_recorded(&created) {
                return Err(Report::new(RestorePlanError::DomainExists {
                    domain: target,
                }));
            }
            let models = match archive.models.get(source) {
                Some(models) => models.as_slice(),
                None => &[],
            };
            let planned = plan_domain(
                described,
                target.clone(),
                models,
                description.manifest.resources,
            )?;
            domains.insert(target, planned);
        }
        Ok(Self { users, domains })
    }

    /// Every step of the restore, in the order it applies them: the users of a cluster restore,
    /// then each domain's creation, resource versions and models.
    pub(crate) fn steps(&self) -> Vec<RestoreStep> {
        let mut steps = Vec::new();
        if self.users.is_some() {
            steps.push(RestoreStep::Users);
        }
        for domain in self.domains.values() {
            steps.push(RestoreStep::CreateDomain(domain.target.clone()));
            steps.push(RestoreStep::ImportResources(domain.target.clone()));
            steps.push(RestoreStep::ApplyModels(domain.target.clone()));
        }
        steps
    }

    /// The names every recreated domain is restored under, which the restore owns while it
    /// applies.
    pub(crate) fn target_domains(&self) -> BTreeSet<DomainName> {
        self.domains.keys().cloned().collect()
    }
}

/// The archived domains `restore` recreates, in archive order.
fn selected_domains<'archive>(
    restore: &Restore,
    description: &'archive ArchiveDescription,
) -> Result<Vec<&'archive DescribedDomain>, Report<RestorePlanError>> {
    match (&restore.scope, &description.manifest.scope) {
        (RestoreScope::Cluster { .. }, ArchiveScope::Cluster) => {
            Ok(description.domains.iter().collect())
        }
        (RestoreScope::Cluster { .. }, ArchiveScope::Domain(domain)) => {
            Err(Report::new(RestorePlanError::NotAClusterArchive {
                domain: domain.clone(),
            }))
        }
        (RestoreScope::Domain { domain, .. }, ArchiveScope::Cluster | ArchiveScope::Domain(_)) => {
            // An archive holds each domain once, and a cluster archive at most as many domains as
            // one cluster has, which is what bounds this walk.
            for described in &description.domains {
                if described.record.domain == *domain {
                    return Ok(vec![described]);
                }
            }
            Err(Report::new(RestorePlanError::DomainNotInArchive {
                domain: domain.clone(),
            }))
        }
    }
}

/// The archived users and what importing them under `policy` does to the users that exist.
fn plan_users(
    archived: &UsersRecord,
    policy: ExistingUserPolicy,
    existing: &ExistingState<'_>,
) -> Result<PlannedUsers, Report<RestorePlanError>> {
    let recorded = existing.is_recorded(&RestoreStep::Users);
    let mut outcome = RestoredUsers::default();
    let mut users = Vec::with_capacity(archived.users.len());
    for user in &archived.users {
        let exists = existing.users.contains(&user.name);
        let count = match (exists, policy) {
            (false, _) => &mut outcome.created,
            (true, ExistingUserPolicy::Skip) => &mut outcome.skipped,
            (true, ExistingUserPolicy::Replace) => &mut outcome.replaced,
            (true, ExistingUserPolicy::Fail) if recorded => &mut outcome.created,
            (true, ExistingUserPolicy::Fail) => {
                return Err(Report::new(RestorePlanError::UserExists {
                    user: user.name.clone(),
                }));
            }
        };
        *count = count.checked_add(1).assured(
            "each count is at most the number of archived users, and a list holds fewer than u64 \
             counts",
        );
        users.push(UserCredentials {
            name: user.name.clone(),
            password_hash: user.password_hash.clone(),
        });
    }
    Ok(PlannedUsers {
        users,
        policy,
        outcome,
    })
}

/// Plans one archived domain restored under `target`.
fn plan_domain(
    described: &DescribedDomain,
    target: DomainName,
    models: &[Model<RequestedResourceVersion>],
    resource_bytes: BackupResources,
) -> Result<PlannedDomain, Report<RestorePlanError>> {
    let record = &described.record;
    let source = record.domain.clone();
    let mut sequences = BTreeMap::new();
    let mut resources = Vec::with_capacity(record.resources.len());
    for declared in &record.resources {
        sequences.insert(declared.resource.clone(), declared.next_version);
        resources.push(RestoredResource {
            resource: declared.resource.clone(),
            next_version: declared.next_version,
        });
    }
    let mut versions = Vec::new();
    let mut restored_versions = BTreeSet::new();
    for described_version in &described.resource_versions {
        let version = &described_version.record;
        let Some(next_version) = sequences.get(&version.resource) else {
            return Err(Report::new(RestorePlanError::UndeclaredResource {
                domain: source.clone(),
                resource: version.resource.clone(),
            }));
        };
        if version.version >= *next_version {
            return Err(Report::new(RestorePlanError::VersionOutsideSequence {
                domain: source.clone(),
                resource: version.resource.clone(),
                version: version.version,
            }));
        }
        let ResourceVersionState::Completed = version.state else {
            // A version that failed, or was still installing when the backup read it, is not
            // restored; its number stays below the declared sequence, so no upload takes it.
            continue;
        };
        let planned = plan_version(&source, &target, described_version, resource_bytes)?;
        restored_versions.insert(ResourceId::new(
            source.clone(),
            version.resource.clone(),
            version.version.get(),
        ));
        versions.push(planned);
    }
    let mut pinned = Vec::with_capacity(models.len());
    for model in models {
        pinned.push(pin_model(&source, model, &restored_versions)?);
    }
    Ok(PlannedDomain {
        source,
        state: DomainState {
            id: target.clone(),
            config: DomainConfig {
                pace: record.pace,
                placement: record.placement,
            },
            status: DomainStatus::Stopped,
            start_version: record.start_version,
            last_start: record.start_point.clone(),
            clock: None,
        },
        target,
        resources,
        versions,
        models: pinned,
    })
}

/// Plans one completed version: its catalog metadata under the target domain, and where its bytes
/// sit in the archive.
fn plan_version(
    source: &DomainName,
    target: &DomainName,
    described_version: &DescribedResourceVersion,
    resource_bytes: BackupResources,
) -> Result<PlannedVersion, Report<RestorePlanError>> {
    let version = &described_version.record;
    let Some(published) = &version.published else {
        return Err(Report::new(RestorePlanError::UnpublishedVersion {
            domain: source.clone(),
            resource: version.resource.clone(),
            version: version.version,
        }));
    };
    let Some(archive) = &described_version.archive else {
        let missing = match resource_bytes {
            BackupResources::Omitted => RestorePlanError::ResourcesOmitted {
                domain: source.clone(),
                resource: version.resource.clone(),
                version: version.version,
            },
            BackupResources::Included => RestorePlanError::MissingResourceArchive {
                domain: source.clone(),
                resource: version.resource.clone(),
                version: version.version,
            },
        };
        return Err(Report::new(missing));
    };
    if archive.digest.to_string() != published.root_checksum {
        return Err(Report::new(RestorePlanError::ResourceDigestMismatch {
            domain: source.clone(),
            resource: version.resource.clone(),
            version: version.version,
        }));
    }
    Ok(PlannedVersion {
        resource: ResourceVersion {
            id: ResourceId::new(
                target.clone(),
                version.resource.clone(),
                version.version.get(),
            ),
            root_checksum: published.root_checksum.clone(),
            manifest_checksum: published.manifest_checksum.clone(),
            file_count: published.file_count,
            total_bytes: published.total_bytes,
            archive_bytes: published.archive_bytes,
            created_at: published.created_at,
            created_by_node: published.created_by_node.clone(),
        },
        archive: archive.clone(),
    })
}

/// `model` with every resource binding pinned to its number, which must name a version the restore
/// imports as completed.
fn pin_model(
    domain: &DomainName,
    model: &Model<RequestedResourceVersion>,
    restored: &BTreeSet<ResourceId>,
) -> Result<Model<u64>, Report<RestorePlanError>> {
    let kind = model.kind().keyword_phrase();
    let name = model.name();
    let pin = |resource: &ResourceName, requested: RequestedResourceVersion| {
        let RequestedResourceVersion::Number(version) = requested else {
            return Err(RestorePlanError::ModelsNotPinned {
                domain: domain.clone(),
                kind,
                model: name.clone(),
            });
        };
        let id = ResourceId::new(domain.clone(), resource.clone(), version);
        if !restored.contains(&id) {
            return Err(RestorePlanError::UnrestoredBinding {
                domain: domain.clone(),
                kind,
                model: name.clone(),
                resource: resource.clone(),
                version,
            });
        }
        Ok(version)
    };
    match model.clone().try_map_resource_versions(pin) {
        Ok(pinned) => Ok(pinned),
        Err(error) => Err(Report::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_backup::{
        BackupManifest, DeclaredResource, DomainCapture, DomainRecord, PublishedResourceVersion,
        RaftLogPosition, ResourceVersionRecord, SectionDigest, SectionPath, UserRecord,
    };
    use nervix_models::{
        ClusterNodeName, DomainPace, DomainStartPoint, PlacementPolicy, Statement, Timestamp,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statements};

    use super::*;

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain is valid")
    }

    fn resource(name: &str) -> ResourceName {
        ResourceName::parse(name).assured("the test resource is valid")
    }

    fn user(name: &str) -> UserName {
        UserName::parse(name).assured("the test user is valid")
    }

    fn version(number: u64) -> NonZeroU64 {
        NonZeroU64::new(number).assured("test versions are positive")
    }

    fn digest(byte: u8) -> SectionDigest {
        SectionDigest::from_bytes([byte; 32])
    }

    fn models(source: &str) -> Vec<Model<RequestedResourceVersion>> {
        parse_client_statements(source)
            .assured("the test models parse")
            .into_iter()
            .map(|statement| {
                let ClientStatement::Server(Statement::Create(create)) = statement else {
                    panic!("the test source only creates models");
                };
                *create.body
            })
            .collect()
    }

    const MODELS: &str = "
        CREATE SCHEMA entry ( key STRING, value STRING );
        CREATE HASH MAP lookups KEY key FROM RESOURCE table VERSION 3 PATH 'data.jsonl' DECODE \
                          USING entry_codec;
    ";

    fn completed(
        domain_name: &str,
        resource_name: &str,
        number: u64,
        byte: u8,
    ) -> DescribedResourceVersion {
        DescribedResourceVersion {
            record: ResourceVersionRecord {
                domain: domain(domain_name),
                resource: resource(resource_name),
                version: version(number),
                state: ResourceVersionState::Completed,
                published: Some(PublishedResourceVersion {
                    root_checksum: digest(byte).to_string(),
                    manifest_checksum: format!("manifest-{number}"),
                    file_count: 1,
                    total_bytes: 12,
                    archive_bytes: 2048,
                    created_at: Timestamp::from_unix_nanos(5),
                    created_by_node: ClusterNodeName::parse("old-node-1").assured("a valid node"),
                }),
            },
            archive: Some(DescribedSection {
                path: SectionPath::resource_archive(
                    &domain(domain_name),
                    &resource(resource_name),
                    version(number),
                ),
                offset: 4096 * number,
                length: 2048,
                digest: digest(byte),
            }),
        }
    }

    fn failed(domain_name: &str, resource_name: &str, number: u64) -> DescribedResourceVersion {
        DescribedResourceVersion {
            record: ResourceVersionRecord {
                domain: domain(domain_name),
                resource: resource(resource_name),
                version: version(number),
                state: ResourceVersionState::Failed {
                    reason: "installation failed".to_string(),
                },
                published: None,
            },
            archive: None,
        }
    }

    fn described(name: &str, versions: Vec<DescribedResourceVersion>) -> DescribedDomain {
        DescribedDomain {
            capture: DomainCapture {
                domain: domain(name),
                revision: 7,
                raft_log: RaftLogPosition { term: 1, index: 7 },
            },
            record: DomainRecord {
                domain: domain(name),
                pace: DomainPace::Unpaced,
                placement: PlacementPolicy::PreferColocation,
                status: DomainStatus::Running,
                start_version: 4,
                start_point: DomainStartPoint::Resume,
                clock: None,
                logical_frontier: None,
                resources: vec![DeclaredResource {
                    resource: resource("table"),
                    next_version: version(4),
                }],
            },
            models: DescribedSection {
                path: SectionPath::domain_models(&domain(name)),
                offset: 1024,
                length: 64,
                digest: digest(9),
            },
            resource_versions: versions,
        }
    }

    fn archive(scope: ArchiveScope, domains: Vec<DescribedDomain>) -> ArchiveDescription {
        let users = match scope {
            ArchiveScope::Cluster => Some(UsersRecord {
                users: vec![
                    UserRecord {
                        name: user("alice"),
                        password_hash: "$argon2id$alice".to_string(),
                    },
                    UserRecord {
                        name: user("bob"),
                        password_hash: "$argon2id$bob".to_string(),
                    },
                ],
            }),
            ArchiveScope::Domain(_) => None,
        };
        ArchiveDescription {
            manifest: BackupManifest {
                producer_version: "0.1.0".to_string(),
                language_version: "0.1.0".to_string(),
                cluster_id: "source".to_string(),
                captured_at: Timestamp::from_unix_nanos(10),
                scope,
                resources: BackupResources::Included,
                domains: Vec::new(),
                sections: Vec::new(),
            },
            users,
            domains,
        }
    }

    fn cluster_archive() -> ArchiveDescription {
        archive(
            ArchiveScope::Cluster,
            vec![
                described(
                    "prod",
                    vec![
                        completed("prod", "table", 1, 1),
                        failed("prod", "table", 2),
                        completed("prod", "table", 3, 3),
                    ],
                ),
                described("staging", Vec::new()),
            ],
        )
    }

    fn cluster_restore(policy: ExistingUserPolicy) -> Restore {
        Restore {
            scope: RestoreScope::Cluster {
                existing_users: policy,
            },
            source: "cluster.nvxb".to_string(),
            mode: nervix_models::RestoreMode::Apply,
        }
    }

    fn domain_restore(name: &str, target: Option<&str>) -> Restore {
        Restore {
            scope: RestoreScope::Domain {
                domain: domain(name),
                target: target.map(domain),
            },
            source: "domain.nvxb".to_string(),
            mode: nervix_models::RestoreMode::Apply,
        }
    }

    struct Cluster {
        users: BTreeSet<UserName>,
        domains: BTreeSet<DomainName>,
        recorded: BTreeSet<RestoreStep>,
    }

    impl Cluster {
        fn empty() -> Self {
            Self {
                users: BTreeSet::new(),
                domains: BTreeSet::new(),
                recorded: BTreeSet::new(),
            }
        }

        fn state(&self) -> ExistingState<'_> {
            ExistingState {
                users: &self.users,
                domains: &self.domains,
                recorded: &self.recorded,
            }
        }
    }

    fn plan(
        restore: &Restore,
        description: &ArchiveDescription,
        cluster: &Cluster,
    ) -> Result<RestorePlan, Report<RestorePlanError>> {
        let parsed = BTreeMap::from([(domain("prod"), models(MODELS))]);
        RestorePlan::new(
            restore,
            ArchiveToRestore {
                description,
                models: &parsed,
            },
            &cluster.state(),
        )
    }

    #[test]
    fn a_cluster_restore_imports_users_then_recreates_each_domain_in_archive_order() {
        let mut cluster = Cluster::empty();
        cluster.users.insert(user("alice"));
        let planned = plan(
            &cluster_restore(ExistingUserPolicy::Skip),
            &cluster_archive(),
            &cluster,
        )
        .assured("the archive plans");
        assert_eq!(
            planned.steps(),
            [
                RestoreStep::Users,
                RestoreStep::CreateDomain(domain("prod")),
                RestoreStep::ImportResources(domain("prod")),
                RestoreStep::ApplyModels(domain("prod")),
                RestoreStep::CreateDomain(domain("staging")),
                RestoreStep::ImportResources(domain("staging")),
                RestoreStep::ApplyModels(domain("staging")),
            ]
        );
        let users = planned
            .users
            .as_ref()
            .assured("a cluster restore imports users");
        assert_eq!(
            users.outcome,
            RestoredUsers {
                created: 1,
                skipped: 1,
                replaced: 0,
            }
        );
        assert_eq!(users.users[0].password_hash, "$argon2id$alice");
        let prod = &planned.domains[&domain("prod")];
        assert_eq!(prod.state.status, DomainStatus::Stopped);
        assert_eq!(prod.state.start_version, 4);
        assert_eq!(prod.state.clock, None);
        assert_eq!(
            prod.state.config.placement,
            PlacementPolicy::PreferColocation
        );
        assert_eq!(prod.models.len(), 2);
        assert_eq!(
            planned.target_domains(),
            BTreeSet::from([domain("prod"), domain("staging")])
        );
    }

    #[test]
    fn catalog_numbering_keeps_its_gaps_and_imports_only_completed_versions() {
        let planned = plan(
            &cluster_restore(ExistingUserPolicy::Fail),
            &cluster_archive(),
            &Cluster::empty(),
        )
        .assured("the archive plans");
        let prod = &planned.domains[&domain("prod")];
        let numbers = prod
            .versions
            .iter()
            .map(|version| version.resource.id.version)
            .collect::<Vec<_>>();
        assert_eq!(numbers, [1, 3], "the failed version 2 stays a gap");
        assert_eq!(
            prod.resources,
            [RestoredResource {
                resource: resource("table"),
                next_version: version(4),
            }]
        );
        let third = &prod.versions[1];
        assert_eq!(third.resource.root_checksum, digest(3).to_string());
        assert_eq!(third.archive.offset, 3 * 4096);
        assert_eq!(
            third.resource.created_by_node.as_str(),
            "old-node-1",
            "an imported version keeps the node that created it"
        );
    }

    #[test]
    fn a_domain_restored_under_a_new_name_carries_it_everywhere() {
        let description = archive(
            ArchiveScope::Domain(domain("prod")),
            vec![described("prod", vec![completed("prod", "table", 3, 3)])],
        );
        let mut cluster = Cluster::empty();
        cluster.domains.insert(domain("prod"));
        let planned = plan(
            &domain_restore("prod", Some("prod_copy")),
            &description,
            &cluster,
        )
        .assured("a new name does not collide");
        assert!(planned.users.is_none());
        let copy = &planned.domains[&domain("prod_copy")];
        assert_eq!(copy.source.as_str(), "prod");
        assert_eq!(copy.target.as_str(), "prod_copy");
        assert_eq!(copy.state.id.as_str(), "prod_copy");
        assert_eq!(copy.versions[0].resource.id.domain.as_str(), "prod_copy");
        assert_eq!(
            planned.steps(),
            [
                RestoreStep::CreateDomain(domain("prod_copy")),
                RestoreStep::ImportResources(domain("prod_copy")),
                RestoreStep::ApplyModels(domain("prod_copy")),
            ]
        );
    }

    #[test]
    fn an_existing_name_is_refused_unless_this_restore_created_it() {
        let mut cluster = Cluster::empty();
        cluster.domains.insert(domain("prod"));
        let refused = plan(&domain_restore("prod", None), &cluster_archive(), &cluster)
            .expect_err("the name is taken");
        assert_eq!(
            refused.current_context(),
            &RestorePlanError::DomainExists {
                domain: domain("prod"),
            }
        );
        cluster
            .recorded
            .insert(RestoreStep::CreateDomain(domain("prod")));
        plan(&domain_restore("prod", None), &cluster_archive(), &cluster)
            .assured("the domain is the restore's own");
    }

    #[test]
    fn each_user_policy_behaves_as_declared() {
        let mut cluster = Cluster::empty();
        cluster.users.insert(user("bob"));
        let refused = plan(
            &cluster_restore(ExistingUserPolicy::Fail),
            &cluster_archive(),
            &cluster,
        )
        .expect_err("bob exists");
        assert_eq!(
            refused.current_context(),
            &RestorePlanError::UserExists { user: user("bob") }
        );
        let replaced = plan(
            &cluster_restore(ExistingUserPolicy::Replace),
            &cluster_archive(),
            &cluster,
        )
        .assured("replace takes over bob");
        assert_eq!(
            replaced.users.assured("users are imported").outcome,
            RestoredUsers {
                created: 1,
                skipped: 0,
                replaced: 1,
            }
        );
        cluster.recorded.insert(RestoreStep::Users);
        plan(
            &cluster_restore(ExistingUserPolicy::Fail),
            &cluster_archive(),
            &cluster,
        )
        .assured("users the restore imported are its own");
    }

    #[test]
    fn the_scope_must_match_the_archive() {
        let domain_archive = archive(
            ArchiveScope::Domain(domain("prod")),
            vec![described("prod", Vec::new())],
        );
        let refused = plan(
            &cluster_restore(ExistingUserPolicy::Fail),
            &domain_archive,
            &Cluster::empty(),
        )
        .expect_err("a domain archive holds no cluster");
        assert_eq!(
            refused.current_context(),
            &RestorePlanError::NotAClusterArchive {
                domain: domain("prod"),
            }
        );
        let missing = plan(
            &domain_restore("absent", None),
            &cluster_archive(),
            &Cluster::empty(),
        )
        .expect_err("the archive does not hold it");
        assert_eq!(
            missing.current_context(),
            &RestorePlanError::DomainNotInArchive {
                domain: domain("absent"),
            }
        );
    }

    fn refusal_of(
        versions: Vec<DescribedResourceVersion>,
        resources: BackupResources,
    ) -> RestorePlanError {
        let mut description = archive(
            ArchiveScope::Domain(domain("prod")),
            vec![described("prod", versions)],
        );
        description.manifest.resources = resources;
        plan(
            &domain_restore("prod", None),
            &description,
            &Cluster::empty(),
        )
        .expect_err("the archive must be refused")
        .current_context()
        .clone()
    }

    #[test]
    fn an_archive_that_cannot_reproduce_its_catalog_is_refused() {
        let mut omitted = completed("prod", "table", 3, 3);
        omitted.archive = None;
        assert!(matches!(
            refusal_of(vec![omitted.clone()], BackupResources::Omitted),
            RestorePlanError::ResourcesOmitted { .. }
        ));
        assert!(matches!(
            refusal_of(vec![omitted], BackupResources::Included),
            RestorePlanError::MissingResourceArchive { .. }
        ));
        let mut mismatched = completed("prod", "table", 3, 3);
        if let Some(archive) = mismatched.archive.as_mut() {
            archive.digest = digest(8);
        }
        assert!(matches!(
            refusal_of(vec![mismatched], BackupResources::Included),
            RestorePlanError::ResourceDigestMismatch { .. }
        ));
        assert!(matches!(
            refusal_of(
                vec![completed("prod", "table", 4, 4)],
                BackupResources::Included
            ),
            RestorePlanError::VersionOutsideSequence { .. }
        ));
        assert!(matches!(
            refusal_of(
                vec![completed("prod", "other", 1, 1)],
                BackupResources::Included
            ),
            RestorePlanError::UndeclaredResource { .. }
        ));
        let mut unpublished = completed("prod", "table", 3, 3);
        unpublished.record.published = None;
        assert!(matches!(
            refusal_of(vec![unpublished], BackupResources::Included),
            RestorePlanError::UnpublishedVersion { .. }
        ));
    }

    #[test]
    fn every_binding_must_name_a_version_the_restore_imports() {
        let unrestored = plan(
            &domain_restore("prod", None),
            &archive(
                ArchiveScope::Domain(domain("prod")),
                vec![described(
                    "prod",
                    vec![completed("prod", "table", 1, 1), failed("prod", "table", 3)],
                )],
            ),
            &Cluster::empty(),
        )
        .expect_err("version 3 failed");
        assert_eq!(
            unrestored.current_context(),
            &RestorePlanError::UnrestoredBinding {
                domain: domain("prod"),
                kind: "HASH MAP",
                model: ModelName::parse("lookups").assured("a valid name"),
                resource: resource("table"),
                version: 3,
            }
        );
        let latest = BTreeMap::from([(
            domain("prod"),
            models(
                "CREATE HASH MAP lookups KEY key FROM RESOURCE table VERSION LATEST PATH \
                 'data.jsonl' DECODE USING entry_codec;",
            ),
        )]);
        let description = cluster_archive();
        let refused = RestorePlan::new(
            &domain_restore("prod", None),
            ArchiveToRestore {
                description: &description,
                models: &latest,
            },
            &Cluster::empty().state(),
        )
        .expect_err("an archive names versions by number");
        assert!(matches!(
            refused.current_context(),
            RestorePlanError::ModelsNotPinned { .. }
        ));
    }
}
