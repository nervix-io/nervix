//! The sections of a configuration backup, planned from one captured revision.
//!
//! Layer: control plane.
//!
//! - **Owns.** Turning a configuration capture into the archive's sections in archive order — the
//!   users, and per domain its record, its Models as canonical NSPL, and the catalog metadata and
//!   original archive of each resource version — and verifying that each domain's NSPL parses back
//!   to the Models it was rendered from.
//! - **Depends on.** The consensus capture, the registry's creation order, the archive format, the
//!   vocabulary's canonical rendering, and the language layer to parse the rendering back.
//! - **Must not know.** How sections are measured, staged or downloaded.
//!
//! The only coupling between internal state and the archive format is the conversion here: every
//! record is built from the capture's vocabulary values, never from a consensus or registry
//! struct.

use std::num::NonZeroU64;

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_backup::{
    ArchiveRecord, ArchiveScope, DeclaredResource, DomainRecord, PublishedResourceVersion,
    ResourceVersionRecord, ResourceVersionState, SectionContent, SectionPath, UserRecord,
    UsersRecord,
};
use nervix_consensus::ConfigurationCapture;
use nervix_models::{
    BackupResources, DomainName, DomainSchedule, DomainState, DomainStatus, Model,
    RequestedResourceVersion, ResourceId, ResourceName, ResourceUploadState, ResourceVersion,
    ResourceVersionStatus, Statement, Timestamp, canonical_nspl_document,
};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statements};

use super::BackupError;
use crate::registry::creation_order;

/// Where a planned section's bytes come from.
pub(super) enum PlannedContent {
    /// Bytes the plan holds: a record or a domain's NSPL.
    Held {
        content: SectionContent,
        bytes: Vec<u8>,
    },
    /// The original archive of a resource version, read from this node's resource store.
    ResourceArchive { id: ResourceId, archive_bytes: u64 },
}

/// One section of the archive, before it is measured.
pub(super) struct PlannedSection {
    pub(super) path: SectionPath,
    /// The domain the section belongs to. Absent for the users section.
    pub(super) domain: Option<DomainName>,
    pub(super) content: PlannedContent,
}

/// What a backup covers, resolved against the capture.
pub(super) struct PlannedScope {
    pub(super) scope: ArchiveScope,
    /// The domains the archive holds, in name order.
    pub(super) domains: Vec<DomainName>,
}

impl PlannedScope {
    /// Whether the archive holds the users, which only a cluster backup does.
    pub(super) fn with_users(&self) -> bool {
        matches!(self.scope, ArchiveScope::Cluster)
    }
}

/// Plans every section of an archive of `scope` from `capture`, in archive order.
pub(super) fn plan_sections(
    capture: &ConfigurationCapture,
    scope: &PlannedScope,
    resources: BackupResources,
    captured_at: Timestamp,
) -> Result<Vec<PlannedSection>, Report<BackupError>> {
    let mut sections = Vec::new();
    if scope.with_users() {
        let users = UsersRecord {
            users: capture
                .users
                .values()
                .map(|credentials| UserRecord {
                    name: credentials.name.clone(),
                    password_hash: credentials.password_hash.clone(),
                })
                .collect(),
        };
        sections.push(record_section(SectionPath::users(), None, &users)?);
    }
    for domain in &scope.domains {
        let Some(state) = capture.domains.get(domain) else {
            return Err(Report::new(BackupError::DomainNotFound {
                domain: domain.clone(),
            }));
        };
        plan_domain(capture, state, resources, captured_at, &mut sections)?;
    }
    Ok(sections)
}

/// Plans the sections of one domain: its record, its Models, and its resource versions.
fn plan_domain(
    capture: &ConfigurationCapture,
    state: &DomainState,
    resources: BackupResources,
    captured_at: Timestamp,
    sections: &mut Vec<PlannedSection>,
) -> Result<(), Report<BackupError>> {
    let domain = &state.id;
    let declared = declared_resources(&capture.resources, domain)?;
    let record = DomainRecord {
        domain: domain.clone(),
        pace: state.config.pace,
        placement: state.config.placement,
        status: state.status.clone(),
        start_version: state.start_version,
        start_point: state.last_start.clone(),
        clock: state.clock.clone(),
        logical_frontier: logical_frontier(state, captured_at)?,
        resources: declared.clone(),
    };
    sections.push(record_section(
        SectionPath::domain_record(domain),
        Some(domain),
        &record,
    )?);

    let document = models_document(domain, capture.schedule.domain(domain))?;
    sections.push(PlannedSection {
        path: SectionPath::domain_models(domain),
        domain: Some(domain.clone()),
        content: PlannedContent::Held {
            content: SectionContent::Nspl,
            bytes: document.into_bytes(),
        },
    });

    for resource in &declared {
        plan_resource_versions(&capture.resources, domain, resource, resources, sections)?;
    }
    Ok(())
}

/// Every resource `domain` declares, in name order, with the version its next upload receives.
fn declared_resources(
    catalog: &ResourceVersionStatus,
    domain: &DomainName,
) -> Result<Vec<DeclaredResource>, Report<BackupError>> {
    // The counters are sorted by domain and then resource, so one domain's run is located by
    // binary search.
    let counters = &catalog.next_version_by_resource;
    let start = counters.partition_point(|counter| counter.domain < *domain);
    let end = counters.partition_point(|counter| counter.domain <= *domain);
    let run = end
        .checked_sub(start)
        .verified("both partition points split one sorted sequence, so the second is not first");
    let mut declared = Vec::with_capacity(run);
    for counter in &counters[start..end] {
        let Some(next_version) = NonZeroU64::new(counter.next_version) else {
            return Err(Report::new(BackupError::InvalidCatalog {
                domain: domain.clone(),
                resource: counter.identifier.clone(),
            }));
        };
        declared.push(DeclaredResource {
            resource: counter.identifier.clone(),
            next_version,
        });
    }
    Ok(declared)
}

/// Plans the catalog metadata of every version of `declared`, and the original archive of every
/// completed version unless the backup omits resource bytes.
fn plan_resource_versions(
    catalog: &ResourceVersionStatus,
    domain: &DomainName,
    declared: &DeclaredResource,
    resources: BackupResources,
    sections: &mut Vec<PlannedSection>,
) -> Result<(), Report<BackupError>> {
    let mut number = NonZeroU64::MIN;
    while number < declared.next_version {
        let id = ResourceId::new(domain.clone(), declared.resource.clone(), number.get());
        // A number below the counter was assigned to an upload; one whose upload left no record
        // has nothing to archive.
        if let Some(upload) = catalog.uploads.get_version(&id) {
            let state = match &upload.state {
                ResourceUploadState::Completed { .. } => ResourceVersionState::Completed,
                ResourceUploadState::Failed { reason, .. } => ResourceVersionState::Failed {
                    reason: reason.clone(),
                },
                ResourceUploadState::Applying { .. } => ResourceVersionState::Unfinished,
            };
            let published = catalog.version(&id).map(published_version);
            let archive_bytes = published.as_ref().map(|published| published.archive_bytes);
            let completed = matches!(state, ResourceVersionState::Completed);
            let record = ResourceVersionRecord {
                domain: domain.clone(),
                resource: declared.resource.clone(),
                version: number,
                state,
                published,
            };
            sections.push(record_section(
                SectionPath::resource_version_record(domain, &declared.resource, number),
                Some(domain),
                &record,
            )?);
            if completed && resources == BackupResources::Included {
                let Some(archive_bytes) = archive_bytes else {
                    return Err(Report::new(BackupError::ResourceUnavailable {
                        domain: domain.clone(),
                        resource: declared.resource.clone(),
                        version: number,
                    }));
                };
                sections.push(PlannedSection {
                    path: SectionPath::resource_archive(domain, &declared.resource, number),
                    domain: Some(domain.clone()),
                    content: PlannedContent::ResourceArchive { id, archive_bytes },
                });
            }
        }
        let Some(next) = number.checked_add(1) else {
            break;
        };
        number = next;
    }
    Ok(())
}

fn published_version(version: &ResourceVersion) -> PublishedResourceVersion {
    PublishedResourceVersion {
        root_checksum: version.root_checksum.clone(),
        manifest_checksum: version.manifest_checksum.clone(),
        file_count: version.file_count,
        total_bytes: version.total_bytes,
        archive_bytes: version.archive_bytes,
        created_at: version.created_at,
        created_by_node: version.created_by_node.clone(),
    }
}

/// The logical instant a running or paused domain's committed clock mapping had reached at
/// `captured_at`. A stopped domain advances no clock, and a domain without a committed mapping has
/// none to project, so neither has a frontier.
fn logical_frontier(
    state: &DomainState,
    captured_at: Timestamp,
) -> Result<Option<Timestamp>, Report<BackupError>> {
    if state.status == DomainStatus::Stopped {
        return Ok(None);
    }
    let Some(clock) = &state.clock else {
        return Ok(None);
    };
    match clock.logical_time_at(captured_at) {
        Ok(frontier) => Ok(Some(frontier)),
        Err(error) => Err(error.change_context(BackupError::ClockProjection {
            domain: state.id.clone(),
        })),
    }
}

/// The canonical NSPL of `domain`'s Models, each after every Model it names, verified to parse
/// back to exactly those Models.
fn models_document(
    domain: &DomainName,
    schedule: Option<&DomainSchedule>,
) -> Result<String, Report<BackupError>> {
    let Some(schedule) = schedule else {
        return Ok(String::new());
    };
    let order = creation_order(schedule).map_err(|error| {
        error.change_context(BackupError::InvalidModels {
            domain: domain.clone(),
        })
    })?;
    let mut models = Vec::with_capacity(order.len());
    for node in &order {
        let Some(scheduled) = schedule.nodes.get(node) else {
            return Err(Report::new(BackupError::InvalidModels {
                domain: domain.clone(),
            }));
        };
        models.push(scheduled.config.as_ref());
    }
    let document = canonical_nspl_document(models.iter().copied()).map_err(|error| {
        error.change_context(BackupError::Rendering {
            domain: domain.clone(),
        })
    })?;
    verify_document(domain, &document, &models)?;
    Ok(document)
}

/// Parses `document` back and requires exactly `models`, in order.
///
/// A stored Model binds resource versions by number, so its rendering names every version as a
/// number; a `LATEST` read back means the rendering drifted from the Model.
fn verify_document(
    domain: &DomainName,
    document: &str,
    models: &[&Model],
) -> Result<(), Report<BackupError>> {
    let drifted = || {
        Report::new(BackupError::Verification {
            domain: domain.clone(),
        })
    };
    let statements = match parse_client_statements(document) {
        Ok(statements) => statements,
        Err(error) => {
            return Err(error.change_context(BackupError::Verification {
                domain: domain.clone(),
            }));
        }
    };
    if statements.len() != models.len() {
        return Err(drifted());
    }
    for (statement, model) in statements.into_iter().zip(models) {
        let ClientStatement::Server(Statement::Create(create)) = statement else {
            return Err(drifted());
        };
        if create.if_not_exists {
            return Err(drifted());
        }
        let reparsed = (*create.body).try_map_resource_versions(pinned_version);
        let Ok(reparsed) = reparsed else {
            return Err(drifted());
        };
        if reparsed != **model {
            return Err(drifted());
        }
    }
    Ok(())
}

/// The number a reparsed binding names. A rendering of a stored Model never writes `LATEST`.
fn pinned_version(
    resource: &ResourceName,
    requested: RequestedResourceVersion,
) -> Result<u64, ResourceName> {
    match requested {
        RequestedResourceVersion::Number(version) => Ok(version),
        RequestedResourceVersion::Latest => Err(resource.clone()),
    }
}

fn record_section(
    path: SectionPath,
    domain: Option<&DomainName>,
    record: &impl ArchiveRecord,
) -> Result<PlannedSection, Report<BackupError>> {
    let bytes = record
        .encode()
        .map_err(|error| error.change_context(BackupError::Encoding))?;
    Ok(PlannedSection {
        path,
        domain: domain.cloned(),
        content: PlannedContent::Held {
            content: SectionContent::Record(record_kind(record)),
            bytes,
        },
    })
}

fn record_kind<R: ArchiveRecord>(_record: &R) -> nervix_backup::RecordKind {
    R::KIND
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use meticulous::ResultExt as _;
    use nervix_consensus::{AppliedLogPosition, UserCredentials};
    use nervix_models::{
        ClusterNodeName, ClusterSchedule, DomainConfig, DomainPace, DomainStartPoint,
        PlacementPolicy, ResourceUpload, ResourceUploadIdentity, ResourceUploadKey,
        ResourceUploads, ResourceVersionCounter, ScheduledNode, SchemaFingerprint, UserName,
    };
    use sorted_vec::SortedVec;

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

    fn state(name: &str, status: DomainStatus) -> DomainState {
        DomainState {
            id: domain(name),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                placement: PlacementPolicy::Neutral,
            },
            status,
            start_version: 2,
            last_start: DomainStartPoint::Resume,
            clock: Some(nervix_models::DomainClockState::new(
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(5_000),
                nervix_models::DomainTimeRate::ONE,
            )),
        }
    }

    fn models(source: &str) -> Vec<Model> {
        parse_client_statements(source)
            .assured("the test models parse")
            .into_iter()
            .map(|statement| {
                let ClientStatement::Server(Statement::Create(create)) = statement else {
                    panic!("the test source only creates models");
                };
                (*create.body)
                    .try_map_resource_versions(pinned_version)
                    .assured("the test source pins its versions")
            })
            .collect()
    }

    const PROD_MODELS: &str = "
        CREATE RELAY orders SCHEMA order_event UNBRANCHED;
        CREATE SCHEMA order_event ( id U64, amount I64 );
    ";

    fn capture() -> ConfigurationCapture {
        let prod = domain("prod");
        let schedule = DomainSchedule::new(
            prod.clone(),
            models(PROD_MODELS)
                .into_iter()
                .map(|model| ScheduledNode::new(model, SchemaFingerprint::from_digest([0; 32]))),
            Vec::new(),
        );
        let upload = |version: u64, state: ResourceUploadState| ResourceUpload {
            key: ResourceUploadKey::new(
                user("alice"),
                prod.clone(),
                resource("model"),
                ResourceUploadIdentity::parse(format!("upload-{version}"))
                    .assured("the test identity is valid"),
            ),
            version,
            state,
        };
        let published = ResourceVersion {
            id: ResourceId::new(prod.clone(), resource("model"), 1),
            root_checksum: "root-1".to_string(),
            manifest_checksum: "manifest-1".to_string(),
            file_count: 1,
            total_bytes: 5,
            archive_bytes: 2048,
            created_at: Timestamp::from_unix_nanos(7),
            created_by_node: ClusterNodeName::parse("node-1").assured("valid node"),
        };
        ConfigurationCapture {
            applied: AppliedLogPosition { term: 3, index: 42 },
            domains: BTreeMap::from([
                (prod.clone(), state("prod", DomainStatus::Running)),
                (domain("staging"), state("staging", DomainStatus::Stopped)),
            ]),
            schedule: ClusterSchedule::from_iter([schedule]),
            users: BTreeMap::from([(
                user("alice"),
                UserCredentials {
                    name: user("alice"),
                    password_hash: "$argon2id$hash".to_string(),
                },
            )]),
            resources: ResourceVersionStatus {
                next_version_by_resource: SortedVec::from_unsorted(vec![ResourceVersionCounter {
                    domain: prod.clone(),
                    identifier: resource("model"),
                    next_version: 3,
                }]),
                versions: SortedVec::from_unsorted(vec![published]),
                replicas: SortedVec::new(),
                uploads: ResourceUploads::try_from_uploads([
                    upload(
                        1,
                        ResourceUploadState::Completed {
                            root_checksum: "root-1".to_string(),
                            outcome_revision: 9,
                        },
                    ),
                    upload(
                        2,
                        ResourceUploadState::Failed {
                            root_checksum: "root-2".to_string(),
                            outcome_revision: 11,
                            reason: "installation failed".to_string(),
                        },
                    ),
                ])
                .assured("the test uploads are distinct"),
            },
        }
    }

    fn paths(sections: &[PlannedSection]) -> Vec<&str> {
        sections
            .iter()
            .map(|section| section.path.as_str())
            .collect()
    }

    #[test]
    fn a_cluster_backup_plans_users_then_each_domain_in_archive_order() {
        let scope = PlannedScope {
            scope: ArchiveScope::Cluster,
            domains: vec![domain("prod"), domain("staging")],
        };
        let sections = plan_sections(
            &capture(),
            &scope,
            BackupResources::Included,
            Timestamp::from_unix_nanos(10_000),
        )
        .assured("the capture plans");
        assert_eq!(
            paths(&sections),
            [
                "users.rkyv",
                "domains/prod/domain.rkyv",
                "domains/prod/models.nspl",
                "domains/prod/resources/model/1/version.rkyv",
                "domains/prod/resources/model/1/archive.tar",
                "domains/prod/resources/model/2/version.rkyv",
                "domains/staging/domain.rkyv",
                "domains/staging/models.nspl",
            ]
        );
        let PlannedContent::Held { bytes, .. } = &sections[2].content else {
            panic!("a domain's NSPL is held");
        };
        let document = std::str::from_utf8(bytes).assured("NSPL is UTF-8");
        let schema = document
            .find("CREATE SCHEMA")
            .assured("the schema is rendered");
        let relay = document
            .find("CREATE RELAY")
            .assured("the relay is rendered");
        assert!(
            schema < relay,
            "the schema precedes the relay that names it"
        );
        let PlannedContent::ResourceArchive { archive_bytes, .. } = &sections[4].content else {
            panic!("a completed version's archive is read from the store");
        };
        assert_eq!(*archive_bytes, 2048);
    }

    #[test]
    fn without_resources_no_archive_is_planned() {
        let scope = PlannedScope {
            scope: ArchiveScope::Domain(domain("prod")),
            domains: vec![domain("prod")],
        };
        let sections = plan_sections(
            &capture(),
            &scope,
            BackupResources::Omitted,
            Timestamp::from_unix_nanos(10_000),
        )
        .assured("the capture plans");
        assert_eq!(
            paths(&sections),
            [
                "domains/prod/domain.rkyv",
                "domains/prod/models.nspl",
                "domains/prod/resources/model/1/version.rkyv",
                "domains/prod/resources/model/2/version.rkyv",
            ]
        );
    }

    #[test]
    fn only_a_running_domain_records_its_logical_frontier() {
        let running = logical_frontier(
            &state("prod", DomainStatus::Running),
            Timestamp::from_unix_nanos(10_000),
        )
        .assured("the mapping projects");
        assert_eq!(running, Some(Timestamp::from_unix_nanos(14_000)));
        let stopped = logical_frontier(
            &state("prod", DomainStatus::Stopped),
            Timestamp::from_unix_nanos(10_000),
        )
        .assured("a stopped domain projects nothing");
        assert_eq!(stopped, None);
    }

    #[test]
    fn a_document_that_drifted_from_its_models_is_refused() {
        let committed = models(PROD_MODELS);
        let references = committed.iter().collect::<Vec<_>>();
        let missing = "CREATE SCHEMA order_event ( id U64, amount I64 );\n";
        let error = verify_document(&domain("prod"), missing, &references)
            .expect_err("a document missing a model drifted");
        assert!(matches!(
            error.current_context(),
            BackupError::Verification { .. }
        ));
        let document = canonical_nspl_document(references.iter().copied())
            .assured("the committed models render");
        verify_document(&domain("prod"), &document, &references)
            .assured("the rendering of the committed models parses back to them");
    }

    #[test]
    fn a_domain_missing_from_the_capture_is_refused() {
        let scope = PlannedScope {
            scope: ArchiveScope::Domain(domain("gone")),
            domains: vec![domain("gone")],
        };
        let error = plan_sections(
            &capture(),
            &scope,
            BackupResources::Included,
            Timestamp::from_unix_nanos(10_000),
        )
        .err()
        .assured("an unknown domain is refused");
        assert!(matches!(
            error.current_context(),
            BackupError::DomainNotFound { .. }
        ));
    }
}
