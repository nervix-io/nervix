//! Where each section sits in an archive.
//!
//! ```text
//! manifest.rkyv
//! users.rkyv                                              (cluster backups)
//! domains/<domain>/domain.rkyv
//! domains/<domain>/models.nspl
//! domains/<domain>/resources/<resource>/<version>/version.rkyv
//! domains/<domain>/resources/<resource>/<version>/archive.tar   (unless WITHOUT RESOURCES)
//! domains/<domain>/state/wasm_processor/<processor>/<branch>/descriptor.rkyv
//! domains/<domain>/state/wasm_processor/<processor>/<branch>/guest.bin
//! domains/<domain>/state/kafka_offset/<ingestor>/offsets.rkyv
//! domains/<domain>/state/branch_lifecycle/<kind>/<processor>/branches.rkyv
//! domains/<domain>/state/materialized_relay/<relay>/descriptor.rkyv
//! domains/<domain>/state/materialized_relay/<relay>/groups/<group>/identities.rkyv
//! domains/<domain>/state/materialized_relay/<relay>/groups/<group>/columns.arrow
//! domains/<domain>/state/deduplicator/<deduplicator>/<branch>/descriptor.rkyv
//! domains/<domain>/state/deduplicator/<deduplicator>/<branch>/groups/<group>/keys.arrow
//! domains/<domain>/state/window_processor/<processor>/<branch>/descriptor.rkyv
//! domains/<domain>/state/window_processor/<processor>/<branch>/groups/<group>/input.arrow
//! domains/<domain>/state/window_processor/<processor>/<branch>/groups/<group>/arguments.arrow
//! ```
//!
//! A path is for people and for tar tools; a reader identifies each section by the manifest entry
//! that names it, never by parsing the path. Names are written as they are, except that a resource
//! named `.` or `..` is written `%2E` or `%2E%2E`, because those segments mean something else to
//! every filesystem. `%` is outside the name alphabet, so the escape cannot collide with a name.

use std::{borrow::Cow, fmt, num::NonZeroU64};

use error_stack::Report;
use nervix_models::{BranchKeyFingerprint, DomainName, ModelKind, ModelName, ResourceName};

use crate::error::ArchiveReadError;

/// The manifest's own path, which is always the archive's first entry.
pub(crate) const MANIFEST_PATH: &str = "manifest.rkyv";

/// The longest section path a manifest may name.
const MAX_PATH_BYTES: usize = 4096;

/// A validated relative path of one section inside an archive.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SectionPath(String);

impl SectionPath {
    /// The users of a cluster backup.
    pub fn users() -> Self {
        Self("users.rkyv".to_string())
    }

    /// A domain's lifecycle, clock and resource catalog.
    pub fn domain_record(domain: &DomainName) -> Self {
        Self(format!("{}/domain.rkyv", domain_directory(domain)))
    }

    /// A domain's Models as canonical NSPL.
    pub fn domain_models(domain: &DomainName) -> Self {
        Self(format!("{}/models.nspl", domain_directory(domain)))
    }

    /// The catalog metadata of one resource version.
    pub fn resource_version_record(
        domain: &DomainName,
        resource: &ResourceName,
        version: NonZeroU64,
    ) -> Self {
        Self(format!(
            "{}/version.rkyv",
            version_directory(domain, resource, version)
        ))
    }

    /// The original archive of one resource version.
    pub fn resource_archive(
        domain: &DomainName,
        resource: &ResourceName,
        version: NonZeroU64,
    ) -> Self {
        Self(format!(
            "{}/archive.tar",
            version_directory(domain, resource, version)
        ))
    }

    /// The descriptor of one WASM processor branch's saved guest state.
    pub fn wasm_state_descriptor(
        domain: &DomainName,
        entity: &ModelName,
        branch: Option<&BranchKeyFingerprint>,
    ) -> Self {
        Self(format!(
            "{}/state/wasm_processor/{}/{}/descriptor.rkyv",
            domain_directory(domain),
            segment(entity.as_str()),
            branch_segment(branch)
        ))
    }

    /// The generation descriptor of one materialized relay.
    pub fn materialized_descriptor(domain: &DomainName, entity: &ModelName) -> Self {
        Self(format!(
            "{}/state/materialized_relay/{}/descriptor.rkyv",
            domain_directory(domain),
            segment(entity.as_str())
        ))
    }

    /// One bounded group's typed record identities.
    pub fn materialized_identities(domain: &DomainName, entity: &ModelName, group: u32) -> Self {
        Self(format!(
            "{}/state/materialized_relay/{}/groups/{group:010}/identities.rkyv",
            domain_directory(domain),
            segment(entity.as_str())
        ))
    }

    /// One bounded group's Arrow IPC columns.
    pub fn materialized_columns(domain: &DomainName, entity: &ModelName, group: u32) -> Self {
        Self(format!(
            "{}/state/materialized_relay/{}/groups/{group:010}/columns.arrow",
            domain_directory(domain),
            segment(entity.as_str())
        ))
    }

    /// The descriptor of one deduplicator branch's keyspace.
    pub fn deduplicator_descriptor(
        domain: &DomainName,
        entity: &ModelName,
        branch: Option<&BranchKeyFingerprint>,
    ) -> Self {
        Self(format!(
            "{}/descriptor.rkyv",
            branch_state_directory(domain, "deduplicator", entity, branch)
        ))
    }

    /// One bounded group of a deduplicator branch's typed keys.
    pub fn deduplicator_keys(
        domain: &DomainName,
        entity: &ModelName,
        branch: Option<&BranchKeyFingerprint>,
        group: u32,
    ) -> Self {
        Self(format!(
            "{}/groups/{group:010}/keys.arrow",
            branch_state_directory(domain, "deduplicator", entity, branch)
        ))
    }

    /// The descriptor of one window processor branch's state.
    pub fn window_descriptor(
        domain: &DomainName,
        entity: &ModelName,
        branch: Option<&BranchKeyFingerprint>,
    ) -> Self {
        Self(format!(
            "{}/descriptor.rkyv",
            branch_state_directory(domain, "window_processor", entity, branch)
        ))
    }

    /// One bounded group of a window branch's retained input rows.
    pub fn window_input_rows(
        domain: &DomainName,
        entity: &ModelName,
        branch: Option<&BranchKeyFingerprint>,
        group: u32,
    ) -> Self {
        Self(format!(
            "{}/groups/{group:010}/input.arrow",
            branch_state_directory(domain, "window_processor", entity, branch)
        ))
    }

    /// The aggregate argument columns of the rows in the same window group.
    pub fn window_argument_columns(
        domain: &DomainName,
        entity: &ModelName,
        branch: Option<&BranchKeyFingerprint>,
        group: u32,
    ) -> Self {
        Self(format!(
            "{}/groups/{group:010}/arguments.arrow",
            branch_state_directory(domain, "window_processor", entity, branch)
        ))
    }

    /// The raw guest save named by a WASM state descriptor.
    pub fn wasm_guest_blob(
        domain: &DomainName,
        entity: &ModelName,
        branch: Option<&BranchKeyFingerprint>,
    ) -> Self {
        Self(format!(
            "{}/state/wasm_processor/{}/{}/guest.bin",
            domain_directory(domain),
            segment(entity.as_str()),
            branch_segment(branch)
        ))
    }

    pub fn kafka_offsets(domain: &DomainName, entity: &ModelName) -> Self {
        Self(format!(
            "{}/state/kafka_offset/{}/offsets.rkyv",
            domain_directory(domain),
            segment(entity.as_str())
        ))
    }

    pub fn branch_lifecycle(domain: &DomainName, kind: ModelKind, entity: &ModelName) -> Self {
        Self(format!(
            "{}/state/branch_lifecycle/{}/{}/branches.rkyv",
            domain_directory(domain),
            kind.as_str(),
            segment(entity.as_str())
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validates a path a manifest names: relative, made of non-empty segments, none of them `.`
    /// or `..`, and within the length a tar header can carry.
    pub fn parse(raw: &str) -> Result<Self, Report<ArchiveReadError>> {
        let valid = !raw.is_empty()
            && raw.len() <= MAX_PATH_BYTES
            && raw != MANIFEST_PATH
            && raw
                .split('/')
                .all(|segment| !matches!(segment, "" | "." | ".."))
            && !raw.contains(['\\', '\0']);
        if !valid {
            return Err(Report::new(ArchiveReadError::InvalidPath));
        }
        Ok(Self(raw.to_string()))
    }
}

impl fmt::Display for SectionPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn domain_directory(domain: &DomainName) -> String {
    format!("domains/{}", segment(domain.as_str()))
}

/// Where one branch of a deduplicator or window processor keeps its sections.
fn branch_state_directory(
    domain: &DomainName,
    kind: &str,
    entity: &ModelName,
    branch: Option<&BranchKeyFingerprint>,
) -> String {
    format!(
        "{}/state/{kind}/{}/{}",
        domain_directory(domain),
        segment(entity.as_str()),
        branch_segment(branch)
    )
}

fn branch_segment(branch: Option<&BranchKeyFingerprint>) -> String {
    match branch {
        Some(fingerprint) => fingerprint
            .fingerprint()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        None => "unbranched".to_string(),
    }
}

fn version_directory(domain: &DomainName, resource: &ResourceName, version: NonZeroU64) -> String {
    format!(
        "{}/resources/{}/{version}",
        domain_directory(domain),
        segment(resource.as_str())
    )
}

/// A name as one path segment.
fn segment(name: &str) -> Cow<'_, str> {
    match name {
        "." => Cow::Borrowed("%2E"),
        ".." => Cow::Borrowed("%2E%2E"),
        name => Cow::Borrowed(name),
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain is valid")
    }

    fn resource(name: &str) -> ResourceName {
        ResourceName::parse(name).assured("the test resource is valid")
    }

    #[test]
    fn sections_sit_under_their_domain() {
        let version = NonZeroU64::new(3).assured("three is not zero");
        assert_eq!(SectionPath::users().as_str(), "users.rkyv");
        assert_eq!(
            SectionPath::domain_record(&domain("prod")).as_str(),
            "domains/prod/domain.rkyv"
        );
        assert_eq!(
            SectionPath::domain_models(&domain("prod")).as_str(),
            "domains/prod/models.nspl"
        );
        assert_eq!(
            SectionPath::resource_version_record(&domain("prod"), &resource("model.v2"), version)
                .as_str(),
            "domains/prod/resources/model.v2/3/version.rkyv"
        );
        assert_eq!(
            SectionPath::resource_archive(&domain("prod"), &resource("model.v2"), version).as_str(),
            "domains/prod/resources/model.v2/3/archive.tar"
        );
    }

    #[test]
    fn a_dot_resource_is_escaped_into_a_valid_segment() {
        let version = NonZeroU64::new(1).assured("one is not zero");
        for (name, written) in [(".", "%2E"), ("..", "%2E%2E"), ("...", "...")] {
            let path = SectionPath::resource_archive(&domain("prod"), &resource(name), version);
            assert_eq!(
                path.as_str(),
                format!("domains/prod/resources/{written}/1/archive.tar")
            );
            SectionPath::parse(path.as_str()).verified("the path was just written validly");
        }
    }

    #[test]
    fn a_manifest_may_only_name_safe_relative_paths() {
        for raw in [
            "",
            "/etc/passwd",
            "domains//domain.rkyv",
            "domains/../x",
            "./users.rkyv",
            "a\\b",
            "manifest.rkyv",
        ] {
            assert!(SectionPath::parse(raw).is_err(), "{raw:?} must be refused");
        }
        assert!(SectionPath::parse(&"a/".repeat(4096)).is_err());
        assert_eq!(
            SectionPath::parse("domains/prod/models.nspl")
                .assured("a plain relative path is valid")
                .as_str(),
            "domains/prod/models.nspl"
        );
    }
}
