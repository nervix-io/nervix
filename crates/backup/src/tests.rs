//! Round trips of every record, header refusals, and archives written and read as streams.

use std::{io::Read as _, num::NonZeroU64};

use bolero::check;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BackupResources, ClusterNodeName, DomainClockPeriod, DomainClockSkew, DomainClockState,
    DomainName, DomainPace, DomainStartPoint, DomainStatus, DomainTimeRate, PlacementPolicy,
    ResourceName, Timestamp, UserName,
};

use crate::{
    ARCHIVE_FORMAT_MAJOR, ArchiveLayout, ArchivePiece, ArchiveReadError, ArchiveRecord,
    ArchiveScope, ArchiveWriteError, BackupManifest, DeclaredResource, DomainCapture, DomainRecord,
    PublishedResourceVersion, RaftLogPosition, RecordKind, ResourceVersionRecord,
    ResourceVersionState, SectionContent, SectionDigester, SectionEntry, SectionPath,
    SectionReader, SectionVisitor, UserRecord, UsersRecord, describe_archive, read_archive,
    read_archive_contents,
    section::{RECORD_HEADER_BYTES, RECORD_MAGIC, decode_record, encode_record},
    wire::{
        DeclaredResourceWire, DomainWire, ManifestWire, PaceWire, PlacementWire, StartPointWire,
        StatusWire,
    },
};

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test domain is valid")
}

fn resource(name: &str) -> ResourceName {
    ResourceName::parse(name).assured("the test resource is valid")
}

fn version(number: u64) -> NonZeroU64 {
    NonZeroU64::new(number).assured("test versions count from one")
}

fn rate(value: f64) -> DomainTimeRate {
    DomainTimeRate::try_from(value).assured("the test rate is positive and finite")
}

fn users() -> UsersRecord {
    UsersRecord {
        users: vec![
            UserRecord {
                name: UserName::parse("alice").assured("the test user is valid"),
                password_hash: "$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA".to_string(),
            },
            UserRecord {
                name: UserName::parse("default").assured("the test user is valid"),
                password_hash: "$argon2id$v=19$m=8,t=1,p=1$c2FsdA$aGFzaA".to_string(),
            },
        ],
    }
}

fn paced_domain(name: &str) -> DomainRecord {
    DomainRecord {
        domain: domain(name),
        pace: DomainPace::Paced {
            period: DomainClockPeriod::from_nanos(version(100_000_000)),
            skew: DomainClockSkew::from_nanos(10_000_000),
        },
        placement: PlacementPolicy::PreferColocation,
        status: DomainStatus::Running,
        start_version: 3,
        start_point: DomainStartPoint::At {
            timestamp: Timestamp::from_unix_nanos(1_893_456_000_000_000_000),
            time_rate: rate(2.5),
        },
        clock: Some(DomainClockState::new(
            Timestamp::from_unix_nanos(1_790_000_000_000_000_000),
            Timestamp::from_unix_nanos(1_893_456_000_000_000_000),
            rate(2.5),
        )),
        logical_frontier: Some(Timestamp::from_unix_nanos(1_893_456_100_000_000_000)),
        resources: vec![
            DeclaredResource {
                resource: resource("model.v2"),
                next_version: version(3),
            },
            DeclaredResource {
                resource: resource(".."),
                next_version: version(1),
            },
        ],
    }
}

fn unpaced_domain(name: &str) -> DomainRecord {
    DomainRecord {
        domain: domain(name),
        pace: DomainPace::Unpaced,
        placement: PlacementPolicy::Neutral,
        status: DomainStatus::Stopped,
        start_version: 0,
        start_point: DomainStartPoint::Resume,
        clock: None,
        logical_frontier: None,
        resources: Vec::new(),
    }
}

fn completed_version(domain_name: &str, number: u64) -> ResourceVersionRecord {
    ResourceVersionRecord {
        domain: domain(domain_name),
        resource: resource("model.v2"),
        version: version(number),
        state: ResourceVersionState::Completed,
        published: Some(PublishedResourceVersion {
            root_checksum: format!("sha256:root-{number}"),
            manifest_checksum: format!("sha256:manifest-{number}"),
            file_count: 2,
            total_bytes: 11,
            archive_bytes: 2048,
            created_at: Timestamp::from_unix_nanos(1_790_000_000_000_000_000),
            created_by_node: ClusterNodeName::parse("node-2").assured("the test node is valid"),
        }),
    }
}

fn failed_version(domain_name: &str, number: u64) -> ResourceVersionRecord {
    ResourceVersionRecord {
        domain: domain(domain_name),
        resource: resource("model.v2"),
        version: version(number),
        state: ResourceVersionState::Failed {
            reason: "installation failed on node-3".to_string(),
        },
        published: None,
    }
}

#[test]
fn every_record_round_trips_through_its_section() {
    let encoded = users().encode().assured("the users record encodes");
    assert_eq!(
        UsersRecord::decode("users.rkyv", &encoded).assured("its own encoding decodes"),
        users()
    );
    for record in [paced_domain("prod"), unpaced_domain("staging")] {
        let encoded = record.encode().assured("a domain record encodes");
        assert_eq!(
            DomainRecord::decode("domain.rkyv", &encoded).assured("its own encoding decodes"),
            record
        );
    }
    for record in [
        completed_version("prod", 1),
        failed_version("prod", 2),
        ResourceVersionRecord {
            state: ResourceVersionState::Unfinished,
            ..completed_version("prod", 3)
        },
    ] {
        let encoded = record.encode().assured("a resource version record encodes");
        assert_eq!(
            ResourceVersionRecord::decode("version.rkyv", &encoded)
                .assured("its own encoding decodes"),
            record
        );
    }
    let manifest = manifest_of(ArchiveScope::Cluster, Vec::new());
    let encoded = manifest.encode().assured("the manifest encodes");
    assert_eq!(
        BackupManifest::decode("manifest.rkyv", &encoded).assured("its own encoding decodes"),
        manifest
    );
}

#[test]
fn a_section_of_another_kind_is_refused_at_its_header() {
    let domain_section = paced_domain("prod")
        .encode()
        .assured("a domain record encodes");
    let error = UsersRecord::decode("users.rkyv", &domain_section)
        .expect_err("a domain record is not a users record");
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::ForeignRecordKind {
            path: "users.rkyv".to_string(),
            expected: RecordKind::Users,
            found: RecordKind::Domain.tag(),
        }
    );
}

#[test]
fn a_section_of_another_version_is_refused_at_its_header() {
    let mut section = paced_domain("prod")
        .encode()
        .assured("a domain record encodes");
    let version_at = RECORD_MAGIC.len() + 2;
    section[version_at..version_at + 2].copy_from_slice(&2_u16.to_le_bytes());
    let error = DomainRecord::decode("domains/prod/domain.rkyv", &section)
        .expect_err("version 2 of the domain record is unknown");
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::UnsupportedRecordVersion {
            path: "domains/prod/domain.rkyv".to_string(),
            kind: RecordKind::Domain,
            found: 2,
            supported: 1,
        }
    );
}

#[test]
fn bytes_without_the_record_magic_are_refused() {
    for bytes in [&b""[..], &b"NVXB"[..], &b"NOTARECORDATALL!"[..]] {
        let error = UsersRecord::decode("users.rkyv", bytes).expect_err("this is no record");
        assert!(
            matches!(
                error.current_context(),
                ArchiveReadError::ForeignMagic { .. }
            ),
            "{error:?}"
        );
    }
}

#[test]
fn a_payload_that_fails_bytecheck_is_refused() {
    let mut section = users().encode().assured("the users record encodes");
    // Point the root at bytes that are not a users record by cutting the payload short.
    section.truncate(RECORD_HEADER_BYTES + 3);
    let error = UsersRecord::decode("users.rkyv", &section).expect_err("a cut payload is invalid");
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::InvalidRecord {
            path: "users.rkyv".to_string(),
            kind: RecordKind::Users,
        }
    );
}

#[test]
fn values_that_fail_their_vocabulary_are_refused() {
    let invalid_name = DomainWire {
        domain: "Not A Domain".to_string(),
        pace: PaceWire::Unpaced,
        placement: PlacementWire::Neutral,
        status: StatusWire::Stopped,
        start_version: 0,
        start_point: StartPointWire::Resume,
        clock: None,
        logical_frontier_unix_nanos: None,
        resources: Vec::new(),
    };
    let zero_period = DomainWire {
        domain: "prod".to_string(),
        pace: PaceWire::Paced {
            period_nanos: 0,
            skew_nanos: 0,
        },
        ..invalid_name.clone()
    };
    let negative_rate = DomainWire {
        domain: "prod".to_string(),
        start_point: StartPointWire::Now { time_rate: -1.0 },
        ..invalid_name.clone()
    };
    let zero_next_version = DomainWire {
        domain: "prod".to_string(),
        resources: vec![DeclaredResourceWire {
            resource: "model".to_string(),
            next_version: 0,
        }],
        ..invalid_name.clone()
    };
    for (wire, field) in [
        (invalid_name, "domain name"),
        (zero_period, "clock period"),
        (negative_rate, "time rate"),
        (zero_next_version, "next resource version"),
    ] {
        let section = encode_record(RecordKind::Domain, 1, &wire).assured("the wire encodes");
        let error =
            DomainRecord::decode("domain.rkyv", &section).expect_err("the value is invalid");
        assert_eq!(
            error.current_context(),
            &ArchiveReadError::InvalidValue {
                path: "domain.rkyv".to_string(),
                field,
            }
        );
    }
}

#[test]
fn a_manifest_of_another_major_version_is_refused() {
    let manifest = manifest_of(ArchiveScope::Cluster, Vec::new());
    let mut wire = ManifestWire::from(&manifest);
    wire.format_major = ARCHIVE_FORMAT_MAJOR + 1;
    let section = encode_record(RecordKind::Manifest, 1, &wire).assured("the wire encodes");
    let error = BackupManifest::decode("manifest.rkyv", &section).expect_err("major 2 is unknown");
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::UnsupportedArchiveFormat {
            found: ARCHIVE_FORMAT_MAJOR + 1,
            supported: ARCHIVE_FORMAT_MAJOR,
        }
    );
    let decoded: ManifestWire = decode_record("manifest.rkyv", RecordKind::Manifest, 1, &section)
        .assured("the wire shape itself is valid");
    assert_eq!(decoded.format_major, ARCHIVE_FORMAT_MAJOR + 1);
}

/// One section of a test archive and its bytes.
struct TestSection {
    entry: SectionEntry,
    bytes: Vec<u8>,
}

fn record_section(path: SectionPath, record: &impl ArchiveRecord) -> TestSection {
    let bytes = record.encode().assured("the test record encodes");
    bytes_section(path, SectionContent::Record(record_kind(record)), bytes)
}

fn record_kind<R: ArchiveRecord>(_record: &R) -> RecordKind {
    R::KIND
}

fn bytes_section(path: SectionPath, content: SectionContent, bytes: Vec<u8>) -> TestSection {
    let entry = SectionEntry {
        path,
        content,
        length: u64::try_from(bytes.len()).assured("test sections are small"),
        digest: SectionDigester::digest_of(&bytes),
    };
    TestSection { entry, bytes }
}

fn manifest_of(scope: ArchiveScope, sections: Vec<SectionEntry>) -> BackupManifest {
    let domains = match &scope {
        ArchiveScope::Cluster => vec![
            DomainCapture {
                domain: domain("prod"),
                revision: 42,
                raft_log: RaftLogPosition { term: 3, index: 42 },
            },
            DomainCapture {
                domain: domain("staging"),
                revision: 42,
                raft_log: RaftLogPosition { term: 3, index: 42 },
            },
        ],
        ArchiveScope::Domain(name) => vec![DomainCapture {
            domain: name.clone(),
            revision: 7,
            raft_log: RaftLogPosition { term: 1, index: 7 },
        }],
    };
    BackupManifest {
        producer_version: "0.1.0-dev".to_string(),
        language_version: "0.1.0-dev".to_string(),
        cluster_id: "nervix-test".to_string(),
        captured_at: Timestamp::from_unix_nanos(1_790_000_000_123_456_789),
        scope,
        resources: BackupResources::Included,
        domains,
        sections,
    }
}

/// A cluster archive of two domains, one with a resource version whose name needs a long tar path.
fn cluster_sections(resources: BackupResources) -> Vec<TestSection> {
    let long_domain = "prod";
    let mut sections = vec![
        record_section(SectionPath::users(), &users()),
        record_section(
            SectionPath::domain_record(&domain(long_domain)),
            &paced_domain(long_domain),
        ),
        bytes_section(
            SectionPath::domain_models(&domain(long_domain)),
            SectionContent::Nspl,
            b"CREATE SCHEMA order_event (\n  id U64\n);\n".to_vec(),
        ),
    ];
    let completed = completed_version(long_domain, 1);
    sections.push(record_section(
        SectionPath::resource_version_record(
            &completed.domain,
            &completed.resource,
            completed.version,
        ),
        &completed,
    ));
    if resources == BackupResources::Included {
        sections.push(bytes_section(
            SectionPath::resource_archive(
                &completed.domain,
                &completed.resource,
                completed.version,
            ),
            SectionContent::ResourceArchive,
            (0..1500_u32).map(|value| value.to_le_bytes()[0]).collect(),
        ));
    }
    let failed = failed_version(long_domain, 2);
    sections.push(record_section(
        SectionPath::resource_version_record(&failed.domain, &failed.resource, failed.version),
        &failed,
    ));
    sections.push(record_section(
        SectionPath::domain_record(&domain("staging")),
        &unpaced_domain("staging"),
    ));
    sections.push(bytes_section(
        SectionPath::domain_models(&domain("staging")),
        SectionContent::Nspl,
        Vec::new(),
    ));
    sections
}

fn write_archive(manifest: BackupManifest, sections: &[TestSection]) -> (ArchiveLayout, Vec<u8>) {
    let layout = ArchiveLayout::new(manifest).assured("the test archive lays out");
    let mut bytes = Vec::new();
    let mut remaining = sections.iter();
    layout
        .write_to(&mut bytes, |entry, sink| {
            let section = remaining
                .next()
                .assured("the layout asks for the sections in manifest order");
            assert_eq!(&section.entry, entry);
            std::io::Write::write_all(sink, &section.bytes)
        })
        .assured("the test archive writes");
    (layout, bytes)
}

fn cluster_archive(resources: BackupResources) -> (ArchiveLayout, Vec<u8>) {
    let sections = cluster_sections(resources);
    let mut manifest = manifest_of(
        ArchiveScope::Cluster,
        sections
            .iter()
            .map(|section| section.entry.clone())
            .collect(),
    );
    manifest.resources = resources;
    write_archive(manifest, &sections)
}

#[test]
fn an_archive_is_exactly_as_long_as_its_layout_says_and_starts_with_its_manifest() {
    let (layout, bytes) = cluster_archive(BackupResources::Included);
    assert_eq!(
        u64::try_from(bytes.len()).assured("test archives are small"),
        layout.total_bytes()
    );
    assert_eq!(bytes.len() % 512, 0);
    let mut archive = tar::Archive::new(bytes.as_slice());
    let paths = archive
        .entries()
        .assured("the archive is a tar stream")
        .map(|entry| {
            let entry = entry.assured("every entry reads");
            String::from_utf8(entry.path_bytes().into_owned()).assured("paths are ASCII")
        })
        .collect::<Vec<_>>();
    assert_eq!(paths.first().map(String::as_str), Some("manifest.rkyv"));
    let listed = layout
        .manifest()
        .sections
        .iter()
        .map(|entry| entry.path.to_string())
        .collect::<Vec<_>>();
    assert_eq!(&paths[1..], listed.as_slice());
}

#[test]
fn an_archive_reads_back_as_the_records_and_sections_it_was_written_from() {
    let (layout, bytes) = cluster_archive(BackupResources::Included);
    let description = describe_archive(bytes.as_slice()).assured("the archive reads back");
    assert_eq!(&description.manifest, layout.manifest());
    assert_eq!(description.users, Some(users()));
    assert_eq!(description.domains.len(), 2);
    let prod = &description.domains[0];
    assert_eq!(prod.record, paced_domain("prod"));
    assert_eq!(prod.capture.revision, 42);
    assert_eq!(prod.models.length, 40);
    assert_eq!(prod.resource_versions.len(), 2);
    assert_eq!(
        prod.resource_versions[0].record,
        completed_version("prod", 1)
    );
    let archive = prod.resource_versions[0]
        .archive
        .as_ref()
        .assured("the completed version carries its archive");
    assert_eq!(archive.length, 1500);
    assert_eq!(prod.resource_versions[1].record, failed_version("prod", 2));
    assert_eq!(prod.resource_versions[1].archive, None);
    let staging = &description.domains[1];
    assert_eq!(staging.record, unpaced_domain("staging"));
    assert_eq!(staging.models.length, 0);
    assert!(staging.resource_versions.is_empty());
}

#[test]
fn without_resources_an_archive_keeps_the_catalog_and_omits_the_bytes() {
    let (layout, bytes) = cluster_archive(BackupResources::Omitted);
    let description = describe_archive(bytes.as_slice()).assured("the archive reads back");
    assert_eq!(description.manifest.resources, BackupResources::Omitted);
    let prod = &description.domains[0];
    let completed = &prod.resource_versions[0];
    assert_eq!(completed.archive, None);
    let published = completed
        .record
        .published
        .as_ref()
        .assured("the catalog metadata stays");
    assert_eq!(published.root_checksum, "sha256:root-1");
    let (full, _) = cluster_archive(BackupResources::Included);
    assert!(layout.total_bytes() < full.total_bytes());
}

#[test]
fn a_domain_archive_holds_no_users() {
    let prod = domain("prod");
    let sections = vec![
        record_section(SectionPath::domain_record(&prod), &unpaced_domain("prod")),
        bytes_section(
            SectionPath::domain_models(&prod),
            SectionContent::Nspl,
            b"CREATE SCHEMA s (id U64);".to_vec(),
        ),
    ];
    let manifest = manifest_of(
        ArchiveScope::Domain(prod.clone()),
        sections
            .iter()
            .map(|section| section.entry.clone())
            .collect(),
    );
    let (_, bytes) = write_archive(manifest, &sections);
    let description = describe_archive(bytes.as_slice()).assured("the archive reads back");
    assert_eq!(description.users, None);
    assert_eq!(description.domains.len(), 1);
    assert_eq!(description.manifest.scope, ArchiveScope::Domain(prod));
}

#[test]
fn a_long_path_travels_in_a_gnu_long_name_entry() {
    let long_domain = domain(&"d".repeat(100));
    let long_resource = resource(&"r".repeat(128));
    let record = ResourceVersionRecord {
        domain: long_domain.clone(),
        resource: long_resource.clone(),
        ..completed_version("prod", 1)
    };
    let sections = vec![
        record_section(
            SectionPath::domain_record(&long_domain),
            &DomainRecord {
                domain: long_domain.clone(),
                ..unpaced_domain("prod")
            },
        ),
        bytes_section(
            SectionPath::domain_models(&long_domain),
            SectionContent::Nspl,
            b"".to_vec(),
        ),
        record_section(
            SectionPath::resource_version_record(&long_domain, &long_resource, version(1)),
            &record,
        ),
        bytes_section(
            SectionPath::resource_archive(&long_domain, &long_resource, version(1)),
            SectionContent::ResourceArchive,
            vec![7; 513],
        ),
    ];
    assert!(sections[3].entry.path.as_str().len() > 255);
    let manifest = manifest_of(
        ArchiveScope::Domain(long_domain.clone()),
        sections
            .iter()
            .map(|section| section.entry.clone())
            .collect(),
    );
    let (layout, bytes) = write_archive(manifest, &sections);
    assert_eq!(
        u64::try_from(bytes.len()).assured("test archives are small"),
        layout.total_bytes()
    );
    let description = describe_archive(bytes.as_slice()).assured("long paths read back");
    assert_eq!(
        description.domains[0].resource_versions[0]
            .archive
            .as_ref()
            .map(|archive| archive.length),
        Some(513)
    );
}

/// The bytes a described section's place names inside `archive`.
fn bytes_at<'archive>(
    archive: &'archive [u8],
    section: &crate::DescribedSection,
) -> &'archive [u8] {
    let start = usize::try_from(section.offset).assured("test archives are small");
    let length = usize::try_from(section.length).assured("test archives are small");
    &archive[start..start + length]
}

#[test]
fn every_described_section_names_the_place_of_its_bytes() {
    let (_, bytes) = cluster_archive(BackupResources::Included);
    let description = describe_archive(bytes.as_slice()).assured("the archive reads back");
    let prod = &description.domains[0];
    let models = bytes_at(&bytes, &prod.models);
    assert_eq!(models, b"CREATE SCHEMA order_event (\n  id U64\n);\n");
    assert_eq!(SectionDigester::digest_of(models), prod.models.digest);
    let archive = prod.resource_versions[0]
        .archive
        .as_ref()
        .assured("the completed version carries its archive");
    let archived = bytes_at(&bytes, archive);
    assert_eq!(archived.len(), 1500);
    assert_eq!(SectionDigester::digest_of(archived), archive.digest);
    assert_eq!(
        archive.offset,
        u64::try_from(section_range(&bytes, "domains/prod/resources/model.v2/1/archive.tar").start)
            .assured("small archive")
    );
}

#[test]
fn a_long_named_section_names_the_place_of_its_bytes() {
    let long_domain = domain(&"d".repeat(100));
    let long_resource = resource(&"r".repeat(128));
    let record = ResourceVersionRecord {
        domain: long_domain.clone(),
        resource: long_resource.clone(),
        ..completed_version("prod", 1)
    };
    let content = (0..700_u32)
        .map(|value| value.to_le_bytes()[1])
        .collect::<Vec<_>>();
    let sections = vec![
        record_section(
            SectionPath::domain_record(&long_domain),
            &DomainRecord {
                domain: long_domain.clone(),
                ..unpaced_domain("prod")
            },
        ),
        bytes_section(
            SectionPath::domain_models(&long_domain),
            SectionContent::Nspl,
            Vec::new(),
        ),
        record_section(
            SectionPath::resource_version_record(&long_domain, &long_resource, version(1)),
            &record,
        ),
        bytes_section(
            SectionPath::resource_archive(&long_domain, &long_resource, version(1)),
            SectionContent::ResourceArchive,
            content.clone(),
        ),
    ];
    let manifest = manifest_of(
        ArchiveScope::Domain(long_domain),
        sections
            .iter()
            .map(|section| section.entry.clone())
            .collect(),
    );
    let (_, bytes) = write_archive(manifest, &sections);
    let description = describe_archive(bytes.as_slice()).assured("long paths read back");
    let archive = description.domains[0].resource_versions[0]
        .archive
        .as_ref()
        .assured("the version carries its archive");
    assert_eq!(bytes_at(&bytes, archive), content.as_slice());
}

#[test]
fn the_contents_of_an_archive_hold_every_domains_models() {
    let (_, bytes) = cluster_archive(BackupResources::Included);
    let contents = read_archive_contents(bytes.as_slice()).assured("the archive reads back");
    assert_eq!(
        contents.description,
        describe_archive(bytes.as_slice()).assured("the archive describes")
    );
    assert_eq!(
        contents.models.get(&domain("prod")).map(String::as_str),
        Some("CREATE SCHEMA order_event (\n  id U64\n);\n")
    );
    assert_eq!(
        contents.models.get(&domain("staging")).map(String::as_str),
        Some("")
    );
    assert_eq!(contents.models.len(), 2);
}

#[test]
fn models_that_are_not_utf8_are_refused() {
    let prod = domain("prod");
    let sections = vec![
        record_section(SectionPath::domain_record(&prod), &unpaced_domain("prod")),
        bytes_section(
            SectionPath::domain_models(&prod),
            SectionContent::Nspl,
            vec![0xff, 0xfe, b';'],
        ),
    ];
    let manifest = manifest_of(
        ArchiveScope::Domain(prod),
        sections
            .iter()
            .map(|section| section.entry.clone())
            .collect(),
    );
    let (_, bytes) = write_archive(manifest, &sections);
    let error = read_archive_contents(bytes.as_slice()).expect_err("the NSPL is not text");
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::InvalidText {
            path: "domains/prod/models.nspl".to_string(),
        }
    );
}

#[test]
fn the_contents_reader_refuses_what_the_description_refuses() {
    let (_, mut bytes) = cluster_archive(BackupResources::Included);
    let range = section_range(&bytes, "domains/prod/models.nspl");
    bytes[range.start] ^= 0x20;
    let error = read_archive_contents(bytes.as_slice()).expect_err("a changed byte is refused");
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::DigestMismatch {
            path: "domains/prod/models.nspl".to_string(),
        }
    );
}

fn read_error(bytes: &[u8]) -> Report<ArchiveReadError> {
    describe_archive(bytes).expect_err("the archive must be refused")
}

/// The byte range of the section `path` inside `bytes`, found by walking the tar stream.
fn section_range(bytes: &[u8], path: &str) -> std::ops::Range<usize> {
    let mut archive = tar::Archive::new(bytes);
    for entry in archive.entries().assured("the archive is a tar stream") {
        let entry = entry.assured("every entry reads");
        if entry.path_bytes().as_ref() == path.as_bytes() {
            let start = usize::try_from(entry.raw_file_position()).assured("small archive");
            let length = usize::try_from(entry.size()).assured("small archive");
            return start..start + length;
        }
    }
    panic!("section {path} is not in the archive");
}

#[test]
fn a_changed_byte_is_refused_at_the_section_it_changed() {
    let (_, mut bytes) = cluster_archive(BackupResources::Included);
    let range = section_range(&bytes, "domains/prod/models.nspl");
    bytes[range.start] ^= 0x20;
    let error = read_error(&bytes);
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::DigestMismatch {
            path: "domains/prod/models.nspl".to_string(),
        }
    );
}

#[test]
fn a_truncated_archive_is_refused() {
    let (_, bytes) = cluster_archive(BackupResources::Included);
    let range = section_range(&bytes, "domains/staging/domain.rkyv");
    let error = read_error(&bytes[..range.start - 512]);
    assert!(
        matches!(
            error.current_context(),
            ArchiveReadError::MissingSection { .. } | ArchiveReadError::Read
        ),
        "{error:?}"
    );
    let error = read_error(&bytes[..range.start + 1]);
    assert!(
        matches!(
            error.current_context(),
            ArchiveReadError::LengthMismatch { actual: 1, .. }
        ),
        "{error:?}"
    );
}

#[test]
fn a_section_the_manifest_does_not_name_is_refused() {
    let (_, bytes) = cluster_archive(BackupResources::Included);
    let mut builder = tar::Builder::new(Vec::new());
    let end_of_sections = bytes.len() - 1024;
    let mut extended = bytes[..end_of_sections].to_vec();
    let mut header = tar::Header::new_gnu();
    header.set_size(3);
    header.set_mode(0o600);
    header.set_cksum();
    builder
        .append_data(&mut header, "stray.bin", &b"abc"[..])
        .assured("an in-memory tar entry writes");
    extended.extend(builder.into_inner().assured("an in-memory tar finishes"));
    let error = read_error(&extended);
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::UnexpectedSection {
            path: "stray.bin".to_string(),
        }
    );
}

#[test]
fn an_archive_that_does_not_begin_with_its_manifest_is_refused() {
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(3);
    header.set_mode(0o600);
    header.set_cksum();
    builder
        .append_data(&mut header, "users.rkyv", &b"abc"[..])
        .assured("an in-memory tar entry writes");
    let bytes = builder.into_inner().assured("an in-memory tar finishes");
    let error = read_error(&bytes);
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::UnexpectedFirstEntry {
            path: "users.rkyv".to_string(),
        }
    );
    assert_eq!(
        read_error(&[0_u8; 1024]).current_context(),
        &ArchiveReadError::MissingManifest
    );
}

#[test]
fn a_cluster_archive_without_users_is_refused() {
    let prod = domain("prod");
    let sections = vec![
        record_section(SectionPath::domain_record(&prod), &unpaced_domain("prod")),
        bytes_section(
            SectionPath::domain_models(&prod),
            SectionContent::Nspl,
            Vec::new(),
        ),
    ];
    let mut manifest = manifest_of(
        ArchiveScope::Cluster,
        sections
            .iter()
            .map(|section| section.entry.clone())
            .collect(),
    );
    manifest.domains.truncate(1);
    let (_, bytes) = write_archive(manifest, &sections);
    assert_eq!(
        read_error(&bytes).current_context(),
        &ArchiveReadError::MissingUsers
    );
}

#[test]
fn a_domain_without_its_models_is_refused() {
    let prod = domain("prod");
    let sections = vec![record_section(
        SectionPath::domain_record(&prod),
        &unpaced_domain("prod"),
    )];
    let manifest = manifest_of(
        ArchiveScope::Domain(prod),
        sections
            .iter()
            .map(|section| section.entry.clone())
            .collect(),
    );
    let (_, bytes) = write_archive(manifest, &sections);
    assert_eq!(
        read_error(&bytes).current_context(),
        &ArchiveReadError::IncompleteDomain {
            domain: "prod".to_string(),
            missing: "models.nspl",
        }
    );
}

#[test]
fn a_record_at_another_domains_path_is_refused() {
    let prod = domain("prod");
    let sections = vec![
        record_section(
            SectionPath::domain_record(&prod),
            &unpaced_domain("staging"),
        ),
        bytes_section(
            SectionPath::domain_models(&prod),
            SectionContent::Nspl,
            Vec::new(),
        ),
    ];
    let manifest = manifest_of(
        ArchiveScope::Domain(prod),
        sections
            .iter()
            .map(|section| section.entry.clone())
            .collect(),
    );
    let (_, bytes) = write_archive(manifest, &sections);
    assert_eq!(
        read_error(&bytes).current_context(),
        &ArchiveReadError::MisplacedSection {
            path: "domains/prod/domain.rkyv".to_string(),
        }
    );
}

#[test]
fn a_manifest_that_names_a_section_twice_cannot_be_laid_out() {
    let section = record_section(SectionPath::users(), &users());
    let manifest = manifest_of(
        ArchiveScope::Cluster,
        vec![section.entry.clone(), section.entry],
    );
    let error = ArchiveLayout::new(manifest).expect_err("a duplicate section is refused");
    assert_eq!(
        error.current_context(),
        &ArchiveWriteError::DuplicateSection {
            path: "users.rkyv".to_string(),
        }
    );
}

#[test]
fn supplied_bytes_that_differ_from_their_entry_are_refused() {
    let section = record_section(SectionPath::users(), &users());
    let manifest = manifest_of(ArchiveScope::Cluster, vec![section.entry]);
    let layout = ArchiveLayout::new(manifest).assured("the archive lays out");
    let mut bytes = Vec::new();
    let error = layout
        .write_to(&mut bytes, |_, sink| {
            std::io::Write::write_all(sink, b"other")
        })
        .expect_err("the supplied bytes differ");
    assert_eq!(
        error.current_context(),
        &ArchiveWriteError::SectionMismatch {
            path: "users.rkyv".to_string(),
        }
    );
}

#[test]
fn the_pieces_of_a_layout_add_up_to_its_total() {
    let (layout, bytes) = cluster_archive(BackupResources::Included);
    let mut counted = 0_u64;
    for piece in layout.pieces() {
        let length = match piece {
            ArchivePiece::Bytes(bytes) => u64::try_from(bytes.len()).assured("small piece"),
            ArchivePiece::Section(entry) => entry.length,
        };
        counted += length;
    }
    assert_eq!(counted, layout.total_bytes());
    assert_eq!(
        counted,
        u64::try_from(bytes.len()).assured("test archives are small")
    );
}

/// A visitor that keeps the bytes of every NSPL section.
#[derive(Default)]
struct NsplCollector {
    texts: Vec<String>,
}

impl SectionVisitor for NsplCollector {
    fn manifest(&mut self, _manifest: &BackupManifest) -> Result<(), Report<ArchiveReadError>> {
        Ok(())
    }

    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        if entry.content == SectionContent::Nspl {
            let mut text = String::new();
            content
                .read_to_string(&mut text)
                .assured("the test NSPL is UTF-8");
            self.texts.push(text);
        }
        Ok(())
    }
}

#[test]
fn a_visitor_that_reads_part_of_a_section_still_gets_every_section_verified() {
    let (_, bytes) = cluster_archive(BackupResources::Included);
    let mut collector = NsplCollector::default();
    read_archive(bytes.as_slice(), &mut collector).assured("the archive reads back");
    assert_eq!(
        collector.texts,
        [
            "CREATE SCHEMA order_event (\n  id U64\n);\n".to_string(),
            String::new()
        ]
    );
}

#[test]
fn bolero_domain_records_and_manifests_round_trip() {
    check!()
        .with_type::<(u64, u64, i64, i64, u16, bool, u8)>()
        .for_each(
            |&(period, skew, wall, logical, start_version, running, placement)| {
                let Some(period) = NonZeroU64::new(period) else {
                    return;
                };
                let record = DomainRecord {
                    pace: DomainPace::Paced {
                        period: DomainClockPeriod::from_nanos(period),
                        skew: DomainClockSkew::from_nanos(skew),
                    },
                    placement: match placement % 4 {
                        0 => PlacementPolicy::RequireColocation,
                        1 => PlacementPolicy::PreferColocation,
                        2 => PlacementPolicy::Neutral,
                        _ => PlacementPolicy::SuggestSeparation,
                    },
                    status: if running {
                        DomainStatus::Running
                    } else {
                        DomainStatus::Paused
                    },
                    start_version: u64::from(start_version),
                    start_point: DomainStartPoint::At {
                        timestamp: Timestamp::from_unix_nanos(logical),
                        time_rate: rate(0.5),
                    },
                    clock: Some(DomainClockState::new(
                        Timestamp::from_unix_nanos(wall),
                        Timestamp::from_unix_nanos(logical),
                        rate(0.5),
                    )),
                    logical_frontier: Some(Timestamp::from_unix_nanos(wall)),
                    ..paced_domain("prod")
                };
                let encoded = record.encode().assured("every valid record encodes");
                assert_eq!(
                    DomainRecord::decode("domain.rkyv", &encoded)
                        .assured("its own encoding decodes"),
                    record
                );
                let entry = SectionEntry {
                    path: SectionPath::domain_record(&record.domain),
                    content: SectionContent::Record(RecordKind::Domain),
                    length: skew,
                    digest: SectionDigester::digest_of(&wall.to_le_bytes()),
                };
                let manifest = BackupManifest {
                    captured_at: Timestamp::from_unix_nanos(logical),
                    ..manifest_of(ArchiveScope::Cluster, vec![entry])
                };
                let encoded = manifest.encode().assured("every valid manifest encodes");
                assert_eq!(
                    BackupManifest::decode("manifest.rkyv", &encoded)
                        .assured("its own encoding decodes"),
                    manifest
                );
            },
        );
}
