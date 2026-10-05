//! Bounded malformed current archives and records, checked before restore installation.
//!
//! Layer: test harness.
//! - **Owns.** Current-input corruption cases and safe typed refusal assertions.
//! - **Depends on.** Production archive validation, current wire records and synthetic values.
//! - **Must not know.** Historical formats, private databases or runtime installation algorithms.

use std::fmt::Debug;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use strum::IntoEnumIterator as _;

use crate::{
    ArchiveReadError, ArchiveRecord, BackupManifest, BranchLifecycleRecord, DomainRecord,
    KafkaOffsetsRecord, MaterializedIdentitiesRecord, MaterializedRelayDescriptor, RecordKind,
    ResourceVersionRecord, SectionContent, SectionDigester, SectionPath, UsersRecord,
    WasmStateDescriptor,
    archive_properties::assert_record,
    archive_values::{Case, Section, Values},
    read_archive_contents,
    section::{RECORD_HEADER_BYTES, RECORD_MAGIC, encode_record},
    wire::{
        BranchLifecycleWire, DomainWire, KafkaOffsetsWire, ManifestWire, ResourceVersionWire,
        UsersWire,
    },
};

fn validate<R: ArchiveRecord + PartialEq + Debug>(
    bytes: &[u8],
) -> Result<(), Report<ArchiveReadError>> {
    let record = R::decode("synthetic.rkyv", bytes)?;
    assert_record(&record);
    Ok(())
}

fn validate_kind(kind: RecordKind, bytes: &[u8]) -> Result<(), Report<ArchiveReadError>> {
    match kind {
        RecordKind::Manifest => validate::<BackupManifest>(bytes),
        RecordKind::Users => validate::<UsersRecord>(bytes),
        RecordKind::Domain => validate::<DomainRecord>(bytes),
        RecordKind::ResourceVersion => validate::<ResourceVersionRecord>(bytes),
        RecordKind::WasmStateDescriptor => validate::<WasmStateDescriptor>(bytes),
        RecordKind::KafkaOffsets => validate::<KafkaOffsetsRecord>(bytes),
        RecordKind::BranchLifecycle => validate::<BranchLifecycleRecord>(bytes),
        RecordKind::MaterializedRelayDescriptor => validate::<MaterializedRelayDescriptor>(bytes),
        RecordKind::MaterializedIdentities => validate::<MaterializedIdentitiesRecord>(bytes),
    }
}

fn current_version(kind: RecordKind) -> u16 {
    match kind {
        RecordKind::Manifest => BackupManifest::VERSION,
        RecordKind::Users => UsersRecord::VERSION,
        RecordKind::Domain => DomainRecord::VERSION,
        RecordKind::ResourceVersion => ResourceVersionRecord::VERSION,
        RecordKind::WasmStateDescriptor => WasmStateDescriptor::VERSION,
        RecordKind::KafkaOffsets => KafkaOffsetsRecord::VERSION,
        RecordKind::BranchLifecycle => BranchLifecycleRecord::VERSION,
        RecordKind::MaterializedRelayDescriptor => MaterializedRelayDescriptor::VERSION,
        RecordKind::MaterializedIdentities => MaterializedIdentitiesRecord::VERSION,
    }
}

fn safe_record_failure(error: &Report<ArchiveReadError>) {
    assert!(matches!(
        error.current_context(),
        ArchiveReadError::ForeignMagic { .. }
            | ArchiveReadError::ForeignRecordKind { .. }
            | ArchiveReadError::UnsupportedRecordVersion { .. }
            | ArchiveReadError::UnsupportedArchiveFormat { .. }
            | ArchiveReadError::InvalidRecord { .. }
            | ArchiveReadError::InvalidValue { .. }
            | ArchiveReadError::InvalidPath
            | ArchiveReadError::DuplicateSection { .. }
    ));
}

pub(super) fn rejects_field<R: ArchiveRecord>(bytes: Vec<u8>, field: &'static str) {
    let error = match R::decode("synthetic.rkyv", &bytes) {
        Ok(_) => panic!("an invalid current {field} was accepted"),
        Err(error) => error,
    };
    assert_eq!(
        error.current_context(),
        &ArchiveReadError::InvalidValue {
            path: "synthetic.rkyv".into(),
            field
        }
    );
}

impl Case {
    fn assert_invalid_fields(&self) {
        let original = &self.domains[0];
        let encoded = original
            .record
            .encode()
            .assured("the original domain encodes");
        let mut domain: DomainWire = crate::section::decode_record(
            "synthetic.rkyv",
            DomainRecord::KIND,
            DomainRecord::VERSION,
            &encoded,
        )
        .assured("the current domain wire decodes");
        domain.domain.clear();
        rejects_field::<DomainRecord>(
            encode_record(DomainRecord::KIND, DomainRecord::VERSION, &domain)
                .assured("the current wire encodes"),
            "domain name",
        );
        let encoded = original.resources[0]
            .record
            .encode()
            .assured("the original resource encodes");
        let mut resource: ResourceVersionWire = crate::section::decode_record(
            "synthetic.rkyv",
            ResourceVersionRecord::KIND,
            ResourceVersionRecord::VERSION,
            &encoded,
        )
        .assured("the current resource wire decodes");
        resource.version = 0;
        rejects_field::<ResourceVersionRecord>(
            encode_record(
                ResourceVersionRecord::KIND,
                ResourceVersionRecord::VERSION,
                &resource,
            )
            .assured("the current wire encodes"),
            "resource version",
        );
        let users = UsersWire {
            users: vec![crate::wire::UserWire {
                name: "synthetic".into(),
                password_hash: String::new(),
            }],
        };
        rejects_field::<UsersRecord>(
            encode_record(UsersRecord::KIND, UsersRecord::VERSION, &users)
                .assured("the current wire encodes"),
            "password hash",
        );
        for state in &original.state {
            match state {
                crate::archive_values::State::Kafka(offsets) => {
                    let encoded = offsets.encode().assured("the valid offsets encode");
                    let mut wire: KafkaOffsetsWire = crate::section::decode_record(
                        "synthetic.rkyv",
                        KafkaOffsetsRecord::KIND,
                        KafkaOffsetsRecord::VERSION,
                        &encoded,
                    )
                    .assured("the current offsets decode");
                    wire.offsets = vec![crate::wire::KafkaPartitionWire {
                        topic: "synthetic".into(),
                        partition: -1,
                        next_offset: 0,
                    }];
                    rejects_field::<KafkaOffsetsRecord>(
                        encode_record(KafkaOffsetsRecord::KIND, KafkaOffsetsRecord::VERSION, &wire)
                            .assured("the current wire encodes"),
                        "Kafka partition offset",
                    );
                    wire.offsets = vec![
                        crate::wire::KafkaPartitionWire {
                            topic: "synthetic".into(),
                            partition: 0,
                            next_offset: 0
                        };
                        2
                    ];
                    rejects_field::<KafkaOffsetsRecord>(
                        encode_record(KafkaOffsetsRecord::KIND, KafkaOffsetsRecord::VERSION, &wire)
                            .assured("the current wire encodes"),
                        "Kafka offset order",
                    );
                }
                crate::archive_values::State::Lifecycle(lifecycle) => {
                    let encoded = lifecycle.encode().assured("the valid lifecycle encodes");
                    let mut wire: BranchLifecycleWire = crate::section::decode_record(
                        "synthetic.rkyv",
                        BranchLifecycleRecord::KIND,
                        BranchLifecycleRecord::VERSION,
                        &encoded,
                    )
                    .assured("the current lifecycle decodes");
                    wire.branches = vec![crate::wire::BranchLifecycleEntryWire {
                        key: None,
                        last_ingestion_unix_nanos: 0,
                        incarnation: 0,
                    }];
                    rejects_field::<BranchLifecycleRecord>(
                        encode_record(
                            BranchLifecycleRecord::KIND,
                            BranchLifecycleRecord::VERSION,
                            &wire,
                        )
                        .assured("the current wire encodes"),
                        "branch incarnation",
                    );
                }
                crate::archive_values::State::Wasm { .. } => {}
                crate::archive_values::State::Materialized(value) => value.assert_invalid_fields(),
            }
        }
        let mut manifest = ManifestWire::from(&self.manifest);
        manifest.domains[0].domain.clear();
        let encoded = encode_record(BackupManifest::KIND, BackupManifest::VERSION, &manifest)
            .assured("the current manifest wire encodes");
        let error = BackupManifest::decode("manifest.rkyv", &encoded)
            .expect_err("an invalid domain cannot enter the manifest");
        assert_eq!(
            error.current_context(),
            &ArchiveReadError::InvalidValue {
                path: "manifest.rkyv".into(),
                field: "domain name"
            }
        );
    }

    fn assert_record_headers(&self) {
        let manifest = self.manifest.encode().assured("the valid manifest encodes");
        let mut records = vec![(RecordKind::Manifest, manifest)];
        for section in &self.sections {
            if let SectionContent::Record(kind) = section.entry.content {
                records.push((kind, section.bytes.clone()));
            }
        }
        for (kind, original) in records {
            validate_kind(kind, &original).assured("the original current record is valid");
            for end in 0..RECORD_HEADER_BYTES {
                let error = validate_kind(kind, &original[..end])
                    .expect_err("an incomplete header is refused");
                safe_record_failure(&error);
            }
            let mut changed = original.clone();
            changed[0] ^= 1;
            let error = validate_kind(kind, &changed).expect_err("a foreign magic is refused");
            assert!(matches!(
                error.current_context(),
                ArchiveReadError::ForeignMagic { .. }
            ));
            changed = original.clone();
            changed[8..10].copy_from_slice(&0_u16.to_le_bytes());
            let error = validate_kind(kind, &changed).expect_err("an invalid kind tag is refused");
            assert!(matches!(
                error.current_context(),
                ArchiveReadError::ForeignRecordKind { .. }
            ));
            changed = original;
            changed[10..12].copy_from_slice(
                &current_version(kind)
                    .checked_add(1)
                    .assured("current format versions are below u16::MAX")
                    .to_le_bytes(),
            );
            let error = validate_kind(kind, &changed)
                .expect_err("an unsupported current header version is refused");
            assert!(matches!(
                error.current_context(),
                ArchiveReadError::UnsupportedRecordVersion { .. }
            ));
        }
    }
}

#[test]
fn bolero_malformed_current_records_return_safe_typed_errors() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes| {
            let case = Values::new(bytes).case(true, true, 1);
            case.assert_invalid_fields();
            case.assert_record_headers();
            for kind in RecordKind::iter() {
                let mut framed = RECORD_MAGIC.to_vec();
                framed.extend_from_slice(&kind.tag().to_le_bytes());
                framed.extend_from_slice(&current_version(kind).to_le_bytes());
                framed.extend_from_slice(bytes);
                for input in [bytes, framed.as_slice()] {
                    if let Err(error) = validate_kind(kind, input) {
                        safe_record_failure(&error);
                    }
                }
            }
        });
}

/// Writes deliberately inconsistent current metadata and section membership for reader tests.
pub(super) fn damaged_archive(manifest: &BackupManifest, sections: &[Section]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    let manifest = manifest
        .encode()
        .assured("the bounded malformed current manifest encodes");
    let mut header = tar::Header::new_gnu();
    header.set_size(u64::try_from(manifest.len()).assured("the manifest is bounded"));
    header.set_mode(0o600);
    header.set_cksum();
    builder
        .append_data(&mut header, "manifest.rkyv", manifest.as_slice())
        .assured("in-memory tar writes");
    for section in sections {
        let mut header = tar::Header::new_gnu();
        header.set_size(u64::try_from(section.bytes.len()).assured("the section is bounded"));
        header.set_mode(0o600);
        header.set_cksum();
        builder
            .append_data(
                &mut header,
                section.entry.path.as_str(),
                section.bytes.as_slice(),
            )
            .assured("in-memory tar writes");
    }
    builder.into_inner().assured("in-memory tar finishes")
}

#[test]
fn bolero_corrupt_archives_fail_before_restore_contents_are_returned() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes| {
            let case = Values::new(bytes).case(true, true, 1);
            let original = case.export();
            read_archive_contents(original.as_slice()).assured("the unmodified archive validates");
            case.assert_materialized_faults();
            for fault in 0..7 {
                let mut manifest = case.manifest.clone();
                let mut sections = case.sections.clone();
                let path = sections[0].entry.path.to_string();
                let expected = match fault {
                    0 => {
                        manifest.sections[0].digest =
                            SectionDigester::digest_of(b"different synthetic section");
                        ArchiveReadError::DigestMismatch { path }
                    }
                    1 => {
                        let actual = manifest.sections[0].length;
                        let declared = actual
                            .checked_add(1)
                            .assured("generated sections are bounded");
                        manifest.sections[0].length = declared;
                        ArchiveReadError::LengthMismatch {
                            path,
                            declared,
                            actual,
                        }
                    }
                    2 => {
                        sections.swap(0, 1);
                        ArchiveReadError::SectionOutOfOrder {
                            expected: path,
                            found: sections[0].entry.path.to_string(),
                        }
                    }
                    3 => {
                        let missing = sections.pop().assured("stateful cases have sections");
                        ArchiveReadError::MissingSection {
                            path: missing.entry.path.to_string(),
                        }
                    }
                    4 => {
                        sections.push(Section::new(
                            SectionPath::parse("unexpected.bin")
                                .assured("a relative path is valid"),
                            SectionContent::ResourceArchive,
                            Vec::new(),
                        ));
                        ArchiveReadError::UnexpectedSection {
                            path: "unexpected.bin".into(),
                        }
                    }
                    5 => {
                        manifest.sections[0].content = SectionContent::Record(RecordKind::Manifest);
                        ArchiveReadError::MisplacedSection { path }
                    }
                    _ => {
                        // Keep transport length/digest consistent while invalidating the record
                        // magic, so owning record validation must reject before installation.
                        let index = sections
                            .len()
                            .checked_sub(1)
                            .assured("the case has sections");
                        let section = &mut sections[index];
                        section.bytes[0] ^= 1;
                        manifest.sections[index].digest =
                            SectionDigester::digest_of(&section.bytes);
                        ArchiveReadError::ForeignMagic {
                            path: section.entry.path.to_string(),
                        }
                    }
                };
                let malformed = damaged_archive(&manifest, &sections);
                let error = read_archive_contents(malformed.as_slice())
                    .expect_err("corrupt archive contents cannot reach restore installation");
                assert_eq!(error.current_context(), &expected);
            }
            // End inside the last section, before padding or tar terminators.
            let contents = read_archive_contents(original.as_slice())
                .assured("the baseline archive validates");
            let domain = contents
                .description
                .domains
                .last()
                .assured("the case holds a domain");
            let state = domain
                .state
                .last()
                .assured("a stopped cut includes lifecycle state");
            let crate::DescribedRuntimeState::BranchLifecycle { record, .. } = state else {
                panic!("the last generated state is lifecycle");
            };
            let end = record
                .offset
                .checked_add(record.length / 2)
                .assured("the generated section is bounded");
            let end = usize::try_from(end).assured("generated archives fit the address space");
            assert!(read_archive_contents(&original[..end]).is_err());
            // Arbitrary tar inputs are bounded independently of valid-archive generation.
            if let Ok(contents) = read_archive_contents(bytes) {
                assert_record(&contents.description.manifest);
            }
        });
}
