//! Complete ordered Model documents through current multi-domain public archives.
//!
//! Layer: test harness.
//! - **Owns.** Canonical Model export/reparse equality and archive extraction/re-export assertions.
//! - **Depends on.** Current semantic Model generators, canonical rendering and public archives.
//! - **Must not know.** Node storage, graph execution or capture/install synchronization.

use std::{collections::BTreeMap, io::Write as _};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain, ModelVariant};
use nervix_backup::{
    ArchiveLayout, ArchiveReadError, ArchiveRecord, ArchiveScope, BackupManifest, DomainCapture,
    DomainRecord, RaftLogPosition, SectionContent, SectionDigester, SectionEntry, SectionPath,
    SectionReader, SectionVisitor, UserRecord, UsersRecord, read_archive, read_archive_contents,
};
use nervix_models::{
    BackupCut, BackupResources, CreateStatement, DomainName, DomainPace, DomainStartPoint,
    DomainStatus, Model, PlacementPolicy, RequestedResourceVersion, Statement, Timestamp, UserName,
    canonical_nspl_document,
};

use crate::client_statement::{ClientStatement, parse_client_statements};

struct OriginalDomain {
    record: DomainRecord,
    capture: DomainCapture,
    models: Vec<Model<RequestedResourceVersion>>,
    document: String,
}

struct ModelArchive {
    manifest: BackupManifest,
    domains: Vec<OriginalDomain>,
    users: Option<UsersRecord>,
    sections: BTreeMap<SectionPath, Vec<u8>>,
}

#[derive(Default)]
struct Extracted {
    sections: BTreeMap<SectionPath, Vec<u8>>,
    entries: Vec<SectionEntry>,
}

impl SectionVisitor for Extracted {
    fn manifest(&mut self, _manifest: &BackupManifest) -> Result<(), Report<ArchiveReadError>> {
        Ok(())
    }

    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        self.entries.push(entry.clone());
        let bytes = content.read_all(entry, 64 * 1024 * 1024)?;
        assert!(self.sections.insert(entry.path.clone(), bytes).is_none());
        Ok(())
    }
}

impl ModelArchive {
    fn generate(
        arbitrary: &mut Arbitrary<'_>,
        cluster: bool,
        variant: Option<ModelVariant>,
    ) -> Self {
        let users = if cluster {
            Some(UsersRecord {
                users: vec![UserRecord {
                    name: UserName::parse("synthetic").assured("the synthetic user is valid"),
                    password_hash: "$argon2id$v=19$m=8,t=1,p=1$c3ludGhldGlj$c3ludGhldGlj".into(),
                }],
            })
        } else {
            None
        };
        let mut generated = Self {
            manifest: BackupManifest {
                producer_version: "property".into(),
                language_version: nervix_models::NSPL_LANGUAGE_VERSION.into(),
                cluster_id: arbitrary.string(),
                captured_at: Timestamp::from_unix_nanos(arbitrary.entropy().any_i64()),
                scope: ArchiveScope::Cluster,
                resources: BackupResources::Included,
                domains: Vec::new(),
                sections: Vec::new(),
            },
            domains: Vec::new(),
            users,
            sections: BTreeMap::new(),
        };
        if let Some(users) = &generated.users {
            let bytes = users.encode().assured("the synthetic user record encodes");
            generated.push(
                SectionPath::users(),
                SectionContent::Record(UsersRecord::KIND),
                bytes,
            );
        }
        let count = if cluster {
            1 + arbitrary.entropy().count(2)
        } else {
            1
        };
        for index in 0..count {
            let domain = DomainName::parse(&format!("archive_{index}"))
                .assured("indexed domain names are valid");
            let record = DomainRecord {
                domain: domain.clone(),
                pace: DomainPace::Unpaced,
                placement: PlacementPolicy::Neutral,
                status: DomainStatus::Stopped,
                start_version: arbitrary.entropy().any_u64(),
                start_point: DomainStartPoint::Resume,
                clock: None,
                logical_frontier: None,
                resources: Vec::new(),
            };
            let capture = DomainCapture {
                domain: domain.clone(),
                revision: arbitrary.entropy().any_u64(),
                raft_log: RaftLogPosition {
                    term: arbitrary.entropy().any_u64(),
                    index: arbitrary.entropy().any_u64(),
                },
                cut: BackupCut::ConfigurationOnly,
            };
            let mut models = Vec::new();
            if let Some(variant) = variant {
                models.push(arbitrary.model_of(variant));
            }
            let model_count = arbitrary.entropy().count(4);
            for _ in 0..model_count {
                models.push(arbitrary.model());
            }
            let document = canonical_nspl_document(&models)
                .assured("NSPL-domain Models have canonical spelling");
            generated.push(
                SectionPath::domain_record(&domain),
                SectionContent::Record(DomainRecord::KIND),
                record.encode().assured("the generated domain encodes"),
            );
            generated.push(
                SectionPath::domain_models(&domain),
                SectionContent::Nspl,
                document.as_bytes().to_vec(),
            );
            generated.manifest.domains.push(capture.clone());
            generated.domains.push(OriginalDomain {
                record,
                capture,
                models,
                document,
            });
        }
        if !cluster {
            generated.manifest.scope =
                ArchiveScope::Domain(generated.domains[0].record.domain.clone());
        }
        generated
    }

    fn push(&mut self, path: SectionPath, content: SectionContent, bytes: Vec<u8>) {
        self.manifest.sections.push(SectionEntry {
            path: path.clone(),
            content,
            length: u64::try_from(bytes.len()).assured("bounded generated documents fit 64 bits"),
            digest: SectionDigester::digest_of(&bytes),
        });
        assert!(self.sections.insert(path, bytes).is_none());
    }

    fn check(&self) {
        let layout =
            ArchiveLayout::new(self.manifest.clone()).assured("the current manifest lays out");
        let mut archive = Vec::new();
        layout
            .write_to(&mut archive, |entry, sink| {
                sink.write_all(&self.sections[&entry.path])
            })
            .assured("canonical domains export through the public archive writer");
        let mut extracted = Extracted::default();
        let manifest = read_archive(archive.as_slice(), &mut extracted)
            .assured("the complete archive extracts");
        assert_eq!(manifest, self.manifest);
        assert_eq!(extracted.entries, self.manifest.sections);
        assert_eq!(extracted.sections, self.sections);
        let contents = read_archive_contents(archive.as_slice())
            .assured("the production restore reader verifies the archive");
        assert_eq!(contents.description.manifest, self.manifest);
        assert_eq!(contents.description.users, self.users);
        assert_eq!(contents.description.domains.len(), self.domains.len());
        let mut expected_documents = BTreeMap::new();
        for (actual, original) in contents.description.domains.iter().zip(&self.domains) {
            assert_eq!(actual.record, original.record);
            assert_eq!(actual.capture, original.capture);
            let exported = &contents.models[&original.record.domain];
            assert_eq!(exported, &original.document);
            let statements =
                parse_client_statements(exported).assured("canonical generated documents reparse");
            let expected = original
                .models
                .iter()
                .map(|model| {
                    ClientStatement::Server(Statement::Create(CreateStatement::new(
                        Box::new(model.clone()),
                        false,
                    )))
                })
                .collect::<Vec<_>>();
            assert_eq!(statements, expected);
            let mut restored_models = Vec::new();
            for statement in statements {
                let ClientStatement::Server(Statement::Create(create)) = statement else {
                    panic!("canonical archives contain Model creations");
                };
                restored_models.push(*create.body);
            }
            let restored_document = canonical_nspl_document(&restored_models)
                .assured("parsed archive Models render canonically");
            assert_eq!(restored_document, original.document);
            let path = SectionPath::domain_models(&original.record.domain);
            extracted
                .sections
                .insert(path, restored_document.into_bytes());
            expected_documents.insert(original.record.domain.clone(), original.document.clone());
        }
        assert_eq!(contents.models, expected_documents);
        let mut reexported = Vec::new();
        ArchiveLayout::new(manifest)
            .assured("the extracted manifest lays out")
            .write_to(&mut reexported, |entry, sink| {
                sink.write_all(&extracted.sections[&entry.path])
            })
            .assured("restored canonical Models re-export");
        assert_eq!(reexported, archive);
    }
}

pub(super) fn assert_archive_models(bytes: &[u8]) {
    let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
    for cluster in [false, true] {
        ModelArchive::generate(&mut arbitrary, cluster, None).check();
    }
}

#[test]
fn every_model_family_reparses_from_a_complete_current_archive() {
    for seed in [0, 1, 127, 255] {
        let bytes = [seed; 4096];
        for variant in ModelVariant::ALL {
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Nspl);
            ModelArchive::generate(&mut arbitrary, true, Some(variant)).check();
        }
    }
}
