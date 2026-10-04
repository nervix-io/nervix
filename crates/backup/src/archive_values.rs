//! Bounded current public archive values, independent of node storage and capture.
//!
//! Layer: test harness.
//! - **Owns.** Synthetic users, catalog/lifecycle facts, cut metadata and supported state sections.
//! - **Depends on.** Current archive records and the dev-only vocabulary generator.
//! - **Must not know.** Consensus membership, execution plans, live attempts or capture fencing.

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    BackupCut, BackupQuiesceCounters, BackupResources, BranchKeyFingerprint, DomainClockPeriod,
    DomainClockSkew, DomainClockState, DomainName, DomainPace, DomainStartPoint, DomainStatus,
    DomainTimeRate, ModelName, PlacementPolicy, ResourceName, SchemaFingerprint, Timestamp,
    UserName,
};

use crate::{
    ArchiveRecord, ArchiveScope, BackupManifest, BranchLifecycleEntry, BranchLifecycleRecord,
    DeclaredResource, DomainCapture, DomainRecord, KafkaOffsetsRecord, KafkaPartitionOffset,
    PublishedResourceVersion, RaftLogPosition, ResourceVersionRecord, ResourceVersionState,
    SectionContent, SectionDigester, SectionEntry, SectionPath, UserRecord, UsersRecord,
    WasmStateDescriptor, wasm_properties::Descriptors,
};

pub(super) struct Values<'a>(pub(super) Arbitrary<'a>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Section {
    pub entry: SectionEntry,
    pub bytes: Vec<u8>,
}

impl Section {
    pub fn new(path: SectionPath, content: SectionContent, bytes: Vec<u8>) -> Self {
        Self {
            entry: SectionEntry {
                path,
                content,
                length: u64::try_from(bytes.len()).assured("generated sections fit 64 bits"),
                digest: SectionDigester::digest_of(&bytes),
            },
            bytes,
        }
    }

    pub fn record<R: ArchiveRecord>(path: SectionPath, record: &R) -> Self {
        Self::new(
            path,
            SectionContent::Record(R::KIND),
            record.encode().assured("bounded current records encode"),
        )
    }
}

pub(super) struct Resource {
    pub record: ResourceVersionRecord,
    pub bytes: Option<Vec<u8>>,
}

pub(super) enum State {
    Wasm {
        descriptor: WasmStateDescriptor,
        bytes: Vec<u8>,
    },
    Kafka(KafkaOffsetsRecord),
    Lifecycle(BranchLifecycleRecord),
}

impl State {
    pub fn sections(&self, output: &mut Vec<Section>) {
        match self {
            Self::Wasm { descriptor, bytes } => {
                output.push(Section::record(
                    SectionPath::wasm_state_descriptor(
                        &descriptor.domain,
                        &descriptor.entity,
                        descriptor.branch_fingerprint.as_ref(),
                    ),
                    descriptor,
                ));
                output.push(Section::new(
                    SectionPath::wasm_guest_blob(
                        &descriptor.domain,
                        &descriptor.entity,
                        descriptor.branch_fingerprint.as_ref(),
                    ),
                    SectionContent::WasmGuestBlob,
                    bytes.clone(),
                ));
            }
            Self::Kafka(record) => output.push(Section::record(
                SectionPath::kafka_offsets(&record.domain, &record.entity),
                record,
            )),
            Self::Lifecycle(record) => output.push(Section::record(
                SectionPath::branch_lifecycle(&record.domain, record.owner_kind, &record.entity),
                record,
            )),
        }
    }
}

pub(super) struct DomainValues {
    pub capture: DomainCapture,
    pub record: DomainRecord,
    pub models: String,
    pub resources: Vec<Resource>,
    pub state: Vec<State>,
}

pub(super) struct Case {
    pub manifest: BackupManifest,
    pub users: Option<UsersRecord>,
    pub domains: Vec<DomainValues>,
    pub sections: Vec<Section>,
}

impl<'a> Values<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(Arbitrary::new(bytes, Domain::Vocabulary))
    }

    pub fn timestamp(&mut self) -> Timestamp {
        Timestamp::from_unix_nanos(self.0.entropy().any_i64())
    }

    pub fn digest(&mut self) -> [u8; 32] {
        std::array::from_fn(|_| self.0.entropy().byte())
    }

    pub fn payload(&mut self) -> Vec<u8> {
        let length = self.0.entropy().pick([0, 1, 511, 512, 513, 1024]);
        (0..length).map(|_| self.0.entropy().byte()).collect()
    }

    pub fn users(&mut self) -> UsersRecord {
        let count = self.0.entropy().count(4);
        let mut users = Vec::new();
        for index in 0..count {
            users.push(UserRecord {
                name: UserName::parse(&format!("user_{index}"))
                    .assured("indexed synthetic user names are valid"),
                password_hash: format!(
                    "$argon2id$v=19$m=8,t=1,p=1$c3ludGhldGlj$c3ludGhldGlj{}",
                    self.0.entropy().byte(),
                ),
            });
        }
        UsersRecord { users }
    }

    pub fn domain_record(&mut self, domain: DomainName) -> DomainRecord {
        let pace = if self.0.entropy().flag() {
            DomainPace::Paced {
                period: DomainClockPeriod::from_nanos(self.0.positive_u64()),
                skew: DomainClockSkew::from_nanos(self.0.entropy().any_u64()),
            }
        } else {
            DomainPace::Unpaced
        };
        let time_rate = DomainTimeRate::try_from(self.0.entropy().pick([
            f64::MIN_POSITIVE,
            0.5,
            1.0,
            2.5,
            f64::MAX,
        ]))
        .assured("the generated rates are positive and finite");
        let start_point = match self.0.entropy().byte() % 3 {
            0 => DomainStartPoint::Resume,
            1 => DomainStartPoint::Now { time_rate },
            _ => DomainStartPoint::At {
                timestamp: self.timestamp(),
                time_rate,
            },
        };
        let clock = if self.0.entropy().flag() {
            Some(DomainClockState::new(
                self.timestamp(),
                self.timestamp(),
                time_rate,
            ))
        } else {
            None
        };
        let logical_frontier = clock.as_ref().map(|_| self.timestamp());
        let count = self.0.entropy().count(4);
        let mut resources = Vec::new();
        for index in 0..count {
            resources.push(DeclaredResource {
                resource: ResourceName::parse(&format!("resource_{index}"))
                    .assured("indexed resource names are valid"),
                next_version: self.0.positive_u64(),
            });
        }
        DomainRecord {
            domain,
            pace,
            placement: self.0.entropy().pick([
                PlacementPolicy::RequireColocation,
                PlacementPolicy::PreferColocation,
                PlacementPolicy::Neutral,
                PlacementPolicy::SuggestSeparation,
            ]),
            status: self.0.entropy().pick([
                DomainStatus::Stopped,
                DomainStatus::Running,
                DomainStatus::Paused,
            ]),
            start_version: self.0.entropy().any_u64(),
            start_point,
            clock,
            logical_frontier,
            resources,
        }
    }

    pub fn cut(&mut self, kind: u8) -> BackupCut {
        match kind % 4 {
            0 => BackupCut::ConfigurationOnly,
            1 => BackupCut::Stopped,
            2 => BackupCut::Live,
            _ => {
                let first = self.timestamp();
                let second = self.timestamp();
                BackupCut::Quiesced {
                    engaged_at: first.min(second),
                    released_at: first.max(second),
                    quiesce: BackupQuiesceCounters {
                        buffered_records: self.0.entropy().any_u64(),
                        buffered_bytes: self.0.entropy().any_u64(),
                        dropped_records: self.0.entropy().any_u64(),
                        rejected_records: self.0.entropy().any_u64(),
                    },
                }
            }
        }
    }

    pub fn resource(
        &mut self,
        domain: &DomainName,
        name: &ResourceName,
        status: u8,
        included: bool,
    ) -> Resource {
        let state = match status % 3 {
            0 => ResourceVersionState::Completed,
            1 => ResourceVersionState::Failed {
                reason: self.0.string(),
            },
            _ => ResourceVersionState::Unfinished,
        };
        let payload = self.payload();
        let published = if status == 0 || self.0.entropy().flag() {
            Some(PublishedResourceVersion {
                root_checksum: self.0.string(),
                manifest_checksum: self.0.string(),
                file_count: self.0.entropy().any_u64(),
                total_bytes: self.0.entropy().any_u64(),
                archive_bytes: u64::try_from(payload.len()).assured("payloads are bounded"),
                created_at: self.timestamp(),
                created_by_node: self.0.name(),
            })
        } else {
            None
        };
        let bytes = if included && published.is_some() {
            Some(payload)
        } else {
            None
        };
        Resource {
            record: ResourceVersionRecord {
                domain: domain.clone(),
                resource: name.clone(),
                version: NonZeroU64::new(u64::from(status) + 1)
                    .assured("three indexed resource versions are positive"),
                state,
                published,
            },
            bytes,
        }
    }

    pub fn offsets(&mut self, domain: DomainName) -> KafkaOffsetsRecord {
        let mut offsets = Vec::new();
        let count = self.0.entropy().count(6);
        for index in 0..count {
            offsets.push(KafkaPartitionOffset {
                topic: format!("synthetic-{:02}", index / 2),
                partition: i32::try_from(index % 2).assured("a partition is zero or one"),
                next_offset: i64::try_from(
                    self.0
                        .entropy()
                        .boundary_biased(0..=i64::MAX.cast_unsigned()),
                )
                .assured("the generated offset is at most i64::MAX"),
            });
        }
        KafkaOffsetsRecord {
            domain,
            entity: self.0.name(),
            schema: SchemaFingerprint::from_digest(self.digest()),
            revision: self.0.entropy().any_u64(),
            offsets,
        }
    }

    pub fn lifecycle(&mut self, domain: DomainName) -> BranchLifecycleRecord {
        let mut branches = Vec::new();
        let count = self.0.entropy().count(4);
        for index in 0..count {
            let mut descriptor = Descriptors(self.0.clone()).descriptor(true);
            let fields = descriptor
                .branch
                .as_mut()
                .assured("a branched descriptor has fields");
            fields.push(crate::StateField {
                name: "zz_identity".into(),
                value: crate::StateValue::U64(
                    u64::try_from(index).assured("four branch indices fit 64 bits"),
                ),
            });
            branches.push(BranchLifecycleEntry {
                key: descriptor.branch,
                last_ingestion: self.timestamp(),
                incarnation: self.0.positive_u64().get(),
            });
        }
        // LRU order is a sequence; deliberately do not sort by branch identity or timestamp.
        if self.0.entropy().flag() {
            branches.reverse();
        }
        BranchLifecycleRecord {
            domain,
            owner_kind: self.0.model_kind(),
            entity: self.0.name(),
            schema: SchemaFingerprint::from_digest(self.digest()),
            revision: self.0.entropy().any_u64(),
            branches,
        }
    }

    pub fn case(&mut self, cluster: bool, included: bool, cut_kind: u8) -> Case {
        let resources = if included {
            BackupResources::Included
        } else {
            BackupResources::Omitted
        };
        let users = if cluster { Some(self.users()) } else { None };
        let count = if cluster {
            1 + self.0.entropy().count(2)
        } else {
            1
        };
        let mut domains = Vec::new();
        let mut sections = Vec::new();
        if let Some(users) = &users {
            sections.push(Section::record(SectionPath::users(), users));
        }
        for index in 0..count {
            let name = self.0.entropy().pick([
                format!("domain_{index}"),
                format!("domain_{index}{}", "x".repeat(120)),
            ]);
            let domain =
                DomainName::parse(&name).assured("unique generated domains are at most 128 bytes");
            let mut record = self.domain_record(domain.clone());
            let generated_resource = self.0.name();
            let resource_name = self.0.entropy().pick([
                ResourceName::parse("..").assured("a dot resource is supported"),
                generated_resource,
            ]);
            record.resources = vec![DeclaredResource {
                resource: resource_name.clone(),
                next_version: NonZeroU64::new(4).assured("four is positive"),
            }];
            let capture = DomainCapture {
                domain: domain.clone(),
                revision: self.0.entropy().any_u64(),
                raft_log: RaftLogPosition {
                    term: self.0.entropy().any_u64(),
                    index: self.0.entropy().any_u64(),
                },
                cut: self.cut(cut_kind),
            };
            let models = if self.0.entropy().flag() {
                "CREATE SCHEMA synthetic ( first U64, second STRING OPTIONAL SENSITIVE );\nCREATE \
                 RELAY synthetic_relay SCHEMA synthetic UNBRANCHED;\n"
                    .into()
            } else {
                String::new()
            };
            sections.push(Section::record(
                SectionPath::domain_record(&domain),
                &record,
            ));
            sections.push(Section::new(
                SectionPath::domain_models(&domain),
                SectionContent::Nspl,
                models.as_bytes().to_vec(),
            ));
            let mut versions = Vec::new();
            for status in 0..3 {
                let resource = self.resource(&domain, &resource_name, status, included);
                sections.push(Section::record(
                    SectionPath::resource_version_record(
                        &domain,
                        &resource_name,
                        resource.record.version,
                    ),
                    &resource.record,
                ));
                if let Some(bytes) = &resource.bytes {
                    sections.push(Section::new(
                        SectionPath::resource_archive(
                            &domain,
                            &resource_name,
                            resource.record.version,
                        ),
                        SectionContent::ResourceArchive,
                        bytes.clone(),
                    ));
                }
                versions.push(resource);
            }
            let mut state = Vec::new();
            if !cut_kind.is_multiple_of(4) {
                for branch in 0..3 {
                    let mut descriptor = Descriptors(self.0.clone()).descriptor(branch != 0);
                    descriptor.domain = domain.clone();
                    descriptor.entity = ModelName::parse(if branch == 0 {
                        "unbranched_worker"
                    } else {
                        "branched_worker"
                    })
                    .assured("the worker names are valid");
                    if branch != 0 {
                        descriptor.branch_fingerprint =
                            Some(BranchKeyFingerprint::new([branch; 32]));
                        descriptor
                            .branch
                            .as_mut()
                            .assured("a branched descriptor has fields")
                            .push(crate::StateField {
                                name: "zz_identity".into(),
                                value: crate::StateValue::U8(branch),
                            });
                    }
                    state.push(State::Wasm {
                        descriptor,
                        bytes: self.payload(),
                    });
                }
                state.push(State::Kafka(self.offsets(domain.clone())));
                state.push(State::Lifecycle(self.lifecycle(domain.clone())));
            }
            for section in &state {
                section.sections(&mut sections);
            }
            domains.push(DomainValues {
                capture,
                record,
                models,
                resources: versions,
                state,
            });
        }
        let scope = if cluster {
            ArchiveScope::Cluster
        } else {
            ArchiveScope::Domain(domains[0].record.domain.clone())
        };
        let manifest = BackupManifest {
            producer_version: self.0.string(),
            language_version: nervix_models::NSPL_LANGUAGE_VERSION.into(),
            cluster_id: self.0.string(),
            captured_at: self.timestamp(),
            scope,
            resources,
            domains: domains
                .iter()
                .map(|domain| domain.capture.clone())
                .collect(),
            sections: sections
                .iter()
                .map(|section| section.entry.clone())
                .collect(),
        };
        Case {
            manifest,
            users,
            domains,
            sections,
        }
    }
}
