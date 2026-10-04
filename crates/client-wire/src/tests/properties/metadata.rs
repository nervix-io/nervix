//! Structured command metadata stays typed and complete beside the command's display text.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use nervix_models::{
    ArchiveDigest, BackupArchiveSummary, BackupCut, BackupCutKind, BackupDomainSummary,
    BackupQuiesceCounters, BackupResources, ModelKind, NodeRef, ResourceDescription,
    ResourceEntryContent, ResourceManifestEntry, ResourceUsage, ResourceVersionDescription,
    ResourceVersionEntries, RestoreArchive, RestoreMode, RestoreReport, RestoreStep,
    RestoreStepOutcome, RestoreStepReport, RestoredDomain, RestoredUsers, TransactionInspection,
    TransactionLifecycle,
};

use super::WireValues;
use crate::{
    CommandOutcome,
    tests::samples::{impact_report, transaction},
};

impl WireValues<'_> {
    pub(super) fn backup_summary(&mut self) -> BackupArchiveSummary {
        let mut domains = Vec::new();
        for _ in 0..self.arbitrary.entropy().count(3) {
            domains.push(BackupDomainSummary {
                domain: self.arbitrary.name(),
                revision: self.arbitrary.entropy().any_u64(),
                cut: self.backup_cut(),
                sections: self.arbitrary.entropy().any_u64(),
                section_bytes: self.arbitrary.entropy().any_u64(),
            });
        }
        domains.sort_by(|left, right| left.domain.cmp(&right.domain));
        domains.dedup_by(|left, right| left.domain == right.domain);
        BackupArchiveSummary {
            total_bytes: self.arbitrary.positive_u64(),
            digest: ArchiveDigest::from_bytes(self.digest()),
            captured_at: self.timestamp(),
            retained_until: self.timestamp(),
            resources: self
                .arbitrary
                .entropy()
                .pick([BackupResources::Included, BackupResources::Omitted]),
            users: if self.arbitrary.entropy().flag() {
                Some(self.arbitrary.entropy().any_u64())
            } else {
                None
            },
            domains,
        }
    }

    fn backup_cut(&mut self) -> BackupCut {
        match self.arbitrary.entropy().pick([
            BackupCutKind::Quiesced,
            BackupCutKind::Live,
            BackupCutKind::Stopped,
            BackupCutKind::ConfigurationOnly,
        ]) {
            BackupCutKind::Quiesced => {
                let first = self.timestamp();
                let second = self.timestamp();
                let (engaged_at, released_at) = if first.unix_nanos() <= second.unix_nanos() {
                    (first, second)
                } else {
                    (second, first)
                };
                BackupCut::Quiesced {
                    engaged_at,
                    released_at,
                    quiesce: BackupQuiesceCounters {
                        buffered_records: self.arbitrary.entropy().any_u64(),
                        buffered_bytes: self.arbitrary.entropy().any_u64(),
                        dropped_records: self.arbitrary.entropy().any_u64(),
                        rejected_records: self.arbitrary.entropy().any_u64(),
                    },
                }
            }
            BackupCutKind::Live => BackupCut::Live,
            BackupCutKind::Stopped => BackupCut::Stopped,
            BackupCutKind::ConfigurationOnly => BackupCut::ConfigurationOnly,
        }
    }

    pub(super) fn restore_report(&mut self) -> RestoreReport {
        let domain: nervix_models::DomainName = self.arbitrary.name();
        let steps = [
            RestoreStep::Users,
            RestoreStep::CreateDomain(domain.clone()),
            RestoreStep::ImportResources(domain.clone()),
            RestoreStep::ApplyModels(domain.clone()),
        ];
        let mut reports = Vec::new();
        for step in steps {
            reports.push(RestoreStepReport {
                step,
                outcome: self.arbitrary.entropy().pick([
                    RestoreStepOutcome::Applied,
                    RestoreStepOutcome::Planned,
                    RestoreStepOutcome::Failed,
                    RestoreStepOutcome::NotAttempted,
                ]),
            });
        }
        RestoreReport {
            mode: self
                .arbitrary
                .entropy()
                .pick([RestoreMode::Apply, RestoreMode::DryRun]),
            archive: RestoreArchive {
                total_bytes: self.arbitrary.positive_u64(),
                digest: ArchiveDigest::from_bytes(self.digest()),
            },
            captured_at: self.timestamp(),
            domains: vec![RestoredDomain {
                source: domain,
                domain: self.arbitrary.name(),
                models: self.arbitrary.entropy().any_u64(),
                resource_versions: self.arbitrary.entropy().any_u64(),
                planned_models: if self.arbitrary.entropy().flag() {
                    Some(impact_report())
                } else {
                    None
                },
            }],
            users: if self.arbitrary.entropy().flag() {
                Some(RestoredUsers {
                    created: self.arbitrary.entropy().any_u64(),
                    skipped: self.arbitrary.entropy().any_u64(),
                    replaced: self.arbitrary.entropy().any_u64(),
                })
            } else {
                None
            },
            steps: reports,
        }
    }

    pub(super) fn command_metadata(&mut self, command: &mut CommandOutcome) {
        if self.arbitrary.entropy().flag() {
            command.inspection = Some(Box::new(TransactionInspection {
                transaction: transaction(TransactionLifecycle::Committed),
                operation: None,
                report: impact_report(),
            }));
        }
        if self.arbitrary.entropy().flag() {
            command.backup = Some(Box::new(self.backup_summary()));
        }
        if self.arbitrary.entropy().flag() {
            command.restore = Some(Box::new(self.restore_report()));
        }
        if self.arbitrary.entropy().flag() {
            let number = self.arbitrary.positive_u64();
            let entries = vec![
                ResourceManifestEntry {
                    path: self.arbitrary.string(),
                    content: ResourceEntryContent::Directory,
                },
                ResourceManifestEntry {
                    path: self.arbitrary.string(),
                    content: ResourceEntryContent::File {
                        size: self.arbitrary.entropy().any_u64(),
                        checksum: self.arbitrary.string(),
                    },
                },
            ];
            let entries = if self.arbitrary.entropy().flag() {
                ResourceVersionEntries::Listed(entries)
            } else {
                ResourceVersionEntries::Unavailable {
                    reason: self.arbitrary.string(),
                }
            };
            command.resource = Some(Box::new(ResourceDescription {
                resource: self.arbitrary.name(),
                latest_version: if self.arbitrary.entropy().flag() {
                    Some(number)
                } else {
                    None
                },
                versions: vec![ResourceVersionDescription {
                    version: number,
                    root_checksum: self.arbitrary.string(),
                    manifest_checksum: self.arbitrary.string(),
                    file_count: self.arbitrary.entropy().any_u64(),
                    total_bytes: self.arbitrary.entropy().any_u64(),
                    created_at: self.timestamp(),
                    created_by_node: self.arbitrary.name(),
                    entries,
                }],
                usages: vec![ResourceUsage {
                    node: NodeRef::new(
                        ModelKind::WasmProcessor,
                        self.arbitrary.name::<nervix_models::ModelName>(),
                    ),
                    version: number,
                }],
            }));
        }
        if self.arbitrary.entropy().flag() {
            command.wasm_state = Some(Box::new(self.wasm_state()));
        }
    }
}

#[test]
fn backup_summary_generator_covers_every_cut_kind() {
    let mut covered = std::collections::BTreeSet::new();
    for marker in 0..=u8::MAX {
        let bytes = [marker; 64];
        covered.insert(WireValues::new(&bytes).backup_cut().kind().as_str());
    }
    assert_eq!(
        covered,
        std::collections::BTreeSet::from([
            BackupCutKind::Quiesced.as_str(),
            BackupCutKind::Live.as_str(),
            BackupCutKind::Stopped.as_str(),
            BackupCutKind::ConfigurationOnly.as_str(),
        ])
    );
}
