//! Current deduplicator and window record and archive consistency failures.
//!
//! Layer: test harness.
//! - **Owns.** Bounded corruptions whose transport metadata remains internally consistent.
//! - **Depends on.** Current record validators, the restore reader and synthetic archive inputs.
//! - **Must not know.** Runtime keyspaces or windows, native checkpoints or restore installation.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{BranchKeyFingerprint, Timestamp};

use crate::{
    ArchiveReadError, ArchiveRecord, DeduplicatorStateDescriptor, SectionContent, SectionPath,
    WindowStateDescriptor,
    archive_values::{Case, Section, State},
    branch_state_values::{Deduplicator, Window},
    malformed_properties::{damaged_archive, rejects_field},
    read_archive_contents,
};

impl Deduplicator {
    pub fn assert_invalid_fields(&self) {
        let mut descriptor = self.descriptor.clone();
        descriptor.keys = 0;
        descriptor.groups = 1;
        rejects_field::<DeduplicatorStateDescriptor>(
            descriptor
                .encode()
                .assured("a malformed bounded descriptor encodes"),
            "deduplicator group count",
        );
        descriptor = self.descriptor.clone();
        descriptor.branch_fingerprint = match descriptor.branch_fingerprint {
            Some(_) => None,
            None => Some(BranchKeyFingerprint::new([9; 32])),
        };
        rejects_field::<DeduplicatorStateDescriptor>(
            descriptor
                .encode()
                .assured("a malformed bounded descriptor encodes"),
            "deduplicator branch fingerprint",
        );
        descriptor = self.descriptor.clone();
        descriptor.branch_fingerprint = Some(BranchKeyFingerprint::new([9; 32]));
        descriptor.branch = Some(Vec::new());
        rejects_field::<DeduplicatorStateDescriptor>(
            descriptor.encode().assured("an empty branch encodes"),
            "branch key",
        );
    }
}

impl Window {
    pub fn assert_invalid_fields(&self) {
        let mut descriptor = self.descriptor.clone();
        descriptor.incarnation = 0;
        rejects_field::<WindowStateDescriptor>(
            descriptor
                .encode()
                .assured("a malformed bounded descriptor encodes"),
            "window branch incarnation",
        );
        descriptor = self.descriptor.clone();
        descriptor.groups = descriptor
            .groups
            .checked_add(1)
            .assured("a bounded window has fewer groups than u32 counts");
        if descriptor.rows.is_empty() {
            rejects_field::<WindowStateDescriptor>(
                descriptor
                    .encode()
                    .assured("a malformed bounded descriptor encodes"),
                "window group count",
            );
        }
        descriptor = self.descriptor.clone();
        descriptor.branch_fingerprint = match descriptor.branch_fingerprint {
            Some(_) => None,
            None => Some(BranchKeyFingerprint::new([9; 32])),
        };
        rejects_field::<WindowStateDescriptor>(
            descriptor
                .encode()
                .assured("a malformed bounded descriptor encodes"),
            "window branch fingerprint",
        );
        descriptor = self.descriptor.clone();
        descriptor.first_sequence = match descriptor.first_sequence {
            Some(_) => None,
            None => Some(0),
        };
        rejects_field::<WindowStateDescriptor>(
            descriptor
                .encode()
                .assured("a malformed bounded descriptor encodes"),
            "window row sequence",
        );
        if let Some(first) = self.descriptor.first_sequence {
            descriptor = self.descriptor.clone();
            descriptor.next_sequence = first;
            rejects_field::<WindowStateDescriptor>(
                descriptor
                    .encode()
                    .assured("a malformed bounded descriptor encodes"),
                "window row sequence",
            );
            descriptor = self.descriptor.clone();
            descriptor.rows[0].ingested_at_low_watermark = Timestamp::from_unix_nanos(1);
            descriptor.rows[0].ingested_at_high_watermark = Timestamp::from_unix_nanos(0);
            rejects_field::<WindowStateDescriptor>(
                descriptor.encode().assured("reversed watermarks encode"),
                "window row watermarks",
            );
        }
    }
}

impl Case {
    /// The index of the section at `path` among this case's sections.
    fn section_index(&self, path: &SectionPath) -> usize {
        self.sections
            .iter()
            .position(|section| &section.entry.path == path)
            .assured("the generated state has its section")
    }

    /// Reads the case with one consistent change and checks the reader refuses it as `expected`.
    fn assert_refused(
        &self,
        change: impl FnOnce(&mut crate::BackupManifest, &mut Vec<Section>),
        expected: ArchiveReadError,
    ) {
        let mut manifest = self.manifest.clone();
        let mut sections = self.sections.clone();
        change(&mut manifest, &mut sections);
        let archive = damaged_archive(&manifest, &sections);
        let error = read_archive_contents(archive.as_slice())
            .expect_err("inconsistent branch state cannot reach installation");
        assert_eq!(error.current_context(), &expected);
    }

    pub fn assert_branch_state_faults(&self) {
        let deduplicator = self.domains[0]
            .state
            .iter()
            .find_map(|state| match state {
                State::Deduplicator(value) if value.descriptor.groups > 0 => Some(value),
                _ => None,
            })
            .assured("a stateful case has one branched keyspace");
        let window = self.domains[0]
            .state
            .iter()
            .find_map(|state| match state {
                State::Window(value) if value.descriptor.groups > 0 => Some(value),
                _ => None,
            })
            .assured("a stateful case has one branched window");
        let domain = deduplicator.descriptor.domain.to_string();
        let incomplete = |missing| ArchiveReadError::IncompleteDomain {
            domain: domain.clone(),
            missing,
        };

        let descriptor = &deduplicator.descriptor;
        let branch = descriptor.branch_fingerprint.as_ref();
        let keys_path =
            SectionPath::deduplicator_keys(&descriptor.domain, &descriptor.entity, branch, 0);
        let keys_index = self.section_index(&keys_path);
        self.assert_refused(
            |manifest, sections| {
                sections.remove(keys_index);
                manifest.sections.remove(keys_index);
            },
            incomplete("deduplicator key groups"),
        );
        let misplaced = SectionPath::deduplicator_keys(
            &descriptor.domain,
            &descriptor.entity,
            branch,
            descriptor.groups,
        );
        self.assert_refused(
            |manifest, sections| {
                sections[keys_index] = Section::new(
                    misplaced.clone(),
                    SectionContent::DeduplicatorKeys,
                    sections[keys_index].bytes.clone(),
                );
                manifest.sections[keys_index] = sections[keys_index].entry.clone();
            },
            ArchiveReadError::MisplacedSection {
                path: misplaced.to_string(),
            },
        );

        let descriptor = &window.descriptor;
        let branch = descriptor.branch_fingerprint.as_ref();
        let arguments_path =
            SectionPath::window_argument_columns(&descriptor.domain, &descriptor.entity, branch, 0);
        let arguments_index = self.section_index(&arguments_path);
        self.assert_refused(
            |manifest, sections| {
                sections.remove(arguments_index);
                manifest.sections.remove(arguments_index);
            },
            incomplete("window argument columns"),
        );
        let input_path =
            SectionPath::window_input_rows(&descriptor.domain, &descriptor.entity, branch, 0);
        let input_index = self.section_index(&input_path);
        self.assert_refused(
            |manifest, sections| {
                sections.remove(input_index);
                manifest.sections.remove(input_index);
            },
            incomplete("window input rows"),
        );
        let window_descriptor_path =
            SectionPath::window_descriptor(&descriptor.domain, &descriptor.entity, branch);
        let window_descriptor_index = self.section_index(&window_descriptor_path);
        self.assert_refused(
            |manifest, sections| {
                let mut record = descriptor.clone();
                let retained = u64::try_from(record.rows.len()).assured("bounded rows fit");
                record.groups = u32::try_from(retained).assured("bounded rows fit u32");
                record.rows.push(record.rows[0].clone());
                record.next_sequence = record
                    .next_sequence
                    .checked_add(1)
                    .assured("the generated sequence leaves room for one more row");
                let more_groups = record
                    .groups
                    .checked_add(1)
                    .assured("a bounded window has fewer groups than u32 counts");
                record.groups = more_groups;
                sections[window_descriptor_index] =
                    Section::record(window_descriptor_path.clone(), &record);
                manifest.sections[window_descriptor_index] =
                    sections[window_descriptor_index].entry.clone();
            },
            incomplete("window row groups"),
        );
    }
}
