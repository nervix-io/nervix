//! Current materialized record and archive consistency failures.
//!
//! Layer: test harness.
//! - **Owns.** Bounded corruptions whose transport metadata remains internally consistent.
//! - **Depends on.** Current record validators, restore reader and synthetic archive inputs.
//! - **Must not know.** Runtime assignment, native containers or restore installation.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::Timestamp;

use crate::{
    ArchiveReadError, ArchiveRecord, MaterializedIdentitiesRecord, MaterializedRelayDescriptor,
    SectionDigester, SectionPath,
    archive_values::{Case, Section, State},
    malformed_properties::{damaged_archive, rejects_field},
    materialized_values::Materialized,
    read_archive_contents,
};

impl Materialized {
    pub fn assert_invalid_fields(&self) {
        let mut descriptor = self.descriptor.clone();
        descriptor.record_count = 0;
        descriptor.groups = 1;
        rejects_field::<MaterializedRelayDescriptor>(
            descriptor
                .encode()
                .assured("malformed bounded descriptor encodes"),
            "materialized group count",
        );
        for group in &self.groups {
            let mut record = group.identities.clone();
            record.identities.clear();
            rejects_field::<MaterializedIdentitiesRecord>(
                record.encode().assured("empty identities encode"),
                "materialized identities",
            );
            record = group.identities.clone();
            record.identities[0].watermarks.ingested_at_low_watermark =
                Timestamp::from_unix_nanos(1);
            record.identities[0].watermarks.ingested_at_high_watermark =
                Timestamp::from_unix_nanos(0);
            rejects_field::<MaterializedIdentitiesRecord>(
                record.encode().assured("reversed watermarks encode"),
                "materialized watermarks",
            );
            record = group.identities.clone();
            record.identities[0].branch = Some(Vec::new());
            rejects_field::<MaterializedIdentitiesRecord>(
                record.encode().assured("an empty branch encodes"),
                "branch key",
            );
        }
    }
}

impl Case {
    pub fn assert_materialized_faults(&self) {
        let value = self.domains[0]
            .state
            .iter()
            .find_map(|state| match state {
                State::Materialized(value)
                    if value.descriptor.entity.as_str() == "unbranched_materialized" =>
                {
                    Some(value)
                }
                _ => None,
            })
            .assured("a stateful case has one unbranched materialized row");
        let descriptor = &value.descriptor;
        let path = SectionPath::materialized_descriptor(&descriptor.domain, &descriptor.entity);
        let descriptor_index = self
            .sections
            .iter()
            .position(|section| section.entry.path == path)
            .assured("the materialized descriptor has a section");
        let group = &value.groups[0].identities;
        let identity_path =
            SectionPath::materialized_identities(&group.domain, &group.entity, group.group);
        let identity_index = self
            .sections
            .iter()
            .position(|section| section.entry.path == identity_path)
            .assured("the identities have a section");
        let columns_path =
            SectionPath::materialized_columns(&group.domain, &group.entity, group.group);
        let columns_index = self
            .sections
            .iter()
            .position(|section| section.entry.path == columns_path)
            .assured("the Arrow columns have a section");
        for fault in 0..6 {
            let mut manifest = self.manifest.clone();
            let mut sections = self.sections.clone();
            let incomplete = |missing| ArchiveReadError::IncompleteDomain {
                domain: descriptor.domain.to_string(),
                missing,
            };
            let expected = match fault {
                0 | 1 => {
                    let mut record = descriptor.clone();
                    record.record_count = 2;
                    if fault == 1 {
                        record.groups = 2;
                    }
                    sections[descriptor_index] = Section::record(path.clone(), &record);
                    manifest.sections[descriptor_index] = sections[descriptor_index].entry.clone();
                    incomplete(if fault == 0 {
                        "materialized record count"
                    } else {
                        "materialized groups"
                    })
                }
                2 | 3 => {
                    let index = if fault == 2 {
                        identity_index
                    } else {
                        columns_index
                    };
                    sections.remove(index);
                    manifest.sections.remove(index);
                    incomplete(if fault == 2 {
                        "materialized identities"
                    } else {
                        "materialized columns"
                    })
                }
                4 => {
                    let mut record = group.clone();
                    record.group = descriptor.groups;
                    let path = SectionPath::materialized_identities(
                        &record.domain,
                        &record.entity,
                        record.group,
                    );
                    sections[identity_index] = Section::record(path.clone(), &record);
                    manifest.sections[identity_index] = sections[identity_index].entry.clone();
                    ArchiveReadError::MisplacedSection {
                        path: path.to_string(),
                    }
                }
                _ => {
                    let section = &mut sections[columns_index];
                    section.bytes[0] ^= 1;
                    assert_ne!(
                        SectionDigester::digest_of(&section.bytes),
                        section.entry.digest
                    );
                    ArchiveReadError::DigestMismatch {
                        path: columns_path.to_string(),
                    }
                }
            };
            let archive = damaged_archive(&manifest, &sections);
            let error = read_archive_contents(archive.as_slice())
                .expect_err("invalid materialized contents cannot reach installation");
            assert_eq!(error.current_context(), &expected);
        }
    }
}
