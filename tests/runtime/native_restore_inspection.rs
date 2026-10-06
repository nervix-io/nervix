//! Complete-value inspection of restored native checkpoints after a public restore.
//!
//! Layer: test harness.
//! - **Owns.** Reopening stopped test nodes and comparing every native value and revision.
//! - **Depends on.** The production snapshot reader and native checkpoint decoders.
//! - **Must not know.** Restore installation implementation or a second stored representation.

use std::path::Path;

use meticulous::ResultExt as _;
use nervix_backup::{BranchLifecycleRecord, KafkaOffsetsRecord, StateField};
use nervix_models::DomainName;
use nervix_server::runtime::native_checkpoint_inspection::{
    NativeCheckpointInspection, read_native_checkpoint_values,
};

/// The cluster must have stopped every node before opening its database. Inspection allocations
/// belong to the test oracle and happen after the measured public restore interval.
pub fn assert_native_checkpoint_values(
    path: &Path,
    domain: &DomainName,
    lifecycles: &[BranchLifecycleRecord],
    offsets: &KafkaOffsetsRecord,
) -> (Vec<bool>, bool) {
    let entries = read_native_checkpoint_values(path, domain)
        .assured("the selected complete generation's current native metadata reads");
    let mut lifecycle_found = vec![false; lifecycles.len()];
    let mut offsets_found = false;
    for entry in entries {
        match entry {
            NativeCheckpointInspection::BranchLifecycle {
                owner_kind,
                entity,
                revision,
                branches,
            } => {
                let Some((index, lifecycle)) =
                    lifecycles.iter().enumerate().find(|(_, lifecycle)| {
                        owner_kind == lifecycle.owner_kind && entity == lifecycle.entity
                    })
                else {
                    continue;
                };
                assert_eq!(revision, lifecycle.revision);
                let actual = branches
                    .into_iter()
                    .map(|entry| {
                        (
                            entry.key.map(|fields| {
                                fields
                                    .into_iter()
                                    .map(StateField::from_remote)
                                    .collect::<Vec<_>>()
                            }),
                            entry.last_ingestion,
                            entry.incarnation,
                        )
                    })
                    .collect::<Vec<_>>();
                let expected = lifecycle
                    .branches
                    .iter()
                    .map(|entry| (entry.key.clone(), entry.last_ingestion, entry.incarnation))
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
                lifecycle_found[index] = true;
            }
            NativeCheckpointInspection::KafkaOffsets {
                entity,
                revision,
                offsets: actual,
            } if entity == offsets.entity => {
                assert_eq!(revision, offsets.revision);
                assert_eq!(
                    actual,
                    offsets
                        .offsets
                        .iter()
                        .map(|entry| (entry.topic.clone(), entry.partition, entry.next_offset))
                        .collect::<Vec<_>>()
                );
                offsets_found = true;
            }
            _ => {}
        }
    }
    (lifecycle_found, offsets_found)
}
