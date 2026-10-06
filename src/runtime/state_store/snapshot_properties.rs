//! Durable runtime-state snapshots that a node keeps in its stored checkpoint envelope: Kafka
//! offsets, branch lifecycles, deduplicator keyspaces and branch-aggregated metrics.
//!
//! Layer: test harness.
//! - **Owns.** The properties that store each kind's generated snapshot through its production
//!   codec and the checkpoint envelope, and read arbitrary bytes through every payload decoder.
//! - **Depends on.** The stored checkpoint codec and the snapshot generators and oracles each
//!   state's owner declares for tests.
//! - **Must not know.** Replication, assignment authority, or the tasks that publish snapshots.

use nervix_arbitrary::{Arbitrary, Domain};

use super::*;
use crate::runtime::{branch_aggregated_state, branch_lru_state, deduplicator, kafka_offset_state};

/// Stores a payload the way a node keeps a checkpoint inline, at revision `lsm`, and reads the
/// payload back the way recovery does.
fn through_stored_checkpoint(lsm: u64) -> impl FnOnce(Vec<u8>) -> Vec<u8> {
    move |payload| {
        let checkpoint = StoredCheckpoint::Inline(PersistedRuntimeStateEntry { lsm, payload });
        let encoded = checkpoint
            .encode()
            .assured("an inline checkpoint of a bounded payload encodes");
        let decoded = StoredCheckpoint::decode(&encoded)
            .assured("a stored checkpoint decodes from its own encoding");
        let StoredCheckpoint::Inline(entry) = decoded else {
            panic!("an inline checkpoint decoded as another kind: {decoded:?}");
        };
        assert_eq!(entry.lsm, lsm);
        entry.payload
    }
}

/// Every kind's generated snapshot stores through its codec and the checkpoint envelope and
/// restores every value its owner promises: offsets and schedules, lifecycle keys, activity and
/// incarnations, keyspace parts bit for bit in arrival order, and metrics series.
#[test]
fn bolero_runtime_state_snapshots_restore_every_stored_value() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let lsm = arbitrary.entropy().any_u64();
            kafka_offset_state::assert_generated_offsets_survive(
                &mut arbitrary,
                through_stored_checkpoint(lsm),
            );
            branch_lru_state::assert_generated_lifecycle_survives(
                &mut arbitrary,
                through_stored_checkpoint(lsm),
            );
            deduplicator::assert_generated_keys_survive(
                &mut arbitrary,
                through_stored_checkpoint(lsm),
            );
            branch_aggregated_state::assert_generated_metrics_survive(
                &mut arbitrary,
                through_stored_checkpoint(lsm),
            );
        });
}

/// Arbitrary bytes read as any kind's stored payload either fail with that kind's typed decode
/// failure or restore a value that stores back unchanged. A payload reaches its decoder as the
/// owned buffer the checkpoint envelope hands over.
#[test]
fn bolero_malformed_runtime_state_snapshots_fail_typed() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let payload = bytes.to_vec();
            kafka_offset_state::assert_offset_payload_decodes_typed(&payload);
            branch_lru_state::assert_lifecycle_payload_decodes_typed(&payload);
            deduplicator::assert_key_payload_decodes_typed(&payload);
            branch_aggregated_state::assert_metrics_payload_decodes_typed(&payload);
        });
}
