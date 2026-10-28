//! Scheduling checks of the production log chunk's reservation lifetime.
//!
//! Layer: test harness.
//! - **Owns.** The invariant that taking the final entry frees its chunk before the consumer parks.
//! - **Depends on.** The production chunk state, executor and Shuttle runner.
//! - **Must not know.** Fjall I/O, peer transport or application lifecycle policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "explores the production log chunk's reservation lifetime"
    )
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Executor, MemoryClass};
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_models::ClusterNodeName;
use openraft::{
    entry::RaftEntry as _, type_config::alias::CommittedLeaderIdOf, vote::RaftLeaderId as _,
};

use super::{LogChunkState, LogEntryChunk, WorkingSet};
use crate::{LogIdOf, TypeConfig};

#[test]
fn shuttle_a_consumed_log_chunk_releases_its_charge_before_the_consumer_parks() {
    check_random_and_pct(|| {
        nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("the modeled runtime needs no external resources")
            .block_on(async {
                let executor = Executor::default();
                let bytes = executor
                    .limits()
                    .command_state_storage_working_bytes()
                    .assured("the validated command storage working set is addressable");
                let charge = executor
                    .try_reserve(MemoryClass::Commands, bytes)
                    .assured("the first read fits the untouched Commands class");
                let node = ClusterNodeName::parse("node-1")
                    .assured("the test node name follows the node-name grammar");
                let leader = CommittedLeaderIdOf::<TypeConfig>::new(3, node);
                let entries = (1..=2)
                    .map(|index| {
                        openraft::type_config::alias::EntryOf::<TypeConfig>::new_blank(
                            LogIdOf::new(leader.clone(), index),
                        )
                    })
                    .collect::<Vec<_>>();
                let mut chunk = LogChunkState::Buffered(LogEntryChunk {
                    entries: entries.into_iter(),
                    _working_set: WorkingSet {
                        reservation: charge,
                        turn: None,
                    },
                });
                assert_eq!(
                    chunk
                        .next_entry()
                        .verified("two entries are buffered")
                        .log_id
                        .index,
                    1
                );
                assert_eq!(executor.snapshot().commands_memory.reserved_bytes, bytes);

                let peer_executor = executor.clone();
                let peer = nervix_primitives::task::spawn(async move {
                    let read = peer_executor
                        .reserve(MemoryClass::Commands, bytes)
                        .await
                        .assured("consuming the first chunk admits another reader");
                    let control = peer_executor
                        .try_reserve(MemoryClass::Commands, 176)
                        .assured("the other reader leaves room for source control");
                    drop(control);
                    drop(read);
                });
                nervix_primitives::task::yield_now().await;
                assert_eq!(
                    chunk
                        .next_entry()
                        .verified("one entry remains buffered")
                        .log_id
                        .index,
                    2
                );
                // The chunk owner remains alive without asking it for another entry. A peer
                // queued before the final entry was consumed must still finish.
                peer.await
                    .assured("the peer finishes without a further consumer poll");
                assert!(chunk.begin_read());
                assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
            });
    });
}
