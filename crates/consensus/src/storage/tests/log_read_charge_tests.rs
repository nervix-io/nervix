//! The working set each read of the consensus log holds while it reads stored entries.
//!
//! Layer: test harness.
//! - **Owns.** Evidence that the log state read and the retention boundary scan, which each read
//!   stored log entries whole, are charged for an entry larger than the management working set.
//! - **Depends on.** The production storage traits, the executor's memory classes, and the store
//!   harness with its filled consensus worker.
//! - **Must not know.** Raft elections, the interconnect or node startup.

use std::{error::Error, future::Future, io, time::Duration};

use futures_util::FutureExt as _;
use nervix_execution::{ExecutionConfig, Executor, MemoryClass};
use openraft::storage::{RaftLogStorage as _, RaftLogStorageExt as _};

use super::{FilledConsensusStorage, Harness, TestResult};
use crate::storage::{StorageCharge, StoreInner};

/// An executor whose single consensus worker, once filled, keeps a storage job waiting after the
/// store has admitted its working set.
fn executor_with_one_consensus_worker() -> Result<Executor, Box<dyn Error>> {
    Ok(Executor::new(ExecutionConfig::default())?)
}

/// A harness whose log holds, at index 1, a command larger than the management working set.
async fn harness_with_large_first_entry(executor: &Executor) -> Result<Harness, Box<dyn Error>> {
    let management = StorageCharge::Management.working_bytes(executor)?;
    let source_bytes = usize::try_from(management)?
        .checked_mul(3)
        .ok_or("three management working sets fit the address space")?;
    let mut harness = Harness::with_executor(executor.clone()).await?;
    harness
        .store
        .blocking_append([Harness::entry(1, Harness::admission(1, source_bytes)?)])
        .await?;
    Ok(harness)
}

/// The encoded bytes the log holds for `index`.
fn stored_entry_bytes(harness: &Harness, index: u64) -> Result<u64, Box<dyn Error>> {
    let value = harness
        .store
        .inner
        .logs
        .get(StoreInner::log_key(index))?
        .ok_or("the entry was appended")?;
    Ok(u64::try_from(value.len())?)
}

/// What the classes a consensus storage operation can be charged to hold now.
fn storage_charges(executor: &Executor) -> Result<u64, Box<dyn Error>> {
    let management = executor.memory(MemoryClass::Management).reserved_bytes;
    let commands = executor.memory(MemoryClass::Commands).reserved_bytes;
    let bulk = executor.memory(MemoryClass::Bulk).reserved_bytes;
    let charges = management
        .checked_add(commands)
        .ok_or("two memory classes hold fewer bytes than u64 counts")?;
    let charges = charges
        .checked_add(bulk)
        .ok_or("three memory classes hold fewer bytes than u64 counts")?;
    Ok(charges)
}

/// The charge `read` holds while its job waits for the filled consensus worker, and what the read
/// returns once the worker frees.
///
/// The store admits an operation's working set before it queues the operation's job, so the whole
/// charge is in place while the job waits behind the filled worker.
async fn charge_while_queued<T>(
    executor: &Executor,
    read: impl Future<Output = io::Result<T>>,
) -> Result<(u64, T), Box<dyn Error>> {
    let filled = FilledConsensusStorage::fill(executor).await;
    let before = storage_charges(executor)?;
    let mut read = std::pin::pin!(read);
    assert!(
        read.as_mut().now_or_never().is_none(),
        "the read waits for the filled consensus worker"
    );
    let queued = storage_charges(executor)?;
    let charge = queued
        .checked_sub(before)
        .ok_or("a queued read adds to the charges and releases none")?;
    filled.release();
    let returned = nervix_primitives::time::timeout(Duration::from_secs(10), read)
        .await
        .map_err(|_| "the read did not finish once the consensus worker freed")??;
    Ok((charge, returned))
}

/// Raft learns where its log ends by reading the last stored entry whole. The read is charged for
/// that entry, with the store's Commands working set every read of a log entry reserves.
#[nervix_primitives::test]
async fn the_log_state_read_is_charged_for_the_last_entry_it_reads() -> TestResult {
    let executor = executor_with_one_consensus_worker()?;
    let mut harness = harness_with_large_first_entry(&executor).await?;
    let stored = stored_entry_bytes(&harness, 1)?;
    assert!(stored > StorageCharge::Management.working_bytes(&executor)?);

    let (charge, state) = charge_while_queued(&executor, harness.store.get_log_state()).await?;

    assert!(
        charge >= stored,
        "the log state read holds a {stored}-byte entry under a {charge}-byte charge"
    );
    assert_eq!(charge, StorageCharge::Commands.working_bytes(&executor)?);
    assert_eq!(state.last_log_id, Some(Harness::log_id(1)));
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(executor.memory(MemoryClass::Commands).reserved_bytes, 0);
    Ok(())
}

/// The retention scan reads every covered entry it passes. It is charged for the largest of them,
/// with the store's Commands working set, and still finds the same boundary.
#[nervix_primitives::test]
async fn the_retention_scan_is_charged_for_each_covered_entry_it_reads() -> TestResult {
    let executor = executor_with_one_consensus_worker()?;
    let mut harness = harness_with_large_first_entry(&executor).await?;
    harness
        .store
        .blocking_append([
            Harness::entry(2, Harness::admission(2, 16)?),
            Harness::entry(3, Harness::admission(3, 16)?),
        ])
        .await?;
    let stored = stored_entry_bytes(&harness, 1)?;
    assert!(stored > StorageCharge::Management.working_bytes(&executor)?);

    // Keeping two covered entries puts the boundary at the large first entry, so the scan reads it.
    let (charge, boundary) = charge_while_queued(
        &executor,
        harness.store.covered_retention_boundary(3, 2, u64::MAX),
    )
    .await?;

    assert!(
        charge >= stored,
        "the retention scan holds a {stored}-byte entry under a {charge}-byte charge"
    );
    assert_eq!(charge, StorageCharge::Commands.working_bytes(&executor)?);
    assert_eq!(boundary, Some(1));
    assert_eq!(executor.memory(MemoryClass::Commands).reserved_bytes, 0);
    Ok(())
}
