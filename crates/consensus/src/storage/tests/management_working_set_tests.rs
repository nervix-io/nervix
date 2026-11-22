//! The management storage working set through the actual consensus store.
//!
//! Layer: test harness.
//! - **Owns.** Evidence that the store reserves exactly the working set the executor validates the
//!   management budget against, and that a budget of exactly that size serves the store.
//! - **Depends on.** The production storage traits, the executor's operation limits and the store
//!   harness.
//! - **Must not know.** Raft elections, the interconnect or node startup.

use nervix_execution::{ExecutionConfig, Executor, MemoryBudgets, MemoryClass, OperationLimits};
use openraft::storage::{RaftLogReader as _, RaftLogStorage as _};
use ubyte::ByteUnit;

use super::{Harness, TestResult};
use crate::{VoteOf, storage::StorageCharge};

/// The smallest management budget a node starts with holds every management storage operation:
/// the store reserves exactly the working set the executor validated that budget against, and a
/// store on a class of that size writes and reads its vote and committed marker.
#[nervix_primitives::test]
async fn a_management_class_of_one_consensus_vote_holds_the_vote_and_committed_marker() -> TestResult
{
    let limits = OperationLimits::default();
    let working_set = limits
        .management_storage_working_bytes()
        .ok_or("the default management storage working set fits a u64")?;
    let executor = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            management: ByteUnit::Byte(working_set),
            ..MemoryBudgets::default()
        },
        limits,
        ..ExecutionConfig::default()
    })?;
    assert_eq!(
        StorageCharge::Management.working_bytes(&executor)?,
        working_set
    );
    let mut harness = Harness::with_executor(executor.clone()).await?;
    let vote = VoteOf::new(3, Harness::node());
    harness.store.save_vote(&vote).await?;
    harness
        .store
        .save_committed(Some(Harness::log_id(1)))
        .await?;
    let stored_vote = harness.store.get_log_reader().await.read_vote().await?;
    assert_eq!(stored_vote, Some(vote));
    assert_eq!(
        harness.store.read_committed().await?,
        Some(Harness::log_id(1))
    );
    assert_eq!(executor.memory(MemoryClass::Management).reserved_bytes, 0);
    Ok(())
}
