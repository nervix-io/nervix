//! Admitted immutable views of completed, quota-owned staging files.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Unlinking a completed file before mapping it, its memory and disk charges, and
//!   admitted inspection or copying of its bytes off the async workers.
//! - **Depends on.** The staging owner, the bounded executor and the native mapping library.
//! - **Must not know.** Checkpoint encodings, branches, graph decisions or archive sections.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns admitted immutable file views")
)]

use std::ops::Range;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_execution::{
    Cancellation, ChargedBytes, CpuClass, MemoryClass, Reservation, StorageClass,
};
use nervix_primitives::sync::Arc;

use super::{SnapshotStagingError, StagedArtifact};

/// An immutable mapping shared with inspection jobs that can outlive their caller's borrow.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct MappedArtifact {
    inner: Arc<MappedArtifactData>,
}

#[derive(Debug)]
struct MappedArtifactData {
    bytes: memmap2::Mmap,
    executor: nervix_execution::Executor,
    _memory: Reservation,
    _disk: nervix_primitives::sync::OwnedSemaphorePermit,
}

impl StagedArtifact {
    /// Admit the complete addressable view, then remove the file's name before mapping it.
    /// No writer or path survives this conversion, so another owner cannot change its bytes.
    pub(in crate::runtime) async fn into_mapped(
        self,
    ) -> error_stack::Result<MappedArtifact, SnapshotStagingError> {
        let executor = self.executor.clone();
        let memory = executor
            .try_reserve(MemoryClass::RestoreMetadata, self.length.max(1))
            .change_context(SnapshotStagingError::Admission)?;
        let mapped_executor = executor.clone();
        executor
            .run_storage(
                StorageClass::Filesystem,
                memory,
                move |memory, cancellation| {
                    cancellation
                        .check()
                        .change_context(SnapshotStagingError::Cancelled)?;
                    let (file, path) = self.file.into_parts();
                    path.close()
                        .map_err(Report::new)
                        .change_context(SnapshotStagingError::Read)?;
                    // SAFETY: close successfully removed the private temporary file's name. The only remaining
                    // file handle is owned here, never cloned or handed to a writer, and is dropped
                    // after creating this read-only mapping. Its bytes and length cannot change.
                    let bytes = unsafe { memmap2::Mmap::map(&file) }
                        .map_err(Report::new)
                        .change_context(SnapshotStagingError::Read)?;
                    cancellation
                        .check()
                        .change_context(SnapshotStagingError::Cancelled)?;
                    Ok::<_, Report<SnapshotStagingError>>(MappedArtifact {
                        inner: Arc::new(MappedArtifactData {
                            bytes,
                            executor: mapped_executor,
                            _memory: memory,
                            _disk: self._reservation,
                        }),
                    })
                },
            )
            .await
            .change_context(SnapshotStagingError::Execution)?
    }
}

impl MappedArtifact {
    pub(in crate::runtime) fn length(&self) -> usize {
        self.inner.bytes.len()
    }

    /// Run a bounded inspector while retaining the mapping and its admission through completion.
    /// Returned values are owned; a reference into the mapping cannot escape the job.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller inspects an immutable mapping in an admitted \
                                   cancellable bulk CPU job")
    )]
    pub(in crate::runtime) async fn inspect<T: Send + 'static>(
        &self,
        working: Reservation,
        inspect: impl FnOnce(&[u8], Reservation, &Cancellation) -> T + Send + 'static,
    ) -> error_stack::Result<T, SnapshotStagingError> {
        let view = self.clone();
        self.inner
            .executor
            .run_cpu(CpuClass::Bulk, working, move |working, cancellation| {
                inspect(&view.inner.bytes, working, cancellation)
            })
            .await
            .change_context(SnapshotStagingError::Execution)
    }

    /// Copy one bounded decoder input under its bulk charge, outside the async workers.
    pub(in crate::runtime) async fn copy_section(
        &self,
        range: Range<usize>,
        limit: u64,
    ) -> error_stack::Result<ChargedBytes, SnapshotStagingError> {
        let Some(length) = range.end.checked_sub(range.start) else {
            return Err(Report::new(SnapshotStagingError::Read));
        };
        let length = u64::try_from(length).verified("a mapped section fits u64");
        if length > limit || range.end > self.length() {
            return Err(Report::new(SnapshotStagingError::Window {
                position: u64::try_from(range.start).verified("an addressable offset fits u64"),
                length,
                artifact_length: u64::try_from(self.length()).verified("a mapped length fits u64"),
            }));
        }
        let working = self
            .inner
            .executor
            .try_reserve(MemoryClass::Bulk, length.max(1))
            .change_context(SnapshotStagingError::Admission)?;
        let view = self.clone();
        self.inner
            .executor
            .run_cpu(CpuClass::Bulk, working, move |working, cancellation| {
                cancellation
                    .check()
                    .change_context(SnapshotStagingError::Cancelled)?;
                let bytes = view.inner.bytes[range].to_vec();
                Ok::<_, Report<SnapshotStagingError>>(ChargedBytes::from_owned(bytes, working))
            })
            .await
            .change_context(SnapshotStagingError::Execution)?
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::time::Duration;

    use nervix_execution::{ExecutionConfig, Executor};

    use super::*;
    use crate::runtime::snapshot_staging::{SnapshotStaging, SnapshotStagingLimits};

    async fn artifact(
        executor: &Executor,
        staging: &SnapshotStaging,
        bytes: Vec<u8>,
    ) -> StagedArtifact {
        let mut writer = staging
            .try_stage(u64::try_from(bytes.len()).assured("bounded fixture fits"))
            .await
            .assured("fixture has disk quota");
        writer
            .write_chunk(
                executor
                    .charge_owned(MemoryClass::Bulk, bytes)
                    .await
                    .assured("bounded fixture is admitted"),
            )
            .await
            .assured("fixture writes");
        writer.finish_artifact().await.assured("fixture finishes")
    }

    #[nervix_primitives::test]
    async fn a_refused_mapping_returns_its_disk_quota_without_reading_a_view() {
        let directory = tempfile::tempdir().assured("staging directory opens");
        let executor = Executor::new(ExecutionConfig {
            budgets: nervix_execution::MemoryBudgets {
                restore_metadata: ubyte::ByteUnit::Mebibyte(1),
                ..Default::default()
            },
            ..Default::default()
        })
        .assured("small metadata budget is valid");
        let length = 2 * 1024 * 1024;
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits {
                staging_bytes: u64::try_from(length).assured("length fits"),
                snapshot_bytes: u64::try_from(length).assured("length fits"),
            },
        );
        let selected = artifact(&executor, &staging, vec![7; length]).await;
        let failure = selected
            .into_mapped()
            .await
            .expect_err("the required immutable view exceeds admission");
        assert!(matches!(
            failure.current_context(),
            SnapshotStagingError::Admission
        ));
        assert_eq!(
            executor.snapshot().restore_metadata_memory.reserved_bytes,
            0
        );
        let replacement = staging
            .try_stage(u64::try_from(length).assured("length fits"))
            .await
            .assured("the refused conversion released all of its disk quota");
        drop(replacement);
    }

    #[nervix_primitives::test]
    async fn a_mapping_retains_its_charge_until_its_last_inspector_finishes() {
        let directory = tempfile::tempdir().assured("staging directory opens");
        let executor = Executor::default();
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let view = artifact(&executor, &staging, b"immutable native bytes".to_vec())
            .await
            .into_mapped()
            .await
            .assured("the immutable view is admitted");
        let expected = u64::try_from(view.length()).assured("length fits");
        assert_eq!(
            executor.snapshot().restore_metadata_memory.reserved_bytes,
            expected
        );
        let working = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("inspection is admitted");
        let bytes = view
            .inspect(working, |bytes, _working, cancellation| {
                cancellation
                    .check()
                    .assured("the inspection remains awaited");
                bytes.to_vec()
            })
            .await
            .assured("the inspector runs");
        assert_eq!(bytes, b"immutable native bytes");
        assert_eq!(
            executor.snapshot().restore_metadata_memory.reserved_bytes,
            expected
        );
        drop(view);
        assert_eq!(
            executor.snapshot().restore_metadata_memory.reserved_bytes,
            0
        );
    }

    #[nervix_primitives::test]
    async fn a_section_copy_checks_its_bounds_and_admission_before_allocating() {
        let directory = tempfile::tempdir().assured("staging directory opens");
        let executor = Executor::default();
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let view = artifact(&executor, &staging, b"prefix:group:suffix".to_vec())
            .await
            .into_mapped()
            .await
            .assured("the immutable view is admitted");
        let group = view
            .copy_section(7..12, 5)
            .await
            .assured("one bounded group copies");
        assert_eq!(&*group, b"group");
        drop(group);
        for (range, limit) in [(7..12, 4), (12..30, 30), (Range { start: 12, end: 7 }, 30)] {
            assert!(view.copy_section(range, limit).await.is_err());
            assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        }
        let budget = executor.snapshot().bulk_memory.capacity_bytes;
        let occupied = executor
            .try_reserve(MemoryClass::Bulk, budget)
            .assured("the test occupies copying admission");
        let refused = view
            .copy_section(7..12, 5)
            .await
            .expect_err("an occupied bulk budget refuses copying");
        assert!(matches!(
            refused.current_context(),
            SnapshotStagingError::Admission
        ));
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, budget);
        drop(occupied);
        drop(view);
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        assert_eq!(
            executor.snapshot().restore_metadata_memory.reserved_bytes,
            0
        );
    }

    #[nervix_primitives::test]
    async fn a_cancelled_inspector_releases_its_mapping_after_observing_cancellation() {
        let directory = tempfile::tempdir().assured("staging directory opens");
        let executor = Executor::default();
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let view = artifact(&executor, &staging, vec![9; 1024])
            .await
            .into_mapped()
            .await
            .assured("the immutable view is admitted");
        let working = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("inspection is admitted");
        let (started, ready) = nervix_primitives::sync::oneshot::channel();
        let (finished, done) = nervix_primitives::sync::oneshot::channel();
        let inspection = nervix_primitives::task::spawn(async move {
            view.inspect(working, move |_bytes, _working, cancellation| {
                started.send(()).assured("the test waits for the inspector");
                while !cancellation.is_cancelled() {
                    nervix_primitives::thread::yield_now();
                }
                finished
                    .send(())
                    .assured("the test waits for cancellation observation");
            })
            .await
        });
        nervix_primitives::time::timeout(Duration::from_secs(30), ready)
            .await
            .assured("the admitted inspector starts within the test bound")
            .assured("the inspector starts");
        inspection.abort();
        assert!(
            inspection
                .await
                .expect_err("the awaiting task was cancelled")
                .is_cancelled()
        );
        nervix_primitives::time::timeout(Duration::from_secs(30), done)
            .await
            .assured("cancellation observation completes within the test bound")
            .assured("the production job observed its cancellation");
        nervix_primitives::time::timeout(Duration::from_secs(30), async {
            while executor.snapshot().restore_metadata_memory.reserved_bytes != 0 {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .assured("a completed cancelled job releases its mapping and charge");
    }
}
