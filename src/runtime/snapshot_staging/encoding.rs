//! Admitted streaming artifact encoding into quota-owned staging files.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Bounded writes, cancellation, actual length and digest, and the encoding job.
//! - **Depends on.** Staging quota and the executor's caller-owned memory reservation.
//! - **Must not know.** Checkpoint shapes, graph placement, or restore authority.

use std::io::{self, BufWriter, Write};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Cancellation, Reservation, StorageClass};
use nervix_primitives::sync::Arc;

use super::{SnapshotStagingError, StagedArtifact, StagedSnapshotWriter};

impl StagedSnapshotWriter {
    /// Encode directly into a fresh staging file. The declared length bounds disk consumption;
    /// the finished artifact carries the actual length. The supplied reservation and anything
    /// the encoder retains stay owned by the job until encoding and synchronization finish.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller encodes through a quota-owned cancellable writer inside the \
                      admitted storage job while retaining its memory reservation"
        )
    )]
    pub(crate) async fn encode_artifact(
        self,
        charge: Reservation,
        encode: impl FnOnce(&mut dyn Write, &Cancellation) -> Result<(), Report<SnapshotStagingError>>
        + Send
        + 'static,
    ) -> Result<StagedArtifact, Report<SnapshotStagingError>> {
        self.encode_artifact_with_result(charge, encode)
            .await
            .map(|(artifact, ())| artifact)
    }

    /// Encode a section while returning small metadata observed during the same source read.
    /// The metadata is published only after the staged bytes have been synchronized.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies a cancellable encoder and a retained \
                                   reservation to the admitted storage job")
    )]
    pub(crate) async fn encode_artifact_with_result<T: Send + 'static>(
        mut self,
        charge: Reservation,
        encode: impl FnOnce(&mut dyn Write, &Cancellation) -> Result<T, Report<SnapshotStagingError>>
        + Send
        + 'static,
    ) -> Result<(StagedArtifact, T), Report<SnapshotStagingError>> {
        if self.written != 0 {
            return Err(Report::new(SnapshotStagingError::LengthMismatch {
                actual: self.written,
                declared: 0,
            }));
        }
        let mut file = self
            .file
            .take()
            .assured("a fresh staging writer owns its file");
        let mut hasher = self
            .hasher
            .take()
            .assured("a fresh staging writer owns its hasher");
        let executor = self.executor.clone();
        let artifact_executor = executor.clone();
        let maximum = self.declared;
        let mut quota = self._reservation;
        executor
            .run_storage(
                StorageClass::Filesystem,
                charge,
                move |_charge, cancellation| {
                    cancellation
                        .check()
                        .change_context(SnapshotStagingError::Cancelled)?;
                    let (length, result) = {
                        let mut buffered = BufWriter::with_capacity(
                            usize::try_from(super::READ_BLOCK_BYTES)
                                .verified("a transfer block fits the address width"),
                            file.as_file_mut(),
                        );
                        let mut writer = EncodingWriter {
                            file: &mut buffered,
                            hasher: &mut hasher,
                            cancellation,
                            maximum,
                            written: 0,
                        };
                        let result = encode(&mut writer, cancellation)?;
                        writer
                            .flush()
                            .map_err(Report::new)
                            .change_context(SnapshotStagingError::Encode)?;
                        (writer.written, result)
                    };
                    cancellation
                        .check()
                        .change_context(SnapshotStagingError::Cancelled)?;
                    file.as_file()
                        .sync_all()
                        .map_err(Report::new)
                        .change_context(SnapshotStagingError::Encode)?;
                    let blocks =
                        usize::try_from(length.div_ceil(super::STAGING_PERMIT_BYTES).max(1))
                            .verified(
                                "an artifact cannot exceed its previously admitted disk quota",
                            );
                    let excess = quota
                        .num_permits()
                        .checked_sub(blocks)
                        .verified("encoding cannot exceed the admitted maximum length");
                    drop(quota.split(excess));
                    Ok::<_, Report<SnapshotStagingError>>((
                        StagedArtifact {
                            file,
                            length,
                            digest: *hasher.finalize().as_bytes(),
                            executor: artifact_executor,
                            _reservation: quota,
                        },
                        result,
                    ))
                },
            )
            .await
            .change_context(SnapshotStagingError::Execution)?
    }
}

impl StagedArtifact {
    /// Read at most one transfer chunk at an exact position. The reservation covers the read
    /// buffer and its outgoing transport copy and stays with the returned bytes.
    pub(crate) async fn read_window(
        artifact: Arc<Self>,
        position: u64,
        length: u64,
    ) -> Result<nervix_execution::ChargedBytes, Report<SnapshotStagingError>> {
        use std::io::{Read as _, Seek as _, SeekFrom};

        if length > super::READ_BLOCK_BYTES
            || position
                .checked_add(length)
                .is_none_or(|end| end > artifact.length)
        {
            return Err(Report::new(SnapshotStagingError::Window {
                position,
                length,
                artifact_length: artifact.length,
            }));
        }
        let working = length
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(1))
            .verified("the validated window is capped at one 64 KiB transfer chunk");
        let executor = artifact.executor.clone();
        let charge = executor
            .reserve(nervix_execution::MemoryClass::Bulk, working)
            .await
            .change_context(SnapshotStagingError::Admission)?;
        executor
            .run_storage(
                StorageClass::Filesystem,
                charge,
                move |charge, cancellation| {
                    cancellation
                        .check()
                        .change_context(SnapshotStagingError::Cancelled)?;
                    let read = || -> io::Result<Vec<u8>> {
                        let mut file = std::fs::File::open(artifact.path())?;
                        file.seek(SeekFrom::Start(position))?;
                        let mut bytes = vec![0; usize::try_from(length).map_err(io::Error::other)?];
                        file.read_exact(&mut bytes)?;
                        Ok(bytes)
                    };
                    let bytes = read()
                        .map_err(Report::new)
                        .change_context(SnapshotStagingError::Read)?;
                    Ok::<_, Report<SnapshotStagingError>>(
                        nervix_execution::ChargedBytes::from_owned(bytes, charge),
                    )
                },
            )
            .await
            .change_context(SnapshotStagingError::Execution)?
    }
}

/// Every writer call is split into bounded units even when the codec presents a large string.
struct EncodingWriter<'a> {
    file: &'a mut dyn Write,
    hasher: &'a mut blake3::Hasher,
    cancellation: &'a Cancellation,
    maximum: u64,
    written: u64,
}

impl Write for EncodingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = u64::try_from(bytes.len()).map_err(io::Error::other)?;
        let written = self
            .written
            .checked_add(length)
            .ok_or_else(|| io::Error::other("artifact length exceeds address space"))?;
        if written > self.maximum {
            return Err(io::Error::other(
                "encoded artifact exceeds its admitted disk quota",
            ));
        }
        for chunk in bytes.chunks(
            usize::try_from(super::READ_BLOCK_BYTES)
                .verified("a transfer block fits the address width"),
        ) {
            self.cancellation.check().map_err(io::Error::other)?;
            self.file.write_all(chunk)?;
            self.hasher.update(chunk);
        }
        self.written = written;
        Ok(bytes.len())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the retained admitted file writer flushes its fixed-size \
                                   buffer on the storage worker after cancellation is checked")
    )]
    fn flush(&mut self) -> io::Result<()> {
        self.cancellation.check().map_err(io::Error::other)?;
        self.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use nervix_execution::{ChargedBytes, ExecutionConfig, Executor, MemoryClass};

    use super::*;
    use crate::runtime::snapshot_staging::{SnapshotStaging, SnapshotStagingLimits};

    #[nervix_primitives::test]
    async fn streamed_artifacts_keep_exact_length_digest_and_bounded_read_charges() {
        let directory = tempfile::tempdir().assured("the staging directory opens");
        let executor = Executor::new(ExecutionConfig::default()).assured("default limits validate");
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let writer = staging
            .stage(4 * 1024 * 1024)
            .await
            .assured("disk quota is available");
        let charge = executor
            .reserve(MemoryClass::Bulk, 2 * 1024 * 1024)
            .await
            .assured("the fixed working set is available");
        let (artifact, revision) = writer
            .encode_artifact_with_result(charge, |output, cancellation| {
                let block = vec![71; 64 * 1024];
                for _ in 0..48 {
                    cancellation
                        .check()
                        .change_context(SnapshotStagingError::Cancelled)?;
                    output
                        .write_all(&block)
                        .map_err(Report::new)
                        .change_context(SnapshotStagingError::Encode)?;
                }
                output
                    .flush()
                    .map_err(Report::new)
                    .change_context(SnapshotStagingError::Encode)?;
                Ok(37_u64)
            })
            .await
            .assured("encoding finishes under its admitted quota");
        let artifact = Arc::new(artifact);
        assert_eq!(revision, 37);
        assert_eq!(artifact.length(), 3 * 1024 * 1024);
        assert_eq!(
            artifact.digest(),
            *blake3::hash(&vec![71; 3 * 1024 * 1024]).as_bytes()
        );
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        let chunk = StagedArtifact::read_window(artifact.clone(), 64 * 1024, 64 * 1024)
            .await
            .assured("one bounded window reads");
        assert_eq!(chunk.as_ref(), &vec![71; 64 * 1024]);
        assert_eq!(
            executor.snapshot().bulk_memory.reserved_bytes,
            128 * 1024 + 1
        );
        drop(chunk);
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        for (position, length) in [(artifact.length(), 1), (0, 64 * 1024 + 1), (u64::MAX, 1)] {
            let failure = StagedArtifact::read_window(artifact.clone(), position, length)
                .await
                .err()
                .assured("a window outside the artifact is refused");
            assert!(matches!(
                failure.current_context(),
                SnapshotStagingError::Window { position: actual_position, length: actual_length, artifact_length }
                    if *actual_position == position && *actual_length == length && *artifact_length == artifact.length()
            ));
        }
    }

    #[nervix_primitives::test]
    async fn failed_encoding_returns_disk_and_memory_admission() {
        let directory = tempfile::tempdir().assured("the staging directory opens");
        let executor = Executor::new(ExecutionConfig::default()).assured("default limits validate");
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits {
                staging_bytes: 1024 * 1024,
                snapshot_bytes: 1024 * 1024,
            },
        );
        let writer = staging
            .try_stage(10)
            .await
            .assured("the first artifact owns the disk quota");
        let charge = executor
            .reserve(MemoryClass::Bulk, 64 * 1024)
            .await
            .assured("encoding is admitted");
        let error = writer
            .encode_artifact(charge, |output, _cancellation| {
                output
                    .write_all(&[5; 11])
                    .map_err(Report::new)
                    .change_context(SnapshotStagingError::Encode)
            })
            .await
            .err()
            .assured("output beyond the admitted disk bound fails");
        assert!(matches!(
            error.current_context(),
            SnapshotStagingError::Encode
        ));
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        let mut writer = staging
            .try_stage(10)
            .await
            .assured("the failed artifact returned its disk quota");
        let charge = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .assured("the chunk is charged");
        writer
            .write_chunk(ChargedBytes::from_owned(vec![1], charge))
            .await
            .assured("the writer has started");
        let charge = executor
            .reserve(MemoryClass::Bulk, 64 * 1024)
            .await
            .assured("encoding is admitted");
        let error = writer
            .encode_artifact(charge, |_output, _cancellation| Ok(()))
            .await
            .err()
            .assured("encoding requires a fresh artifact");
        assert!(matches!(
            error.current_context(),
            SnapshotStagingError::LengthMismatch {
                actual: 1,
                declared: 0
            }
        ));
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
    }

    #[nervix_primitives::test]
    async fn cancelled_encoding_keeps_its_disk_and_memory_until_the_job_exits() {
        let directory = tempfile::tempdir().assured("the staging directory opens");
        let executor = Executor::new(ExecutionConfig::default()).assured("default limits validate");
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits {
                staging_bytes: 1024 * 1024,
                snapshot_bytes: 1024 * 1024,
            },
        );
        let writer = staging
            .try_stage(10)
            .await
            .assured("one artifact owns the disk quota");
        let charge = executor
            .reserve(MemoryClass::Bulk, 2 * 1024 * 1024)
            .await
            .assured("the fixed working set is admitted");
        let (started, running) = nervix_primitives::sync::oneshot::channel();
        let (release, paused) = nervix_primitives::sync::blocking::mpsc::channel();
        let task = nervix_primitives::task::spawn(async move {
            writer
                .encode_artifact(charge, move |_output, cancellation| {
                    started
                        .send(())
                        .assured("the harness holds the start receiver");
                    paused
                        .recv()
                        .assured("the harness releases this bounded test unit");
                    cancellation
                        .check()
                        .change_context(SnapshotStagingError::Cancelled)
                })
                .await
        });
        running
            .await
            .assured("the encoding job owns its quota and memory");
        task.abort();
        assert!(task.await.is_err(), "the encoding future was cancelled");
        assert_eq!(
            executor.snapshot().bulk_memory.reserved_bytes,
            2 * 1024 * 1024
        );
        let failure = staging
            .try_stage(10)
            .await
            .err()
            .assured("the paused job retains the disk quota");
        assert!(matches!(
            failure.current_context(),
            SnapshotStagingError::Full { .. }
        ));
        release
            .send(())
            .assured("the admitted job holds the release receiver");
        let writer = staging
            .stage(10)
            .await
            .assured("the cancelled job returns its disk quota on exit");
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        drop(writer);
        assert_eq!(
            std::fs::read_dir(directory.path())
                .assured("the directory exists")
                .count(),
            0
        );
    }
}
