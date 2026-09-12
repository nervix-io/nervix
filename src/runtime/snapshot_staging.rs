//! The bounded on-disk staging area a sealed snapshot is received into before it is opened.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The staging quota, the temporary file one transfer writes into, the length and
//!   digest check that accepts it, and the bounded sequential reads that open it afterwards.
//! - **Depends on.** The executor that admits and charges filesystem work and bulk memory, and the
//!   directory the node stages into.
//! - **Must not know.** What a snapshot contains, which state it belongs to, or how its bytes
//!   arrived.
//!
//! A transfer larger than the node's transfer-memory budget is normal: the bytes land on disk as
//! they arrive and are read back one bounded section at a time, so no step of a transfer holds a
//! whole snapshot in memory.

use std::{
    io::{Read as _, Seek as _, SeekFrom, Write as _},
    path::PathBuf,
};

use arch_into::ArchInto as _;
use blake3::Hasher;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{BudgetedBuffer, ChargedBytes, Executor, MemoryClass, StorageClass};
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;

/// Why a snapshot could not be staged or read back.
#[derive(Debug, Error)]
pub(in crate::runtime) enum SnapshotStagingError {
    #[error("the node has no bulk capacity to stage this snapshot")]
    Admission,
    #[error("staging this snapshot could not be admitted for execution")]
    Execution,
    #[error("the staging area was cancelled before the snapshot was complete")]
    Cancelled,
    #[error("failed to create a staging file for the snapshot: {reason}")]
    Create { reason: String },
    #[error("failed to write the staged snapshot: {reason}")]
    Write { reason: String },
    #[error("failed to read the staged snapshot: {reason}")]
    Read { reason: String },
    #[error(
        "a staged snapshot of {actual} bytes exceeds the {limit} bytes of node staging quota that \
         remain"
    )]
    QuotaExceeded { actual: u64, limit: u64 },
    #[error("the staged snapshot is {actual} bytes where {declared} were declared")]
    LengthMismatch { actual: u64, declared: u64 },
    #[error("the staged snapshot does not match the digest its source declared")]
    DigestMismatch,
}

/// The disk a node will hold incomplete snapshot transfers on at one time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct SnapshotStagingLimits {
    /// The staging bytes every concurrent transfer shares.
    pub(in crate::runtime) staging_bytes: u64,
    /// The largest sealed snapshot one transfer may stage.
    pub(in crate::runtime) snapshot_bytes: u64,
}

impl Default for SnapshotStagingLimits {
    /// 128 GiB of node staging quota and 64 GiB for one sealed snapshot, matching the bulk storage
    /// bounds the interconnection settings declare.
    fn default() -> Self {
        Self {
            staging_bytes: 128 * GIBIBYTE,
            snapshot_bytes: 64 * GIBIBYTE,
        }
    }
}

const GIBIBYTE: u64 = 1024 * 1024 * 1024;

/// How many staging bytes one quota permit stands for. A quota this large is counted in blocks so
/// the whole node budget fits the permit count a semaphore is expressed in.
const STAGING_PERMIT_BYTES: u64 = 1024 * 1024;

/// How much of a staged snapshot one filesystem read moves into the buffer it is building.
const READ_BLOCK_BYTES: u64 = 64 * 1024;

/// The node's staging area: one directory and the quota every transfer into it shares.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct SnapshotStaging {
    root: PathBuf,
    executor: Executor,
    limits: SnapshotStagingLimits,
    quota: triomphe::Arc<StagingQuota>,
}

#[derive(Debug)]
struct StagingQuota {
    permits: std::sync::Arc<tokio::sync::Semaphore>,
}

impl SnapshotStaging {
    pub(in crate::runtime) fn new(
        root: PathBuf,
        executor: Executor,
        limits: SnapshotStagingLimits,
    ) -> Self {
        let blocks = limits.staging_bytes.div_ceil(STAGING_PERMIT_BYTES);
        // A configured quota beyond what this platform can count in permits becomes the largest
        // permit count it can express: the cap is the meaning here, not an avoided decision.
        let permits = usize::try_from(blocks).unwrap_or(usize::MAX);
        Self {
            root,
            executor,
            limits,
            quota: triomphe::Arc::new(StagingQuota {
                permits: std::sync::Arc::new(tokio::sync::Semaphore::new(permits)),
            }),
        }
    }

    /// Reserve staging disk for a snapshot of `length` bytes and open the file it lands in.
    ///
    /// The quota is taken before the first chunk is written, so a node that cannot hold the
    /// declared snapshot refuses the transfer instead of discovering it when the disk fills.
    pub(in crate::runtime) async fn stage(
        &self,
        length: u64,
    ) -> Result<StagedSnapshotWriter, Report<SnapshotStagingError>> {
        if length > self.limits.snapshot_bytes {
            return Err(Report::new(SnapshotStagingError::QuotaExceeded {
                actual: length,
                limit: self.limits.snapshot_bytes,
            }));
        }
        let blocks = length.div_ceil(STAGING_PERMIT_BYTES).max(1);
        let blocks = u32::try_from(blocks).map_err(|_| {
            Report::new(SnapshotStagingError::QuotaExceeded {
                actual: length,
                limit: self.limits.staging_bytes,
            })
        })?;
        let reservation = std::sync::Arc::clone(&self.quota.permits)
            .acquire_many_owned(blocks)
            .await
            .map_err(|_| {
                Report::new(SnapshotStagingError::QuotaExceeded {
                    actual: length,
                    limit: self.limits.staging_bytes,
                })
            })?;
        let root = self.root.clone();
        let executor = self.executor.clone();
        let file = executor
            .run_storage(
                StorageClass::Filesystem,
                self.executor
                    .reserve(MemoryClass::Bulk, 1)
                    .await
                    .change_context(SnapshotStagingError::Admission)?,
                move |_charge, cancellation| {
                    if cancellation.is_cancelled() {
                        return Err(Report::new(SnapshotStagingError::Cancelled));
                    }
                    std::fs::create_dir_all(&root).map_err(|error| {
                        Report::new(SnapshotStagingError::Create {
                            reason: error.to_string(),
                        })
                    })?;
                    tempfile::NamedTempFile::new_in(&root).map_err(|error| {
                        Report::new(SnapshotStagingError::Create {
                            reason: error.to_string(),
                        })
                    })
                },
            )
            .await
            .change_context(SnapshotStagingError::Execution)??;
        Ok(StagedSnapshotWriter {
            file: Some(file),
            hasher: Some(Hasher::new()),
            written: 0,
            declared: length,
            executor,
            _reservation: reservation,
        })
    }
}

/// One incomplete transfer, writing into its own staging file under the node's quota.
pub(in crate::runtime) struct StagedSnapshotWriter {
    file: Option<tempfile::NamedTempFile>,
    hasher: Option<Hasher>,
    written: u64,
    declared: u64,
    executor: Executor,
    /// Held for as long as the staged bytes exist, so the quota is released when the staged file
    /// is dropped or promoted, and not before.
    _reservation: OwnedSemaphorePermit,
}

/// One file that failed a write, returned with the failure so the writer keeps owning its file.
struct StagedWrite {
    file: tempfile::NamedTempFile,
    hasher: Hasher,
    result: Result<(), Report<SnapshotStagingError>>,
}

impl StagedSnapshotWriter {
    /// Write one already-admitted chunk, refusing bytes past the length the source declared.
    pub(in crate::runtime) async fn write_chunk(
        &mut self,
        chunk: ChargedBytes,
    ) -> Result<(), Report<SnapshotStagingError>> {
        if chunk.is_empty() {
            return Ok(());
        }
        let chunk_bytes: u64 = chunk.len().arch_into();
        let written = self.written.checked_add(chunk_bytes).ok_or_else(|| {
            Report::new(SnapshotStagingError::LengthMismatch {
                actual: u64::MAX,
                declared: self.declared,
            })
        })?;
        if written > self.declared {
            return Err(Report::new(SnapshotStagingError::LengthMismatch {
                actual: written,
                declared: self.declared,
            }));
        }
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(SnapshotStagingError::Admission)?;
        let file = self
            .file
            .take()
            .assured("a staging writer holds its file between completed storage jobs");
        let hasher = self
            .hasher
            .take()
            .assured("a staging writer holds its hasher between completed storage jobs");
        let write = self
            .executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    let mut file = file;
                    let mut hasher = hasher;
                    let result = if cancellation.is_cancelled() {
                        Err(Report::new(SnapshotStagingError::Cancelled))
                    } else {
                        file.write_all(chunk.as_ref())
                            .map(|()| hasher.update(chunk.as_ref()))
                            .map(|_| ())
                            .map_err(|error| {
                                Report::new(SnapshotStagingError::Write {
                                    reason: error.to_string(),
                                })
                            })
                    };
                    StagedWrite {
                        file,
                        hasher,
                        result,
                    }
                },
            )
            .await
            .change_context(SnapshotStagingError::Execution)?;
        self.file = Some(write.file);
        self.hasher = Some(write.hasher);
        write.result?;
        self.written = written;
        Ok(())
    }

    /// Synchronize the staged file and accept it only when it is exactly what its source declared.
    ///
    /// A truncated or corrupted transfer fails here, before anything reads it, so a partial
    /// generation never reaches the state it would have replaced.
    pub(in crate::runtime) async fn finish(
        mut self,
        digest: [u8; 32],
    ) -> Result<StagedSnapshot, Report<SnapshotStagingError>> {
        if self.written != self.declared {
            return Err(Report::new(SnapshotStagingError::LengthMismatch {
                actual: self.written,
                declared: self.declared,
            }));
        }
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(SnapshotStagingError::Admission)?;
        let file = self
            .file
            .take()
            .assured("an unfinished staging writer owns its file");
        let hasher = self
            .hasher
            .take()
            .assured("an unfinished staging writer owns its hasher");
        let executor = self.executor.clone();
        let length = self.declared;
        let file = executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    if cancellation.is_cancelled() {
                        return Err(Report::new(SnapshotStagingError::Cancelled));
                    }
                    if *hasher.finalize().as_bytes() != digest {
                        return Err(Report::new(SnapshotStagingError::DigestMismatch));
                    }
                    let mut file = file;
                    file.as_file_mut().sync_all().map_err(|error| {
                        Report::new(SnapshotStagingError::Write {
                            reason: error.to_string(),
                        })
                    })?;
                    file.as_file_mut()
                        .seek(SeekFrom::Start(0))
                        .map_err(|error| {
                            Report::new(SnapshotStagingError::Read {
                                reason: error.to_string(),
                            })
                        })?;
                    Ok(file)
                },
            )
            .await
            .change_context(SnapshotStagingError::Execution)??;
        Ok(StagedSnapshot {
            file: Some(file),
            offset: 0,
            length,
            executor,
            _reservation: self._reservation,
        })
    }
}

/// One complete, verified snapshot on disk, read back one bounded section at a time.
pub(in crate::runtime) struct StagedSnapshot {
    file: Option<tempfile::NamedTempFile>,
    offset: u64,
    length: u64,
    executor: Executor,
    _reservation: OwnedSemaphorePermit,
}

/// One file that finished a read, returned with what it produced so the reader keeps its file.
struct StagedRead {
    file: tempfile::NamedTempFile,
    result: Result<ChargedBytes, Report<SnapshotStagingError>>,
}

impl StagedSnapshot {
    /// Read the next `length` bytes, charged to the bulk budget for as long as the caller holds
    /// them. Reading past the end of the staged snapshot is a truncation, not a short read.
    pub(in crate::runtime) async fn read(
        &mut self,
        length: u64,
    ) -> Result<ChargedBytes, Report<SnapshotStagingError>> {
        let truncated = || {
            Report::new(SnapshotStagingError::LengthMismatch {
                actual: self.length,
                declared: length,
            })
        };
        let Some(end) = self.offset.checked_add(length) else {
            return Err(truncated());
        };
        if end > self.length {
            return Err(truncated());
        }
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, length.max(1))
            .await
            .change_context(SnapshotStagingError::Admission)?;
        let file = self
            .file
            .take()
            .assured("a staged snapshot holds its file between completed storage jobs");
        let read = self
            .executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |charge, cancellation| {
                    let mut file = file;
                    let result = if cancellation.is_cancelled() {
                        Err(Report::new(SnapshotStagingError::Cancelled))
                    } else {
                        read_exact(&mut file, charge, length)
                    };
                    StagedRead { file, result }
                },
            )
            .await
            .change_context(SnapshotStagingError::Execution)?;
        self.file = Some(read.file);
        let bytes = read.result?;
        self.offset = end;
        Ok(bytes)
    }
}

fn read_exact(
    file: &mut tempfile::NamedTempFile,
    charge: nervix_execution::Reservation,
    length: u64,
) -> Result<ChargedBytes, Report<SnapshotStagingError>> {
    let mut buffer = BudgetedBuffer::with_limit(charge, length.max(1));
    let mut remaining = length;
    let block_bytes = usize::try_from(length.min(READ_BLOCK_BYTES))
        .verified("the block size is capped at the read block, which fits every pointer width");
    let mut block = vec![0_u8; block_bytes];
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(block.len().arch_into()))
            .verified("the wanted count is capped at the block length, which is a usize");
        let read = file
            .as_file_mut()
            .read(&mut block[..wanted])
            .map_err(|error| {
                Report::new(SnapshotStagingError::Read {
                    reason: error.to_string(),
                })
            })?;
        if read == 0 {
            return Err(Report::new(SnapshotStagingError::LengthMismatch {
                actual: length
                    .checked_sub(remaining)
                    .verified("the loop only ever subtracts bytes it has already read"),
                declared: length,
            }));
        }
        buffer.write_all(&block[..read]).map_err(|error| {
            Report::new(SnapshotStagingError::Read {
                reason: error.to_string(),
            })
        })?;
        let read: u64 = read.arch_into();
        remaining = remaining
            .checked_sub(read)
            .verified("a read is bounded by the wanted count, which is bounded by remaining");
    }
    Ok(ChargedBytes::from_buffer(buffer))
}
