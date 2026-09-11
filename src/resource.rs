//! The on-disk store for published resource versions.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Bounded archive and bundle staging, content-addressed installation of a version,
//!   its manifest and checksums, staging cleanup, and atomic promotion into place.
//! - **Depends on.** The vocabulary's resource identities and the filesystem.
//! - **Must not know.** How a version was uploaded, replicated or referenced. Those are
//!   control-plane use cases; this store installs bytes and reports what is installed.

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{self, Read as _, Write as _},
    path::{Component, Path, PathBuf},
};

#[cfg(test)]
use arch_into::ArchInto as _;
#[cfg(test)]
use async_tar::{Builder as AsyncTarBuilder, EntryType, Header, HeaderMode};
use blake3::Hasher;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
#[cfg(test)]
use nervix_execution::CpuClass;
use nervix_execution::{Cancellation, ChargedBytes, Executor, MemoryClass, StorageClass};
use nervix_models::{ClusterNodeName, ResourceId, ResourceVersion, Timestamp};
use serde::{Deserialize, Serialize};
use tar::{
    Archive as TarArchive, Builder as TarBuilder, EntryType as TarEntryType, Header as TarHeader,
    HeaderMode as TarHeaderMode,
};
#[cfg(test)]
use tokio::io::AsyncReadExt;
use tracing::warn;

const DEFAULT_MAX_RESOURCE_ARCHIVE_BYTES: u64 = 4_294_967_296;
const DEFAULT_MAX_RESOURCE_EXTRACTED_BYTES: u64 = 17_179_869_184;
const DEFAULT_MAX_RESOURCE_FILE_COUNT: u64 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceManifest {
    pub resource: ResourceVersion,
    pub entries: Vec<ResourceManifestEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceManifestEntry {
    pub path: String,
    pub content: ResourceEntryContent,
}

/// What one manifest entry names inside a version.
///
/// A directory has no bytes of its own, so it carries neither a size nor a checksum. A file
/// carries both, and they always describe the same bytes because they are written together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceEntryContent {
    Directory,
    File { size: u64, checksum: String },
}

impl ResourceEntryContent {
    /// The bytes this entry contributes to its version's total. A directory contributes none.
    pub fn size(&self) -> u64 {
        match self {
            Self::Directory => 0,
            Self::File { size, .. } => *size,
        }
    }

    pub fn is_file(&self) -> bool {
        matches!(self, Self::File { .. })
    }
}

#[derive(Debug, Clone)]
pub struct ResourceStore {
    root: PathBuf,
    /// The node's bounded execution and memory admission. Walking, reading and hashing a version's
    /// contents is bulk work, so it is admitted and charged rather than run on an async worker.
    executor: Executor,
    limits: ResourceStoreLimits,
}

/// Per-version disk bounds applied independently to the staged archive and extracted tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceStoreLimits {
    pub max_archive_bytes: u64,
    pub max_extracted_bytes: u64,
    pub max_file_count: u64,
}

impl Default for ResourceStoreLimits {
    fn default() -> Self {
        Self {
            max_archive_bytes: DEFAULT_MAX_RESOURCE_ARCHIVE_BYTES,
            max_extracted_bytes: DEFAULT_MAX_RESOURCE_EXTRACTED_BYTES,
            max_file_count: DEFAULT_MAX_RESOURCE_FILE_COUNT,
        }
    }
}

/// One open immutable archive. Each read runs on the filesystem pool and allocates at most one
/// configured bulk chunk under its memory charge.
pub struct ResourceArchiveReader {
    file: Option<fs::File>,
    remaining: u64,
    archive_bytes: u64,
    executor: Executor,
}

/// One temporary archive assembled from bounded, admitted chunks on filesystem workers.
pub struct ResourceArchiveStager {
    path: Option<tempfile::TempPath>,
    file: Option<fs::File>,
    hasher: Option<Hasher>,
    archive_bytes: u64,
    max_archive_bytes: u64,
    executor: Executor,
}

/// A complete temporary archive whose byte count and digest describe the staged file.
pub struct StagedResourceArchive {
    path: tempfile::TempPath,
    root_checksum: String,
    archive_bytes: u64,
}

struct OpenArchive {
    file: fs::File,
    archive_bytes: u64,
}

struct ArchiveRead {
    file: fs::File,
    result: Result<ChargedBytes, Report<ResourceStoreError>>,
}

struct OpenArchiveStager {
    path: tempfile::TempPath,
    file: fs::File,
}

struct ArchiveStageWrite {
    file: fs::File,
    hasher: Hasher,
    result: Result<(), Report<ResourceStoreError>>,
}

/// A temporary tree assembled from bounded chunks before it becomes one deterministic archive.
pub(crate) struct ResourceBundleStager {
    root: Option<PathBuf>,
    current_file: Option<PathBuf>,
    file_count: u64,
    extracted_bytes: u64,
    limits: ResourceStoreLimits,
    executor: Executor,
}

enum StagedBundleEntry {
    Directory {
        relative: PathBuf,
    },
    File {
        full_path: PathBuf,
        relative: PathBuf,
        size: u64,
    },
}

struct StagedBundleInventory {
    entries: Vec<StagedBundleEntry>,
    file_count: u64,
    total_bytes: u64,
}

#[derive(Clone, Copy)]
enum ArchiveWriteFailure {
    Cancelled,
    QuotaExceeded { actual: u64 },
}

struct ArchiveStagingWriter<'a> {
    file: fs::File,
    hasher: Hasher,
    written: u64,
    limit: u64,
    failure: Option<ArchiveWriteFailure>,
    cancellation: &'a Cancellation,
}

impl StagedBundleEntry {
    fn relative(&self) -> &Path {
        match self {
            Self::Directory { relative } | Self::File { relative, .. } => relative,
        }
    }
}

/// Where one resource version is installed: the directory it will finally occupy, the staging
/// directory it is built in, and the content directory inside that staging directory.
#[derive(Debug)]
#[cfg(test)]
struct InstallPaths {
    install_root: PathBuf,
    staging_root: PathBuf,
    content_root: PathBuf,
}

#[derive(Debug)]
struct PendingInstall {
    id: ResourceId,
    install_root: PathBuf,
    staging_root: PathBuf,
    content_root: PathBuf,
    created_by_node: ClusterNodeName,
    created_at: Timestamp,
}

struct ArchiveInstallPlan<'a> {
    source_archive_path: &'a Path,
    declared_archive_bytes: u64,
    declared_root_checksum: &'a str,
    limits: ResourceStoreLimits,
    chunk_bytes: u64,
    expected_resource: Option<&'a ResourceVersion>,
}

#[derive(Debug, thiserror::Error)]
pub enum ResourceStoreError {
    #[error("failed to create resource storage directory")]
    CreateRoot,
    #[error("resource source directory does not exist")]
    MissingSource,
    #[error("resource source must be a directory")]
    InvalidSource,
    #[error("failed to read directory entry")]
    ReadDirectory,
    #[error("failed to read file")]
    ReadFile,
    #[error("failed to serialize manifest")]
    SerializeManifest,
    #[error("failed to write manifest")]
    WriteManifest,
    #[error("failed to write resource archive")]
    WriteArchive,
    #[error("failed to read resource archive")]
    ReadArchive,
    #[error("resource archive is invalid")]
    InvalidArchive,
    #[error("resource archive checksum does not match its declared digest")]
    ArchiveChecksumMismatch,
    #[error("resource archive manifest does not match its published metadata")]
    ResourceMetadataMismatch,
    #[error("resource archive contains {actual} bytes, exceeding the {limit}-byte staging quota")]
    ArchiveQuotaExceeded { actual: u64, limit: u64 },
    #[error(
        "resource archive declares {actual} extracted bytes, exceeding the {limit}-byte \
         extraction quota"
    )]
    ExtractedQuotaExceeded { actual: u64, limit: u64 },
    #[error(
        "resource archive declares {actual} files, exceeding the {limit}-file extraction quota"
    )]
    FileCountQuotaExceeded { actual: u64, limit: u64 },
    #[error("resource archive path escapes bundle root")]
    InvalidArchivePath,
    #[error("resource archive contains an unsupported entry type")]
    UnsupportedArchiveEntry,
    #[error("resource archive contains the same path more than once")]
    DuplicateArchivePath,
    #[error("failed to create resource directory")]
    CreateResourceDir,
    #[error("failed to rename installed resource")]
    RenameResourceDir,
    #[error("failed to delete installed resource")]
    DeleteResourceDir,
    #[error("resource installation task failed")]
    JoinBlockingTask,
    #[error("resource path escapes bundle root")]
    InvalidResourcePath,
    #[error("the node has no bulk capacity for this resource version")]
    BulkAdmission,
    #[error("resource archive chunk size must be greater than zero")]
    EmptyArchiveChunk,
    #[error("reading the resource version was cancelled")]
    Cancelled,
    #[error("resource archive ended before its declared size")]
    ArchiveTruncated,
    #[error("resource archive changed size while it was staged")]
    ArchiveSizeChanged,
    #[error("resource archive byte count overflowed while it was staged")]
    ArchiveByteCountOverflow,
    #[error("resource bundle staging has no open file")]
    BundleFileNotOpen,
    #[error("resource bundle contains no files")]
    EmptyBundle,
}

impl ResourceArchiveReader {
    pub fn archive_bytes(&self) -> u64 {
        self.archive_bytes
    }

    pub async fn next_chunk(&mut self) -> Result<Option<ChargedBytes>, Report<ResourceStoreError>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let wanted = self
            .remaining
            .min(self.executor.limits().bulk_chunk_bytes.as_u64());
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, wanted)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let file = self
            .file
            .take()
            .assured("an archive reader returns its file after every completed storage job");
        let read = self
            .executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |charge, cancellation| {
                    let mut file = file;
                    let result = if cancellation.is_cancelled() {
                        Err(Report::new(ResourceStoreError::Cancelled))
                    } else {
                        match usize::try_from(wanted) {
                            Ok(wanted) => {
                                let mut bytes = vec![0_u8; wanted];
                                match file.read_exact(&mut bytes) {
                                    Ok(()) => Ok(ChargedBytes::from_owned(bytes, charge)),
                                    Err(_) => {
                                        Err(Report::new(ResourceStoreError::ArchiveTruncated))
                                    }
                                }
                            }
                            Err(_) => Err(Report::new(ResourceStoreError::ReadArchive)),
                        }
                    };
                    ArchiveRead { file, result }
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)?;
        self.file = Some(read.file);
        let bytes = read.result?;
        let read_bytes = u64::try_from(bytes.len())
            .assured("supported targets have a pointer width no larger than u64");
        self.remaining = self
            .remaining
            .checked_sub(read_bytes)
            .verified("the read size was bounded by the remaining archive bytes");
        Ok(Some(bytes))
    }
}

impl ResourceArchiveStager {
    /// Write one already-admitted chunk without copying it.
    pub async fn write_chunk(
        &mut self,
        chunk: ChargedBytes,
    ) -> Result<(), Report<ResourceStoreError>> {
        if chunk.is_empty() {
            return Ok(());
        }
        let chunk_bytes = u64::try_from(chunk.len())
            .map_err(|_| Report::new(ResourceStoreError::ArchiveByteCountOverflow))?;
        let next_archive_bytes = self
            .archive_bytes
            .checked_add(chunk_bytes)
            .ok_or_else(|| Report::new(ResourceStoreError::ArchiveByteCountOverflow))?;
        if next_archive_bytes > self.max_archive_bytes {
            return Err(Report::new(ResourceStoreError::ArchiveQuotaExceeded {
                actual: next_archive_bytes,
                limit: self.max_archive_bytes,
            }));
        }
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let file = self
            .file
            .take()
            .assured("an archive stager returns its file after every completed storage job");
        let hasher = self
            .hasher
            .take()
            .assured("an archive stager returns its hasher after every completed storage job");
        let write = self
            .executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    let mut file = file;
                    let mut hasher = hasher;
                    let result = if cancellation.is_cancelled() {
                        Err(Report::new(ResourceStoreError::Cancelled))
                    } else {
                        match file.write_all(&chunk) {
                            Ok(()) => {
                                hasher.update(&chunk);
                                Ok(())
                            }
                            Err(_) => Err(Report::new(ResourceStoreError::WriteArchive)),
                        }
                    };
                    ArchiveStageWrite {
                        file,
                        hasher,
                        result,
                    }
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)?;
        self.file = Some(write.file);
        self.hasher = Some(write.hasher);
        write.result?;
        self.archive_bytes = next_archive_bytes;
        Ok(())
    }

    /// Flush and sync the staged file before returning its exact size and digest.
    pub async fn finish(mut self) -> Result<StagedResourceArchive, Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let path = self
            .path
            .take()
            .assured("an unfinished archive stager owns its temporary path");
        let file = self
            .file
            .take()
            .assured("an unfinished archive stager owns its temporary file");
        let hasher = self
            .hasher
            .take()
            .assured("an unfinished archive stager owns its hasher");
        let archive_bytes = self.archive_bytes;
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    if cancellation.is_cancelled() {
                        return Err(Report::new(ResourceStoreError::Cancelled));
                    }
                    let mut file = file;
                    file.flush()
                        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
                    file.sync_all()
                        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
                    let hash = hasher.finalize();
                    Ok(StagedResourceArchive {
                        path,
                        root_checksum: encode_hex(hash.as_bytes()),
                        archive_bytes,
                    })
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)?
    }
}

impl ResourceBundleStager {
    /// Create one empty file after validating its path and the file-count quota.
    pub(crate) async fn create_file(
        &mut self,
        relative: &Path,
    ) -> Result<(), Report<ResourceStoreError>> {
        let relative = sanitize_archive_path(relative)?;
        let next_file_count = self.file_count.checked_add(1).ok_or_else(|| {
            Report::new(ResourceStoreError::FileCountQuotaExceeded {
                actual: u64::MAX,
                limit: self.limits.max_file_count,
            })
        })?;
        if next_file_count > self.limits.max_file_count {
            return Err(Report::new(ResourceStoreError::FileCountQuotaExceeded {
                actual: next_file_count,
                limit: self.limits.max_file_count,
            }));
        }
        let root = self
            .root
            .as_ref()
            .assured("an unfinished resource bundle stager owns its staging tree");
        let destination = root.join(&relative);
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| -> Result<(), Report<ResourceStoreError>> {
                    cancellation
                        .check()
                        .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
                    if let Some(parent) = destination.parent() {
                        fs::create_dir_all(parent)
                            .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
                    }
                    let file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(destination)
                        .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
                    file.sync_all()
                        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)??;
        self.current_file = Some(relative);
        self.file_count = next_file_count;
        Ok(())
    }

    /// Append one already-admitted chunk after checking the extracted-byte quota.
    pub(crate) async fn write_chunk(
        &mut self,
        chunk: ChargedBytes,
    ) -> Result<(), Report<ResourceStoreError>> {
        if chunk.is_empty() {
            return Ok(());
        }
        let relative = self
            .current_file
            .as_ref()
            .ok_or_else(|| Report::new(ResourceStoreError::BundleFileNotOpen))?;
        let chunk_bytes = u64::try_from(chunk.len())
            .map_err(|_| Report::new(ResourceStoreError::ArchiveByteCountOverflow))?;
        let next_extracted_bytes =
            self.extracted_bytes
                .checked_add(chunk_bytes)
                .ok_or_else(|| {
                    Report::new(ResourceStoreError::ExtractedQuotaExceeded {
                        actual: u64::MAX,
                        limit: self.limits.max_extracted_bytes,
                    })
                })?;
        if next_extracted_bytes > self.limits.max_extracted_bytes {
            return Err(Report::new(ResourceStoreError::ExtractedQuotaExceeded {
                actual: next_extracted_bytes,
                limit: self.limits.max_extracted_bytes,
            }));
        }
        let root = self
            .root
            .as_ref()
            .assured("an unfinished resource bundle stager owns its staging tree");
        let destination = root.join(relative);
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    cancellation
                        .check()
                        .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
                    let mut file = OpenOptions::new()
                        .append(true)
                        .open(destination)
                        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
                    file.write_all(&chunk)
                        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)??;
        self.extracted_bytes = next_extracted_bytes;
        Ok(())
    }

    /// Build, hash and sync one deterministic archive, then recursively remove the source tree.
    pub(crate) async fn finish(
        mut self,
    ) -> Result<StagedResourceArchive, Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(
                MemoryClass::Bulk,
                self.executor.limits().bulk_chunk_bytes.as_u64(),
            )
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let root = self
            .root
            .take()
            .assured("an unfinished resource bundle stager owns its staging tree");
        let limits = self.limits;
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    let result = build_bundle_archive(&root, limits, cancellation);
                    let cleanup = remove_tree_after_failure(&root);
                    match (result, cleanup) {
                        (Ok(archive), Ok(())) => Ok(archive),
                        (Err(error), Ok(())) => Err(error),
                        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
                        (Err(error), Err(cleanup_error)) => {
                            Err(cleanup_error.attach_printable(error.to_string()))
                        }
                    }
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)?
    }

    /// Recursively discard a partially received bundle on the filesystem worker pool.
    pub(crate) async fn abort(mut self) -> Result<(), Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let root = self
            .root
            .take()
            .assured("an unfinished resource bundle stager owns its staging tree");
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, _cancellation| remove_tree_after_failure(&root),
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)??;
        Ok(())
    }
}

impl Drop for ResourceBundleStager {
    fn drop(&mut self) {
        let Some(root) = self.root.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!(
                path = %root.display(),
                "resource bundle staging cleanup will resume during the next store startup"
            );
            return;
        };
        let executor = self.executor.clone();
        drop(runtime.spawn(async move {
            let cleanup: Result<(), Report<ResourceStoreError>> = async {
                let reservation = executor
                    .reserve(MemoryClass::Bulk, 1)
                    .await
                    .change_context(ResourceStoreError::BulkAdmission)?;
                executor
                    .run_storage(
                        StorageClass::Filesystem,
                        reservation,
                        move |_charge, _cancellation| remove_tree_after_failure(&root),
                    )
                    .await
                    .change_context(ResourceStoreError::JoinBlockingTask)??;
                Ok(())
            }
            .await;
            if let Err(error) = cleanup {
                warn!(error = %error, "failed to clean dropped resource bundle staging tree");
            }
        }));
    }
}

impl StagedResourceArchive {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn root_checksum(&self) -> &str {
        &self.root_checksum
    }

    pub fn archive_bytes(&self) -> u64 {
        self.archive_bytes
    }
}

impl ResourceStore {
    pub fn open(
        root: impl AsRef<Path>,
        executor: Executor,
    ) -> Result<Self, Report<ResourceStoreError>> {
        Self::open_with_limits(root, executor, ResourceStoreLimits::default())
    }

    pub fn open_with_limits(
        root: impl AsRef<Path>,
        executor: Executor,
        limits: ResourceStoreLimits,
    ) -> Result<Self, Report<ResourceStoreError>> {
        if executor.limits().bulk_chunk_bytes.as_u64() == 0 {
            return Err(Report::new(ResourceStoreError::EmptyArchiveChunk));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|_| Report::new(ResourceStoreError::CreateRoot))?;
        Ok(Self {
            root,
            executor,
            limits,
        })
    }

    pub fn limits(&self) -> ResourceStoreLimits {
        self.limits
    }

    pub fn validate_archive_bytes(
        &self,
        archive_bytes: u64,
    ) -> Result<(), Report<ResourceStoreError>> {
        if archive_bytes > self.limits.max_archive_bytes {
            return Err(Report::new(ResourceStoreError::ArchiveQuotaExceeded {
                actual: archive_bytes,
                limit: self.limits.max_archive_bytes,
            }));
        }
        Ok(())
    }

    pub fn validate_extracted_bytes(
        &self,
        extracted_bytes: u64,
    ) -> Result<(), Report<ResourceStoreError>> {
        if extracted_bytes > self.limits.max_extracted_bytes {
            return Err(Report::new(ResourceStoreError::ExtractedQuotaExceeded {
                actual: extracted_bytes,
                limit: self.limits.max_extracted_bytes,
            }));
        }
        Ok(())
    }

    pub fn validate_file_count(&self, file_count: u64) -> Result<(), Report<ResourceStoreError>> {
        if file_count > self.limits.max_file_count {
            return Err(Report::new(ResourceStoreError::FileCountQuotaExceeded {
                actual: file_count,
                limit: self.limits.max_file_count,
            }));
        }
        Ok(())
    }

    /// Admit bytes received outside the executor before a staging worker retains them.
    pub async fn admit_staging_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<ChargedBytes, Report<ResourceStoreError>> {
        let bytes_len = u64::try_from(bytes.len())
            .map_err(|_| Report::new(ResourceStoreError::ArchiveByteCountOverflow))?;
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, bytes_len)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        Ok(ChargedBytes::from_owned(bytes.to_vec(), reservation))
    }

    /// Create a private temporary tree whose writes and cleanup use the filesystem worker pool.
    pub(crate) async fn create_bundle_stager(
        &self,
    ) -> Result<ResourceBundleStager, Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let parent = self.root.clone();
        let root = self
            .executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| -> Result<PathBuf, Report<ResourceStoreError>> {
                    cancellation
                        .check()
                        .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
                    let directory = tempfile::Builder::new()
                        .prefix(".upload-")
                        .suffix(".staging")
                        .tempdir_in(parent)
                        .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
                    Ok(directory.keep())
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)??;
        Ok(ResourceBundleStager {
            root: Some(root),
            current_file: None,
            file_count: 0,
            extracted_bytes: 0,
            limits: self.limits,
            executor: self.executor.clone(),
        })
    }

    /// Create a temporary archive and open it on the filesystem worker pool.
    pub async fn create_archive_stager(
        &self,
    ) -> Result<ResourceArchiveStager, Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let parent = self.root.clone();
        let opened = self
            .executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    if cancellation.is_cancelled() {
                        return Err(Report::new(ResourceStoreError::Cancelled));
                    }
                    let temporary = tempfile::Builder::new()
                        .prefix(".archive-")
                        .suffix(".staging")
                        .tempfile_in(parent)
                        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
                    let (file, path) = temporary.into_parts();
                    Ok(OpenArchiveStager { path, file })
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)??;
        Ok(ResourceArchiveStager {
            path: Some(opened.path),
            file: Some(opened.file),
            hasher: Some(Hasher::new()),
            archive_bytes: 0,
            max_archive_bytes: self.limits.max_archive_bytes,
            executor: self.executor.clone(),
        })
    }

    /// Remove staging paths left by a process that stopped before atomic promotion.
    pub async fn cleanup_abandoned_staging(&self) -> Result<(), Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let root = self.root.clone();
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| cleanup_staging_paths(&root, 2, cancellation),
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)??;
        Ok(())
    }

    /// Walk, read and hash one version's installed contents on the node's bulk workers.
    ///
    /// This is the only entry point for that work. It is charged before it allocates, and it stops
    /// between files when the caller stops waiting rather than reading a whole tree it will
    /// discard.
    #[cfg(test)]
    async fn manifest_entries(
        &self,
        content_root: PathBuf,
    ) -> Result<Vec<ResourceManifestEntry>, Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(
                MemoryClass::Bulk,
                self.executor.limits().bulk_chunk_bytes.as_u64(),
            )
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        self.executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                collect_manifest_entries(&content_root, cancellation)
            })
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)?
    }

    #[cfg(test)]
    pub async fn install_from_directory(
        &self,
        id: ResourceId,
        source_dir: impl AsRef<Path>,
        created_by_node: ClusterNodeName,
        created_at: Timestamp,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        let source_dir = source_dir.as_ref();
        if !source_dir.exists() {
            return Err(Report::new(ResourceStoreError::MissingSource));
        }
        if !source_dir.is_dir() {
            return Err(Report::new(ResourceStoreError::InvalidSource));
        }

        let install = self
            .prepare_install(id, created_by_node, created_at)
            .await?;
        copy_directory_recursive(source_dir, &install.content_root).await?;
        self.finalize_install(install).await
    }

    pub fn manifest_path(&self, id: &ResourceId) -> PathBuf {
        self.version_root(id).join("manifest.json")
    }

    pub fn content_root(&self, id: &ResourceId) -> PathBuf {
        self.version_root(id).join("content")
    }

    pub fn archive_path(&self, id: &ResourceId) -> PathBuf {
        self.version_root(id).join("archive.tar")
    }

    pub async fn remove_version(&self, id: &ResourceId) -> Result<(), Report<ResourceStoreError>> {
        let install_root = self.version_root(id);
        let staging_root = self.staging_root(id);
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    remove_resource_paths(&install_root, &staging_root, cancellation)
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)??;
        Ok(())
    }

    /// Open one immutable installed archive on a filesystem worker. The returned reader retains
    /// that handle and performs each bounded read through the same execution policy.
    pub async fn open_archive(
        &self,
        id: &ResourceId,
    ) -> Result<ResourceArchiveReader, Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let archive_path = self.archive_path(id);
        let opened = self
            .executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| -> Result<OpenArchive, Report<ResourceStoreError>> {
                    if cancellation.is_cancelled() {
                        return Err(Report::new(ResourceStoreError::Cancelled));
                    }
                    let file = fs::File::open(archive_path)
                        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
                    let archive_bytes = file
                        .metadata()
                        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?
                        .len();
                    Ok(OpenArchive {
                        file,
                        archive_bytes,
                    })
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)??;
        Ok(ResourceArchiveReader {
            file: Some(opened.file),
            remaining: opened.archive_bytes,
            archive_bytes: opened.archive_bytes,
            executor: self.executor.clone(),
        })
    }

    pub async fn read_manifest(
        &self,
        id: &ResourceId,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        let reservation = self
            .executor
            .reserve(
                MemoryClass::Bulk,
                self.executor.limits().bulk_chunk_bytes.as_u64(),
            )
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let manifest_path = self.manifest_path(id);
        let chunk_bytes = usize::try_from(self.executor.limits().bulk_chunk_bytes.as_u64())
            .map_err(|_| Report::new(ResourceStoreError::ReadFile))?;
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    if cancellation.is_cancelled() {
                        return Err(Report::new(ResourceStoreError::Cancelled));
                    }
                    let file = fs::File::open(manifest_path)
                        .map_err(|_| Report::new(ResourceStoreError::ReadFile))?;
                    let reader = std::io::BufReader::with_capacity(chunk_bytes, file);
                    serde_json::from_reader(reader)
                        .map_err(|_| Report::new(ResourceStoreError::SerializeManifest))
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)?
    }

    pub async fn install_from_archive_path(
        &self,
        id: ResourceId,
        archive_path: impl AsRef<Path>,
        root_checksum: String,
        created_by_node: ClusterNodeName,
        created_at: Timestamp,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        self.install_archive_path(
            id,
            archive_path.as_ref().to_path_buf(),
            root_checksum,
            created_by_node,
            created_at,
            None,
        )
        .await
    }

    /// Verify every published field before atomically installing a replicated archive.
    pub async fn install_replica_from_archive_path(
        &self,
        resource: ResourceVersion,
        archive_path: impl AsRef<Path>,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        self.install_archive_path(
            resource.id.clone(),
            archive_path.as_ref().to_path_buf(),
            resource.root_checksum.clone(),
            resource.created_by_node.clone(),
            resource.created_at,
            Some(resource),
        )
        .await
    }

    async fn install_archive_path(
        &self,
        id: ResourceId,
        archive_path: PathBuf,
        root_checksum: String,
        created_by_node: ClusterNodeName,
        created_at: Timestamp,
        expected_resource: Option<ResourceVersion>,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        let install_root = self.version_root(&id);
        let staging_root = self.staging_root(&id);
        let content_root = staging_root.join("content");
        let install = PendingInstall {
            id,
            install_root,
            staging_root,
            content_root,
            created_by_node,
            created_at,
        };
        let reservation = self
            .executor
            .reserve(
                MemoryClass::Bulk,
                self.executor.limits().bulk_chunk_bytes.as_u64(),
            )
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let limits = self.limits;
        let chunk_bytes = self.executor.limits().bulk_chunk_bytes.as_u64();
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    install_archive(
                        install,
                        &archive_path,
                        &root_checksum,
                        limits,
                        chunk_bytes,
                        expected_resource.as_ref(),
                        cancellation,
                    )
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)?
    }

    pub fn resolve_content_path(
        &self,
        id: &ResourceId,
        path: &str,
    ) -> Result<PathBuf, Report<ResourceStoreError>> {
        let relative = sanitize_relative_path(path)?;
        Ok(self.content_root(id).join(relative))
    }

    fn resource_root(&self, id: &ResourceId) -> PathBuf {
        self.root
            .join(id.domain.as_str())
            .join(id.identifier.as_str())
    }

    fn version_root(&self, id: &ResourceId) -> PathBuf {
        self.resource_root(id).join(id.version.to_string())
    }

    fn staging_root(&self, id: &ResourceId) -> PathBuf {
        self.resource_root(id)
            .join(format!(".{}.staging", id.version))
    }

    #[cfg(test)]
    async fn prepare_install_paths(
        &self,
        id: &ResourceId,
    ) -> Result<InstallPaths, Report<ResourceStoreError>> {
        let install_root = self.version_root(id);
        if install_root.exists() {
            tokio::fs::remove_dir_all(&install_root)
                .await
                .map_err(|_| Report::new(ResourceStoreError::RenameResourceDir))?;
        }

        let staging_root = self.staging_root(id);
        if staging_root.exists() {
            tokio::fs::remove_dir_all(&staging_root)
                .await
                .map_err(|_| Report::new(ResourceStoreError::RenameResourceDir))?;
        }
        let content_root = staging_root.join("content");
        tokio::fs::create_dir_all(&content_root)
            .await
            .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
        Ok(InstallPaths {
            install_root,
            staging_root,
            content_root,
        })
    }

    #[cfg(test)]
    async fn prepare_install(
        &self,
        id: ResourceId,
        created_by_node: ClusterNodeName,
        created_at: Timestamp,
    ) -> Result<PendingInstall, Report<ResourceStoreError>> {
        let paths = self.prepare_install_paths(&id).await?;
        Ok(PendingInstall {
            id,
            install_root: paths.install_root,
            staging_root: paths.staging_root,
            content_root: paths.content_root,
            created_by_node,
            created_at,
        })
    }

    #[cfg(test)]
    async fn finalize_install(
        &self,
        install: PendingInstall,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        let entries = self.manifest_entries(install.content_root.clone()).await?;
        let root_checksum = write_archive_file(
            &install.content_root,
            &entries,
            &install.staging_root.join("archive.tar"),
        )
        .await?;
        self.finalize_install_with_root_checksum(install, root_checksum, entries)
            .await
    }

    #[cfg(test)]
    async fn finalize_install_with_root_checksum(
        &self,
        install: PendingInstall,
        root_checksum: String,
        entries: Vec<ResourceManifestEntry>,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        let total_bytes = entries.iter().map(|entry| entry.content.size()).sum();
        let file_count = entries
            .iter()
            .filter(|entry| entry.content.is_file())
            .count()
            .arch_into();
        let archive_bytes = tokio::fs::metadata(install.staging_root.join("archive.tar"))
            .await
            .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?
            .len();
        let manifest_checksum = manifest_checksum(&entries)?;
        let resource = ResourceVersion {
            id: install.id.clone(),
            root_checksum,
            manifest_checksum,
            file_count,
            total_bytes,
            archive_bytes,
            created_at: install.created_at,
            created_by_node: install.created_by_node,
        };
        let manifest = ResourceManifest { resource, entries };
        let manifest_path = install.staging_root.join("manifest.json");
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|_| Report::new(ResourceStoreError::SerializeManifest))?;
        tokio::fs::write(&manifest_path, manifest_bytes)
            .await
            .map_err(|_| Report::new(ResourceStoreError::WriteManifest))?;

        if let Some(parent) = install.install_root.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
        }
        tokio::fs::rename(&install.staging_root, &install.install_root)
            .await
            .map_err(|_| Report::new(ResourceStoreError::RenameResourceDir))?;
        Ok(manifest)
    }
}

#[cfg(test)]
async fn write_archive_file(
    root: &Path,
    entries: &[ResourceManifestEntry],
    destination: &Path,
) -> Result<String, Report<ResourceStoreError>> {
    let file = tokio::fs::File::create(destination)
        .await
        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
    let mut archive = AsyncTarBuilder::new(file);
    archive.mode(HeaderMode::Deterministic);

    for entry in entries {
        tokio::task::consume_budget().await;
        let mut header = Header::new_ustar();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);

        let entry_path = Path::new(&entry.path);
        match entry.content {
            ResourceEntryContent::Directory => {
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(EntryType::Directory);
                header.set_cksum();
                archive
                    .append_data(&mut header, entry_path, tokio::io::empty())
                    .await
                    .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
            }
            ResourceEntryContent::File { .. } => {
                let path = root.join(entry_path);
                let size = tokio::fs::metadata(&path)
                    .await
                    .map_err(|_| Report::new(ResourceStoreError::ReadFile))?
                    .len();
                header.set_size(size);
                header.set_mode(0o644);
                header.set_entry_type(EntryType::Regular);
                header.set_cksum();
                let file = tokio::fs::File::open(&path)
                    .await
                    .map_err(|_| Report::new(ResourceStoreError::ReadFile))?;
                archive
                    .append_data(&mut header, entry_path, file)
                    .await
                    .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
            }
        }
    }

    let _file = archive
        .into_inner()
        .await
        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
    checksum_path(destination).await
}

fn install_archive(
    install: PendingInstall,
    source_archive_path: &Path,
    declared_root_checksum: &str,
    limits: ResourceStoreLimits,
    chunk_bytes: u64,
    expected_resource: Option<&ResourceVersion>,
    cancellation: &Cancellation,
) -> Result<ResourceManifest, Report<ResourceStoreError>> {
    let result = (|| {
        let declared_archive_bytes = fs::metadata(source_archive_path)
            .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?
            .len();
        if install.staging_root.exists() {
            remove_tree(&install.staging_root, cancellation)?;
        }
        if declared_archive_bytes > limits.max_archive_bytes {
            return Err(Report::new(ResourceStoreError::ArchiveQuotaExceeded {
                actual: declared_archive_bytes,
                limit: limits.max_archive_bytes,
            }));
        }
        fs::create_dir_all(&install.content_root)
            .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;

        install_archive_into_staging(
            &install,
            ArchiveInstallPlan {
                source_archive_path,
                declared_archive_bytes,
                declared_root_checksum,
                limits,
                chunk_bytes,
                expected_resource,
            },
            cancellation,
        )
    })();
    match result {
        Ok(manifest) => Ok(manifest),
        Err(error) => {
            if install.staging_root.exists()
                && let Err(cleanup_error) = remove_tree_after_failure(&install.staging_root)
            {
                return Err(cleanup_error.attach_printable(error.to_string()));
            }
            Err(error)
        }
    }
}

fn install_archive_into_staging(
    install: &PendingInstall,
    plan: ArchiveInstallPlan<'_>,
    cancellation: &Cancellation,
) -> Result<ResourceManifest, Report<ResourceStoreError>> {
    let buffer_bytes = usize::try_from(plan.chunk_bytes)
        .map_err(|_| Report::new(ResourceStoreError::BulkAdmission))?;
    let mut buffer = vec![0_u8; buffer_bytes];
    let staged_archive_path = install.staging_root.join("archive.tar");
    let (root_checksum, archive_bytes) = copy_archive_to_staging(
        plan.source_archive_path,
        &staged_archive_path,
        plan.limits.max_archive_bytes,
        &mut buffer,
        cancellation,
    )?;
    if archive_bytes != plan.declared_archive_bytes {
        return Err(Report::new(ResourceStoreError::ArchiveSizeChanged));
    }
    if root_checksum != plan.declared_root_checksum {
        return Err(Report::new(ResourceStoreError::ArchiveChecksumMismatch));
    }

    let (entries, total_bytes, file_count) = extract_archive(
        &staged_archive_path,
        &install.content_root,
        plan.limits,
        &mut buffer,
        cancellation,
    )?;
    let manifest_checksum = manifest_checksum(&entries)?;
    let resource = ResourceVersion {
        id: install.id.clone(),
        root_checksum,
        manifest_checksum,
        file_count,
        total_bytes,
        archive_bytes,
        created_at: install.created_at,
        created_by_node: install.created_by_node.clone(),
    };
    if let Some(expected_resource) = plan.expected_resource
        && &resource != expected_resource
    {
        return Err(Report::new(ResourceStoreError::ResourceMetadataMismatch));
    }
    let manifest = ResourceManifest { resource, entries };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|_| Report::new(ResourceStoreError::SerializeManifest))?;
    let mut manifest_file = fs::File::create(install.staging_root.join("manifest.json"))
        .map_err(|_| Report::new(ResourceStoreError::WriteManifest))?;
    manifest_file
        .write_all(&manifest_bytes)
        .map_err(|_| Report::new(ResourceStoreError::WriteManifest))?;
    manifest_file
        .sync_all()
        .map_err(|_| Report::new(ResourceStoreError::WriteManifest))?;
    sync_directories_recursive(&install.staging_root, cancellation)?;

    cancellation
        .check()
        .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
    if install.install_root.exists() {
        remove_tree(&install.install_root, cancellation)?;
    }
    fs::rename(&install.staging_root, &install.install_root)
        .map_err(|_| Report::new(ResourceStoreError::RenameResourceDir))?;
    let parent = install
        .install_root
        .parent()
        .assured("a version directory always has a resource parent");
    sync_directory(parent)?;
    Ok(manifest)
}

impl ArchiveStagingWriter<'_> {
    fn report_failure(&self) -> Report<ResourceStoreError> {
        match self.failure {
            Some(ArchiveWriteFailure::Cancelled) => Report::new(ResourceStoreError::Cancelled),
            Some(ArchiveWriteFailure::QuotaExceeded { actual }) => {
                Report::new(ResourceStoreError::ArchiveQuotaExceeded {
                    actual,
                    limit: self.limit,
                })
            }
            None => Report::new(ResourceStoreError::WriteArchive),
        }
    }

    fn finish(
        self,
        path: tempfile::TempPath,
    ) -> Result<StagedResourceArchive, Report<ResourceStoreError>> {
        let Self {
            file,
            hasher,
            written,
            failure: _,
            limit: _,
            cancellation: _,
        } = self;
        file.sync_all()
            .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
        Ok(StagedResourceArchive {
            path,
            root_checksum: encode_hex(hasher.finalize().as_bytes()),
            archive_bytes: written,
        })
    }
}

impl std::io::Write for ArchiveStagingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            self.failure = Some(ArchiveWriteFailure::Cancelled);
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "resource archive construction was cancelled",
            ));
        }
        let bytes_len = u64::try_from(bytes.len())
            .assured("supported targets have a pointer width no larger than u64");
        let Some(next) = self.written.checked_add(bytes_len) else {
            self.failure = Some(ArchiveWriteFailure::QuotaExceeded { actual: u64::MAX });
            return Err(io::Error::other("resource archive byte count overflowed"));
        };
        if next > self.limit {
            self.failure = Some(ArchiveWriteFailure::QuotaExceeded { actual: next });
            return Err(io::Error::other("resource archive staging quota exceeded"));
        }
        let written = self.file.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.written = self
            .written
            .checked_add(
                u64::try_from(written)
                    .assured("supported targets have a pointer width no larger than u64"),
            )
            .verified("the archive write was checked against the remaining quota");
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.cancellation.is_cancelled() {
            self.failure = Some(ArchiveWriteFailure::Cancelled);
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "resource archive construction was cancelled",
            ));
        }
        self.file.flush()
    }
}

fn build_bundle_archive(
    root: &Path,
    limits: ResourceStoreLimits,
    cancellation: &Cancellation,
) -> Result<StagedResourceArchive, Report<ResourceStoreError>> {
    let entries = collect_staged_bundle_entries(root, limits, cancellation)?;
    let archive_parent = root
        .parent()
        .assured("a bundle staging tree is created inside the resource-store root");
    let temporary = tempfile::Builder::new()
        .prefix(".archive-")
        .suffix(".staging")
        .tempfile_in(archive_parent)
        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
    let (file, path) = temporary.into_parts();
    let writer = ArchiveStagingWriter {
        file,
        hasher: Hasher::new(),
        written: 0,
        limit: limits.max_archive_bytes,
        failure: None,
        cancellation,
    };
    let mut archive = TarBuilder::new(writer);
    archive.mode(TarHeaderMode::Deterministic);
    archive.follow_symlinks(false);

    for entry in entries {
        cancellation
            .check()
            .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
        let mut header = TarHeader::new_ustar();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        let result = match entry {
            StagedBundleEntry::Directory { relative } => {
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(TarEntryType::Directory);
                header.set_cksum();
                archive.append_data(&mut header, relative, io::empty())
            }
            StagedBundleEntry::File {
                full_path,
                relative,
                size,
            } => {
                header.set_size(size);
                header.set_mode(0o644);
                header.set_entry_type(TarEntryType::Regular);
                header.set_cksum();
                let file = fs::File::open(full_path)
                    .map_err(|_| Report::new(ResourceStoreError::ReadFile))?;
                archive.append_data(&mut header, relative, file)
            }
        };
        if result.is_err() {
            return Err(archive.get_ref().report_failure());
        }
    }

    if archive.finish().is_err() {
        return Err(archive.get_ref().report_failure());
    }
    let writer = archive
        .into_inner()
        .verified("the tar builder was already finished successfully");
    writer.finish(path)
}

fn collect_staged_bundle_entries(
    root: &Path,
    limits: ResourceStoreLimits,
    cancellation: &Cancellation,
) -> Result<Vec<StagedBundleEntry>, Report<ResourceStoreError>> {
    let mut inventory = StagedBundleInventory {
        entries: Vec::new(),
        file_count: 0,
        total_bytes: 0,
    };
    collect_staged_bundle_entries_recursive(root, root, limits, cancellation, &mut inventory)?;
    if inventory.file_count == 0 {
        return Err(Report::new(ResourceStoreError::EmptyBundle));
    }
    inventory
        .entries
        .sort_by(|left, right| left.relative().cmp(right.relative()));
    Ok(inventory.entries)
}

fn collect_staged_bundle_entries_recursive(
    root: &Path,
    current: &Path,
    limits: ResourceStoreLimits,
    cancellation: &Cancellation,
    inventory: &mut StagedBundleInventory,
) -> Result<(), Report<ResourceStoreError>> {
    for entry in
        fs::read_dir(current).map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?
    {
        cancellation
            .check()
            .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
        let entry = entry.map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|_| Report::new(ResourceStoreError::InvalidResourcePath))?
            .to_path_buf();
        let file_type = entry
            .file_type()
            .map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
        if file_type.is_dir() {
            inventory
                .entries
                .push(StagedBundleEntry::Directory { relative });
            collect_staged_bundle_entries_recursive(root, &path, limits, cancellation, inventory)?;
            continue;
        }
        if !file_type.is_file() {
            return Err(Report::new(ResourceStoreError::UnsupportedArchiveEntry));
        }
        let size = entry
            .metadata()
            .map_err(|_| Report::new(ResourceStoreError::ReadFile))?
            .len();
        let next_file_count = inventory.file_count.checked_add(1).ok_or_else(|| {
            Report::new(ResourceStoreError::FileCountQuotaExceeded {
                actual: u64::MAX,
                limit: limits.max_file_count,
            })
        })?;
        if next_file_count > limits.max_file_count {
            return Err(Report::new(ResourceStoreError::FileCountQuotaExceeded {
                actual: next_file_count,
                limit: limits.max_file_count,
            }));
        }
        let next_total_bytes = inventory.total_bytes.checked_add(size).ok_or_else(|| {
            Report::new(ResourceStoreError::ExtractedQuotaExceeded {
                actual: u64::MAX,
                limit: limits.max_extracted_bytes,
            })
        })?;
        if next_total_bytes > limits.max_extracted_bytes {
            return Err(Report::new(ResourceStoreError::ExtractedQuotaExceeded {
                actual: next_total_bytes,
                limit: limits.max_extracted_bytes,
            }));
        }
        inventory.entries.push(StagedBundleEntry::File {
            full_path: path,
            relative,
            size,
        });
        inventory.file_count = next_file_count;
        inventory.total_bytes = next_total_bytes;
    }
    Ok(())
}

fn copy_archive_to_staging(
    source: &Path,
    destination: &Path,
    max_archive_bytes: u64,
    buffer: &mut [u8],
    cancellation: &Cancellation,
) -> Result<(String, u64), Report<ResourceStoreError>> {
    let mut source =
        fs::File::open(source).map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
    let mut destination =
        fs::File::create(destination).map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
    let mut hasher = Hasher::new();
    let mut total = 0_u64;
    loop {
        cancellation
            .check()
            .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
        let read = source
            .read(buffer)
            .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
        if read == 0 {
            break;
        }
        let read_bytes = u64::try_from(read)
            .assured("supported targets have a pointer width no larger than u64");
        let next_total = total.checked_add(read_bytes).ok_or_else(|| {
            Report::new(ResourceStoreError::ArchiveQuotaExceeded {
                actual: u64::MAX,
                limit: max_archive_bytes,
            })
        })?;
        if next_total > max_archive_bytes {
            return Err(Report::new(ResourceStoreError::ArchiveQuotaExceeded {
                actual: next_total,
                limit: max_archive_bytes,
            }));
        }
        destination
            .write_all(&buffer[..read])
            .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
        hasher.update(&buffer[..read]);
        total = next_total;
    }
    destination
        .sync_all()
        .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
    Ok((encode_hex(hasher.finalize().as_bytes()), total))
}

fn extract_archive(
    archive_path: &Path,
    content_root: &Path,
    limits: ResourceStoreLimits,
    buffer: &mut [u8],
    cancellation: &Cancellation,
) -> Result<(Vec<ResourceManifestEntry>, u64, u64), Report<ResourceStoreError>> {
    let file =
        fs::File::open(archive_path).map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
    let mut archive = TarArchive::new(file);
    let archive_entries = archive
        .entries()
        .map_err(|_| Report::new(ResourceStoreError::InvalidArchive))?;
    let mut contents = BTreeMap::new();
    let mut extracted_bytes = 0_u64;
    let mut file_count = 0_u64;

    for entry in archive_entries {
        cancellation
            .check()
            .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
        let mut entry = entry.map_err(|_| Report::new(ResourceStoreError::InvalidArchive))?;
        let relative = sanitize_archive_path(
            &entry
                .path()
                .map_err(|_| Report::new(ResourceStoreError::InvalidArchive))?,
        )?;
        let manifest_path = relative
            .to_str()
            .ok_or_else(|| Report::new(ResourceStoreError::InvalidArchivePath))?
            .replace('\\', "/");
        if contents.contains_key(&manifest_path) {
            return Err(Report::new(ResourceStoreError::DuplicateArchivePath));
        }
        let output_path = content_root.join(&relative);
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            if entry.size() != 0 {
                return Err(Report::new(ResourceStoreError::InvalidArchive));
            }
            fs::create_dir_all(&output_path)
                .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
            contents.insert(manifest_path, ResourceEntryContent::Directory);
            continue;
        }
        if !entry_type.is_file() {
            return Err(Report::new(ResourceStoreError::UnsupportedArchiveEntry));
        }

        let size = entry.size();
        let next_file_count = file_count.checked_add(1).ok_or_else(|| {
            Report::new(ResourceStoreError::FileCountQuotaExceeded {
                actual: u64::MAX,
                limit: limits.max_file_count,
            })
        })?;
        if next_file_count > limits.max_file_count {
            return Err(Report::new(ResourceStoreError::FileCountQuotaExceeded {
                actual: next_file_count,
                limit: limits.max_file_count,
            }));
        }
        let next_extracted_bytes = extracted_bytes.checked_add(size).ok_or_else(|| {
            Report::new(ResourceStoreError::ExtractedQuotaExceeded {
                actual: u64::MAX,
                limit: limits.max_extracted_bytes,
            })
        })?;
        if next_extracted_bytes > limits.max_extracted_bytes {
            return Err(Report::new(ResourceStoreError::ExtractedQuotaExceeded {
                actual: next_extracted_bytes,
                limit: limits.max_extracted_bytes,
            }));
        }

        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
        }
        let mut output = fs::File::create(&output_path)
            .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
        let mut hasher = Hasher::new();
        let mut remaining = size;
        while remaining != 0 {
            cancellation
                .check()
                .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
            let wanted = remaining.min(
                u64::try_from(buffer.len())
                    .assured("supported targets have a pointer width no larger than u64"),
            );
            let wanted = usize::try_from(wanted)
                .verified("the requested read is bounded by the buffer length");
            let read = entry
                .read(&mut buffer[..wanted])
                .map_err(|_| Report::new(ResourceStoreError::InvalidArchive))?;
            if read == 0 {
                return Err(Report::new(ResourceStoreError::ArchiveTruncated));
            }
            output
                .write_all(&buffer[..read])
                .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
            hasher.update(&buffer[..read]);
            remaining = remaining
                .checked_sub(
                    u64::try_from(read)
                        .assured("supported targets have a pointer width no larger than u64"),
                )
                .verified("the tar entry reader cannot return more bytes than requested");
        }
        output
            .sync_all()
            .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
        contents.insert(
            manifest_path,
            ResourceEntryContent::File {
                size,
                checksum: encode_hex(hasher.finalize().as_bytes()),
            },
        );
        file_count = next_file_count;
        extracted_bytes = next_extracted_bytes;
    }

    let entries = contents
        .into_iter()
        .map(|(path, content)| ResourceManifestEntry { path, content })
        .collect();
    Ok((entries, extracted_bytes, file_count))
}

fn sanitize_archive_path(path: &Path) -> Result<PathBuf, Report<ResourceStoreError>> {
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(Report::new(ResourceStoreError::InvalidArchivePath));
            }
        }
    }
    if clean.as_os_str().is_empty() {
        return Err(Report::new(ResourceStoreError::InvalidArchivePath));
    }
    Ok(clean)
}

fn sync_directories_recursive(
    root: &Path,
    cancellation: &Cancellation,
) -> Result<(), Report<ResourceStoreError>> {
    for entry in fs::read_dir(root).map_err(|_| Report::new(ResourceStoreError::ReadDirectory))? {
        cancellation
            .check()
            .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
        let entry = entry.map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
        if entry
            .file_type()
            .map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?
            .is_dir()
        {
            sync_directories_recursive(&entry.path(), cancellation)?;
        }
    }
    sync_directory(root)
}

fn sync_directory(path: &Path) -> Result<(), Report<ResourceStoreError>> {
    let directory =
        fs::File::open(path).map_err(|_| Report::new(ResourceStoreError::WriteManifest))?;
    directory
        .sync_all()
        .map_err(|_| Report::new(ResourceStoreError::WriteManifest))
}

fn cleanup_staging_paths(
    root: &Path,
    remaining_namespace_levels: u8,
    cancellation: &Cancellation,
) -> Result<(), Report<ResourceStoreError>> {
    for entry in fs::read_dir(root).map_err(|_| Report::new(ResourceStoreError::ReadDirectory))? {
        cancellation
            .check()
            .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
        let entry = entry.map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
        let file_type = entry
            .file_type()
            .map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
        let path = entry.path();
        let is_staging = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with('.') && name.ends_with(".staging"));
        if is_staging {
            remove_tree(&path, cancellation)?;
        } else if file_type.is_dir() && remaining_namespace_levels != 0 {
            let next_level = remaining_namespace_levels
                .checked_sub(1)
                .verified("the zero namespace-depth branch was handled above");
            cleanup_staging_paths(&path, next_level, cancellation)?;
        }
    }
    Ok(())
}

fn remove_resource_paths(
    install_root: &Path,
    staging_root: &Path,
    cancellation: &Cancellation,
) -> Result<(), Report<ResourceStoreError>> {
    if install_root.exists() {
        remove_tree(install_root, cancellation)?;
    }
    if staging_root.exists() {
        remove_tree(staging_root, cancellation)?;
    }
    Ok(())
}

fn remove_tree(root: &Path, cancellation: &Cancellation) -> Result<(), Report<ResourceStoreError>> {
    remove_tree_inner(root, Some(cancellation))
}

fn remove_tree_after_failure(root: &Path) -> Result<(), Report<ResourceStoreError>> {
    remove_tree_inner(root, None)
}

fn remove_tree_inner(
    root: &Path,
    cancellation: Option<&Cancellation>,
) -> Result<(), Report<ResourceStoreError>> {
    let metadata = fs::symlink_metadata(root)
        .map_err(|_| Report::new(ResourceStoreError::DeleteResourceDir))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return fs::remove_file(root)
            .map_err(|_| Report::new(ResourceStoreError::DeleteResourceDir));
    }
    for entry in
        fs::read_dir(root).map_err(|_| Report::new(ResourceStoreError::DeleteResourceDir))?
    {
        if let Some(cancellation) = cancellation {
            cancellation
                .check()
                .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
        }
        let entry = entry.map_err(|_| Report::new(ResourceStoreError::DeleteResourceDir))?;
        remove_tree_inner(&entry.path(), cancellation)?;
    }
    fs::remove_dir(root).map_err(|_| Report::new(ResourceStoreError::DeleteResourceDir))
}

fn sanitize_relative_path(path: &str) -> Result<PathBuf, Report<ResourceStoreError>> {
    let candidate = Path::new(path);
    let mut clean = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(Report::new(ResourceStoreError::InvalidResourcePath));
            }
        }
    }
    Ok(clean)
}

#[cfg(test)]
async fn copy_directory_recursive(
    source: &Path,
    destination: &Path,
) -> Result<(), Report<ResourceStoreError>> {
    tokio::fs::create_dir_all(destination)
        .await
        .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
    let mut entries = tokio::fs::read_dir(source)
        .await
        .map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?
    {
        tokio::task::consume_budget().await;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .await
            .map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
        if file_type.is_dir() {
            Box::pin(copy_directory_recursive(&source_path, &destination_path)).await?;
        } else if file_type.is_file() {
            if let Some(parent) = destination_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|_| Report::new(ResourceStoreError::CreateResourceDir))?;
            }
            tokio::fs::copy(&source_path, &destination_path)
                .await
                .map_err(|_| Report::new(ResourceStoreError::ReadFile))?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn collect_manifest_entries(
    root: &Path,
    cancellation: &Cancellation,
) -> Result<Vec<ResourceManifestEntry>, Report<ResourceStoreError>> {
    let mut entries = Vec::new();
    collect_manifest_entries_recursive(root, root, &mut entries, cancellation)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

#[cfg(test)]
fn collect_manifest_entries_recursive(
    root: &Path,
    current: &Path,
    entries: &mut Vec<ResourceManifestEntry>,
    cancellation: &Cancellation,
) -> Result<(), Report<ResourceStoreError>> {
    for entry in
        fs::read_dir(current).map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?
    {
        // One directory entry is the bounded unit this work is cancelled between.
        cancellation
            .check()
            .map_err(|_| Report::new(ResourceStoreError::Cancelled))?;
        let entry = entry.map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|_| Report::new(ResourceStoreError::ReadDirectory))?;
        let relative = path
            .strip_prefix(root)
            .verified("the walk only yields entries below the root it started from")
            .to_string_lossy()
            .replace('\\', "/");
        if file_type.is_dir() {
            entries.push(ResourceManifestEntry {
                path: relative.clone(),
                content: ResourceEntryContent::Directory,
            });
            collect_manifest_entries_recursive(root, &path, entries, cancellation)?;
        } else if file_type.is_file() {
            let size = entry
                .metadata()
                .map_err(|_| Report::new(ResourceStoreError::ReadFile))?
                .len();
            entries.push(ResourceManifestEntry {
                path: relative,
                content: ResourceEntryContent::File {
                    size,
                    checksum: checksum_file(&path)?,
                },
            });
        }
    }
    Ok(())
}

#[cfg(test)]
fn checksum_file(path: &Path) -> Result<String, Report<ResourceStoreError>> {
    let mut file = fs::File::open(path).map_err(|_| Report::new(ResourceStoreError::ReadFile))?;
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| Report::new(ResourceStoreError::ReadFile))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let hash = hasher.finalize();
    Ok(encode_hex(hash.as_bytes()))
}

#[cfg(test)]
async fn checksum_path(path: &Path) -> Result<String, Report<ResourceStoreError>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 8192];
    loop {
        tokio::task::consume_budget().await;
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let hash = hasher.finalize();
    Ok(encode_hex(hash.as_bytes()))
}

fn manifest_checksum(
    entries: &[ResourceManifestEntry],
) -> Result<String, Report<ResourceStoreError>> {
    let bytes = serde_json::to_vec(entries)
        .map_err(|_| Report::new(ResourceStoreError::SerializeManifest))?;
    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    let hash = hasher.finalize();
    Ok(encode_hex(hash.as_bytes()))
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").assured("writing a byte into a String cannot fail");
    }
    out
}

#[cfg(test)]
mod tests {
    use std::{io::Write as _, path::Path, time::Duration};

    use nervix_execution::{ExecutionConfig, Executor};
    use nervix_models::{ClusterNodeName, DomainName, ResourceId, ResourceName, Timestamp};
    use tempfile::{NamedTempFile, tempdir};
    use ubyte::ByteUnit;

    use super::{
        ResourceEntryContent, ResourceStore, ResourceStoreError, ResourceStoreLimits, encode_hex,
    };

    fn resource_id(domain: &str, identifier: &str, version: u64) -> ResourceId {
        ResourceId::new(
            DomainName::parse(domain).expect("valid domain"),
            ResourceName::parse(identifier).expect("valid identifier"),
            version,
        )
    }

    async fn read_archive(store: &ResourceStore, id: &ResourceId) -> Vec<u8> {
        let mut archive = Vec::new();
        let mut reader = store.open_archive(id).await.expect("archive should open");
        while let Some(chunk) = reader
            .next_chunk()
            .await
            .expect("archive chunk should be readable")
        {
            archive.extend_from_slice(&chunk);
        }
        archive
    }

    #[tokio::test]
    async fn archive_stager_writes_bounded_chunks_and_reports_their_digest() {
        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");
        let mut stager = store
            .create_archive_stager()
            .await
            .expect("archive stager should open");
        for bytes in [b"first".as_slice(), b"-second".as_slice()] {
            let chunk = store
                .admit_staging_bytes(bytes)
                .await
                .expect("archive chunk should be admitted");
            stager
                .write_chunk(chunk)
                .await
                .expect("archive chunk should be staged");
        }

        let staged = stager.finish().await.expect("archive should finish");

        assert!(staged.path().starts_with(install_root.path()));
        assert_eq!(staged.archive_bytes(), 12);
        assert_eq!(
            staged.root_checksum(),
            encode_hex(blake3::hash(b"first-second").as_bytes())
        );
        assert_eq!(
            std::fs::read(staged.path()).expect("staged archive should be readable"),
            b"first-second"
        );
    }

    #[tokio::test]
    async fn bundle_stager_builds_an_installable_archive_and_removes_its_source_tree() {
        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");
        let mut stager = store
            .create_bundle_stager()
            .await
            .expect("bundle stager should open");
        let staging_root = stager
            .root
            .as_ref()
            .expect("unfinished stager should own its tree")
            .clone();
        stager
            .create_file(Path::new("nested/model.bin"))
            .await
            .expect("bundle file should be created");
        for bytes in [b"model".as_slice(), b"-bytes".as_slice()] {
            let chunk = store
                .admit_staging_bytes(bytes)
                .await
                .expect("bundle bytes should be admitted");
            stager
                .write_chunk(chunk)
                .await
                .expect("bundle bytes should be written");
        }

        let staged = stager.finish().await.expect("bundle archive should finish");

        assert!(!staging_root.exists());
        assert!(staged.path().starts_with(install_root.path()));
        assert_eq!(
            std::fs::metadata(staged.path())
                .expect("staged archive should exist")
                .len(),
            staged.archive_bytes()
        );
        let id = resource_id("tenant", "model", 1);
        let manifest = store
            .install_from_archive_path(
                id.clone(),
                staged.path(),
                staged.root_checksum().to_string(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("bundle archive should install");
        assert_eq!(manifest.resource.file_count, 1);
        assert_eq!(
            std::fs::read(store.content_root(&id).join("nested/model.bin"))
                .expect("installed bundle file should be readable"),
            b"model-bytes"
        );
    }

    #[tokio::test]
    async fn bundle_stager_checks_extracted_quota_before_writing_a_chunk() {
        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open_with_limits(
            install_root.path(),
            Executor::default(),
            ResourceStoreLimits {
                max_archive_bytes: 1024 * 1024,
                max_extracted_bytes: 3,
                max_file_count: 1,
            },
        )
        .expect("store should open");
        let mut stager = store
            .create_bundle_stager()
            .await
            .expect("bundle stager should open");
        let staging_root = stager
            .root
            .as_ref()
            .expect("unfinished stager should own its tree")
            .clone();
        stager
            .create_file(Path::new("model.bin"))
            .await
            .expect("bundle file should be created");
        let chunk = store
            .admit_staging_bytes(b"four")
            .await
            .expect("bundle bytes should be admitted");

        let error = stager
            .write_chunk(chunk)
            .await
            .expect_err("the extracted quota must reject the chunk");

        assert!(matches!(
            error.current_context(),
            ResourceStoreError::ExtractedQuotaExceeded {
                actual: 4,
                limit: 3
            }
        ));
        assert_eq!(
            std::fs::metadata(staging_root.join("model.bin"))
                .expect("rejected destination should remain present")
                .len(),
            0,
            "a rejected chunk must not be partially written"
        );
        stager.abort().await.expect("staging should be removed");
        assert!(!staging_root.exists());
    }

    #[tokio::test]
    async fn dropping_bundle_stager_removes_its_source_tree() {
        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");
        let stager = store
            .create_bundle_stager()
            .await
            .expect("bundle stager should open");
        let staging_root = stager
            .root
            .as_ref()
            .expect("unfinished stager should own its tree")
            .clone();

        drop(stager);

        tokio::time::timeout(Duration::from_secs(5), async {
            while staging_root.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping a bundle stager should schedule source-tree cleanup");
    }

    #[tokio::test]
    async fn install_from_directory_writes_manifest_and_preserves_tree() {
        let source = tempdir().expect("source tempdir");
        std::fs::create_dir_all(source.path().join("proto"))
            .expect("source directory should be created");
        std::fs::write(source.path().join("model.onnx"), b"onnx")
            .expect("model file should be written");
        std::fs::write(
            source.path().join("proto/schema.proto"),
            b"syntax = \"proto3\";",
        )
        .expect("proto file should be written");

        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");
        let manifest = store
            .install_from_directory(
                resource_id("tenant", "fraud_model", 1),
                source.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("resource should install");

        assert_eq!(manifest.resource.id.version, 1);
        assert_eq!(manifest.resource.file_count, 2);
        assert!(store.manifest_path(&manifest.resource.id).exists());
        assert!(
            store
                .content_root(&manifest.resource.id)
                .join("proto/schema.proto")
                .exists()
        );
        assert!(
            !store
                .content_root(&resource_id("other", "fraud_model", 1))
                .exists(),
            "the same name in another domain must have its own content root"
        );
        assert!(manifest.entries.iter().any(|entry| {
            entry.path == "proto" && entry.content == ResourceEntryContent::Directory
        }));
        assert!(
            manifest
                .entries
                .iter()
                .any(|entry| { entry.path == "model.onnx" && entry.content.is_file() })
        );
    }

    #[tokio::test]
    async fn install_from_archive_path_rehydrates_same_resource_content() {
        let source = tempdir().expect("source tempdir");
        std::fs::create_dir_all(source.path().join("proto/nested"))
            .expect("nested source directory should be created");
        std::fs::write(source.path().join("model.onnx"), b"onnx")
            .expect("model file should be written");
        std::fs::write(
            source.path().join("proto/nested/schema.proto"),
            b"syntax = \"proto3\";",
        )
        .expect("proto file should be written");

        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");
        let source_id = resource_id("tenant", "fraud_model", 1);
        let source_manifest = store
            .install_from_directory(
                source_id.clone(),
                source.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("resource should install");
        let archive_bytes = read_archive(&store, &source_id).await;

        let temp_archive = NamedTempFile::new().expect("temp archive should be created");
        std::fs::write(temp_archive.path(), &archive_bytes)
            .expect("temp archive should be written");

        let replica_id = resource_id("other", "fraud_model_replica", 7);
        let replica_manifest = store
            .install_from_archive_path(
                replica_id.clone(),
                temp_archive.path(),
                source_manifest.resource.root_checksum.clone(),
                ClusterNodeName::parse("node-2").expect("valid name"),
                Timestamp::from_unix_nanos(84),
            )
            .await
            .expect("resource should install from archive");

        assert_eq!(
            source_manifest.resource.root_checksum,
            replica_manifest.resource.root_checksum
        );
        assert_eq!(
            source_manifest.resource.manifest_checksum,
            replica_manifest.resource.manifest_checksum
        );
        assert!(
            store
                .content_root(&replica_id)
                .join("proto/nested/schema.proto")
                .exists()
        );
        assert!(
            store.archive_path(&replica_id).exists(),
            "replica archive should be written"
        );
    }

    #[tokio::test]
    async fn install_from_archive_path_preserves_streamed_checksum() {
        let source = tempdir().expect("source tempdir");
        std::fs::create_dir_all(source.path().join("proto"))
            .expect("source directory should be created");
        std::fs::write(source.path().join("model.onnx"), b"onnx")
            .expect("model file should be written");
        std::fs::write(
            source.path().join("proto/schema.proto"),
            b"syntax = \"proto3\";",
        )
        .expect("proto file should be written");

        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");
        let source_id = resource_id("tenant", "fraud_model", 1);
        let source_manifest = store
            .install_from_directory(
                source_id.clone(),
                source.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("resource should install");
        let archive_bytes = read_archive(&store, &source_id).await;

        let temp_archive = NamedTempFile::new().expect("temp archive should be created");
        std::fs::write(temp_archive.path(), &archive_bytes)
            .expect("temp archive should be written");

        let replica_manifest = store
            .install_from_archive_path(
                resource_id("tenant", "fraud_model_streamed", 8),
                temp_archive.path(),
                source_manifest.resource.root_checksum.clone(),
                ClusterNodeName::parse("node-2").expect("valid name"),
                Timestamp::from_unix_nanos(84),
            )
            .await
            .expect("resource should install from archive path");

        assert_eq!(
            replica_manifest.resource.root_checksum,
            source_manifest.resource.root_checksum
        );
        assert_eq!(
            replica_manifest.resource.manifest_checksum,
            source_manifest.resource.manifest_checksum
        );
    }

    #[tokio::test]
    async fn replica_metadata_is_verified_before_atomic_install() {
        let source = tempdir().expect("source tempdir");
        std::fs::write(source.path().join("model.bin"), b"model")
            .expect("source file should be written");
        let source_root = tempdir().expect("source install tempdir");
        let source_store = ResourceStore::open(source_root.path(), Executor::default())
            .expect("source store should open");
        let source_id = resource_id("tenant", "model", 1);
        let source_manifest = source_store
            .install_from_directory(
                source_id.clone(),
                source.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("source resource should install");
        let replica_root = tempdir().expect("replica install tempdir");
        let replica_store = ResourceStore::open(replica_root.path(), Executor::default())
            .expect("replica store should open");
        let replica_id = resource_id("tenant", "model", 2);
        let mut expected = source_manifest.resource;
        expected.id = replica_id.clone();
        expected.created_by_node = ClusterNodeName::parse("node-2").expect("valid name");
        expected.created_at = Timestamp::from_unix_nanos(84);
        expected.manifest_checksum = "different-manifest".to_string();

        let error = replica_store
            .install_replica_from_archive_path(expected, source_store.archive_path(&source_id))
            .await
            .expect_err("mismatched published metadata must fail");

        assert!(matches!(
            error.current_context(),
            ResourceStoreError::ResourceMetadataMismatch
        ));
        assert!(!replica_store.staging_root(&replica_id).exists());
        assert!(!replica_store.version_root(&replica_id).exists());
    }

    #[tokio::test]
    async fn failed_archive_install_removes_staging() {
        let source = tempdir().expect("source tempdir");
        std::fs::write(source.path().join("model.bin"), b"model")
            .expect("source file should be written");
        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");
        let source_id = resource_id("tenant", "model", 1);
        let source_manifest = store
            .install_from_directory(
                source_id.clone(),
                source.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("source resource should install");
        let archive = NamedTempFile::new().expect("temporary archive should open");
        std::fs::copy(store.archive_path(&source_id), archive.path()).expect("archive should copy");
        std::fs::OpenOptions::new()
            .append(true)
            .open(archive.path())
            .expect("archive should open for corruption")
            .write_all(b"corrupt")
            .expect("archive should be corrupted");
        let replica_id = resource_id("tenant", "model", 2);

        let error = store
            .install_from_archive_path(
                replica_id.clone(),
                archive.path(),
                source_manifest.resource.root_checksum,
                ClusterNodeName::parse("node-2").expect("valid name"),
                Timestamp::from_unix_nanos(84),
            )
            .await
            .expect_err("a mismatched archive must fail");
        assert!(matches!(
            error.current_context(),
            ResourceStoreError::ArchiveChecksumMismatch
        ));

        assert!(
            !store.staging_root(&replica_id).exists(),
            "a failed install must remove its partial staging tree"
        );
        assert!(
            !store.version_root(&replica_id).exists(),
            "a failed install must not publish a version directory"
        );
    }

    #[tokio::test]
    async fn archive_quota_is_checked_before_staging_is_created() {
        let archive = NamedTempFile::new().expect("temporary archive should open");
        std::fs::write(archive.path(), b"too large").expect("archive should be written");
        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open_with_limits(
            install_root.path(),
            Executor::default(),
            ResourceStoreLimits {
                max_archive_bytes: 4,
                ..ResourceStoreLimits::default()
            },
        )
        .expect("store should open");
        let id = resource_id("tenant", "model", 1);

        let error = store
            .install_from_archive_path(
                id.clone(),
                archive.path(),
                "checksum".to_string(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect_err("an archive beyond the staging quota must fail");

        assert!(matches!(
            error.current_context(),
            ResourceStoreError::ArchiveQuotaExceeded {
                actual: 9,
                limit: 4
            }
        ));
        assert!(!store.staging_root(&id).exists());
    }

    #[tokio::test]
    async fn extraction_failure_removes_staging() {
        let archive = NamedTempFile::new().expect("temporary archive should open");
        let bytes = b"this is not a tar archive";
        std::fs::write(archive.path(), bytes).expect("archive should be written");
        let root_checksum = encode_hex(blake3::hash(bytes).as_bytes());
        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");
        let id = resource_id("tenant", "model", 1);

        let error = store
            .install_from_archive_path(
                id.clone(),
                archive.path(),
                root_checksum,
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect_err("an invalid tar archive must fail extraction");

        assert!(matches!(
            error.current_context(),
            ResourceStoreError::InvalidArchive
        ));
        assert!(!store.staging_root(&id).exists());
        assert!(!store.version_root(&id).exists());
    }

    #[tokio::test]
    async fn extracted_quota_failure_removes_staging() {
        let source = tempdir().expect("source tempdir");
        std::fs::write(source.path().join("model.bin"), b"model")
            .expect("source file should be written");
        let source_root = tempdir().expect("source install tempdir");
        let source_store = ResourceStore::open(source_root.path(), Executor::default())
            .expect("source store should open");
        let source_id = resource_id("tenant", "model", 1);
        let source_manifest = source_store
            .install_from_directory(
                source_id.clone(),
                source.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("source resource should install");
        let replica_root = tempdir().expect("replica install tempdir");
        let replica_store = ResourceStore::open_with_limits(
            replica_root.path(),
            Executor::default(),
            ResourceStoreLimits {
                max_extracted_bytes: 4,
                ..ResourceStoreLimits::default()
            },
        )
        .expect("replica store should open");
        let replica_id = resource_id("tenant", "model", 2);

        let error = replica_store
            .install_from_archive_path(
                replica_id.clone(),
                source_store.archive_path(&source_id),
                source_manifest.resource.root_checksum,
                ClusterNodeName::parse("node-2").expect("valid name"),
                Timestamp::from_unix_nanos(84),
            )
            .await
            .expect_err("expanded content beyond its quota must fail");

        assert!(matches!(
            error.current_context(),
            ResourceStoreError::ExtractedQuotaExceeded {
                actual: 5,
                limit: 4
            }
        ));
        assert!(!replica_store.staging_root(&replica_id).exists());
        assert!(!replica_store.version_root(&replica_id).exists());
    }

    #[tokio::test]
    async fn file_count_quota_failure_removes_staging() {
        let source = tempdir().expect("source tempdir");
        std::fs::write(source.path().join("model.bin"), b"model")
            .expect("source file should be written");
        let source_root = tempdir().expect("source install tempdir");
        let source_store = ResourceStore::open(source_root.path(), Executor::default())
            .expect("source store should open");
        let source_id = resource_id("tenant", "model", 1);
        let source_manifest = source_store
            .install_from_directory(
                source_id.clone(),
                source.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("source resource should install");
        let replica_root = tempdir().expect("replica install tempdir");
        let replica_store = ResourceStore::open_with_limits(
            replica_root.path(),
            Executor::default(),
            ResourceStoreLimits {
                max_file_count: 0,
                ..ResourceStoreLimits::default()
            },
        )
        .expect("replica store should open");
        let replica_id = resource_id("tenant", "model", 2);

        let error = replica_store
            .install_from_archive_path(
                replica_id.clone(),
                source_store.archive_path(&source_id),
                source_manifest.resource.root_checksum,
                ClusterNodeName::parse("node-2").expect("valid name"),
                Timestamp::from_unix_nanos(84),
            )
            .await
            .expect_err("a file count beyond its quota must fail");

        assert!(matches!(
            error.current_context(),
            ResourceStoreError::FileCountQuotaExceeded {
                actual: 1,
                limit: 0
            }
        ));
        assert!(!replica_store.staging_root(&replica_id).exists());
        assert!(!replica_store.version_root(&replica_id).exists());
    }

    #[tokio::test]
    async fn startup_cleanup_removes_abandoned_staging_trees() {
        let install_root = tempdir().expect("install tempdir");
        let staging = install_root.path().join("tenant/model/.7.staging/content");
        let staged_archive = install_root.path().join(".archive-abandoned.staging");
        let installed_content = install_root
            .path()
            .join("tenant/model/7/content/.user.staging");
        std::fs::create_dir_all(&staging).expect("staging tree should be created");
        std::fs::create_dir_all(
            installed_content
                .parent()
                .expect("installed content fixture should have a parent"),
        )
        .expect("installed content directory should be created");
        std::fs::write(staging.join("partial.bin"), b"partial")
            .expect("staging file should be written");
        std::fs::write(&staged_archive, b"partial archive")
            .expect("staged archive should be written");
        std::fs::write(&installed_content, b"user content")
            .expect("installed content should be written");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");

        store
            .cleanup_abandoned_staging()
            .await
            .expect("startup cleanup should succeed");

        assert!(!install_root.path().join("tenant/model/.7.staging").exists());
        assert!(!staged_archive.exists());
        assert_eq!(
            std::fs::read(installed_content).expect("installed content must survive cleanup"),
            b"user content"
        );
    }

    #[tokio::test]
    async fn cancelled_archive_install_removes_staging() {
        let source = tempdir().expect("source tempdir");
        std::fs::write(source.path().join("model.bin"), vec![0_u8; 64 * 1024])
            .expect("source file should be written");
        let source_root = tempdir().expect("source install tempdir");
        let source_store = ResourceStore::open(source_root.path(), Executor::default())
            .expect("source store should open");
        let source_id = resource_id("tenant", "model", 1);
        let source_manifest = source_store
            .install_from_directory(
                source_id.clone(),
                source.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("source resource should install");
        let mut execution_config = ExecutionConfig::default();
        execution_config.limits.bulk_chunk_bytes = ByteUnit::Byte(1);
        let executor = Executor::new(execution_config).expect("execution config should be valid");
        let replica_root = tempdir().expect("replica install tempdir");
        let replica_store =
            ResourceStore::open(replica_root.path(), executor).expect("replica store should open");
        let replica_id = resource_id("tenant", "model", 2);
        let staging_root = replica_store.staging_root(&replica_id);
        let installing_store = replica_store.clone();
        let installing_id = replica_id.clone();
        let archive_path = source_store.archive_path(&source_id);
        let root_checksum = source_manifest.resource.root_checksum;
        let install = tokio::spawn(async move {
            installing_store
                .install_from_archive_path(
                    installing_id,
                    archive_path,
                    root_checksum,
                    ClusterNodeName::parse("node-2").expect("valid name"),
                    Timestamp::from_unix_nanos(84),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !staging_root.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the install should create its staging tree");

        install.abort();
        match install.await {
            Err(error) if error.is_cancelled() => {}
            result => panic!("the install task should be cancelled, got {result:?}"),
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while staging_root.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled install cleanup should finish");
        assert!(!replica_store.version_root(&replica_id).exists());
    }

    #[test]
    fn resolve_content_path_rejects_parent_segments() {
        let install_root = tempdir().expect("install tempdir");
        let store = ResourceStore::open(install_root.path(), Executor::default())
            .expect("store should open");

        let err = store
            .resolve_content_path(&resource_id("tenant", "fraud_model", 7), "../escape")
            .expect_err("parent segments must be rejected");
        assert!(matches!(
            err.current_context(),
            ResourceStoreError::InvalidResourcePath
        ));
    }
}
