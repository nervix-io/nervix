//! The on-disk store for uploaded resource versions.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Content-addressed installation of a version: its manifest and checksums, the staging
//!   directory it is built in, and the atomic promotion into place.
//! - **Depends on.** The vocabulary's resource identities and the filesystem.
//! - **Must not know.** How a version was uploaded, replicated or referenced. Those are
//!   control-plane use cases; this store installs bytes and reports what is installed.

use std::{
    fs,
    io::{Read, Seek as _, SeekFrom},
    path::{Component, Path, PathBuf},
};

use arch_into::ArchInto as _;
use async_tar::{
    Archive as AsyncTarArchive, Builder as AsyncTarBuilder, EntryType, Header, HeaderMode,
};
use blake3::Hasher;
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass, StorageClass};
use nervix_models::{ClusterNodeName, ResourceId, ResourceVersion, Timestamp};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

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
}

/// One bounded read from a resource archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceArchiveChunk {
    pub bytes: Vec<u8>,
    pub eof: bool,
}

/// Where one resource version is installed: the directory it will finally occupy, the staging
/// directory it is built in, and the content directory inside that staging directory.
#[derive(Debug)]
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
    #[error("resource archive path escapes bundle root")]
    InvalidArchivePath,
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
    #[error("reading the resource version was cancelled")]
    Cancelled,
}

impl ResourceStore {
    pub fn open(
        root: impl AsRef<Path>,
        executor: Executor,
    ) -> Result<Self, Report<ResourceStoreError>> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|_| Report::new(ResourceStoreError::CreateRoot))?;
        Ok(Self { root, executor })
    }

    /// Walk, read and hash one version's installed contents on the node's bulk workers.
    ///
    /// This is the only entry point for that work. It is charged before it allocates, and it stops
    /// between files when the caller stops waiting rather than reading a whole tree it will
    /// discard.
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

    pub fn remove_version(&self, id: &ResourceId) -> Result<(), Report<ResourceStoreError>> {
        let install_root = self.version_root(id);
        if install_root.exists() {
            fs::remove_dir_all(&install_root)
                .map_err(|_| Report::new(ResourceStoreError::DeleteResourceDir))?;
        }
        let staging_root = self.staging_root(id);
        if staging_root.exists() {
            fs::remove_dir_all(&staging_root)
                .map_err(|_| Report::new(ResourceStoreError::DeleteResourceDir))?;
        }
        Ok(())
    }

    /// Read one configured bulk-sized piece of an installed archive on a filesystem worker.
    pub async fn read_archive_chunk(
        &self,
        id: &ResourceId,
        offset: u64,
    ) -> Result<ResourceArchiveChunk, Report<ResourceStoreError>> {
        let chunk_bytes = self.executor.limits().bulk_chunk_bytes.as_u64();
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, chunk_bytes)
            .await
            .change_context(ResourceStoreError::BulkAdmission)?;
        let archive_path = self.archive_path(id);
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    if cancellation.is_cancelled() {
                        return Err(Report::new(ResourceStoreError::Cancelled));
                    }
                    let mut file = fs::File::open(archive_path)
                        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
                    let archive_bytes = file
                        .metadata()
                        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?
                        .len();
                    file.seek(SeekFrom::Start(offset))
                        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
                    let chunk_size = usize::try_from(chunk_bytes)
                        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
                    let mut bytes = vec![0_u8; chunk_size];
                    let read = file
                        .read(&mut bytes)
                        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
                    bytes.truncate(read);
                    let read = u64::try_from(read)
                        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
                    let end = offset
                        .checked_add(read)
                        .ok_or_else(|| Report::new(ResourceStoreError::ReadArchive))?;
                    Ok(ResourceArchiveChunk {
                        bytes,
                        eof: end >= archive_bytes,
                    })
                },
            )
            .await
            .change_context(ResourceStoreError::JoinBlockingTask)?
    }

    pub fn read_manifest(
        &self,
        id: &ResourceId,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        let bytes = fs::read(self.manifest_path(id))
            .map_err(|_| Report::new(ResourceStoreError::ReadFile))?;
        serde_json::from_slice(&bytes)
            .map_err(|_| Report::new(ResourceStoreError::SerializeManifest))
    }

    pub async fn install_from_archive_path(
        &self,
        id: ResourceId,
        archive_path: impl AsRef<Path>,
        root_checksum: String,
        created_by_node: ClusterNodeName,
        created_at: Timestamp,
    ) -> Result<ResourceManifest, Report<ResourceStoreError>> {
        let archive_path = archive_path.as_ref().to_path_buf();
        let install = self
            .prepare_install(id, created_by_node, created_at)
            .await?;
        let staged_archive_path = install.staging_root.join("archive.tar");
        tokio::fs::copy(&archive_path, &staged_archive_path)
            .await
            .map_err(|_| Report::new(ResourceStoreError::WriteArchive))?;
        unpack_archive_path(&staged_archive_path, &install.content_root).await?;
        let entries = self.manifest_entries(install.content_root.clone()).await?;
        self.finalize_install_with_root_checksum(install, root_checksum, entries)
            .await
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
        let manifest_checksum = manifest_checksum(&entries)?;
        let resource = ResourceVersion {
            id: install.id.clone(),
            root_checksum,
            manifest_checksum,
            file_count,
            total_bytes,
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

async fn unpack_archive_path(
    path: &Path,
    destination: &Path,
) -> Result<(), Report<ResourceStoreError>> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| Report::new(ResourceStoreError::ReadArchive))?;
    let archive = AsyncTarArchive::new(file);
    archive
        .unpack(destination)
        .await
        .map_err(|_| Report::new(ResourceStoreError::InvalidArchive))
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

fn collect_manifest_entries(
    root: &Path,
    cancellation: &Cancellation,
) -> Result<Vec<ResourceManifestEntry>, Report<ResourceStoreError>> {
    let mut entries = Vec::new();
    collect_manifest_entries_recursive(root, root, &mut entries, cancellation)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

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
    use nervix_execution::Executor;
    use nervix_models::{ClusterNodeName, DomainName, ResourceId, ResourceName, Timestamp};
    use tempfile::{NamedTempFile, tempdir};

    use super::{ResourceEntryContent, ResourceStore, ResourceStoreError};

    fn resource_id(domain: &str, identifier: &str, version: u64) -> ResourceId {
        ResourceId::new(
            DomainName::parse(domain).expect("valid domain"),
            ResourceName::parse(identifier).expect("valid identifier"),
            version,
        )
    }

    async fn read_archive(store: &ResourceStore, id: &ResourceId) -> Vec<u8> {
        let mut archive = Vec::new();
        let mut offset = 0_u64;
        loop {
            let chunk = store
                .read_archive_chunk(id, offset)
                .await
                .expect("archive chunk should be readable");
            let chunk_len = u64::try_from(chunk.bytes.len())
                .expect("a resource test archive chunk length fits in u64");
            archive.extend_from_slice(&chunk.bytes);
            if chunk.eof {
                return archive;
            }
            assert_ne!(chunk_len, 0, "a non-final archive chunk must make progress");
            offset = offset
                .checked_add(chunk_len)
                .expect("a resource test archive length fits in u64");
        }
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
