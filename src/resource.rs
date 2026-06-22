use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};

use async_tar::{
    Archive as AsyncTarArchive, Builder as AsyncTarBuilder, EntryType, Header, HeaderMode,
};
use blake3::Hasher;
use nervix_models::{Identifier, ResourceId, ResourceVersion, Timestamp};
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
    pub entry_type: ResourceEntryType,
    pub size: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceEntryType {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub struct ResourceStore {
    root: PathBuf,
}

#[derive(Debug)]
struct PendingInstall {
    identifier: Identifier,
    version: u64,
    install_root: PathBuf,
    staging_root: PathBuf,
    content_root: PathBuf,
    created_by_node: String,
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
}

impl ResourceStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ResourceStoreError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|_| ResourceStoreError::CreateRoot)?;
        Ok(Self { root })
    }

    pub async fn install_from_directory(
        &self,
        identifier: Identifier,
        version: u64,
        source_dir: impl AsRef<Path>,
        created_by_node: impl Into<String>,
        created_at: Timestamp,
    ) -> Result<ResourceManifest, ResourceStoreError> {
        let source_dir = source_dir.as_ref();
        if !source_dir.exists() {
            return Err(ResourceStoreError::MissingSource);
        }
        if !source_dir.is_dir() {
            return Err(ResourceStoreError::InvalidSource);
        }

        let install = self
            .prepare_install(identifier, version, created_by_node.into(), created_at)
            .await?;
        copy_directory_recursive(source_dir, &install.content_root).await?;
        self.finalize_install(install).await
    }

    pub fn manifest_path(&self, identifier: &Identifier, version: u64) -> PathBuf {
        self.version_root(identifier, version).join("manifest.json")
    }

    pub fn content_root(&self, identifier: &Identifier, version: u64) -> PathBuf {
        self.version_root(identifier, version).join("content")
    }

    pub fn archive_path(&self, identifier: &Identifier, version: u64) -> PathBuf {
        self.version_root(identifier, version).join("archive.tar")
    }

    pub fn remove_version(
        &self,
        identifier: &Identifier,
        version: u64,
    ) -> Result<(), ResourceStoreError> {
        let install_root = self.version_root(identifier, version);
        if install_root.exists() {
            fs::remove_dir_all(&install_root).map_err(|_| ResourceStoreError::DeleteResourceDir)?;
        }
        let staging_root = self.staging_root(identifier, version);
        if staging_root.exists() {
            fs::remove_dir_all(&staging_root).map_err(|_| ResourceStoreError::DeleteResourceDir)?;
        }
        Ok(())
    }

    pub fn read_archive_bytes(
        &self,
        identifier: &Identifier,
        version: u64,
    ) -> Result<Vec<u8>, ResourceStoreError> {
        fs::read(self.archive_path(identifier, version))
            .map_err(|_| ResourceStoreError::ReadArchive)
    }

    pub fn read_manifest(
        &self,
        identifier: &Identifier,
        version: u64,
    ) -> Result<ResourceManifest, ResourceStoreError> {
        let bytes = fs::read(self.manifest_path(identifier, version))
            .map_err(|_| ResourceStoreError::ReadFile)?;
        serde_json::from_slice(&bytes).map_err(|_| ResourceStoreError::SerializeManifest)
    }

    pub async fn install_from_archive_path(
        &self,
        identifier: Identifier,
        version: u64,
        archive_path: impl AsRef<Path>,
        root_checksum: String,
        created_by_node: impl Into<String>,
        created_at: Timestamp,
    ) -> Result<ResourceManifest, ResourceStoreError> {
        let archive_path = archive_path.as_ref().to_path_buf();
        let created_by_node = created_by_node.into();
        let install = self
            .prepare_install(identifier, version, created_by_node, created_at)
            .await?;
        let staged_archive_path = install.staging_root.join("archive.tar");
        tokio::fs::copy(&archive_path, &staged_archive_path)
            .await
            .map_err(|_| ResourceStoreError::WriteArchive)?;
        unpack_archive_path(&staged_archive_path, &install.content_root).await?;
        let content_root = install.content_root.clone();
        let entries = tokio::task::spawn_blocking(move || collect_manifest_entries(&content_root))
            .await
            .map_err(|_| ResourceStoreError::JoinBlockingTask)??;
        self.finalize_install_with_root_checksum(install, root_checksum, entries)
            .await
    }

    pub fn resolve_content_path(
        &self,
        identifier: &Identifier,
        version: u64,
        path: &str,
    ) -> Result<PathBuf, ResourceStoreError> {
        let relative = sanitize_relative_path(path)?;
        Ok(self.content_root(identifier, version).join(relative))
    }

    fn version_root(&self, identifier: &Identifier, version: u64) -> PathBuf {
        self.root
            .join(identifier.as_str())
            .join(version.to_string())
    }

    fn staging_root(&self, identifier: &Identifier, version: u64) -> PathBuf {
        self.root
            .join(identifier.as_str())
            .join(format!(".{version}.staging"))
    }

    async fn prepare_install_paths(
        &self,
        identifier: &Identifier,
        version: u64,
    ) -> Result<(PathBuf, PathBuf, PathBuf), ResourceStoreError> {
        let install_root = self.version_root(identifier, version);
        if install_root.exists() {
            tokio::fs::remove_dir_all(&install_root)
                .await
                .map_err(|_| ResourceStoreError::RenameResourceDir)?;
        }

        let staging_root = self.staging_root(identifier, version);
        if staging_root.exists() {
            tokio::fs::remove_dir_all(&staging_root)
                .await
                .map_err(|_| ResourceStoreError::RenameResourceDir)?;
        }
        let content_root = staging_root.join("content");
        tokio::fs::create_dir_all(&content_root)
            .await
            .map_err(|_| ResourceStoreError::CreateResourceDir)?;
        Ok((install_root, staging_root, content_root))
    }

    async fn prepare_install(
        &self,
        identifier: Identifier,
        version: u64,
        created_by_node: String,
        created_at: Timestamp,
    ) -> Result<PendingInstall, ResourceStoreError> {
        let (install_root, staging_root, content_root) =
            self.prepare_install_paths(&identifier, version).await?;
        Ok(PendingInstall {
            identifier,
            version,
            install_root,
            staging_root,
            content_root,
            created_by_node,
            created_at,
        })
    }

    async fn finalize_install(
        &self,
        install: PendingInstall,
    ) -> Result<ResourceManifest, ResourceStoreError> {
        let content_root = install.content_root.clone();
        let entries = tokio::task::spawn_blocking(move || collect_manifest_entries(&content_root))
            .await
            .map_err(|_| ResourceStoreError::JoinBlockingTask)??;
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
    ) -> Result<ResourceManifest, ResourceStoreError> {
        let total_bytes = entries.iter().map(|entry| entry.size).sum();
        let file_count = u64::try_from(
            entries
                .iter()
                .filter(|entry| entry.entry_type == ResourceEntryType::File)
                .count(),
        )
        .unwrap_or(u64::MAX);
        let manifest_checksum = manifest_checksum(&entries)?;
        let resource = ResourceVersion {
            id: ResourceId::new(install.identifier.clone(), install.version),
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
            .map_err(|_| ResourceStoreError::SerializeManifest)?;
        tokio::fs::write(&manifest_path, manifest_bytes)
            .await
            .map_err(|_| ResourceStoreError::WriteManifest)?;

        if let Some(parent) = install.install_root.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|_| ResourceStoreError::CreateResourceDir)?;
        }
        tokio::fs::rename(&install.staging_root, &install.install_root)
            .await
            .map_err(|_| ResourceStoreError::RenameResourceDir)?;
        Ok(manifest)
    }
}

async fn write_archive_file(
    root: &Path,
    entries: &[ResourceManifestEntry],
    destination: &Path,
) -> Result<String, ResourceStoreError> {
    let file = tokio::fs::File::create(destination)
        .await
        .map_err(|_| ResourceStoreError::WriteArchive)?;
    let mut archive = AsyncTarBuilder::new(file);
    archive.mode(HeaderMode::Deterministic);

    for entry in entries {
        tokio::task::consume_budget().await;
        let mut header = Header::new_ustar();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);

        let entry_path = Path::new(&entry.path);
        match entry.entry_type {
            ResourceEntryType::Directory => {
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(EntryType::Directory);
                header.set_cksum();
                archive
                    .append_data(&mut header, entry_path, tokio::io::empty())
                    .await
                    .map_err(|_| ResourceStoreError::WriteArchive)?;
            }
            ResourceEntryType::File => {
                let path = root.join(entry_path);
                let size = tokio::fs::metadata(&path)
                    .await
                    .map_err(|_| ResourceStoreError::ReadFile)?
                    .len();
                header.set_size(size);
                header.set_mode(0o644);
                header.set_entry_type(EntryType::Regular);
                header.set_cksum();
                let file = tokio::fs::File::open(&path)
                    .await
                    .map_err(|_| ResourceStoreError::ReadFile)?;
                archive
                    .append_data(&mut header, entry_path, file)
                    .await
                    .map_err(|_| ResourceStoreError::WriteArchive)?;
            }
        }
    }

    let _file = archive
        .into_inner()
        .await
        .map_err(|_| ResourceStoreError::WriteArchive)?;
    checksum_path(destination).await
}

async fn unpack_archive_path(path: &Path, destination: &Path) -> Result<(), ResourceStoreError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| ResourceStoreError::ReadArchive)?;
    let archive = AsyncTarArchive::new(file);
    archive
        .unpack(destination)
        .await
        .map_err(|_| ResourceStoreError::InvalidArchive)
}

fn sanitize_relative_path(path: &str) -> Result<PathBuf, ResourceStoreError> {
    let candidate = Path::new(path);
    let mut clean = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(ResourceStoreError::InvalidResourcePath);
            }
        }
    }
    Ok(clean)
}

async fn copy_directory_recursive(
    source: &Path,
    destination: &Path,
) -> Result<(), ResourceStoreError> {
    tokio::fs::create_dir_all(destination)
        .await
        .map_err(|_| ResourceStoreError::CreateResourceDir)?;
    let mut entries = tokio::fs::read_dir(source)
        .await
        .map_err(|_| ResourceStoreError::ReadDirectory)?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| ResourceStoreError::ReadDirectory)?
    {
        tokio::task::consume_budget().await;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .await
            .map_err(|_| ResourceStoreError::ReadDirectory)?;
        if file_type.is_dir() {
            Box::pin(copy_directory_recursive(&source_path, &destination_path)).await?;
        } else if file_type.is_file() {
            if let Some(parent) = destination_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|_| ResourceStoreError::CreateResourceDir)?;
            }
            tokio::fs::copy(&source_path, &destination_path)
                .await
                .map_err(|_| ResourceStoreError::ReadFile)?;
        }
    }
    Ok(())
}

fn collect_manifest_entries(root: &Path) -> Result<Vec<ResourceManifestEntry>, ResourceStoreError> {
    let mut entries = Vec::new();
    collect_manifest_entries_recursive(root, root, &mut entries)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

fn collect_manifest_entries_recursive(
    root: &Path,
    current: &Path,
    entries: &mut Vec<ResourceManifestEntry>,
) -> Result<(), ResourceStoreError> {
    for entry in fs::read_dir(current).map_err(|_| ResourceStoreError::ReadDirectory)? {
        let entry = entry.map_err(|_| ResourceStoreError::ReadDirectory)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|_| ResourceStoreError::ReadDirectory)?;
        let relative = path
            .strip_prefix(root)
            .expect("current path must remain under root")
            .to_string_lossy()
            .replace('\\', "/");
        if file_type.is_dir() {
            entries.push(ResourceManifestEntry {
                path: relative.clone(),
                entry_type: ResourceEntryType::Directory,
                size: 0,
                checksum: String::new(),
            });
            collect_manifest_entries_recursive(root, &path, entries)?;
        } else if file_type.is_file() {
            let size = entry
                .metadata()
                .map_err(|_| ResourceStoreError::ReadFile)?
                .len();
            entries.push(ResourceManifestEntry {
                path: relative,
                entry_type: ResourceEntryType::File,
                size,
                checksum: checksum_file(&path)?,
            });
        }
    }
    Ok(())
}

fn checksum_file(path: &Path) -> Result<String, ResourceStoreError> {
    let mut file = fs::File::open(path).map_err(|_| ResourceStoreError::ReadFile)?;
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| ResourceStoreError::ReadFile)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let hash = hasher.finalize();
    Ok(encode_hex(hash.as_bytes()))
}

async fn checksum_path(path: &Path) -> Result<String, ResourceStoreError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|_| ResourceStoreError::ReadArchive)?;
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 8192];
    loop {
        tokio::task::consume_budget().await;
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|_| ResourceStoreError::ReadArchive)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let hash = hasher.finalize();
    Ok(encode_hex(hash.as_bytes()))
}

fn manifest_checksum(entries: &[ResourceManifestEntry]) -> Result<String, ResourceStoreError> {
    let bytes = serde_json::to_vec(entries).map_err(|_| ResourceStoreError::SerializeManifest)?;
    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    let hash = hasher.finalize();
    Ok(encode_hex(hash.as_bytes()))
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use nervix_models::{Identifier, Timestamp};
    use tempfile::{NamedTempFile, tempdir};

    use super::{ResourceEntryType, ResourceStore, ResourceStoreError};

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
        let store = ResourceStore::open(install_root.path()).expect("store should open");
        let manifest = store
            .install_from_directory(
                Identifier::parse("fraud_model").expect("valid identifier"),
                1,
                source.path(),
                "node-1",
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("resource should install");

        assert_eq!(manifest.resource.id.version, 1);
        assert_eq!(manifest.resource.file_count, 2);
        assert!(
            store
                .manifest_path(&manifest.resource.id.identifier, 1)
                .exists()
        );
        assert!(
            store
                .content_root(&manifest.resource.id.identifier, 1)
                .join("proto/schema.proto")
                .exists()
        );
        assert!(manifest.entries.iter().any(|entry| {
            entry.path == "proto" && entry.entry_type == ResourceEntryType::Directory
        }));
        assert!(manifest.entries.iter().any(|entry| {
            entry.path == "model.onnx" && entry.entry_type == ResourceEntryType::File
        }));
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
        let store = ResourceStore::open(install_root.path()).expect("store should open");
        let source_identifier = Identifier::parse("fraud_model").expect("valid identifier");
        let source_manifest = store
            .install_from_directory(
                source_identifier.clone(),
                1,
                source.path(),
                "node-1",
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("resource should install");
        let archive_bytes = store
            .read_archive_bytes(&source_identifier, 1)
            .expect("archive should be readable");

        let temp_archive = NamedTempFile::new().expect("temp archive should be created");
        std::fs::write(temp_archive.path(), &archive_bytes)
            .expect("temp archive should be written");

        let replica_identifier =
            Identifier::parse("fraud_model_replica").expect("valid identifier");
        let replica_manifest = store
            .install_from_archive_path(
                replica_identifier.clone(),
                7,
                temp_archive.path(),
                source_manifest.resource.root_checksum.clone(),
                "node-2",
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
                .content_root(&replica_identifier, 7)
                .join("proto/nested/schema.proto")
                .exists()
        );
        assert!(
            store.archive_path(&replica_identifier, 7).exists(),
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
        let store = ResourceStore::open(install_root.path()).expect("store should open");
        let source_identifier = Identifier::parse("fraud_model").expect("valid identifier");
        let source_manifest = store
            .install_from_directory(
                source_identifier.clone(),
                1,
                source.path(),
                "node-1",
                Timestamp::from_unix_nanos(42),
            )
            .await
            .expect("resource should install");
        let archive_bytes = store
            .read_archive_bytes(&source_identifier, 1)
            .expect("archive should be readable");

        let temp_archive = NamedTempFile::new().expect("temp archive should be created");
        std::fs::write(temp_archive.path(), &archive_bytes)
            .expect("temp archive should be written");

        let replica_identifier =
            Identifier::parse("fraud_model_streamed").expect("valid identifier");
        let replica_manifest = store
            .install_from_archive_path(
                replica_identifier,
                8,
                temp_archive.path(),
                source_manifest.resource.root_checksum.clone(),
                "node-2",
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
        let store = ResourceStore::open(install_root.path()).expect("store should open");
        let identifier = Identifier::parse("fraud_model").expect("valid identifier");

        let err = store
            .resolve_content_path(&identifier, 7, "../escape")
            .expect_err("parent segments must be rejected");
        assert!(matches!(err, ResourceStoreError::InvalidResourcePath));
    }
}
