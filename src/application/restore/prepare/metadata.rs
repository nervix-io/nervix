//! Admission for the archive description and the plans built from it.
//!
//! Layer: control plane.
//! - **Owns.** Measuring metadata before decoding and retaining its preparation reservation.
//! - **Depends on.** The current archive manifest and the bounded executor.
//! - **Must not know.** Native checkpoint encoding, generation publication, or database keys.

use std::{
    cell::Cell,
    io::{self, Read},
};

use error_stack::{Report, ResultExt as _};
use nervix_backup::{
    ArchiveContents, ArchiveReadError, BackupManifest, DescribedRuntimeState, SectionContent,
    SectionEntry, SectionReader, SectionVisitor, read_archive, read_manifest_header,
};
use nervix_execution::{Cancellation, Executor, MemoryClass, Reservation, StorageClass};
use nervix_primitives::sync::Arc;

use super::{CancellableRead, RestoreRefusal};
use crate::runtime::{StagedArtifact, restored_key_fixed_bytes};

/// Owned archive values overlap decoding, native per-entry conversion and resolver scratch,
/// restore-plan copies, and placement descriptions. NSPL additionally overlaps tokens, parser
/// state, semantic Models and transaction-planner copies. Resources, guest saves and Arrow columns stay on
/// disk and do not contribute to this retained-memory admission.
const RECORD_WORKING_MULTIPLIER: u64 = 16;
const MODEL_WORKING_MULTIPLIER: u64 = 64;
const FIXED_WORKING_BYTES: u64 = 2 * 1024 * 1024;

/// The share every archived deduplicator key takes of its restore conversion whatever its values:
/// its entry in the resident keyspace and its serializer resolver. The description admits it for
/// every key the deduplicator descriptors count, so a keyspace whose keys the node cannot hold is
/// refused before planning. The parts and values of those keys follow their Arrow key groups, which
/// stay on disk, and are charged as each group converts. A window's conversion holds one group at a
/// time beside the per-row watermarks its descriptor record already carries.
pub(super) fn branch_state_working_bytes(contents: &ArchiveContents) -> Option<u64> {
    let per_key = restored_key_fixed_bytes();
    let mut bytes = 0_u64;
    for domain in &contents.description.domains {
        for state in &domain.state {
            let DescribedRuntimeState::Deduplicator { descriptor, .. } = state else {
                continue;
            };
            let keys = descriptor.keys.checked_mul(per_key)?;
            bytes = bytes.checked_add(keys)?;
        }
    }
    Some(bytes)
}

pub(super) async fn reserve_metadata(
    executor: &Executor,
    artifact: Arc<StagedArtifact>,
) -> Result<Reservation, Report<RestoreRefusal>> {
    let header_artifact = artifact.clone();
    let header_charge = executor
        .reserve(MemoryClass::Bulk, 64 * 1024)
        .await
        .change_context(RestoreRefusal::Unreadable)?;
    let manifest_length = executor
        .run_storage(
            StorageClass::Filesystem,
            header_charge,
            move |_charge, cancellation| {
                let mut file = CancellableRead {
                    inner: std::fs::File::open(header_artifact.path())
                        .map_err(Report::new)
                        .change_context(ArchiveReadError::Read)?,
                    cancellation,
                };
                read_manifest_header(&mut file).map(|header| header.length())
            },
        )
        .await
        .change_context(RestoreRefusal::Unreadable)?
        .change_context(RestoreRefusal::InvalidArchive)?;
    // Only the bounded, identified manifest header is read before admission. The archive reader
    // subsequently verifies the manifest shape, section identities and every digest.
    let manifest_working = manifest_length
        .checked_mul(RECORD_WORKING_MULTIPLIER)
        .ok_or_else(|| Report::new(RestoreRefusal::MetadataAdmission))?;
    let manifest_working = manifest_working
        .checked_add(FIXED_WORKING_BYTES)
        .ok_or_else(|| Report::new(RestoreRefusal::MetadataAdmission))?;
    let charge = executor
        .try_reserve(MemoryClass::RestoreMetadata, manifest_working)
        .change_context(RestoreRefusal::MetadataAdmission)?;
    let bytes = executor
        .run_storage(
            StorageClass::Filesystem,
            charge,
            move |_charge, cancellation| measured_metadata(artifact.path(), cancellation),
        )
        .await
        .change_context(RestoreRefusal::Unreadable)?
        .change_context(RestoreRefusal::InvalidArchive)?;
    executor
        .try_reserve(MemoryClass::RestoreMetadata, bytes)
        .change_context(RestoreRefusal::MetadataAdmission)
}

fn measured_metadata(
    path: &std::path::Path,
    cancellation: &Cancellation,
) -> Result<u64, Report<ArchiveReadError>> {
    let read = Cell::new(0_u64);
    let file = std::fs::File::open(path)
        .map_err(Report::new)
        .change_context(ArchiveReadError::Read)?;
    let reader = MeasuredRead {
        inner: CancellableRead {
            inner: std::io::BufReader::new(file),
            cancellation,
        },
        read: &read,
    };
    let mut visitor = MetadataMeasure {
        read: &read,
        bytes: None,
    };
    read_archive(reader, &mut visitor)?;
    visitor
        .bytes
        .ok_or_else(|| Report::new(ArchiveReadError::MissingManifest))
}

struct MeasuredRead<'a, R> {
    inner: R,
    read: &'a Cell<u64>,
}

impl<R: Read> Read for MeasuredRead<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let length = self.inner.read(buffer)?;
        let bytes = u64::try_from(length).map_err(io::Error::other)?;
        self.read.set(
            self.read
                .get()
                .checked_add(bytes)
                .ok_or_else(|| io::Error::other("archive length exceeds address space"))?,
        );
        Ok(length)
    }
}

struct MetadataMeasure<'a> {
    read: &'a Cell<u64>,
    bytes: Option<u64>,
}

impl SectionVisitor for MetadataMeasure<'_> {
    fn manifest(&mut self, manifest: &BackupManifest) -> Result<(), Report<ArchiveReadError>> {
        let mut bytes = self
            .read
            .get()
            .checked_mul(RECORD_WORKING_MULTIPLIER)
            .and_then(|bytes| bytes.checked_add(FIXED_WORKING_BYTES));
        for section in &manifest.sections {
            let multiplier = match section.content {
                SectionContent::Record(_) => RECORD_WORKING_MULTIPLIER,
                SectionContent::Nspl => MODEL_WORKING_MULTIPLIER,
                SectionContent::ResourceArchive
                | SectionContent::WasmGuestBlob
                | SectionContent::MaterializedColumns
                | SectionContent::DeduplicatorKeys
                | SectionContent::WindowInputRows
                | SectionContent::WindowArgumentColumns => 0,
            };
            bytes = bytes.and_then(|bytes| {
                section
                    .length
                    .checked_mul(multiplier)
                    .and_then(|working| bytes.checked_add(working))
            });
        }
        self.bytes = Some(bytes.ok_or_else(|| {
            Report::new(io::Error::other(
                "restore metadata working memory exceeds address space",
            ))
            .change_context(ArchiveReadError::Read)
        })?);
        Ok(())
    }

    fn section(
        &mut self,
        _entry: &SectionEntry,
        _content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        Ok(())
    }
}
