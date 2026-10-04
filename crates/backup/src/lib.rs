//! The public Nervix backup archive.
//!
//! An archive is one tar file written manifest first, so it can be read as a stream without
//! seeking. Every section it holds is canonical NSPL text, the original bytes of a resource version,
//! or a record this crate owns, encoded with rkyv behind a header of its own: a magic, a record kind
//! and a format version, checked before bytecheck validates a single field of the payload. The
//! manifest names every section with its length and BLAKE3 digest, so a reader verifies each section
//! as it passes and refuses an archive that is truncated, reordered or altered.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The archive format: the manifest, the section paths, the record kinds and their
//!   versions, the validated records the sections decode into, the tar layout an archive is written
//!   in, and the streaming reader that verifies one.
//! - **Depends on.** The vocabulary, `rkyv`, `tar` and `blake3`.
//! - **Must not know.** Consensus, the registry, the runtime, or where an archive is staged or sent.
//!   The records are shaped by the archive's contract rather than by any internal store, and the
//!   conversions into them are the only place an internal shape reaches the format.

mod describe;
mod error;
mod layout;
mod manifest;
mod path;
mod reader;
mod records;
mod section;
mod state;
mod wire;

pub use describe::{
    ArchiveContents, ArchiveDescription, DescribedDomain, DescribedResourceVersion,
    DescribedRuntimeState, DescribedSection, SkippedStateReason, SkippedStateSection,
    describe_archive, read_archive_contents,
};
pub use error::{ArchiveReadError, ArchiveWriteError};
pub use layout::{ArchiveLayout, ArchivePiece, SectionSink};
pub use manifest::{
    ARCHIVE_FORMAT_MAJOR, ArchiveScope, BackupManifest, DomainCapture, RaftLogPosition,
    SectionContent, SectionDigest, SectionEntry,
};
pub use path::SectionPath;
pub use reader::{SectionReader, SectionVisitor, read_archive};
pub use records::{
    DeclaredResource, DomainRecord, PublishedResourceVersion, ResourceVersionRecord,
    ResourceVersionState, UserRecord, UsersRecord,
};
pub use section::{ArchiveRecord, RecordKind, SectionDigester};
pub use state::{
    BranchLifecycleEntry, BranchLifecycleRecord, KafkaOffsetsRecord, KafkaPartitionOffset,
    WasmStateDescriptor,
};
pub use wire::{StateField, StateValue};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod wasm_properties;

#[cfg(test)]
mod archive_values;

#[cfg(test)]
mod archive_properties;

#[cfg(test)]
mod malformed_properties;
