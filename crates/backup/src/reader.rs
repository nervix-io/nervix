//! Reading an archive as a stream, verifying every section as it passes.
//!
//! The reader never seeks. It reads the manifest first, then expects exactly the sections the
//! manifest names, in its order, each of its declared length and digest, and nothing after the
//! last. A visitor sees each section while it streams; whatever the visitor leaves unread is read
//! and verified before the next section begins, so a visitor that only needs the manifest still
//! gets a verified archive.

use std::io::{self, Read};

use arch_into::ArchInto as _;
use error_stack::Report;
use tar::{Archive, EntryType};

use crate::{
    error::ArchiveReadError,
    manifest::{BackupManifest, SectionEntry},
    path::MANIFEST_PATH,
    section::{ArchiveRecord as _, MAX_RECORD_BYTES, SectionDigester},
};

/// What a caller does with an archive while it is read.
pub trait SectionVisitor {
    /// Called with the manifest, before any section.
    fn manifest(&mut self, manifest: &BackupManifest) -> Result<(), Report<ArchiveReadError>>;

    /// Called for each section in archive order. `content` yields exactly the section's bytes.
    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>>;
}

/// The bytes of one section, measured as they are read.
pub struct SectionReader<'entry> {
    content: &'entry mut dyn Read,
    digester: SectionDigester,
    /// Where the section's first byte sits in the archive stream.
    offset: u64,
}

impl SectionReader<'_> {
    /// Where the section's first byte sits in the archive, counted from the archive's first byte.
    /// The section's bytes are the `length` bytes its manifest entry declares from there, so a
    /// reader holding the whole archive can read them again later without walking the stream.
    pub fn archive_offset(&self) -> u64 {
        self.offset
    }

    /// Reads the whole section into memory. `limit` bounds what the caller is prepared to hold;
    /// a longer section is refused rather than read.
    pub fn read_all(
        &mut self,
        entry: &SectionEntry,
        limit: u64,
    ) -> Result<Vec<u8>, Report<ArchiveReadError>> {
        if entry.length > limit {
            return Err(Report::new(ArchiveReadError::SectionTooLarge {
                path: entry.path.to_string(),
                length: entry.length,
                limit,
            }));
        }
        let capacity: usize = entry.length.arch_into();
        let mut bytes = Vec::with_capacity(capacity);
        if let Err(error) = self.read_to_end(&mut bytes) {
            return Err(Report::new(error).change_context(ArchiveReadError::Read));
        }
        // A stream that ends inside a section reads short rather than failing, so the length is
        // what tells a truncated section from a whole one.
        let read: u64 = bytes.len().arch_into();
        if read != entry.length {
            return Err(Report::new(ArchiveReadError::LengthMismatch {
                path: entry.path.to_string(),
                declared: entry.length,
                actual: read,
            }));
        }
        Ok(bytes)
    }
}

impl Read for SectionReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.content.read(buffer)?;
        self.digester.update(&buffer[..read]);
        Ok(read)
    }
}

/// Reads an archive from `reader`, handing its manifest and then every section to `visitor`, and
/// returns the manifest once every section has been verified.
pub fn read_archive<R: Read>(
    reader: R,
    visitor: &mut impl SectionVisitor,
) -> Result<BackupManifest, Report<ArchiveReadError>> {
    let mut archive = Archive::new(reader);
    let mut entries = match archive.entries() {
        Ok(entries) => entries,
        Err(error) => {
            return Err(Report::new(error).change_context(ArchiveReadError::NotAnArchive));
        }
    };

    let manifest = {
        let first = match entries.next() {
            Some(Ok(first)) => first,
            Some(Err(error)) => {
                return Err(Report::new(error).change_context(ArchiveReadError::NotAnArchive));
            }
            None => return Err(Report::new(ArchiveReadError::MissingManifest)),
        };
        let path = entry_path(&first)?;
        if path != MANIFEST_PATH || first.header().entry_type() != EntryType::Regular {
            return Err(Report::new(ArchiveReadError::UnexpectedFirstEntry { path }));
        }
        let length = first.size();
        if length > MAX_RECORD_BYTES {
            return Err(Report::new(ArchiveReadError::SectionTooLarge {
                path,
                length,
                limit: MAX_RECORD_BYTES,
            }));
        }
        let capacity: usize = length.arch_into();
        let mut bytes = Vec::with_capacity(capacity);
        let mut first = first;
        if let Err(error) = first.read_to_end(&mut bytes) {
            return Err(Report::new(error).change_context(ArchiveReadError::Read));
        }
        BackupManifest::decode(MANIFEST_PATH, &bytes)?
    };
    visitor.manifest(&manifest)?;

    for section in &manifest.sections {
        let mut entry = match entries.next() {
            Some(Ok(entry)) => entry,
            Some(Err(error)) => {
                return Err(Report::new(error).change_context(ArchiveReadError::Read));
            }
            None => {
                return Err(Report::new(ArchiveReadError::MissingSection {
                    path: section.path.to_string(),
                }));
            }
        };
        let path = entry_path(&entry)?;
        if path != section.path.as_str() || entry.header().entry_type() != EntryType::Regular {
            return Err(Report::new(ArchiveReadError::SectionOutOfOrder {
                expected: section.path.to_string(),
                found: path,
            }));
        }
        let length = entry.size();
        if length != section.length {
            return Err(Report::new(ArchiveReadError::LengthMismatch {
                path,
                declared: section.length,
                actual: length,
            }));
        }
        if let Some(limit) = section.content.length_limit()
            && length > limit
        {
            return Err(Report::new(ArchiveReadError::SectionTooLarge {
                path,
                length,
                limit,
            }));
        }
        let offset = entry.raw_file_position();
        let mut content = SectionReader {
            content: &mut entry,
            digester: SectionDigester::new(),
            offset,
        };
        visitor.section(section, &mut content)?;
        if let Err(error) = io::copy(&mut content, &mut io::sink()) {
            return Err(Report::new(error).change_context(ArchiveReadError::Read));
        }
        let read = content.digester.length();
        if read != section.length {
            return Err(Report::new(ArchiveReadError::LengthMismatch {
                path,
                declared: section.length,
                actual: read,
            }));
        }
        if content.digester.finish() != section.digest {
            return Err(Report::new(ArchiveReadError::DigestMismatch { path }));
        }
    }

    match entries.next() {
        None => Ok(manifest),
        Some(Ok(entry)) => {
            let path = entry_path(&entry)?;
            Err(Report::new(ArchiveReadError::UnexpectedSection { path }))
        }
        Some(Err(error)) => Err(Report::new(error).change_context(ArchiveReadError::Read)),
    }
}

/// The path an entry names, as text. Archive paths are ASCII, so a path that is not UTF-8 is not
/// one this format wrote.
fn entry_path<R: Read>(entry: &tar::Entry<'_, R>) -> Result<String, Report<ArchiveReadError>> {
    let bytes = entry.path_bytes();
    match std::str::from_utf8(&bytes) {
        Ok(path) => Ok(path.to_string()),
        Err(_) => Err(Report::new(ArchiveReadError::InvalidPath)),
    }
}
