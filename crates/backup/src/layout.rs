//! The tar layout an archive is written in, computed before any byte is written.
//!
//! Every entry is a regular file owned by root, readable only by its owner and stamped with time
//! zero, so an archive's bytes depend on its contents alone. The manifest is the first entry and
//! the sections follow in manifest order. A path longer than a tar header's name field travels in a
//! GNU long-name entry before its header, which every tar reader follows.
//!
//! The layout knows the exact size of the archive before the first byte, which is what lets a
//! writer reserve its staging space up front, and it hands the writer the archive as a sequence of
//! pieces: bytes it holds itself, and sections whose bytes the writer supplies.

use std::io::Write;

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use tar::{EntryType, Header};

use crate::{
    error::ArchiveWriteError,
    manifest::{BackupManifest, SectionEntry},
    path::MANIFEST_PATH,
    section::{ArchiveRecord as _, SectionDigester},
};

/// The size of one tar block. Every header and every padded entry is a whole number of blocks.
const BLOCK_BYTES: usize = 512;

/// The zeroes that pad an entry to a block boundary.
static ZERO_BLOCK: [u8; BLOCK_BYTES] = [0; BLOCK_BYTES];

/// The two zero blocks that end a tar stream.
static END_OF_ARCHIVE: [u8; 2 * BLOCK_BYTES] = [0; 2 * BLOCK_BYTES];

/// The name a GNU long-name entry carries in its own header.
const GNU_LONG_NAME: &str = "././@LongLink";

/// The mode every entry is written with: the archive is sensitive as a whole.
const ENTRY_MODE: u32 = 0o600;

/// An archive laid out: its manifest, and every piece of the tar stream in order.
#[derive(Debug, Clone)]
pub struct ArchiveLayout {
    manifest: BackupManifest,
    pieces: Vec<LayoutPiece>,
    total_bytes: u64,
}

#[derive(Debug, Clone)]
enum LayoutPiece {
    /// Bytes the layout holds: tar headers and the encoded manifest.
    Held(Vec<u8>),
    /// The section at this index of the manifest, whose bytes the writer supplies.
    Section(usize),
    /// Zeroes padding the entry before them to a block boundary. Fewer than one block.
    Padding(usize),
    /// The end of the tar stream.
    End,
}

impl LayoutPiece {
    fn archive_piece<'layout>(
        &'layout self,
        manifest: &'layout BackupManifest,
    ) -> ArchivePiece<'layout> {
        match self {
            Self::Held(bytes) => ArchivePiece::Bytes(bytes),
            Self::Section(index) => ArchivePiece::Section(
                manifest
                    .sections
                    .get(*index)
                    .assured("a layout names only the sections of its own manifest"),
            ),
            Self::Padding(length) => ArchivePiece::Bytes(&ZERO_BLOCK[..*length]),
            Self::End => ArchivePiece::Bytes(&END_OF_ARCHIVE),
        }
    }
}

/// One piece of an archive, in the order the archive holds them.
#[derive(Debug, Clone, Copy)]
pub enum ArchivePiece<'layout> {
    /// Bytes to write as they are: a tar header, the manifest, padding, or the end of the archive.
    Bytes(&'layout [u8]),
    /// One section, whose bytes the writer supplies exactly as its entry describes them.
    Section(&'layout SectionEntry),
}

impl ArchiveLayout {
    /// Lays out an archive of `manifest` and the sections it names.
    pub fn new(manifest: BackupManifest) -> Result<Self, Report<ArchiveWriteError>> {
        manifest.ensure_unique_sections()?;
        let encoded = manifest.encode()?;
        let mut pieces = Vec::new();
        let mut total_bytes = 0_u64;

        let manifest_length: u64 = encoded.len().arch_into();
        push_header(
            &mut pieces,
            &mut total_bytes,
            MANIFEST_PATH,
            manifest_length,
        )?;
        add_bytes(&mut total_bytes, manifest_length)?;
        pieces.push(LayoutPiece::Held(encoded));
        push_padding(&mut pieces, &mut total_bytes, manifest_length)?;

        for (index, section) in manifest.sections.iter().enumerate() {
            push_header(
                &mut pieces,
                &mut total_bytes,
                section.path.as_str(),
                section.length,
            )?;
            add_bytes(&mut total_bytes, section.length)?;
            pieces.push(LayoutPiece::Section(index));
            push_padding(&mut pieces, &mut total_bytes, section.length)?;
        }

        add_bytes(&mut total_bytes, END_OF_ARCHIVE.len().arch_into())?;
        pieces.push(LayoutPiece::End);
        Ok(Self {
            manifest,
            pieces,
            total_bytes,
        })
    }

    pub fn manifest(&self) -> &BackupManifest {
        &self.manifest
    }

    /// The exact size of the archive file.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Every piece of the archive, in order.
    pub fn pieces(&self) -> impl Iterator<Item = ArchivePiece<'_>> {
        self.pieces
            .iter()
            .map(|piece| piece.archive_piece(&self.manifest))
    }

    /// Writes the whole archive to `writer`, asking `write_section` for each section's bytes, and
    /// refuses a section whose supplied bytes differ in length or digest from its manifest entry.
    pub fn write_to<W: Write>(
        &self,
        writer: &mut W,
        mut write_section: impl FnMut(&SectionEntry, &mut SectionSink<'_, W>) -> std::io::Result<()>,
    ) -> Result<(), Report<ArchiveWriteError>> {
        for piece in self.pieces() {
            match piece {
                ArchivePiece::Bytes(bytes) => {
                    if let Err(error) = writer.write_all(bytes) {
                        return Err(Report::new(error).change_context(ArchiveWriteError::Write));
                    }
                }
                ArchivePiece::Section(entry) => {
                    let mut sink = SectionSink {
                        writer,
                        digester: SectionDigester::new(),
                    };
                    if let Err(error) = write_section(entry, &mut sink) {
                        return Err(Report::new(error).change_context(ArchiveWriteError::Write));
                    }
                    if sink.digester.length() != entry.length
                        || sink.digester.finish() != entry.digest
                    {
                        return Err(Report::new(ArchiveWriteError::SectionMismatch {
                            path: entry.path.to_string(),
                        }));
                    }
                }
            }
        }
        if let Err(error) = writer.flush() {
            return Err(Report::new(error).change_context(ArchiveWriteError::Write));
        }
        Ok(())
    }
}

/// Where one section's bytes are written, measured as they pass.
pub struct SectionSink<'writer, W> {
    writer: &'writer mut W,
    digester: SectionDigester,
}

impl<W: Write> Write for SectionSink<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.digester.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

/// Adds the header of an entry named `path` of `length` bytes, preceded by a long-name entry when
/// the name does not fit the header.
fn push_header(
    pieces: &mut Vec<LayoutPiece>,
    total_bytes: &mut u64,
    path: &str,
    length: u64,
) -> Result<(), Report<ArchiveWriteError>> {
    let mut header = entry_header(length);
    let mut held = Vec::with_capacity(BLOCK_BYTES);
    if header.set_path(path).is_err() {
        let name = path.as_bytes();
        let fits = header.as_old().name.len();
        if name.len() < fits || !path.is_ascii() {
            return Err(Report::new(ArchiveWriteError::Header {
                path: path.to_string(),
            }));
        }
        // The long name is the path and a terminating NUL, padded to a block.
        let long_name_length = name
            .len()
            .checked_add(1)
            .assured("a path fits the address space it is held in");
        let mut long_name = entry_header(long_name_length.arch_into());
        long_name.set_entry_type(EntryType::GNULongName);
        let named = long_name.set_path(GNU_LONG_NAME);
        if named.is_err() {
            return Err(Report::new(ArchiveWriteError::Header {
                path: path.to_string(),
            }));
        }
        long_name.set_cksum();
        held.extend_from_slice(long_name.as_bytes());
        held.extend_from_slice(name);
        held.push(0);
        let padding = padding_after(long_name_length.arch_into());
        held.extend_from_slice(&ZERO_BLOCK[..padding]);
        // The header itself keeps as much of the path as its name field holds. Readers take the
        // long name instead; the prefix only helps a person reading the raw bytes.
        header.as_old_mut().name.copy_from_slice(&name[..fits]);
    }
    header.set_cksum();
    held.extend_from_slice(header.as_bytes());
    add_bytes(total_bytes, held.len().arch_into())?;
    pieces.push(LayoutPiece::Held(held));
    Ok(())
}

fn entry_header(length: u64) -> Header {
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Regular);
    header.set_size(length);
    header.set_mode(ENTRY_MODE);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header
}

/// Adds the zeroes that pad an entry of `length` bytes to a block boundary, if it needs any.
fn push_padding(
    pieces: &mut Vec<LayoutPiece>,
    total_bytes: &mut u64,
    length: u64,
) -> Result<(), Report<ArchiveWriteError>> {
    let padding = padding_after(length);
    if padding > 0 {
        add_bytes(total_bytes, padding.arch_into())?;
        pieces.push(LayoutPiece::Padding(padding));
    }
    Ok(())
}

/// The zeroes that follow an entry of `length` bytes up to the next block boundary.
fn padding_after(length: u64) -> usize {
    let block: u64 = BLOCK_BYTES.arch_into();
    let remainder = length % block;
    if remainder == 0 {
        return 0;
    }
    let padding = block
        .checked_sub(remainder)
        .verified("a remainder is always smaller than the block it was taken by");
    usize::try_from(padding).verified("the padding is smaller than one block")
}

/// Counts `length` more bytes into the archive, which a tar stream addresses in 64 bits.
fn add_bytes(total_bytes: &mut u64, length: u64) -> Result<(), Report<ArchiveWriteError>> {
    let Some(sum) = total_bytes.checked_add(length) else {
        return Err(Report::new(ArchiveWriteError::ArchiveTooLarge));
    };
    *total_bytes = sum;
    Ok(())
}
