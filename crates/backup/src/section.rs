//! The header every record section begins with, the encoding behind it, and section digests.
//!
//! A record section is the 8-byte magic `NVXBKREC`, its record kind and its format version as
//! little-endian `u16`s, and then the rkyv encoding of the kind's record. The header is checked
//! before the payload is touched: a section of another kind, or of a version this reader does not
//! know, is refused by what its header says rather than validated as a shape it is not.

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{
    Archive, Deserialize,
    api::high::{HighDeserializer, HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    rancor,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};
use strum::{Display, EnumIter, FromRepr};

use crate::{
    error::{ArchiveReadError, ArchiveWriteError},
    manifest::SectionDigest,
};

/// The bytes every record section begins with.
pub(crate) const RECORD_MAGIC: [u8; 8] = *b"NVXBKREC";

/// The magic, the kind and the version.
pub(crate) const RECORD_HEADER_BYTES: usize = RECORD_MAGIC.len() + 2 + 2;

/// The largest record section this crate writes or a reader accepts.
pub(crate) const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

/// The alignment an rkyv payload is validated at.
const RECORD_ALIGNMENT: usize = 16;

/// What a record section holds. The tag is the archive contract, so a kind keeps its tag for as
/// long as the format lives, and a new kind takes a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, EnumIter, FromRepr)]
#[repr(u16)]
pub enum RecordKind {
    #[strum(serialize = "manifest")]
    Manifest = 1,
    #[strum(serialize = "users")]
    Users = 2,
    #[strum(serialize = "domain")]
    Domain = 3,
    #[strum(serialize = "resource version")]
    ResourceVersion = 4,
    #[strum(serialize = "WASM guest state descriptor")]
    WasmStateDescriptor = 5,
    #[strum(serialize = "Kafka domain offsets")]
    KafkaOffsets = 6,
    #[strum(serialize = "branch lifecycle")]
    BranchLifecycle = 7,
}

impl RecordKind {
    /// The tag a section header writes for this kind.
    pub const fn tag(self) -> u16 {
        match self {
            Self::Manifest => 1,
            Self::Users => 2,
            Self::Domain => 3,
            Self::ResourceVersion => 4,
            Self::WasmStateDescriptor => 5,
            Self::KafkaOffsets => 6,
            Self::BranchLifecycle => 7,
        }
    }
}

/// A record an archive section holds: its kind, the format version this crate encodes it in, and
/// the validated conversion from the section's bytes.
pub trait ArchiveRecord: Sized {
    const KIND: RecordKind;
    /// The format version of this kind that this crate writes and reads.
    const VERSION: u16;

    /// The complete section: the header, then the rkyv payload.
    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>>;

    /// Checks the section header, validates the payload with bytecheck, and converts it into the
    /// validated record. `path` names the section in a refusal.
    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>>;
}

/// Encodes `wire` as the complete section of a `kind` record at format `version`.
pub(crate) fn encode_record<W>(
    kind: RecordKind,
    version: u16,
    wire: &W,
) -> Result<Vec<u8>, Report<ArchiveWriteError>>
where
    W: for<'arena> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'arena>, rancor::Error>>,
{
    let payload = match rkyv::to_bytes::<rancor::Error>(wire) {
        Ok(payload) => payload,
        Err(error) => {
            return Err(Report::new(error).change_context(ArchiveWriteError::Encode { kind }));
        }
    };
    let length = RECORD_HEADER_BYTES
        .checked_add(payload.len())
        .assured("an encoded record fits the address space it was encoded in");
    let length_bytes = u64::try_from(length).assured("supported targets address at most 64 bits");
    if length_bytes > MAX_RECORD_BYTES {
        return Err(Report::new(ArchiveWriteError::RecordTooLarge {
            kind,
            length: length_bytes,
            limit: MAX_RECORD_BYTES,
        }));
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(&RECORD_MAGIC);
    bytes.extend_from_slice(&kind.tag().to_le_bytes());
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// Checks the header of the section at `path` against `kind` and `version`, then validates and
/// deserializes its payload.
pub(crate) fn decode_record<W>(
    path: &str,
    kind: RecordKind,
    version: u16,
    bytes: &[u8],
) -> Result<W, Report<ArchiveReadError>>
where
    W: Archive,
    W::Archived: for<'archive> CheckBytes<HighValidator<'archive, rancor::Error>>
        + Deserialize<W, HighDeserializer<rancor::Error>>,
{
    let Some((magic, rest)) = bytes.split_first_chunk::<8>() else {
        return Err(Report::new(ArchiveReadError::ForeignMagic {
            path: path.to_string(),
        }));
    };
    if *magic != RECORD_MAGIC {
        return Err(Report::new(ArchiveReadError::ForeignMagic {
            path: path.to_string(),
        }));
    }
    let Some((found_kind, rest)) = rest.split_first_chunk::<2>() else {
        return Err(Report::new(ArchiveReadError::ForeignMagic {
            path: path.to_string(),
        }));
    };
    let found_kind = u16::from_le_bytes(*found_kind);
    if found_kind != kind.tag() {
        return Err(Report::new(ArchiveReadError::ForeignRecordKind {
            path: path.to_string(),
            expected: kind,
            found: found_kind,
        }));
    }
    let Some((found_version, payload)) = rest.split_first_chunk::<2>() else {
        return Err(Report::new(ArchiveReadError::ForeignMagic {
            path: path.to_string(),
        }));
    };
    let found_version = u16::from_le_bytes(*found_version);
    if found_version != version {
        return Err(Report::new(ArchiveReadError::UnsupportedRecordVersion {
            path: path.to_string(),
            kind,
            found: found_version,
            supported: version,
        }));
    }
    let mut aligned = AlignedVec::<RECORD_ALIGNMENT>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    match rkyv::from_bytes::<W, rancor::Error>(&aligned) {
        Ok(wire) => Ok(wire),
        Err(error) => Err(
            Report::new(error).change_context(ArchiveReadError::InvalidRecord {
                path: path.to_string(),
                kind,
            }),
        ),
    }
}

/// Measures a section as its bytes pass: how many there were and their BLAKE3 digest.
#[derive(Debug, Clone, Default)]
pub struct SectionDigester {
    hasher: blake3::Hasher,
    length: u64,
}

impl SectionDigester {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, bytes: &[u8]) {
        let added = u64::try_from(bytes.len()).assured("supported targets address at most 64 bits");
        self.length = self
            .length
            .checked_add(added)
            .assured("a section is read from storage that holds fewer than 2^64 bytes");
        self.hasher.update(bytes);
    }

    /// The bytes measured so far.
    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn finish(&self) -> SectionDigest {
        SectionDigest::from_bytes(*self.hasher.finalize().as_bytes())
    }

    /// The digest of `bytes`, whole.
    pub fn digest_of(bytes: &[u8]) -> SectionDigest {
        let mut digester = Self::new();
        digester.update(bytes);
        digester.finish()
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator as _;

    use super::*;

    #[test]
    fn every_kind_reads_back_from_its_tag() {
        for kind in RecordKind::iter() {
            assert_eq!(RecordKind::from_repr(kind.tag()), Some(kind));
        }
        assert_eq!(RecordKind::from_repr(0), None);
    }

    #[test]
    fn the_digester_counts_and_hashes_what_passes() {
        let mut digester = SectionDigester::new();
        digester.update(b"hello ");
        digester.update(b"world");
        assert_eq!(digester.length(), 11);
        assert_eq!(
            digester.finish(),
            SectionDigester::digest_of(b"hello world")
        );
        assert_ne!(
            digester.finish(),
            SectionDigester::digest_of(b"hello there")
        );
    }
}
