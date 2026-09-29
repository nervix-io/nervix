//! The backup statements and the summary of the archive a backup assembles.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The `BACKUP` and `DESCRIBE BACKUP` Models, the choice of whether resource bytes
//!   travel in an archive, and the summary a completed backup reports and its outcome records.
//! - **Depends on.** Vocabulary names and timestamps.
//! - **Must not know.** The archive's encoding, how a backup reads cluster state, where an archive
//!   is staged, or how a client stores it.

use std::{fmt, num::NonZeroU64};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use crate::{DomainName, InspectionFormat, Timestamp};

/// `BACKUP CLUSTER TO '<file>' [WITHOUT RESOURCES]` or
/// `BACKUP DOMAIN [<domain>] TO '<file>' [WITHOUT RESOURCES]`.
///
/// The leader assembles the archive; the client that sent the statement writes it to
/// `destination`, which the server never reads.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct Backup {
    pub scope: BackupScope,
    /// The local file the client writes the archive to.
    pub destination: String,
    pub resources: BackupResources,
}

/// What a backup covers.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum BackupScope {
    /// Every user and every domain.
    Cluster,
    /// One domain. Absent names the domain the session has selected.
    Domain(Option<DomainName>),
}

/// Whether an archive carries the bytes of every resource version.
///
/// Either way the archive records each version's catalog metadata and checksums. `WITHOUT
/// RESOURCES` leaves out only the original version archives.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum BackupResources {
    Included,
    /// `WITHOUT RESOURCES`.
    Omitted,
}

/// `DESCRIBE BACKUP '<file>' [FORMAT TEXT | JSON]`, which a client answers from a local archive
/// without asking a server.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeBackup {
    /// The local archive file to read.
    pub source: String,
    pub format: InspectionFormat,
}

/// The BLAKE3 digest of a complete archive file.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct ArchiveDigest([u8; 32]);

impl ArchiveDigest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A digest reads as lowercase hexadecimal, the form `b3sum` prints.
impl fmt::Display for ArchiveDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ArchiveDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ArchiveDigest({self})")
    }
}

/// What a completed backup assembled, as its command outcome reports and records it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct BackupArchiveSummary {
    /// The exact size of the archive file.
    pub total_bytes: NonZeroU64,
    pub digest: ArchiveDigest,
    /// When the archive's contents were read.
    pub captured_at: Timestamp,
    /// The instant after which the archive can no longer be downloaded.
    pub retained_until: Timestamp,
    pub resources: BackupResources,
    /// The users the archive holds. Absent when it holds no users section, which is every domain
    /// backup.
    pub users: Option<u64>,
    /// Every domain the archive holds, in name order.
    pub domains: Vec<BackupDomainSummary>,
}

/// One domain of a completed backup.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct BackupDomainSummary {
    pub domain: DomainName,
    /// The applied consensus revision the domain's configuration was read at.
    pub revision: u64,
    /// The sections the archive holds for the domain.
    pub sections: u64,
    /// The bytes those sections hold.
    pub section_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_digest_displays_as_lowercase_hexadecimal() {
        let mut bytes = [0_u8; 32];
        bytes[0] = 0xab;
        bytes[31] = 0x01;
        let digest = ArchiveDigest::from_bytes(bytes);
        let rendered = digest.to_string();
        assert_eq!(rendered.len(), 64);
        assert!(rendered.starts_with("ab00"));
        assert!(rendered.ends_with("0001"));
        assert_eq!(format!("{digest:?}"), format!("ArchiveDigest({rendered})"));
        assert_eq!(digest.as_bytes(), &bytes);
    }
}
