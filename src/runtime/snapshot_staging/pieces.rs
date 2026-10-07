//! An artifact assembled from bounded pieces that are staged one at a time.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The ordered pieces of one artifact on quota-owned disk, their total length, and
//!   concatenating them into the artifact one bounded chunk at a time.
//! - **Depends on.** The staging quota and its bounded staged reads and writes.
//! - **Must not know.** What the pieces encode or which checkpoint they assemble.

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_execution::ChargedBytes;

use super::{READ_BLOCK_BYTES, SnapshotStaging, SnapshotStagingError, StagedArtifact};

/// The pieces of one artifact, each staged as soon as it was encoded so that its memory charge
/// ends there. Concatenation then holds one 64 KiB chunk at a time, so the artifact never exists
/// whole in memory; its pieces and the artifact briefly occupy twice its disk space.
pub(crate) struct StagedPieces {
    staging: SnapshotStaging,
    pieces: Vec<StagedArtifact>,
    length: u64,
}

/// Where a list of staged pieces ended at one moment: how many pieces it held and their bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StagedPiecesMark {
    pieces: usize,
    length: u64,
}

impl StagedPieces {
    pub(in crate::runtime) fn new(staging: SnapshotStaging) -> Self {
        Self {
            staging,
            pieces: Vec::new(),
            length: 0,
        }
    }

    /// The bytes every staged piece holds together.
    pub(crate) fn length(&self) -> u64 {
        self.length
    }

    /// Stage one bounded encoded piece behind the pieces before it, refusing rather than waiting
    /// when the node's staging quota cannot hold it now.
    pub(crate) async fn stage(
        &mut self,
        bytes: ChargedBytes,
    ) -> Result<(), Report<SnapshotStagingError>> {
        if bytes.is_empty() {
            return Ok(());
        }
        let length: u64 = bytes.len().arch_into();
        let mut writer = self.staging.try_stage(length).await?;
        writer.write_chunk(bytes).await?;
        let piece = writer.finish_artifact().await?;
        self.length = self
            .length
            .checked_add(length)
            .assured("staged pieces hold the node's staging quota, which is far below u64::MAX");
        self.pieces.push(piece);
        Ok(())
    }

    /// Stage one bounded encoded piece ahead of every piece staged before it, such as a header
    /// that counts what follows it.
    pub(crate) async fn stage_first(
        &mut self,
        bytes: ChargedBytes,
    ) -> Result<(), Report<SnapshotStagingError>> {
        let mut first = Self::new(self.staging.clone());
        first.stage(bytes).await?;
        let rest = std::mem::replace(&mut self.pieces, first.pieces);
        self.pieces.extend(rest);
        self.length = self
            .length
            .checked_add(first.length)
            .assured("staged pieces hold the node's staging quota, which is far below u64::MAX");
        Ok(())
    }

    /// Where the pieces staged so far end, for [`Self::rewind`] to return to.
    pub(crate) fn mark(&self) -> StagedPiecesMark {
        StagedPiecesMark {
            pieces: self.pieces.len(),
            length: self.length,
        }
    }

    /// Drop every piece staged behind `mark`, releasing its file and its staging quota, so that
    /// what was abandoned half staged can be staged again from there.
    pub(crate) fn rewind(&mut self, mark: StagedPiecesMark) {
        self.pieces.truncate(mark.pieces);
        self.length = mark.length;
    }

    /// Append every piece of `other` behind these, in its order.
    pub(crate) fn extend(&mut self, other: Self) {
        self.length = self
            .length
            .checked_add(other.length)
            .assured("staged pieces hold the node's staging quota, which is far below u64::MAX");
        self.pieces.extend(other.pieces);
    }

    /// Concatenate every piece, in order, into one artifact. Each piece is released as soon as it
    /// has been copied.
    pub(crate) async fn concatenate(self) -> Result<StagedArtifact, Report<SnapshotStagingError>> {
        let mut writer = self.staging.try_stage(self.length).await?;
        for piece in self.pieces {
            nervix_primitives::task::consume_budget().await;
            let mut reader = piece.open_reader().await?;
            while let Some(chunk) = reader.next_chunk(READ_BLOCK_BYTES).await? {
                nervix_primitives::task::consume_budget().await;
                writer.write_chunk(chunk).await?;
            }
        }
        writer.finish_artifact().await
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_execution::{ExecutionConfig, Executor, MemoryClass};

    use super::*;
    use crate::runtime::snapshot_staging::SnapshotStagingLimits;

    async fn charged(executor: &Executor, bytes: Vec<u8>) -> ChargedBytes {
        executor
            .charge_owned(MemoryClass::Bulk, bytes)
            .await
            .assured("a small test piece is admitted")
    }

    /// Pieces staged in two lists and joined keep their order, length and digest, and release
    /// every byte of their staging quota and memory once the artifact is dropped.
    #[nervix_primitives::test]
    async fn pieces_concatenate_in_order_into_one_artifact() {
        let directory = tempfile::tempdir().assured("the staging directory opens");
        let executor = Executor::new(ExecutionConfig::default()).assured("default limits validate");
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits {
                staging_bytes: 8 * 1024 * 1024,
                snapshot_bytes: 8 * 1024 * 1024,
            },
        );
        let large = vec![9_u8; 150 * 1024];
        let mut first = StagedPieces::new(staging.clone());
        first
            .stage(charged(&executor, b"body".to_vec()).await)
            .await
            .assured("the first piece stages");
        first
            .stage(charged(&executor, Vec::new()).await)
            .await
            .assured("an empty piece stages nothing");
        first
            .stage_first(charged(&executor, b"header".to_vec()).await)
            .await
            .assured("a header stages ahead of the pieces before it");
        let mut second = StagedPieces::new(staging.clone());
        second
            .stage(charged(&executor, large.clone()).await)
            .await
            .assured("a piece larger than one chunk stages");
        second
            .stage(charged(&executor, b"tail".to_vec()).await)
            .await
            .assured("the last piece stages");
        first.extend(second);
        let mut expected = b"headerbody".to_vec();
        expected.extend_from_slice(&large);
        expected.extend_from_slice(b"tail");
        assert_eq!(
            first.length(),
            u64::try_from(expected.len()).assured("fits")
        );
        let artifact = first.concatenate().await.assured("the pieces concatenate");
        assert_eq!(
            artifact.length(),
            u64::try_from(expected.len()).assured("fits")
        );
        assert_eq!(artifact.digest(), *blake3::hash(&expected).as_bytes());
        assert_eq!(
            std::fs::read(artifact.path()).assured("the artifact reads"),
            expected
        );
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        drop(artifact);
        assert_eq!(
            std::fs::read_dir(directory.path())
                .assured("the staging directory exists")
                .count(),
            0,
            "every piece and the artifact released their files"
        );
        staging
            .try_stage(8 * 1024 * 1024)
            .await
            .assured("every piece and the artifact released their staging quota");
    }

    /// Rewinding to a mark drops what was staged behind it, with its files and quota, and the
    /// pieces staged again from there concatenate as if nothing had been abandoned.
    #[nervix_primitives::test]
    async fn rewinding_drops_the_pieces_staged_behind_a_mark() {
        const ABANDONED_BYTES: usize = 3 * 512 * 1024;
        let directory = tempfile::tempdir().assured("the staging directory opens");
        let executor = Executor::new(ExecutionConfig::default()).assured("default limits validate");
        // The quota is granted in whole mebibytes: one for the kept piece and two for a piece of
        // one and a half, so a second such piece fits only once the first released its share.
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits {
                staging_bytes: 3 * 1024 * 1024,
                snapshot_bytes: 3 * 1024 * 1024,
            },
        );
        let mut pieces = StagedPieces::new(staging);
        pieces
            .stage(charged(&executor, b"kept".to_vec()).await)
            .await
            .assured("the kept piece stages");
        let mark = pieces.mark();
        pieces
            .stage(charged(&executor, vec![7_u8; ABANDONED_BYTES]).await)
            .await
            .assured("the abandoned piece stages");
        let abandoned = u64::try_from(ABANDONED_BYTES).assured("a test length fits 64 bits");
        assert_eq!(pieces.length(), 4 + abandoned);

        pieces.rewind(mark);
        assert_eq!(pieces.length(), 4);
        assert_eq!(
            std::fs::read_dir(directory.path())
                .assured("the staging directory exists")
                .count(),
            1,
            "the abandoned piece released its file"
        );
        pieces
            .stage(charged(&executor, vec![8_u8; ABANDONED_BYTES]).await)
            .await
            .assured("the abandoned piece released its quota");

        pieces.rewind(mark);
        pieces
            .stage(charged(&executor, b"again".to_vec()).await)
            .await
            .assured("the piece stages again");
        let artifact = pieces.concatenate().await.assured("the pieces concatenate");
        assert_eq!(
            std::fs::read(artifact.path()).assured("the artifact reads"),
            b"keptagain"
        );
    }
}
