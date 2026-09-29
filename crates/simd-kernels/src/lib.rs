//! Portable kernels over plain buffers and offsets.
//!
//! Layer: primitives.
//!
//! - **Owns.** Runtime SIMD selection, exact byte masks, and exact elapsed-time histograms for
//!   callers with contiguous buffers.
//! - **Depends on.** `fearless_simd`, pointer-width conversions, and self-contained error values.
//! - **Must not know.** Arrow, Nervix models, codecs, metric recorders, or the consumers of a
//!   classified buffer.

mod admission;
mod elapsed;

use error_stack::Report;
use fearless_simd::{Level, dispatch, prelude::*};
use nervix_primitives::unmodeled::sync::OnceLock;
use thiserror::Error;

pub use crate::{
    admission::AdmissionKernel,
    elapsed::{
        ElapsedBucket, ElapsedHistogram, ElapsedLayout, ElapsedLayoutError, elapsed_nanos,
        latest_instant,
    },
};

static LEVEL: OnceLock<Level> = OnceLock::new();

/// Every level the test host supports, the forced scalar fallback, and the baseline, so a kernel
/// test compares each against its scalar reference.
#[cfg(test)]
fn supported_levels() -> Vec<Level> {
    let detected = Level::new();
    let mut levels = vec![Level::fallback(), Level::baseline(), detected];
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        use fearless_simd::Simd as _;
        if let Some(token) = detected.as_sse2() {
            levels.push(token.level());
        }
        if let Some(token) = detected.as_sse4_2() {
            levels.push(token.level());
        }
        if let Some(token) = detected.as_avx2() {
            levels.push(token.level());
        }
        if let Some(token) = detected.as_avx512() {
            levels.push(token.level());
        }
    }
    levels
}

/// A byte mask for each 64-byte block and a bit for each string whose offset range contains an
/// escapable JSON byte. Byte bit zero names the first byte of a block; row bit zero names the first
/// pair of offsets. The classifier does not interpret UTF-8, so non-ASCII bytes remain verbatim.
#[derive(Debug)]
pub struct JsonEscapeClassification {
    byte_masks: Vec<u64>,
    row_masks: Vec<u64>,
}

/// Arrow-style string offsets did not describe increasing ranges inside the values buffer.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EscapeOffsetError {
    #[error("string offsets must contain at least one boundary")]
    Empty,
    #[error("string offset {index} has value {offset} outside a {bytes}-byte buffer")]
    Invalid {
        index: usize,
        offset: i32,
        bytes: usize,
    },
    #[error("string offset {index} moves backward from {previous} to {offset}")]
    Descending {
        index: usize,
        previous: i32,
        offset: i32,
    },
}

impl JsonEscapeClassification {
    /// Classifies one contiguous string-values buffer and summarizes its rows from offsets.
    /// Runtime CPU detection is cached once for the process.
    pub fn new(bytes: &[u8], offsets: &[i32]) -> error_stack::Result<Self, EscapeOffsetError> {
        let level = *LEVEL.get_or_init(Level::new);
        Self::with_level(level, bytes, offsets)
    }

    fn with_level(
        level: Level,
        bytes: &[u8],
        offsets: &[i32],
    ) -> error_stack::Result<Self, EscapeOffsetError> {
        let offsets = Self::check_offsets(offsets, bytes.len())?;
        let byte_masks = dispatch!(level, simd => Self::classify_bytes(simd, bytes));
        let row_masks = Self::summarize_rows(&offsets, &byte_masks);
        Ok(Self {
            byte_masks,
            row_masks,
        })
    }

    fn check_offsets(
        offsets: &[i32],
        bytes: usize,
    ) -> error_stack::Result<Vec<usize>, EscapeOffsetError> {
        if offsets.is_empty() {
            return Err(Report::new(EscapeOffsetError::Empty));
        }
        let mut previous = offsets[0];
        let mut positions = Vec::with_capacity(offsets.len());
        for (index, &offset) in offsets.iter().enumerate() {
            let Ok(position) = usize::try_from(offset) else {
                return Err(Report::new(EscapeOffsetError::Invalid {
                    index,
                    offset,
                    bytes,
                }));
            };
            if position > bytes {
                return Err(Report::new(EscapeOffsetError::Invalid {
                    index,
                    offset,
                    bytes,
                }));
            }
            if offset < previous {
                return Err(Report::new(EscapeOffsetError::Descending {
                    index,
                    previous,
                    offset,
                }));
            }
            previous = offset;
            positions.push(position);
        }
        Ok(positions)
    }

    #[inline(always)]
    fn classify_bytes<S: Simd>(simd: S, bytes: &[u8]) -> Vec<u64> {
        simd.vectorize(
            #[inline(always)]
            || {
                let mut masks = Vec::with_capacity(bytes.len().div_ceil(64));
                for block in bytes.chunks(64) {
                    masks.push(Self::classify_block(simd, block));
                }
                masks
            },
        )
    }

    #[inline(always)]
    fn classify_block<S: Simd>(simd: S, block: &[u8]) -> u64 {
        let quote = S::u8s::splat(simd, b'"');
        let slash = S::u8s::splat(simd, b'\\');
        let control_limit = S::u8s::splat(simd, 0x20);
        let width = S::u8s::LEN;
        let mut mask = 0_u64;
        let mut position = 0;
        while position + width <= block.len() {
            let vector = S::u8s::from_slice(simd, &block[position..position + width]);
            let escapes =
                vector.simd_eq(quote) | vector.simd_eq(slash) | vector.simd_lt(control_limit);
            mask |= escapes.to_bitmask() << position;
            position += width;
        }
        for (tail, &byte) in block[position..].iter().enumerate() {
            if matches!(byte, b'"' | b'\\') || byte < 0x20 {
                mask |= 1_u64 << (position + tail);
            }
        }
        mask
    }

    fn summarize_rows(offsets: &[usize], byte_masks: &[u64]) -> Vec<u64> {
        let rows = offsets.len() - 1;
        let mut row_masks = vec![0_u64; rows.div_ceil(64)];
        for (row, bounds) in offsets.windows(2).enumerate() {
            let start = bounds[0];
            let end = bounds[1];
            if start == end {
                continue;
            }
            let first_block = start / 64;
            let last_block = (end - 1) / 64;
            for (block, &mask) in byte_masks
                .iter()
                .enumerate()
                .take(last_block + 1)
                .skip(first_block)
            {
                let block_start = block * 64;
                let begin = start.max(block_start) - block_start;
                let finish = end.min(block_start + 64) - block_start;
                let lower = u64::MAX << begin;
                let upper = if finish == 64 {
                    u64::MAX
                } else {
                    (1_u64 << finish) - 1
                };
                if mask & lower & upper != 0 {
                    row_masks[row / 64] |= 1_u64 << (row % 64);
                    break;
                }
            }
        }
        row_masks
    }

    /// The packed byte classifications, including zero masks for clean 64-byte blocks.
    pub fn byte_masks(&self) -> &[u64] {
        &self.byte_masks
    }

    /// Whether the row identified by an offsets pair contains JSON syntax or control bytes.
    pub fn row_needs_escape(&self, row: usize) -> bool {
        self.row_masks[row / 64] & (1_u64 << (row % 64)) != 0
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::{JsonEscapeClassification, supported_levels};

    fn reference_masks(bytes: &[u8]) -> Vec<u64> {
        let mut masks = vec![0; bytes.len().div_ceil(64)];
        for (index, &byte) in bytes.iter().enumerate() {
            if matches!(byte, b'"' | b'\\') || byte < 0x20 {
                masks[index / 64] |= 1_u64 << (index % 64);
            }
        }
        masks
    }

    #[test]
    fn json_escape_masks_match_scalar_at_every_supported_level() {
        let mut rng = fastrand::Rng::with_seed(0x8502);
        for length in [
            0_usize, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1023,
        ] {
            for _ in 0..32 {
                let bytes: Vec<u8> = (0..length).map(|_| rng.u8(..)).collect();
                let offsets = [0, i32::try_from(length).assured("test input fits i32")];
                let expected = reference_masks(&bytes);
                for level in supported_levels() {
                    let classified = JsonEscapeClassification::with_level(level, &bytes, &offsets)
                        .assured("test offsets are valid");
                    assert_eq!(
                        classified.byte_masks(),
                        expected,
                        "level={level:?} len={length}"
                    );
                    assert_eq!(
                        classified.row_needs_escape(0),
                        expected.iter().any(|mask| *mask != 0)
                    );
                }
            }
        }
    }

    #[test]
    fn row_summary_respects_offsets_inside_the_same_block() {
        let mut bytes = vec![b'x'; 131];
        for index in [0, 15, 32, 63, 64, 65, 127, 130] {
            bytes[index] = b'\\';
        }
        let offsets = [0, 1, 15, 16, 31, 33, 63, 64, 65, 66, 127, 128, 130, 131];
        for level in supported_levels() {
            let classified = JsonEscapeClassification::with_level(level, &bytes, &offsets)
                .assured("test offsets are valid");
            for (row, bounds) in offsets.windows(2).enumerate() {
                let start = usize::try_from(bounds[0]).assured("test offsets are nonnegative");
                let end = usize::try_from(bounds[1]).assured("test offsets are nonnegative");
                let expected = bytes[start..end]
                    .iter()
                    .any(|&byte| matches!(byte, b'"' | b'\\') || byte < 0x20);
                assert_eq!(
                    classified.row_needs_escape(row),
                    expected,
                    "level={level:?} row={row}"
                );
            }
        }
    }

    #[test]
    fn classification_rejects_invalid_offset_boundaries() {
        for offsets in [&[][..], &[0, 3][..], &[0, -1][..], &[0, 2, 1][..]] {
            assert!(JsonEscapeClassification::new(b"ab", offsets).is_err());
        }
    }
}
