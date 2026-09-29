//! Byte-per-lane flags packed into bitmask words with vector compares.
//!
//! Layer: primitives.
//!
//! - **Owns.** Packing a run of one flag byte per lane into bitmask words at the SIMD level the
//!   process selected.
//! - **Depends on.** Portable SIMD selection.
//! - **Must not know.** Arrow, validity bitmaps, or what a flag records.
//!
//! A caller that computes a condition for each lane writes it as a byte, which a vectorized loop
//! stores without leaving its vector registers. Packing those bytes is a vector compare and a
//! bitmask extraction per register, so no lane is shifted into its word one at a time.

use fearless_simd::{Level, dispatch, prelude::*};

use crate::LEVEL;

/// The lanes one bitmask word covers.
pub const WORD_LANES: usize = 64;

/// Packs runs of flag bytes into bitmask words, lane zero of each word in its lowest bit, at the
/// SIMD level the process selected. A flag is set when its byte is not zero.
///
/// The level is resolved when the packer is made, and one call packs a whole run, so a caller
/// pays the dispatch once for as many lanes as it hands over.
#[derive(Debug, Clone, Copy)]
pub struct FlagPacker {
    level: Level,
}

impl FlagPacker {
    pub fn new() -> Self {
        Self {
            level: *LEVEL.get_or_init(Level::new),
        }
    }

    /// Appends one word to `words` for every [`WORD_LANES`] flags of `flags`, and one for the
    /// flags that remain. A word for fewer lanes has every bit past its last lane clear.
    pub fn pack(self, flags: &[u8], words: &mut Vec<u64>) {
        dispatch!(self.level, simd => pack_run(simd, flags, words));
    }
}

impl Default for FlagPacker {
    fn default() -> Self {
        Self::new()
    }
}

/// A word with a set bit for each of its first `lanes` lanes, and every bit for a count of
/// [`WORD_LANES`] or more.
pub fn lane_mask(lanes: usize) -> u64 {
    if lanes >= WORD_LANES {
        u64::MAX
    } else {
        (1_u64 << lanes) - 1
    }
}

#[inline(always)]
fn pack_run<S: Simd>(simd: S, flags: &[u8], words: &mut Vec<u64>) {
    let mut blocks = flags.chunks_exact(WORD_LANES);
    for block in blocks.by_ref() {
        words.push(pack_block(simd, block));
    }
    let tail = blocks.remainder();
    if !tail.is_empty() {
        // Clear bytes pad the last word, so its lanes past the run pack as clear bits.
        let mut padded = [0_u8; WORD_LANES];
        padded[..tail.len()].copy_from_slice(tail);
        words.push(pack_block(simd, &padded));
    }
}

/// The word of exactly [`WORD_LANES`] flags.
#[inline(always)]
fn pack_block<S: Simd>(simd: S, block: &[u8]) -> u64 {
    let clear = S::u8s::splat(simd, 0);
    let width = S::u8s::LEN;
    // Every native byte vector is 16, 32 or 64 lanes wide, so the registers tile the block.
    let register_lanes = lane_mask(width);
    let mut word = 0_u64;
    for (register, flags) in block.chunks_exact(width).enumerate() {
        let vector = S::u8s::from_slice(simd, flags);
        let unset = vector.simd_eq(clear).to_bitmask();
        word |= (!unset & register_lanes) << (register * width);
    }
    word
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supported_levels;

    /// Sets each lane's bit with its own shift, the work packing replaces.
    fn scalar_words(flags: &[u8]) -> Vec<u64> {
        let mut words = vec![0_u64; flags.len().div_ceil(WORD_LANES)];
        for (lane, flag) in flags.iter().enumerate() {
            if *flag != 0 {
                words[lane / WORD_LANES] |= 1_u64 << (lane % WORD_LANES);
            }
        }
        words
    }

    /// Runs over three words and one lane, holding clear, full, alternating, single-lane,
    /// single-hole and random blocks, where any nonzero byte counts as a set flag.
    fn runs() -> Vec<Vec<u8>> {
        let mut rng = fastrand::Rng::with_seed(0x0006_F1A6);
        let lanes = 3 * WORD_LANES + 1;
        let mut runs = vec![vec![0_u8; lanes], vec![1_u8; lanes], vec![0xFF_u8; lanes]];
        runs.push((0..lanes).map(|lane| u8::from(lane % 2 == 1)).collect());
        for lane in [0, 1, 31, 32, 63, 64, 65, 127, 128, 191, 192] {
            let mut single = vec![0_u8; lanes];
            single[lane] = 1;
            runs.push(single);
            let mut hole = vec![1_u8; lanes];
            hole[lane] = 0;
            runs.push(hole);
        }
        for _ in 0..32 {
            let random = (0..lanes)
                .map(|_| if rng.u8(..4) == 0 { rng.u8(1..) } else { 0 })
                .collect();
            runs.push(random);
        }
        runs
    }

    #[test]
    fn packed_words_match_the_scalar_shifts_for_every_run_length_at_every_level() {
        for level in supported_levels() {
            let packer = FlagPacker { level };
            for run in runs() {
                for lanes in 0..=run.len() {
                    let mut words = Vec::new();
                    packer.pack(&run[..lanes], &mut words);
                    assert_eq!(
                        words,
                        scalar_words(&run[..lanes]),
                        "level={level:?} lanes={lanes} run={run:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn packing_appends_to_the_words_already_held() {
        let mut words = vec![7_u64];
        FlagPacker::default().pack(&[1, 0, 1], &mut words);
        FlagPacker::new().pack(&[], &mut words);
        assert_eq!(words, [7, 0b101]);
    }

    #[test]
    fn lane_masks_cover_the_first_lanes_of_a_word() {
        assert_eq!(lane_mask(0), 0);
        assert_eq!(lane_mask(3), 0b111);
        assert_eq!(lane_mask(WORD_LANES), u64::MAX);
        assert_eq!(lane_mask(WORD_LANES + 1), u64::MAX);
    }
}
