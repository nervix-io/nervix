//! Bytes classified against a fixed class with vector compares, and forward scans over them.
//!
//! Layer: primitives.
//!
//! - **Owns.** A byte class as an ordinary range with excluded and listed bytes, its 64-byte block
//!   masks at the SIMD level the process selected, the first member of a buffer, and forward scans
//!   that classify each block of a buffer at most once.
//! - **Depends on.** Portable SIMD selection and the shared bitmask word.
//! - **Must not know.** The protocols whose delimiters, escapes or forbidden bytes a class names.
//!
//! A class is a type whose constants describe its members, so every kernel compares against
//! splatted constants and the compares a class does not need are not generated. One block is 64
//! bytes and its mask is one word, bit zero naming its first byte, whatever the register width of
//! the selected level.

use std::{marker::PhantomData, ops::RangeInclusive};

use fearless_simd::{Level, dispatch, prelude::*, u8x16};
use meticulous::{OptionExt as _, ResultExt as _};

use crate::{LEVEL, WORD_LANES};

/// The most bytes a class may exclude, and separately list. Each one costs one vector compare per
/// register of every block, and the scalar definition looks each up in a list this short.
pub const MAX_NAMED_BYTES: usize = 4;

/// The lanes of the narrowest register every level offers, which reads a buffer shorter than a
/// block. A buffer shorter than this register is classified byte by byte.
const NARROW_LANES: usize = 16;

/// A set of byte values that vector compares recognize at every SIMD level: every byte outside an
/// inclusive range of ordinary bytes unless it is excluded, and every listed byte.
///
/// ```
/// use std::ops::RangeInclusive;
///
/// use nervix_simd_kernels::ByteClass;
///
/// /// The bytes that end an unquoted token: anything outside printable US-ASCII, and `=`.
/// struct TokenEnd;
///
/// impl ByteClass for TokenEnd {
///     const ORDINARY: RangeInclusive<u8> = b'!'..=b'~';
///     const LISTED: &'static [u8] = b"=";
/// }
///
/// assert_eq!(TokenEnd::first_in(b"name=value"), Some(4));
/// assert_eq!(TokenEnd::first_in(b"name value"), Some(4));
/// assert_eq!(TokenEnd::first_in(b"name"), None);
/// ```
///
/// A class that names more than [`MAX_NAMED_BYTES`] excluded or listed bytes does not compile
/// where a kernel uses it:
///
/// ```compile_fail,E0080
/// use std::ops::RangeInclusive;
///
/// use nervix_simd_kernels::ByteClass;
///
/// struct TooMany;
///
/// impl ByteClass for TooMany {
///     const ORDINARY: RangeInclusive<u8> = u8::MIN..=u8::MAX;
///     const LISTED: &'static [u8] = b"abcde";
/// }
///
/// TooMany::first_in(b"bytes");
/// ```
pub trait ByteClass {
    /// Bytes inside this range are members only when [`ByteClass::LISTED`] names them.
    const ORDINARY: RangeInclusive<u8>;
    /// Bytes outside [`ByteClass::ORDINARY`] that are not members.
    const EXCLUDED: &'static [u8] = &[];
    /// Bytes inside [`ByteClass::ORDINARY`] that are members.
    const LISTED: &'static [u8] = &[];

    /// Whether `byte` is a member: the scalar definition every SIMD level reproduces.
    fn contains(byte: u8) -> bool {
        let outside = !Self::ORDINARY.contains(&byte);
        let excluded = Self::EXCLUDED.contains(&byte);
        let listed = Self::LISTED.contains(&byte);
        (outside && !excluded) || listed
    }

    /// The position of the first member of `bytes`. One call scans block after block at the SIMD
    /// level the process selected and stops at the first block holding a member. A buffer shorter
    /// than a narrow register, the size of most protocol tokens, is read byte by byte without
    /// resolving a level.
    fn first_in(bytes: &[u8]) -> Option<usize>
    where
        Self: Sized,
    {
        if bytes.len() < NARROW_LANES {
            return first_member_byte_by_byte::<Self>(bytes);
        }
        first_member::<Self>(*LEVEL.get_or_init(Level::new), bytes)
    }
}

/// The compile-time bound on a class's named bytes, evaluated where a kernel compares against
/// them. It lives outside the trait so that no implementation can replace it.
struct NamedBytesFit<C>(PhantomData<C>);

impl<C: ByteClass> NamedBytesFit<C> {
    const CHECKED: () = assert!(
        C::EXCLUDED.len() <= MAX_NAMED_BYTES && C::LISTED.len() <= MAX_NAMED_BYTES,
        "a byte class excludes, and lists, at most MAX_NAMED_BYTES bytes"
    );
}

/// The members among one register of bytes, of the native width or the narrow one.
#[inline(always)]
fn register_members<S: Simd, C: ByteClass, R: SimdBase<S, Element = u8>>(
    simd: S,
    register: R,
) -> R::Mask {
    let () = NamedBytesFit::<C>::CHECKED;
    let low = *C::ORDINARY.start();
    let high = *C::ORDINARY.end();
    let mut members = R::Mask::splat(simd, false);
    // Both edges are constants of the class, so a compare against an edge at the end of the byte
    // range, which no byte can pass, is not generated.
    if low > u8::MIN {
        members |= register.simd_lt(R::splat(simd, low));
    }
    if high < u8::MAX {
        members |= register.simd_gt(R::splat(simd, high));
    }
    for &byte in C::EXCLUDED {
        members &= !register.simd_eq(R::splat(simd, byte));
    }
    for &byte in C::LISTED {
        members |= register.simd_eq(R::splat(simd, byte));
    }
    members
}

/// One bit for each member of a 64-byte block, its first byte in the lowest bit.
#[inline(always)]
pub(crate) fn block_mask<S: Simd, C: ByteClass>(simd: S, block: &[u8; WORD_LANES]) -> u64 {
    let width = S::u8s::LEN;
    // Every native byte vector is 16, 32 or 64 lanes wide, so the registers tile the block.
    let mut mask = 0_u64;
    for (register, lanes) in block.chunks_exact(width).enumerate() {
        let vector = S::u8s::from_slice(simd, lanes);
        mask |= register_members::<S, C, S::u8s>(simd, vector).to_bitmask() << (register * width);
    }
    mask
}

/// The mask of the bytes of `bytes` from `start` to its end, fewer than 64 of them, with the bits
/// past the end clear. No byte is copied. A buffer of a block or more classifies its last 64 bytes,
/// which overlap the block before, and keeps their highest lanes. A shorter buffer is read in
/// narrow registers, the last one overlapping the one before, and one shorter than a narrow
/// register byte by byte.
#[inline(always)]
pub(crate) fn tail_mask<S: Simd, C: ByteClass>(simd: S, bytes: &[u8], start: usize) -> u64 {
    if let Some(last) = bytes.len().checked_sub(WORD_LANES) {
        let block = bytes[last..]
            .first_chunk::<WORD_LANES>()
            .verified("the buffer holds a whole block after `last`");
        // The tail is shorter than a block, so it begins after `last`.
        return block_mask::<S, C>(simd, block) >> (start - last);
    }
    let tail = &bytes[start..];
    if tail.len() < NARROW_LANES {
        return scalar_mask::<C>(tail);
    }
    let mut mask = 0_u64;
    let (registers, rest) = tail.as_chunks::<NARROW_LANES>();
    for (register, lanes) in registers.iter().enumerate() {
        let vector = u8x16::<S>::from_slice(simd, lanes);
        let members = register_members::<S, C, u8x16<S>>(simd, vector).to_bitmask();
        mask |= members << (register * NARROW_LANES);
    }
    if !rest.is_empty() {
        // The tail's last narrow register overlaps the one before it, whose members it repeats.
        let last = tail.len() - NARROW_LANES;
        let vector = u8x16::<S>::from_slice(simd, &tail[last..]);
        mask |= register_members::<S, C, u8x16<S>>(simd, vector).to_bitmask() << last;
    }
    mask
}

/// The mask of fewer than [`NARROW_LANES`] bytes, from the scalar class definition.
#[inline(always)]
fn scalar_mask<C: ByteClass>(bytes: &[u8]) -> u64 {
    let mut mask = 0_u64;
    for (lane, byte) in bytes.iter().enumerate() {
        if C::contains(*byte) {
            mask |= 1_u64 << lane;
        }
    }
    mask
}

/// The position of the lowest set bit of a nonzero mask within a block starting at `start`.
fn member_position(start: usize, members: u64) -> usize {
    let lane = usize::try_from(members.trailing_zeros()).assured("a u64 has at most 64 bits");
    start + lane
}

/// The lanes of the block beginning at `block_start` that hold a position at or after `from`. A
/// caller passes the block holding `from` or a later one, so the shift stays inside the word.
fn lanes_at_or_after(from: usize, block_start: usize) -> u64 {
    if from <= block_start {
        u64::MAX
    } else {
        u64::MAX << (from - block_start)
    }
}

/// The first member of a buffer shorter than a narrow register, read byte by byte from the scalar
/// class definition.
fn first_member_byte_by_byte<C: ByteClass>(bytes: &[u8]) -> Option<usize> {
    bytes.iter().position(|byte| C::contains(*byte))
}

/// The first member of `bytes` at the SIMD level `level`.
pub(crate) fn first_member<C: ByteClass>(level: Level, bytes: &[u8]) -> Option<usize> {
    if bytes.len() < NARROW_LANES {
        return first_member_byte_by_byte::<C>(bytes);
    }
    let found = dispatch!(level, simd => first_classified_block::<_, C>(simd, bytes, 0, 0));
    match found {
        Classified::Block { start, members } => Some(member_position(start, members)),
        Classified::MemberFreeFrom { .. } | Classified::Nothing => None,
    }
}

/// What a scanner knows about the bytes it has classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Classified {
    /// No block has been classified yet.
    Nothing,
    /// The block beginning at `start` holds `members`, and is the last block classified.
    Block { start: usize, members: u64 },
    /// No byte from `start` to the end of the buffer is a member.
    MemberFreeFrom { start: usize },
}

/// Classifies the blocks of `bytes` from the one beginning at `block_start` and answers the first
/// that holds a member at or after `from`. The answer keeps the whole mask of the block it found,
/// including members before `from`.
#[inline(always)]
fn first_classified_block<S: Simd, C: ByteClass>(
    simd: S,
    bytes: &[u8],
    from: usize,
    block_start: usize,
) -> Classified {
    let (blocks, tail) = bytes[block_start..].as_chunks::<WORD_LANES>();
    for (index, block) in blocks.iter().enumerate() {
        let start = block_start + index * WORD_LANES;
        let members = block_mask::<S, C>(simd, block);
        if members & lanes_at_or_after(from, start) != 0 {
            return Classified::Block { start, members };
        }
    }
    if !tail.is_empty() {
        let start = block_start + blocks.len() * WORD_LANES;
        let members = tail_mask::<S, C>(simd, bytes, start);
        if members & lanes_at_or_after(from, start) != 0 {
            return Classified::Block { start, members };
        }
    }
    Classified::MemberFreeFrom { start: from }
}

/// Finds the members of one class in one buffer at or after successive positions, classifying each
/// 64-byte block at most once while the positions only move forward.
///
/// A parser that consumes the buffer front to back asks for the next member after each token it
/// read. The scanner keeps the mask of the last block it classified, so a token that ends in that
/// block costs bit operations, and a long run with no member costs one vector pass that stops at
/// the block holding the next one. A position before the remembered block is answered correctly
/// by classifying again.
pub struct ByteScanner<'a, C: ByteClass> {
    bytes: &'a [u8],
    level: Level,
    classified: Classified,
    class: PhantomData<C>,
}

impl<'a, C: ByteClass> ByteScanner<'a, C> {
    /// A scanner over `bytes` at the SIMD level the process selected.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self::with_level(*LEVEL.get_or_init(Level::new), bytes)
    }

    pub(crate) fn with_level(level: Level, bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            level,
            classified: Classified::Nothing,
            class: PhantomData,
        }
    }

    /// The position of the first member at or after `from`.
    pub fn next(&mut self, from: usize) -> Option<usize> {
        if from >= self.bytes.len() {
            return None;
        }
        let block_start = from - from % WORD_LANES;
        match self.classified {
            Classified::Block { start, members } if start == block_start => {
                let later = members & lanes_at_or_after(from, start);
                if later != 0 {
                    return Some(member_position(start, later));
                }
                self.classify(from, block_start + WORD_LANES)
            }
            Classified::MemberFreeFrom { start } if start <= from => None,
            Classified::Block { .. } | Classified::MemberFreeFrom { .. } | Classified::Nothing => {
                self.classify(from, block_start)
            }
        }
    }

    /// Classifies forward from the block beginning at `block_start` for the first member at or
    /// after `from`, and remembers what it found.
    fn classify(&mut self, from: usize, block_start: usize) -> Option<usize> {
        let bytes = self.bytes;
        self.classified = if block_start >= bytes.len() {
            Classified::MemberFreeFrom { start: from }
        } else {
            dispatch!(
                self.level,
                simd => first_classified_block::<_, C>(simd, bytes, from, block_start)
            )
        };
        match self.classified {
            Classified::Block { start, members } => {
                let later = members & lanes_at_or_after(from, start);
                Some(member_position(start, later))
            }
            Classified::MemberFreeFrom { .. } | Classified::Nothing => None,
        }
    }
}

#[cfg(test)]
#[path = "byte_class_tests.rs"]
mod tests;
