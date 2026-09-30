//! Checked numeric execution over Arrow buffers.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The checked lanes a buffer kernel computes with, which the datetime kernels share,
//!   and the batch kernels behind every numeric operator and numeric builtin: integer and
//!   floating-point arithmetic, negation, comparison, `abs`, `sign`, rounding and truncation,
//!   floating-point classification, the math functions, and, in its submodules, integer bit
//!   operations and rounding to decimal digits. Each kernel is one pass over its operands' value
//!   buffers that yields the result column, the lanes whose operation failed, and why a failed lane
//!   failed.
//! - **Depends on.** Arrow arrays and buffers, the value contracts of the semantic catalog, the
//!   side-error reasons of the VM, and the flag packing and checked integer arithmetic of the SIMD
//!   kernel crate.
//! - **Must not know.** Registers, programs, spans, or how a failed lane is recorded as a row
//!   error.
//!
//! A kernel computes a block of up to 1,024 lanes in one branch-free loop that stores every lane's
//! value and a byte saying whether the lane failed, so the loop vectorizes wherever the lane
//! operation itself does. One call to the kernel crate then packs the block's failure bytes into
//! bitmap words with vector compares, so no lane shifts its failure into a word. A kernel never
//! reruns a batch: the failure bitmap, restricted to lanes whose operands are valid, is the only
//! record of a failure, and its set bits are the only lanes an error is built for.
//!
//! Checked integer addition and subtraction of every width, and multiplication of 8-, 16- and
//! 32-bit integers, are explicit SIMD: the kernel crate computes them in vector registers at the
//! level the process selected, with a scalar fallback, and hands back each lane's value together
//! with the failure words, so they need no packing call. Every other lane loop is LLVM's
//! auto-vectorization: no lane operation of this module names a SIMD instruction set or
//! intrinsic. A loop whose lane operation compiles to a few instructions, such as a negation, a
//! comparison, `sqrt`, `trunc` or a multiplication by a constant, is written so the compiler can
//! widen it to the vector instructions of the CPU the binary targets, and its results are the same
//! whether or not it does. The failure packing is explicit SIMD, selected at run time with a scalar
//! fallback, and gives the same word at every level. A function the platform math library
//! computes, such as `sin` or `log2`, is an opaque call per lane that no loop vectorizes, so those
//! kernels read validity a word at a time and compute valid lanes only.

use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use arch_into::ArchInto as _;
use arrow_array::{Array, ArrowPrimitiveType, BooleanArray, PrimitiveArray, types::Float64Type};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, NullBuffer, ScalarBuffer,
    bit_iterator::BitIndexIterator,
};
use nervix_approx_into::ApproxInto as _;
use nervix_simd_kernels::{
    CheckedArithmetic, CheckedLanes, FlagPacker, LaneOperands, WORD_LANES, lane_mask,
};

use crate::{
    batch::TypedArray,
    error::{DivisionOperation, FloatOperation, IntegerOperation, SideErrorReason},
    operand::Operand,
    semantics::{ClassifiedFloat, FloatClass},
};

mod bitwise;
mod decimal_rounding;

pub(crate) use bitwise::{Shift, ShiftCounts, ShiftedInteger, bit_count, bitwise_complement};
pub(crate) use decimal_rounding::{DecimalRounding, IntegerRounding, RoundingDigits};

/// The lanes whose failure bytes one call to the kernel crate packs: sixteen bitmap words, so the
/// call is paid once per 1,024 lanes rather than once per word.
const BLOCK_LANES: usize = 16 * WORD_LANES;

/// The values a lane operation computed for every lane of a batch, and which lanes failed.
///
/// The value of a failed lane is whatever the operation produced while detecting the failure, such
/// as a wrapped integer. [`Checked::from_lanes`] replaces it before a column is exposed.
pub(crate) struct Lanes<N> {
    values: Vec<N>,
    failed: BooleanBuffer,
}

impl<N: Copy + Default> Lanes<N> {
    /// Computes `lane` for every operand. The lanes of each block run in one loop with a fixed,
    /// branch-free body, which the compiler vectorizes whenever `lane` is vectorizable, and one call
    /// packs the block's failure bytes into its failure words through vector compares.
    pub(crate) fn unary<I: Copy>(operands: &[I], mut lane: impl FnMut(I) -> (N, bool)) -> Self {
        let packer = FlagPacker::new();
        let mut values = vec![N::default(); operands.len()];
        let mut words = Vec::with_capacity(operands.len().div_ceil(WORD_LANES));
        let mut flags = [0_u8; BLOCK_LANES];
        let value_blocks = values.chunks_mut(BLOCK_LANES);
        let operand_blocks = operands.chunks(BLOCK_LANES);
        for (value_block, operand_block) in value_blocks.zip(operand_blocks) {
            let block_flags = &mut flags[..value_block.len()];
            Self::unary_run(value_block, operand_block, block_flags, &mut lane);
            packer.pack(block_flags, &mut words);
        }
        Self::new(values, words)
    }

    /// Computes `lane` for every pair of operands, one block of lanes at a time as
    /// [`Lanes::unary`] does. Both operand slices hold one value per lane of the same batch, so
    /// they have the same length.
    pub(crate) fn binary<L: Copy, R: Copy>(
        left: &[L],
        right: &[R],
        mut lane: impl FnMut(L, R) -> (N, bool),
    ) -> Self {
        let packer = FlagPacker::new();
        let mut values = vec![N::default(); left.len()];
        let mut words = Vec::with_capacity(left.len().div_ceil(WORD_LANES));
        let mut flags = [0_u8; BLOCK_LANES];
        let value_blocks = values.chunks_mut(BLOCK_LANES);
        let left_blocks = left.chunks(BLOCK_LANES);
        let right_blocks = right.chunks(BLOCK_LANES);
        for ((value_block, left_block), right_block) in
            value_blocks.zip(left_blocks).zip(right_blocks)
        {
            let block_flags = &mut flags[..value_block.len()];
            Self::binary_run(value_block, left_block, right_block, block_flags, &mut lane);
            packer.pack(block_flags, &mut words);
        }
        Self::new(values, words)
    }

    /// Computes `lane` for every lane of a binary operation's operands. A shared operand is read
    /// once and folded into the lane operation, so a run combined with a constant is one pass over
    /// the run, which the compiler vectorizes as it does a pass over two runs.
    pub(crate) fn of_operands<V: Copy>(
        operands: LaneOperands<'_, V>,
        mut lane: impl FnMut(V, V) -> (N, bool),
    ) -> Self {
        match operands {
            LaneOperands::Runs { left, right } => Self::binary(left, right, lane),
            LaneOperands::SharedLeft { left, right } => {
                Self::unary(right, |right| lane(left, right))
            }
            LaneOperands::SharedRight { left, right } => {
                Self::unary(left, |left| lane(left, right))
            }
        }
    }

    /// Computes `lane` only for the lanes `valid` marks valid, leaving every other lane at its
    /// default value and unfailed.
    ///
    /// Validity is read one word at a time. A word with no valid lane computes nothing and fails
    /// no lane. A word whose lanes are all valid runs the loop [`Lanes::unary`] runs, and any other
    /// word computes its valid lanes only, with the failure bytes of the rest cleared. The words'
    /// failure bytes fill the same blocks [`Lanes::unary`] packs, so no lane writes a failure word
    /// and a word with few valid lanes pays for no call of its own.
    pub(crate) fn unary_valid<I: Copy>(
        operands: &[I],
        valid: &NullBuffer,
        mut lane: impl FnMut(I) -> (N, bool),
    ) -> Self {
        let packer = FlagPacker::new();
        let mut values = vec![N::default(); operands.len()];
        let mut words = Vec::with_capacity(operands.len().div_ceil(WORD_LANES));
        let mut flags = [0_u8; BLOCK_LANES];
        let mut block_lanes = 0;
        let validity = valid.inner().bit_chunks();
        let value_words = values.chunks_mut(WORD_LANES);
        let operand_words = operands.chunks(WORD_LANES);
        let valid_words = validity.iter_padded();
        for ((value_word, operand_word), valid_bits) in
            value_words.zip(operand_words).zip(valid_words)
        {
            let word_end = block_lanes + value_word.len();
            let word_flags = &mut flags[block_lanes..word_end];
            match WordValidity::of(valid_bits, value_word.len()) {
                WordValidity::Empty => word_flags.fill(0),
                WordValidity::Full => {
                    Self::unary_run(value_word, operand_word, word_flags, &mut lane);
                }
                WordValidity::Partial(valid_bits) => {
                    word_flags.fill(0);
                    for index in SetLanes(valid_bits) {
                        let (result, failed) = lane(operand_word[index]);
                        value_word[index] = result;
                        word_flags[index] = u8::from(failed);
                    }
                }
            }
            block_lanes = word_end;
            if block_lanes == BLOCK_LANES {
                packer.pack(&flags, &mut words);
                block_lanes = 0;
            }
        }
        packer.pack(&flags[..block_lanes], &mut words);
        Self::new(values, words)
    }

    /// Computes `lane` only for the pairs of operands `valid` marks valid, reading validity one
    /// word at a time as [`Lanes::unary_valid`] does.
    pub(crate) fn binary_valid<L: Copy, R: Copy>(
        left: &[L],
        right: &[R],
        valid: &NullBuffer,
        mut lane: impl FnMut(L, R) -> (N, bool),
    ) -> Self {
        let packer = FlagPacker::new();
        let mut values = vec![N::default(); left.len()];
        let mut words = Vec::with_capacity(left.len().div_ceil(WORD_LANES));
        let mut flags = [0_u8; BLOCK_LANES];
        let mut block_lanes = 0;
        let validity = valid.inner().bit_chunks();
        let value_words = values.chunks_mut(WORD_LANES);
        let left_words = left.chunks(WORD_LANES);
        let right_words = right.chunks(WORD_LANES);
        let valid_words = validity.iter_padded();
        for (((value_word, left_word), right_word), valid_bits) in value_words
            .zip(left_words)
            .zip(right_words)
            .zip(valid_words)
        {
            let word_end = block_lanes + value_word.len();
            let word_flags = &mut flags[block_lanes..word_end];
            match WordValidity::of(valid_bits, value_word.len()) {
                WordValidity::Empty => word_flags.fill(0),
                WordValidity::Full => {
                    Self::binary_run(value_word, left_word, right_word, word_flags, &mut lane);
                }
                WordValidity::Partial(valid_bits) => {
                    word_flags.fill(0);
                    for index in SetLanes(valid_bits) {
                        let (result, failed) = lane(left_word[index], right_word[index]);
                        value_word[index] = result;
                        word_flags[index] = u8::from(failed);
                    }
                }
            }
            block_lanes = word_end;
            if block_lanes == BLOCK_LANES {
                packer.pack(&flags, &mut words);
                block_lanes = 0;
            }
        }
        packer.pack(&flags[..block_lanes], &mut words);
        Self::new(values, words)
    }

    /// Computes `lane` for every lane of one run, storing each lane's value and failure byte.
    fn unary_run<I: Copy>(
        values: &mut [N],
        operands: &[I],
        flags: &mut [u8],
        lane: &mut impl FnMut(I) -> (N, bool),
    ) {
        let run_lanes = values.iter_mut().zip(operands).zip(flags);
        for ((value, operand), flag) in run_lanes {
            let (result, failed) = lane(*operand);
            *value = result;
            *flag = u8::from(failed);
        }
    }

    /// Computes `lane` for every pair of operands of one run, storing each lane's value and
    /// failure byte.
    fn binary_run<L: Copy, R: Copy>(
        values: &mut [N],
        left: &[L],
        right: &[R],
        flags: &mut [u8],
        lane: &mut impl FnMut(L, R) -> (N, bool),
    ) {
        let operands = left.iter().zip(right);
        let run_lanes = values.iter_mut().zip(operands).zip(flags);
        for ((value, (left, right)), flag) in run_lanes {
            let (result, failed) = lane(*left, *right);
            *value = result;
            *flag = u8::from(failed);
        }
    }

    fn new(values: Vec<N>, words: Vec<u64>) -> Self {
        let lanes = values.len();
        Self {
            values,
            failed: BooleanBuffer::new(Buffer::from_vec(words), 0, lanes),
        }
    }
}

/// The lanes an explicit SIMD kernel computed already carry their failures as bitmap words, so
/// they become a batch's lanes without a packing pass of their own.
impl<N: Copy + Default> From<CheckedLanes<N>> for Lanes<N> {
    fn from(lanes: CheckedLanes<N>) -> Self {
        Self::new(lanes.values, lanes.failed)
    }
}

/// Which lanes of one word have valid operands, which decides how the word is computed.
enum WordValidity {
    /// No lane is valid, so no lane is computed and none fails.
    Empty,
    /// Every lane is valid, so the word runs the loop the compiler can vectorize.
    Full,
    /// The set bits are the valid lanes, and only those lanes are computed.
    Partial(u64),
}

impl WordValidity {
    /// Classifies the validity bits of a word of `lanes` lanes. The bits of lanes past the end of
    /// the batch are clear, as the bitmap's padded remainder leaves them.
    fn of(valid_bits: u64, lanes: usize) -> Self {
        if valid_bits == 0 {
            Self::Empty
        } else if valid_bits == lane_mask(lanes) {
            Self::Full
        } else {
            Self::Partial(valid_bits)
        }
    }
}

/// The set lanes of one validity word, lowest first.
struct SetLanes(u64);

impl Iterator for SetLanes {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        let lane = self.0.trailing_zeros();
        // Clearing the lowest set bit moves to the next valid lane.
        self.0 &= self.0 - 1;
        Some(lane.arch_into())
    }
}

/// A column a checked kernel computed, and the lanes whose operation failed.
///
/// A failed lane is null in the column and holds the default value of the type, so no wrapped or
/// otherwise meaningless result survives in the buffer. A lane with a null operand is null too, but
/// it never fails: there was no value to apply the operation to.
pub(crate) struct Checked<T: ArrowPrimitiveType> {
    pub(crate) column: PrimitiveArray<T>,
    pub(crate) failed: FailedLanes,
}

impl<T: ArrowPrimitiveType> Checked<T> {
    /// The result of an operation whose scalar operand is null: every lane is null, and no lane
    /// failed because no lane had a value to operate on.
    fn all_null(lanes: usize) -> Self {
        Self {
            column: PrimitiveArray::new_null(lanes),
            failed: FailedLanes(None),
        }
    }

    pub(crate) fn from_lanes(lanes: Lanes<T::Native>, operand_nulls: Option<NullBuffer>) -> Self {
        let Lanes { mut values, failed } = lanes;
        let Some(failed) = FailedLanes::among_valid(failed, operand_nulls.as_ref()) else {
            return Self {
                column: PrimitiveArray::new(ScalarBuffer::from(values), operand_nulls),
                failed: FailedLanes(None),
            };
        };
        for lane in failed.set_indices() {
            values[lane] = T::Native::default();
        }
        let succeeded = !&failed;
        let valid = match &operand_nulls {
            Some(operand_nulls) => operand_nulls.inner() & &succeeded,
            None => succeeded,
        };
        Self {
            column: PrimitiveArray::new(ScalarBuffer::from(values), Some(NullBuffer::new(valid))),
            failed: FailedLanes(Some(failed)),
        }
    }
}

/// The lanes of one batch whose checked operation failed. It holds no bitmap when no lane failed,
/// which is the common case and costs no allocation.
pub(crate) struct FailedLanes(Option<BooleanBuffer>);

impl FailedLanes {
    /// Restricts the lanes an operation marked failed to those whose operands are valid, since a
    /// null lane's operands are not values, and answers `None` when none of them failed.
    fn among_valid(
        failed: BooleanBuffer,
        operand_nulls: Option<&NullBuffer>,
    ) -> Option<BooleanBuffer> {
        if failed.count_set_bits() == 0 {
            return None;
        }
        let failed = match operand_nulls {
            Some(operand_nulls) => &failed & operand_nulls.inner(),
            None => failed,
        };
        if failed.count_set_bits() == 0 {
            return None;
        }
        Some(failed)
    }

    /// Every failed lane, in ascending order.
    pub(crate) fn lanes(&self) -> BitIndexIterator<'_> {
        match &self.0 {
            Some(failed) => failed.set_indices(),
            None => BitIndexIterator::new(&[], 0, 0),
        }
    }
}

/// A binary arithmetic operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arithmetic {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl Arithmetic {
    /// Applies this operator to two integer operands of one batch. Every lane whose result does
    /// not fit the type fails, as does every quotient or remainder by zero.
    pub(crate) fn evaluate_integers<T>(
        self,
        left: Operand<'_, PrimitiveArray<T>>,
        right: Operand<'_, PrimitiveArray<T>>,
    ) -> Checked<T>
    where
        T: ArrowPrimitiveType,
        T::Native: CheckedInteger,
    {
        // Each arm hands over its own operation: a SIMD kernel's, or a lane function that is
        // inlined into a loop the compiler can vectorize. Selecting a function first would coerce
        // the five into one function pointer and call it once per lane.
        match self {
            Self::Add => evaluate_operands(left, right, T::Native::sums),
            Self::Sub => evaluate_operands(left, right, T::Native::differences),
            Self::Mul => evaluate_operands(left, right, T::Native::products),
            Self::Div => evaluate_binary(left, right, T::Native::lane_quotient),
            Self::Rem => evaluate_binary(left, right, T::Native::lane_remainder),
        }
    }

    /// Applies this operator to two float operands of one batch at their own width. Every lane
    /// whose result is NaN or an infinity fails.
    pub(crate) fn evaluate_floats<T>(
        self,
        left: Operand<'_, PrimitiveArray<T>>,
        right: Operand<'_, PrimitiveArray<T>>,
    ) -> Checked<T>
    where
        T: ArrowPrimitiveType,
        T::Native: CheckedFloat,
    {
        match self {
            Self::Add => evaluate_binary(left, right, |left, right| (left + right).finite_lane()),
            Self::Sub => evaluate_binary(left, right, |left, right| (left - right).finite_lane()),
            Self::Mul => evaluate_binary(left, right, |left, right| (left * right).finite_lane()),
            Self::Div => evaluate_binary(left, right, |left, right| (left / right).finite_lane()),
            Self::Rem => evaluate_binary(left, right, |left, right| (left % right).finite_lane()),
        }
    }

    /// Why an integer lane of this operator failed, given the lane's right operand. Only a quotient
    /// and a remainder divide, and a remainder by a nonzero divisor always exists.
    pub(crate) fn integer_failure<N: CheckedInteger>(self, right: N) -> SideErrorReason {
        match self {
            Self::Add => SideErrorReason::IntegerOverflow(IntegerOperation::Addition),
            Self::Sub => SideErrorReason::IntegerOverflow(IntegerOperation::Subtraction),
            Self::Mul => SideErrorReason::IntegerOverflow(IntegerOperation::Multiplication),
            Self::Div => {
                if right.is_zero_divisor() {
                    SideErrorReason::DivisionByZero(DivisionOperation::Division)
                } else {
                    SideErrorReason::IntegerOverflow(IntegerOperation::Division)
                }
            }
            Self::Rem => SideErrorReason::DivisionByZero(DivisionOperation::Remainder),
        }
    }
}

/// Computes `lane` for every lane of a binary operation whose operands are columns or scalars, one
/// lane at a time through [`Lanes::of_operands`].
fn evaluate_binary<T>(
    left: Operand<'_, PrimitiveArray<T>>,
    right: Operand<'_, PrimitiveArray<T>>,
    lane: impl FnMut(T::Native, T::Native) -> (T::Native, bool),
) -> Checked<T>
where
    T: ArrowPrimitiveType,
    T::Native: Copy + Default,
{
    evaluate_operands(left, right, |operands| Lanes::of_operands(operands, lane))
}

/// Computes every lane of a binary operation whose operands are columns or scalars: `lanes` is
/// handed the operands' value buffers, and a scalar operand as the one value every lane shares.
///
/// A null scalar makes every lane null without running the operation.
fn evaluate_operands<T>(
    left: Operand<'_, PrimitiveArray<T>>,
    right: Operand<'_, PrimitiveArray<T>>,
    lanes: impl FnOnce(LaneOperands<'_, T::Native>) -> Lanes<T::Native>,
) -> Checked<T>
where
    T: ArrowPrimitiveType,
    T::Native: Copy + Default,
{
    match (left, right) {
        (Operand::Scalar(scalar), Operand::Column(column)) => {
            if scalar.is_null(0) {
                return Checked::all_null(column.len());
            }
            let operands = LaneOperands::SharedLeft {
                left: scalar.value(0),
                right: column.values(),
            };
            Checked::from_lanes(lanes(operands), column.nulls().cloned())
        }
        (Operand::Column(column), Operand::Scalar(scalar)) => {
            if scalar.is_null(0) {
                return Checked::all_null(column.len());
            }
            let operands = LaneOperands::SharedRight {
                left: column.values(),
                right: scalar.value(0),
            };
            Checked::from_lanes(lanes(operands), column.nulls().cloned())
        }
        (Operand::Column(left), Operand::Column(right))
        | (Operand::Scalar(left), Operand::Scalar(right)) => {
            let operands = LaneOperands::Runs {
                left: left.values(),
                right: right.values(),
            };
            Checked::from_lanes(
                lanes(operands),
                NullBuffer::union(left.nulls(), right.nulls()),
            )
        }
    }
}

/// An integer type the checked kernels compute over.
///
/// Every lane operation returns its result and whether it failed. The `overflowing_*` operations
/// compute a result and its overflow flag without a branch, and the flag fails the lane, so the
/// wrapped result of an overflowing lane never reaches a column. Sums, differences and products
/// of whole runs come from the explicit SIMD kernels of `nervix-simd-kernels`, which give every
/// lane the same value and the same failure as the `overflowing_*` operation, except the product
/// of 64-bit lanes, which has no wider lane to be exact in and is computed one lane at a time.
pub(crate) trait CheckedInteger: ArrowNativeType + Default {
    fn lane_sum(self, right: Self) -> (Self, bool);

    fn lane_product(self, right: Self) -> (Self, bool);

    /// Fails for a zero divisor and for a quotient that overflows.
    fn lane_quotient(self, right: Self) -> (Self, bool);

    /// Fails for a zero divisor only.
    fn lane_remainder(self, right: Self) -> (Self, bool);

    fn is_zero_divisor(self) -> bool;

    /// Every lane's sum, failed where the sum does not fit the type.
    fn sums(operands: LaneOperands<'_, Self>) -> Lanes<Self>;

    /// Every lane's difference, failed where the difference does not fit the type.
    fn differences(operands: LaneOperands<'_, Self>) -> Lanes<Self>;

    /// Every lane's product, failed where the product does not fit the type.
    fn products(operands: LaneOperands<'_, Self>) -> Lanes<Self>;
}

/// A signed integer type, which alone has a negation and an absolute value that can overflow.
pub(crate) trait SignedInteger: CheckedInteger {
    fn lane_negation(self) -> (Self, bool);

    fn lane_absolute_value(self) -> (Self, bool);
}

/// The operations of [`CheckedInteger`] that every integer type computes alike: the lane
/// operations, and the sums and differences of the SIMD kernels, which cover every width.
macro_rules! checked_integer_operations {
    () => {
        fn lane_sum(self, right: Self) -> (Self, bool) {
            self.overflowing_add(right)
        }

        fn lane_product(self, right: Self) -> (Self, bool) {
            self.overflowing_mul(right)
        }

        fn lane_quotient(self, right: Self) -> (Self, bool) {
            match self.checked_div(right) {
                Some(quotient) => (quotient, false),
                None => (0, true),
            }
        }

        fn lane_remainder(self, right: Self) -> (Self, bool) {
            if right == 0 {
                return (0, true);
            }
            match self.checked_rem(right) {
                Some(remainder) => (remainder, false),
                // A nonzero divisor fails `checked_rem` only for the minimum signed value over -1.
                // Its quotient overflows, but its remainder is exactly 0.
                None => (0, false),
            }
        }

        fn is_zero_divisor(self) -> bool {
            self == 0
        }

        fn sums(operands: LaneOperands<'_, Self>) -> Lanes<Self> {
            Lanes::from(CheckedArithmetic::new().sums(operands))
        }

        fn differences(operands: LaneOperands<'_, Self>) -> Lanes<Self> {
            Lanes::from(CheckedArithmetic::new().differences(operands))
        }
    };
}

/// An integer type of 8, 16 or 32 bits, whose products the SIMD kernels compute in lanes of twice
/// its width.
macro_rules! widened_checked_integer {
    ($($native:ty),+ $(,)?) => {
        $(
            impl CheckedInteger for $native {
                checked_integer_operations!();

                fn products(operands: LaneOperands<'_, Self>) -> Lanes<Self> {
                    Lanes::from(CheckedArithmetic::new().products(operands))
                }
            }
        )+
    };
}

widened_checked_integer!(u8, i8, u16, i16, u32, i32);

/// A 64-bit integer type, whose products have no wider lane to be exact in, so they are computed
/// one lane at a time.
macro_rules! wide_checked_integer {
    ($($native:ty),+ $(,)?) => {
        $(
            impl CheckedInteger for $native {
                checked_integer_operations!();

                fn products(operands: LaneOperands<'_, Self>) -> Lanes<Self> {
                    Lanes::of_operands(operands, Self::lane_product)
                }
            }
        )+
    };
}

wide_checked_integer!(u64, i64);

macro_rules! signed_integer {
    ($($native:ty),+ $(,)?) => {
        $(
            impl SignedInteger for $native {
                fn lane_negation(self) -> (Self, bool) {
                    self.overflowing_neg()
                }

                fn lane_absolute_value(self) -> (Self, bool) {
                    self.overflowing_abs()
                }
            }
        )+
    };
}

signed_integer!(i8, i16, i32, i64);

/// A floating-point type the checked kernels compute over. Its operations are IEEE 754 operations
/// at the type's own width, and a lane fails exactly when its result is not finite.
pub(crate) trait CheckedFloat:
    ArrowNativeType
    + Default
    + PartialOrd
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + Div<Output = Self>
    + Rem<Output = Self>
    + Neg<Output = Self>
{
    /// The lane's result, failed when it is NaN or an infinity.
    fn finite_lane(self) -> (Self, bool);

    fn magnitude(self) -> Self;

    fn rounded_up(self) -> Self;

    fn rounded_down(self) -> Self;

    /// Rounds to the nearest integer, with halves rounded away from zero.
    fn rounded_half_away(self) -> Self;

    /// Rounds toward zero, which keeps the sign of a result that is zero.
    fn rounded_toward_zero(self) -> Self;

    /// `-1` for a negative value and `1` for a positive one, infinities included, and the value
    /// itself for a zero, which keeps its sign, or for NaN, which has no sign.
    fn sign_or_self(self) -> Self;
}

macro_rules! checked_float {
    ($($native:ty),+ $(,)?) => {
        $(
            impl CheckedFloat for $native {
                fn finite_lane(self) -> (Self, bool) {
                    (self, !self.is_finite())
                }

                fn magnitude(self) -> Self {
                    self.abs()
                }

                fn rounded_up(self) -> Self {
                    self.ceil()
                }

                fn rounded_down(self) -> Self {
                    self.floor()
                }

                fn rounded_half_away(self) -> Self {
                    self.round()
                }

                fn rounded_toward_zero(self) -> Self {
                    self.trunc()
                }

                fn sign_or_self(self) -> Self {
                    if self > 0.0 {
                        1.0
                    } else if self < 0.0 {
                        -1.0
                    } else {
                        self
                    }
                }
            }
        )+
    };
}

checked_float!(f32, f64);

pub(crate) fn integer_negation<T>(input: &PrimitiveArray<T>) -> Checked<T>
where
    T: ArrowPrimitiveType,
    T::Native: SignedInteger,
{
    let lanes = Lanes::unary(input.values(), T::Native::lane_negation);
    Checked::from_lanes(lanes, input.nulls().cloned())
}

pub(crate) fn integer_absolute_value<T>(input: &PrimitiveArray<T>) -> Checked<T>
where
    T: ArrowPrimitiveType,
    T::Native: SignedInteger,
{
    let lanes = Lanes::unary(input.values(), T::Native::lane_absolute_value);
    Checked::from_lanes(lanes, input.nulls().cloned())
}

/// Negates every lane. A float negation cannot fail, so it is the Arrow unary kernel.
pub(crate) fn float_negation<T>(input: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: CheckedFloat,
{
    input.unary(|value| -value)
}

pub(crate) fn float_absolute_value<T>(input: &PrimitiveArray<T>) -> Checked<T>
where
    T: ArrowPrimitiveType,
    T::Native: CheckedFloat,
{
    let lanes = Lanes::unary(input.values(), |value: T::Native| {
        value.magnitude().finite_lane()
    });
    Checked::from_lanes(lanes, input.nulls().cloned())
}

/// An integer type whose `sign` is `-1`, `0` or `1` at the type's own width. The sign of every
/// value fits every integer type, so it never fails.
pub(crate) trait IntegerSign: ArrowNativeType {
    fn sign(self) -> Self;
}

macro_rules! signed_integer_sign {
    ($($native:ty),+ $(,)?) => {
        $(
            impl IntegerSign for $native {
                fn sign(self) -> Self {
                    if self > 0 {
                        1
                    } else if self < 0 {
                        -1
                    } else {
                        0
                    }
                }
            }
        )+
    };
}

signed_integer_sign!(i8, i16, i32, i64);

macro_rules! unsigned_integer_sign {
    ($($native:ty),+ $(,)?) => {
        $(
            impl IntegerSign for $native {
                fn sign(self) -> Self {
                    if self > 0 { 1 } else { 0 }
                }
            }
        )+
    };
}

unsigned_integer_sign!(u8, u16, u32, u64);

pub(crate) fn integer_sign<T>(input: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: IntegerSign,
{
    input.unary(IntegerSign::sign)
}

/// The sign of every lane of a float column. Only NaN has no sign, so only a NaN lane fails.
pub(crate) fn float_sign<T>(input: &PrimitiveArray<T>) -> Checked<T>
where
    T: ArrowPrimitiveType,
    T::Native: CheckedFloat,
{
    let lanes = Lanes::unary(input.values(), |value: T::Native| {
        value.sign_or_self().finite_lane()
    });
    Checked::from_lanes(lanes, input.nulls().cloned())
}

impl FloatClass {
    /// Tests every lane of a float column for this class. A null lane stays null, and no lane
    /// fails: every value, NaN included, has a class.
    pub(crate) fn evaluate<T>(self, input: &PrimitiveArray<T>) -> BooleanArray
    where
        T: ArrowPrimitiveType,
        T::Native: ClassifiedFloat,
    {
        let lanes = input.len();
        let values = &input.values()[..lanes];
        let results = match self {
            Self::Nan => {
                BooleanBuffer::collect_bool(lanes, |lane| Self::Nan.contains(values[lane]))
            }
            Self::Finite => {
                BooleanBuffer::collect_bool(lanes, |lane| Self::Finite.contains(values[lane]))
            }
            Self::Infinite => {
                BooleanBuffer::collect_bool(lanes, |lane| Self::Infinite.contains(values[lane]))
            }
        };
        BooleanArray::new(results, input.nulls().cloned())
    }
}

/// A builtin that rounds to an integral value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rounding {
    Ceil,
    Floor,
    Round,
    Trunc,
}

impl Rounding {
    /// Rounds every lane of a float column.
    pub(crate) fn evaluate_floats<T>(self, input: &PrimitiveArray<T>) -> Checked<T>
    where
        T: ArrowPrimitiveType,
        T::Native: CheckedFloat,
    {
        let values: &[T::Native] = input.values();
        let lanes = match self {
            Self::Ceil => Lanes::unary(values, |value: T::Native| value.rounded_up().finite_lane()),
            Self::Floor => Lanes::unary(values, |value: T::Native| {
                value.rounded_down().finite_lane()
            }),
            Self::Round => Lanes::unary(values, |value: T::Native| {
                value.rounded_half_away().finite_lane()
            }),
            Self::Trunc => Lanes::unary(values, |value: T::Native| {
                value.rounded_toward_zero().finite_lane()
            }),
        };
        Checked::from_lanes(lanes, input.nulls().cloned())
    }

    /// Why a float lane of this rounding failed. Rounding a finite value is always finite, so only
    /// a NaN or an infinite operand fails.
    pub(crate) fn float_failure(self) -> SideErrorReason {
        let operation = match self {
            Self::Ceil => FloatOperation::Ceil,
            Self::Floor => FloatOperation::Floor,
            Self::Round => FloatOperation::Round,
            Self::Trunc => FloatOperation::Trunc,
        };
        SideErrorReason::NonFiniteResult(operation)
    }
}

/// A comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Comparison {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl Comparison {
    /// Compares two numeric operands lane by lane.
    ///
    /// Floats compare by IEEE 754: NaN is unequal to every value including itself, every ordering
    /// comparison with NaN is false, and `0.0` equals `-0.0`. The Arrow comparison kernels order
    /// floats by the IEEE 754 total order instead, so numeric comparison packs the operator's own
    /// result into the bitmap, the same vectorized loop those kernels use. A scalar operand is
    /// read once and compared against every lane of the column, and a null scalar makes every
    /// lane null.
    pub(crate) fn evaluate<T>(
        self,
        left: Operand<'_, PrimitiveArray<T>>,
        right: Operand<'_, PrimitiveArray<T>>,
    ) -> BooleanArray
    where
        T: ArrowPrimitiveType,
        T::Native: PartialOrd,
    {
        match (left, right) {
            (Operand::Scalar(scalar), Operand::Column(column)) => {
                if scalar.is_null(0) {
                    return BooleanArray::new_null(column.len());
                }
                let value = scalar.value(0);
                let lanes = column.len();
                let column_values = &column.values()[..lanes];
                let results = self.collect(lanes, |lane| (value, column_values[lane]));
                BooleanArray::new(results, column.nulls().cloned())
            }
            (Operand::Column(column), Operand::Scalar(scalar)) => {
                if scalar.is_null(0) {
                    return BooleanArray::new_null(column.len());
                }
                let value = scalar.value(0);
                let lanes = column.len();
                let column_values = &column.values()[..lanes];
                let results = self.collect(lanes, |lane| (column_values[lane], value));
                BooleanArray::new(results, column.nulls().cloned())
            }
            (Operand::Column(left), Operand::Column(right))
            | (Operand::Scalar(left), Operand::Scalar(right)) => {
                let lanes = left.len();
                let left_values = &left.values()[..lanes];
                let right_values = &right.values()[..lanes];
                let results = self.collect(lanes, |lane| (left_values[lane], right_values[lane]));
                BooleanArray::new(results, NullBuffer::union(left.nulls(), right.nulls()))
            }
        }
    }

    /// Packs this comparison of the operand pair `operands` yields for each lane into a bitmap.
    fn collect<N: PartialOrd>(
        self,
        lanes: usize,
        operands: impl Fn(usize) -> (N, N),
    ) -> BooleanBuffer {
        match self {
            Self::Eq => BooleanBuffer::collect_bool(lanes, |lane| {
                let (left, right) = operands(lane);
                left == right
            }),
            Self::NotEq => BooleanBuffer::collect_bool(lanes, |lane| {
                let (left, right) = operands(lane);
                left != right
            }),
            Self::Lt => BooleanBuffer::collect_bool(lanes, |lane| {
                let (left, right) = operands(lane);
                left < right
            }),
            Self::LtEq => BooleanBuffer::collect_bool(lanes, |lane| {
                let (left, right) = operands(lane);
                left <= right
            }),
            Self::Gt => BooleanBuffer::collect_bool(lanes, |lane| {
                let (left, right) = operands(lane);
                left > right
            }),
            Self::GtEq => BooleanBuffer::collect_bool(lanes, |lane| {
                let (left, right) = operands(lane);
                left >= right
            }),
        }
    }
}

/// A numeric type whose values a math function reads as `f64`.
pub(crate) trait MathOperand: ArrowNativeType {
    /// Every lane as an `f64`. Integers wider than 32 bits round to the nearest `f64`, and every
    /// other numeric value converts exactly.
    fn f64_lanes(values: &ScalarBuffer<Self>) -> ScalarBuffer<f64>;
}

impl MathOperand for f64 {
    fn f64_lanes(values: &ScalarBuffer<Self>) -> ScalarBuffer<f64> {
        values.clone()
    }
}

macro_rules! exact_math_operand {
    ($($native:ty),+ $(,)?) => {
        $(
            impl MathOperand for $native {
                fn f64_lanes(values: &ScalarBuffer<Self>) -> ScalarBuffer<f64> {
                    values.iter().map(|value| f64::from(*value)).collect()
                }
            }
        )+
    };
}

exact_math_operand!(u8, i8, u16, i16, u32, i32, f32);

macro_rules! rounded_math_operand {
    ($($native:ty),+ $(,)?) => {
        $(
            impl MathOperand for $native {
                fn f64_lanes(values: &ScalarBuffer<Self>) -> ScalarBuffer<f64> {
                    values.iter().map(|value| -> f64 { (*value).approx_into() }).collect()
                }
            }
        )+
    };
}

rounded_math_operand!(u64, i64);

/// One argument of a math function: a numeric column read as `f64` lanes, with its validity.
pub(crate) struct F64Operand {
    values: ScalarBuffer<f64>,
    nulls: Option<NullBuffer>,
}

impl F64Operand {
    /// Reads a numeric column as `f64` lanes, or answers `None` for a column that is not numeric.
    pub(crate) fn from_typed(input: &TypedArray) -> Option<Self> {
        match input {
            TypedArray::UInt8(array) => Some(Self::new(array)),
            TypedArray::Int8(array) => Some(Self::new(array)),
            TypedArray::UInt16(array) => Some(Self::new(array)),
            TypedArray::Int16(array) => Some(Self::new(array)),
            TypedArray::UInt32(array) => Some(Self::new(array)),
            TypedArray::Int32(array) => Some(Self::new(array)),
            TypedArray::UInt64(array) => Some(Self::new(array)),
            TypedArray::Int64(array) => Some(Self::new(array)),
            TypedArray::Float32(array) => Some(Self::new(array)),
            TypedArray::Float64(array) => Some(Self::new(array)),
            TypedArray::Boolean(_)
            | TypedArray::Utf8(_)
            | TypedArray::Binary(_)
            | TypedArray::Datetime(_)
            | TypedArray::Generic(_)
            | TypedArray::Uninitialized { .. } => None,
        }
    }

    fn new<T>(input: &PrimitiveArray<T>) -> Self
    where
        T: ArrowPrimitiveType,
        T::Native: MathOperand,
    {
        Self {
            values: T::Native::f64_lanes(input.values()),
            nulls: input.nulls().cloned(),
        }
    }

    /// Evaluates a function that compiles to a single instruction, such as `sqrt`, over every lane
    /// so the loop vectorizes.
    fn every_lane(&self, function: impl Fn(f64) -> f64) -> Checked<Float64Type> {
        let lanes = Lanes::unary(&self.values, |value| function(value).finite_lane());
        Checked::from_lanes(lanes, self.nulls.clone())
    }

    /// Evaluates a function the platform math library computes. One call costs far more than
    /// visiting a validity bit, so a column with nulls evaluates only its valid lanes.
    fn library_lanes(&self, function: impl Fn(f64) -> f64) -> Checked<Float64Type> {
        let lanes = match &self.nulls {
            Some(nulls) => {
                Lanes::unary_valid(&self.values, nulls, |value| function(value).finite_lane())
            }
            None => Lanes::unary(&self.values, |value| function(value).finite_lane()),
        };
        Checked::from_lanes(lanes, self.nulls.clone())
    }
}

/// The `F64` nearest to π/180, the radians in one degree.
const RADIANS_PER_DEGREE: f64 = 0.017_453_292_519_943_295;

/// The `F64` nearest to 180/π, the degrees in one radian.
const DEGREES_PER_RADIAN: f64 = 57.295_779_513_082_32;

/// A math builtin of one argument, evaluated in `f64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MathFunction {
    Acos,
    Asin,
    Atan,
    Cos,
    Exp,
    Ln,
    /// `log` with one argument, the base-10 logarithm.
    Log10,
    Log2,
    Sqrt,
    Tan,
    Sin,
    /// Degrees to radians: one multiplication by [`RADIANS_PER_DEGREE`].
    Radians,
    /// Radians to degrees: one multiplication by [`DEGREES_PER_RADIAN`].
    Degrees,
}

impl MathFunction {
    /// Why a lane of this function failed: its result was not finite.
    pub(crate) fn failure(self) -> SideErrorReason {
        let operation = match self {
            Self::Acos => FloatOperation::Acos,
            Self::Asin => FloatOperation::Asin,
            Self::Atan => FloatOperation::Atan,
            Self::Cos => FloatOperation::Cos,
            Self::Exp => FloatOperation::Exp,
            Self::Ln => FloatOperation::Ln,
            Self::Log10 => FloatOperation::Log,
            Self::Log2 => FloatOperation::Log2,
            Self::Sqrt => FloatOperation::Sqrt,
            Self::Tan => FloatOperation::Tan,
            Self::Sin => FloatOperation::Sin,
            Self::Radians => FloatOperation::Radians,
            Self::Degrees => FloatOperation::Degrees,
        };
        SideErrorReason::NonFiniteResult(operation)
    }

    pub(crate) fn evaluate(self, operand: &F64Operand) -> Checked<Float64Type> {
        match self {
            Self::Acos => operand.library_lanes(f64::acos),
            Self::Asin => operand.library_lanes(f64::asin),
            Self::Atan => operand.library_lanes(f64::atan),
            Self::Cos => operand.library_lanes(f64::cos),
            Self::Exp => operand.library_lanes(f64::exp),
            Self::Ln => operand.library_lanes(f64::ln),
            Self::Log10 => operand.library_lanes(f64::log10),
            Self::Log2 => operand.library_lanes(f64::log2),
            Self::Sqrt => operand.every_lane(f64::sqrt),
            Self::Tan => operand.library_lanes(f64::tan),
            Self::Sin => operand.library_lanes(f64::sin),
            Self::Radians => operand.every_lane(|degrees| degrees * RADIANS_PER_DEGREE),
            Self::Degrees => operand.every_lane(|radians| radians * DEGREES_PER_RADIAN),
        }
    }
}

/// A math builtin of two arguments, evaluated in `f64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinaryMathFunction {
    /// `log(base, value)`.
    Log,
    /// `pow(base, exponent)`.
    Pow,
    /// `atan2(y, x)`, the angle of the point `(x, y)`.
    Atan2,
}

impl BinaryMathFunction {
    /// Why a lane of this function failed: its result was not finite.
    pub(crate) fn failure(self) -> SideErrorReason {
        let operation = match self {
            Self::Log => FloatOperation::Log,
            Self::Pow => FloatOperation::Pow,
            Self::Atan2 => FloatOperation::Atan2,
        };
        SideErrorReason::NonFiniteResult(operation)
    }

    /// Evaluates the function over two arguments of one batch. Each calls into the platform math
    /// library, so lanes with a null argument are not evaluated.
    pub(crate) fn evaluate(self, left: &F64Operand, right: &F64Operand) -> Checked<Float64Type> {
        let nulls = NullBuffer::union(left.nulls.as_ref(), right.nulls.as_ref());
        let lanes = match (self, &nulls) {
            (Self::Log, Some(valid)) => {
                Lanes::binary_valid(&left.values, &right.values, valid, |base, value: f64| {
                    value.log(base).finite_lane()
                })
            }
            (Self::Log, None) => Lanes::binary(&left.values, &right.values, |base, value: f64| {
                value.log(base).finite_lane()
            }),
            (Self::Pow, Some(valid)) => {
                Lanes::binary_valid(&left.values, &right.values, valid, |base: f64, exponent| {
                    base.powf(exponent).finite_lane()
                })
            }
            (Self::Pow, None) => {
                Lanes::binary(&left.values, &right.values, |base: f64, exponent| {
                    base.powf(exponent).finite_lane()
                })
            }
            (Self::Atan2, Some(valid)) => {
                Lanes::binary_valid(&left.values, &right.values, valid, |y: f64, x| {
                    y.atan2(x).finite_lane()
                })
            }
            (Self::Atan2, None) => Lanes::binary(&left.values, &right.values, |y: f64, x| {
                y.atan2(x).finite_lane()
            }),
        };
        Checked::from_lanes(lanes, nulls)
    }
}

#[cfg(test)]
#[path = "numeric_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "numeric_function_tests.rs"]
mod function_tests;
