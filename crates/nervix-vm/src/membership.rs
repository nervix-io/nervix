//! `IN` tests over a set of constants that is prepared once per compiled program.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The set an `IN` test prepares from its evaluated constant elements, the choice
//!   between comparing a value with each element of a small set and looking it up in a keyed one,
//!   and the membership of every row of a column.
//!
//! The `membership_kernels` benchmarks of the VM suite measure that choice. A fixed-width value is
//! compared with each element of a set of up to [`SMALL_SET_CAPACITY`] faster than it is hashed,
//! while text is hashed faster than it is compared with even a few elements, so a set of `STRING`
//! elements is always keyed.
//! - **Depends on.** Arrow arrays and the equality `=` applies to each type.
//! - **Must not know.** Registers, instructions, programs, spans, or row errors.
//!
//! A set holds each distinct element once, so duplicates written in a set change nothing. Equality
//! is the equality `=` decides: floats compare by IEEE 754, so NaN is a member of no set and no
//! NaN operand is a member of one, and `0.0` and `-0.0` are the same element.

use std::{borrow::Borrow, hash::Hash, sync::Arc};

use ahash::{HashSet, HashSetExt};
use arrow_array::{
    Array, ArrowPrimitiveType, BooleanArray, Float32Array, Float64Array, PrimitiveArray,
    StringArray, TimestampNanosecondArray,
    types::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_buffer::BooleanBuffer;
use sorted_vec::SortedSet;

use crate::{batch::TypedArray, ir::RegisterType};

/// The most distinct fixed-width elements a set compares a value with one by one. A set with more
/// looks each value up by key, so no row is compared with more elements than this.
pub const SMALL_SET_CAPACITY: usize = 8;

/// The prepared constants of one `IN` test.
///
/// The elements are shared, so every clone of a compiled program and every batch it runs over
/// tests against the one set that was prepared when the program was compiled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipSet {
    elements: Arc<Elements>,
}

/// The distinct elements of a set, held as values of the operand's type.
#[derive(Debug, PartialEq, Eq)]
enum Elements {
    /// A set written without elements, which holds no value, not even a null one.
    Empty,
    UInt8(Members<u8>),
    Int8(Members<i8>),
    UInt16(Members<u16>),
    Int16(Members<i16>),
    UInt32(Members<u32>),
    Int32(Members<i32>),
    UInt64(Members<u64>),
    Int64(Members<i64>),
    /// Float elements keyed by [`f32_member`].
    Float32(Members<u32>),
    /// Float elements keyed by [`f64_member`].
    Float64(Members<u64>),
    Boolean(Members<bool>),
    Utf8(Members<Box<str>>),
    /// Datetime elements as nanoseconds since the Unix epoch.
    Datetime(Members<i64>),
}

/// Distinct elements of one type, compared one by one or looked up by key.
#[derive(Debug, PartialEq, Eq)]
enum Members<K: Ord + Hash> {
    /// At most [`SMALL_SET_CAPACITY`] elements.
    Few(SortedSet<K>),
    /// More elements than a value is compared with one by one.
    Keyed(HashSet<K>),
}

impl<K: Ord + Hash> Members<K> {
    /// Fixed-width elements, compared one by one while there are at most [`SMALL_SET_CAPACITY`].
    fn collect(elements: impl IntoIterator<Item = K>) -> Self {
        let distinct = Self::distinct(elements);
        if distinct.len() <= SMALL_SET_CAPACITY {
            return Self::Few(SortedSet::from_unsorted(distinct.into_iter().collect()));
        }
        Self::Keyed(distinct)
    }

    /// Elements looked up by key however few there are.
    fn keyed(elements: impl IntoIterator<Item = K>) -> Self {
        Self::Keyed(Self::distinct(elements))
    }

    /// Each element once.
    fn distinct(elements: impl IntoIterator<Item = K>) -> HashSet<K> {
        let mut distinct = HashSet::new();
        for element in elements {
            distinct.insert(element);
        }
        distinct
    }

    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + Hash + ?Sized,
    {
        match self {
            // Construction keeps a set this small only up to SMALL_SET_CAPACITY elements, so a
            // value is compared with at most that many.
            Self::Few(members) => members.iter().any(|member| member.borrow() == key),
            Self::Keyed(members) => members.contains(key),
        }
    }
}

/// The key of an `F32` value in a set, or `None` for NaN, which equals no value. The two zeros are
/// equal, so both take the key of `0.0`.
fn f32_member(value: f32) -> Option<u32> {
    if value.is_nan() {
        return None;
    }
    if value == 0.0 {
        return Some(0.0_f32.to_bits());
    }
    Some(value.to_bits())
}

/// The key of an `F64` value in a set, or `None` for NaN, which equals no value. The two zeros are
/// equal, so both take the key of `0.0`.
fn f64_member(value: f64) -> Option<u64> {
    if value.is_nan() {
        return None;
    }
    if value == 0.0 {
        return Some(0.0_f64.to_bits());
    }
    Some(value.to_bits())
}

/// The value of each evaluated element, or `None` when one is not a non-null value of `A`.
fn element_values<'a, A: Array + 'static, V>(
    elements: &'a [TypedArray],
    value: impl Fn(&'a A) -> V,
) -> Option<Vec<V>> {
    let mut values = Vec::with_capacity(elements.len());
    for element in elements {
        let array = element.as_array().as_any().downcast_ref::<A>()?;
        if array.len() != 1 || array.is_null(0) {
            return None;
        }
        values.push(value(array));
    }
    Some(values)
}

impl MembershipSet {
    /// Prepares the set an `IN` test compares an operand of type `operand` with, from its written
    /// elements evaluated to one-row arrays of that type. `None` when an element is not a non-null
    /// value of the operand's type, which the compiler rejects before it prepares a set.
    pub(crate) fn prepare(operand: RegisterType, elements: &[TypedArray]) -> Option<Self> {
        if elements.is_empty() {
            return Some(Self {
                elements: Arc::new(Elements::Empty),
            });
        }
        let elements = match operand {
            RegisterType::UInt8 => Elements::UInt8(primitive_members::<UInt8Type>(elements)?),
            RegisterType::Int8 => Elements::Int8(primitive_members::<Int8Type>(elements)?),
            RegisterType::UInt16 => Elements::UInt16(primitive_members::<UInt16Type>(elements)?),
            RegisterType::Int16 => Elements::Int16(primitive_members::<Int16Type>(elements)?),
            RegisterType::UInt32 => Elements::UInt32(primitive_members::<UInt32Type>(elements)?),
            RegisterType::Int32 => Elements::Int32(primitive_members::<Int32Type>(elements)?),
            RegisterType::UInt64 => Elements::UInt64(primitive_members::<UInt64Type>(elements)?),
            RegisterType::Int64 => Elements::Int64(primitive_members::<Int64Type>(elements)?),
            RegisterType::Float32 => {
                let values = element_values(elements, |array: &Float32Array| array.value(0))?;
                Elements::Float32(Members::collect(values.into_iter().filter_map(f32_member)))
            }
            RegisterType::Float64 => {
                let values = element_values(elements, |array: &Float64Array| array.value(0))?;
                Elements::Float64(Members::collect(values.into_iter().filter_map(f64_member)))
            }
            RegisterType::Boolean => {
                let values = element_values(elements, |array: &BooleanArray| array.value(0))?;
                Elements::Boolean(Members::collect(values))
            }
            RegisterType::Utf8 => {
                let values =
                    element_values(elements, |array: &StringArray| Box::from(array.value(0)))?;
                Elements::Utf8(Members::keyed(values))
            }
            RegisterType::Datetime => {
                let values =
                    element_values(elements, |array: &TimestampNanosecondArray| array.value(0))?;
                Elements::Datetime(Members::collect(values))
            }
            RegisterType::Generic => return None,
        };
        Some(Self {
            elements: Arc::new(elements),
        })
    }

    /// Whether both sets share one prepared set of elements.
    #[cfg(test)]
    pub(crate) fn shares_elements_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.elements, &other.elements)
    }

    /// Whether each row of `operand` is a member of the set.
    ///
    /// A null operand is not known to be a member, so its row is null, except that no value is a
    /// member of an empty set, so every row of an empty set is false. `None` when the operand's type
    /// is not the type the set was prepared for.
    pub(crate) fn evaluate(&self, operand: &TypedArray) -> Option<BooleanArray> {
        match (self.elements.as_ref(), operand) {
            (Elements::Empty, operand) => Some(BooleanArray::new(
                BooleanBuffer::new_unset(operand.len()),
                None,
            )),
            (Elements::UInt8(members), TypedArray::UInt8(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::Int8(members), TypedArray::Int8(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::UInt16(members), TypedArray::UInt16(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::Int16(members), TypedArray::Int16(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::UInt32(members), TypedArray::UInt32(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::Int32(members), TypedArray::Int32(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::UInt64(members), TypedArray::UInt64(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::Int64(members), TypedArray::Int64(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::Float32(members), TypedArray::Float32(values)) => {
                Some(primitive_membership(members, values, f32_member))
            }
            (Elements::Float64(members), TypedArray::Float64(values)) => {
                Some(primitive_membership(members, values, f64_member))
            }
            (Elements::Datetime(members), TypedArray::Datetime(values)) => {
                Some(primitive_membership(members, values, Some))
            }
            (Elements::Boolean(members), TypedArray::Boolean(values)) => {
                let lanes = values.len();
                let results = BooleanBuffer::collect_bool(lanes, |lane| {
                    members.contains(&values.value(lane))
                });
                Some(BooleanArray::new(results, values.nulls().cloned()))
            }
            (Elements::Utf8(members), TypedArray::Utf8(values)) => {
                let lanes = values.len();
                let results =
                    BooleanBuffer::collect_bool(lanes, |lane| members.contains(values.value(lane)));
                Some(BooleanArray::new(results, values.nulls().cloned()))
            }
            _ => None,
        }
    }
}

/// The members among `elements`, which hold values of one primitive array type.
fn primitive_members<T>(elements: &[TypedArray]) -> Option<Members<T::Native>>
where
    T: ArrowPrimitiveType,
    T::Native: Ord + Hash,
{
    let values = element_values(elements, |array: &PrimitiveArray<T>| array.value(0))?;
    Some(Members::collect(values))
}

/// Tests every lane of a primitive column for membership, keying each value with `member`, which
/// answers `None` for a value that equals no element. A null lane stays null.
fn primitive_membership<T, K>(
    members: &Members<K>,
    values: &PrimitiveArray<T>,
    member: impl Fn(T::Native) -> Option<K>,
) -> BooleanArray
where
    T: ArrowPrimitiveType,
    K: Ord + Hash,
{
    let lanes = values.len();
    let natives = &values.values()[..lanes];
    let results = BooleanBuffer::collect_bool(lanes, |lane| match member(natives[lane]) {
        Some(key) => members.contains(&key),
        None => false,
    });
    BooleanArray::new(results, values.nulls().cloned())
}

#[cfg(test)]
#[path = "membership_tests.rs"]
mod tests;
