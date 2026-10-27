//! Exact types admitted by an entity's direct value mappings.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The type families and per-field declarations a mapped value must satisfy.
//! - **Depends on.** Arrow type descriptions and the shared mapping vocabulary.
//! - **Must not know.** Connector encoders, registry decisions, runtime batches or publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

#[cfg(feature = "arrow")]
use arrow_schema::{DataType, TimeUnit};

use crate::EmitterValueMapping;

/// The group in which a statement declares a mapping. Keys in separate groups are independent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum MappedValueSection {
    Values,
    Attributes,
    Resource,
}

/// One mapping together with its declared group, in statement order.
#[derive(Debug, Clone, Copy)]
pub struct DeclaredValueMapping<'a> {
    pub section: MappedValueSection,
    pub mapping: &'a EmitterValueMapping,
}

/// An exact type or a declared type family, without coercion between its members.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::IntoStaticStr)]
pub enum MappedValueType {
    #[strum(serialize = "STRING")]
    String,
    #[strum(serialize = "I32")]
    I32,
    #[strum(serialize = "U64")]
    U64,
    #[strum(serialize = "F32 or F64")]
    Float,
    #[strum(serialize = "DATETIME")]
    Datetime,
    #[strum(serialize = "an integer-family value")]
    Integer,
    #[strum(serialize = "an integer-family, F32, or F64 value")]
    Number,
    #[strum(serialize = "a U64 ARRAY or VEC")]
    U64Sequence,
    #[strum(serialize = "an F32 or F64 ARRAY or VEC")]
    FloatSequence,
    #[strum(serialize = "a string, boolean, number, datetime or an ARRAY or VEC of those")]
    Attribute,
    #[strum(serialize = "a literal string, boolean, I64, F64, null or ARRAY")]
    Literal,
    #[strum(serialize = "a Plexus scalar, ARRAY or VEC")]
    Column,
}

#[cfg(feature = "arrow")]
impl MappedValueType {
    /// Whether an expression's inferred Arrow type is in this declaration's exact domain.
    pub fn admits(self, actual: &DataType) -> bool {
        match self {
            Self::String => *actual == DataType::Utf8,
            Self::I32 => *actual == DataType::Int32,
            Self::U64 => *actual == DataType::UInt64,
            Self::Float => matches!(actual, DataType::Float32 | DataType::Float64),
            Self::Datetime => matches!(actual, DataType::Timestamp(TimeUnit::Nanosecond, _)),
            Self::Integer => matches!(
                actual,
                DataType::UInt8
                    | DataType::Int8
                    | DataType::UInt16
                    | DataType::Int16
                    | DataType::UInt32
                    | DataType::Int32
                    | DataType::UInt64
                    | DataType::Int64
            ),
            Self::Number => {
                Self::Integer.admits(actual)
                    || matches!(actual, DataType::Float32 | DataType::Float64)
            }
            Self::U64Sequence => {
                if let Some(element) = Self::sequence_element(actual) {
                    Self::U64.admits(element)
                } else {
                    false
                }
            }
            Self::FloatSequence => {
                if let Some(element) = Self::sequence_element(actual) {
                    matches!(element, DataType::Float32 | DataType::Float64)
                } else {
                    false
                }
            }
            Self::Literal => {
                if matches!(
                    actual,
                    DataType::Utf8
                        | DataType::Boolean
                        | DataType::Int64
                        | DataType::Float64
                        | DataType::Null
                ) {
                    return true;
                }
                if let Some(element) = Self::sequence_element(actual) {
                    self.admits(element)
                } else {
                    false
                }
            }
            Self::Attribute | Self::Column => {
                if Self::Number.admits(actual)
                    || Self::Datetime.admits(actual)
                    || matches!(actual, DataType::Utf8 | DataType::Boolean)
                {
                    return true;
                }
                if self == Self::Column && *actual == DataType::Binary {
                    return true;
                }
                if let Some(element) = Self::sequence_element(actual) {
                    self.admits(element)
                } else {
                    false
                }
            }
        }
    }

    fn sequence_element(actual: &DataType) -> Option<&DataType> {
        match actual {
            DataType::List(element) | DataType::FixedSizeList(element, _) => {
                Some(element.data_type())
            }
            _ => None,
        }
    }
}

/// A public value key and its exact type, declared by the entity that owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappedValueField {
    pub key: &'static str,
    pub value_type: MappedValueType,
}

/// The direct value contract of an entity; a codec or native emitter declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappedValueTypes {
    /// Destination columns take every current Plexus scalar and recursively typed ARRAY or VEC.
    Columns,
    /// Named protocol fields have exact types; metadata uses the declared attribute family.
    Fields {
        values: &'static [MappedValueField],
        attributes: MappedValueType,
        resource: MappedValueType,
    },
}

impl MappedValueTypes {
    /// The type of a key in its group, or absence when that key or group is unavailable.
    pub fn value_type(self, section: MappedValueSection, key: &str) -> Option<MappedValueType> {
        match self {
            Self::Columns => match section {
                MappedValueSection::Values => Some(MappedValueType::Column),
                MappedValueSection::Attributes | MappedValueSection::Resource => None,
            },
            Self::Fields {
                values,
                attributes,
                resource,
            } => match section {
                MappedValueSection::Values => {
                    // A protocol field table is bounded by its entity declaration (ten keys
                    // at most today), independently of a graph or input record count.
                    values
                        .iter()
                        .find(|field| field.key == key)
                        .map(|field| field.value_type)
                }
                MappedValueSection::Attributes => Some(attributes),
                MappedValueSection::Resource => Some(resource),
            },
        }
    }
}

#[cfg(all(test, feature = "arrow"))]
mod tests {
    use arrow_schema::Field;
    use nervix_primitives::sync::StdArc;

    use super::*;

    #[test]
    fn mapping_types_preserve_exact_fields_and_recursive_column_domains() {
        let list = |ty| DataType::List(StdArc::new(Field::new("item", ty, true)));
        let vector = |ty| DataType::FixedSizeList(StdArc::new(Field::new("item", ty, true)), 2);
        for (rule, actual, admitted) in [
            (MappedValueType::I32, DataType::Int32, true),
            (MappedValueType::I32, DataType::Int64, false),
            (MappedValueType::String, DataType::Utf8, true),
            (MappedValueType::String, DataType::Binary, false),
            (
                MappedValueType::Datetime,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            (
                MappedValueType::Datetime,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            (MappedValueType::Number, DataType::UInt64, true),
            (MappedValueType::Number, DataType::Float32, true),
            (MappedValueType::Number, DataType::Boolean, false),
            (MappedValueType::U64Sequence, list(DataType::UInt64), true),
            (
                MappedValueType::U64Sequence,
                vector(DataType::Float64),
                false,
            ),
            (
                MappedValueType::FloatSequence,
                vector(DataType::Float64),
                true,
            ),
            (MappedValueType::FloatSequence, list(DataType::Int32), false),
            (MappedValueType::Literal, list(DataType::Float64), true),
            (MappedValueType::Literal, DataType::Null, true),
            (MappedValueType::Literal, DataType::Int32, false),
            (
                MappedValueType::Column,
                list(vector(DataType::Binary)),
                true,
            ),
            (MappedValueType::Attribute, list(DataType::Boolean), true),
            (MappedValueType::Attribute, DataType::Binary, false),
            (MappedValueType::Column, DataType::Null, false),
        ] {
            assert_eq!(rule.admits(&actual), admitted, "{rule}: {actual}");
        }
    }
}
