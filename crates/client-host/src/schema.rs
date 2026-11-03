//! The schema a subscription's rows, or an endpoint's batches, follow.
//!
//! - **Owns.** The field types and parts a host reads, the levels of a field's type from the
//!   field down to its innermost element, and reading a row schema's fields, their levels and its
//!   branch.
//! - **Depends on.** The wire contract's row schema and the vocabulary's field types.
//! - **Must not know.** Where the schema came from.
//!
//! Level 0 of a field is the field's own type. A `LIST` or `FIXED_LIST` level holds lists whose
//! elements are the next level, and the last level is a scalar.

use meticulous::OptionExt as _;
use nervix_client_core::RowSchema;
use nervix_models::{ParseAsType, SchemaField};
use nervix_primitives::sync::Arc;

use crate::failure::{Failure, FailureKind};

/// The type of a schema field, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
#[repr(i32)]
pub enum FieldType {
    U8 = 1,
    I8 = 2,
    U16 = 3,
    I16 = 4,
    U32 = 5,
    I32 = 6,
    U64 = 7,
    I64 = 8,
    F32 = 9,
    F64 = 10,
    Bool = 11,
    String = 12,
    Bytes = 13,
    Datetime = 14,
    FixedList = 15,
    List = 16,
}

impl From<&ParseAsType> for FieldType {
    fn from(ty: &ParseAsType) -> Self {
        match ty {
            ParseAsType::U8 => Self::U8,
            ParseAsType::I8 => Self::I8,
            ParseAsType::U16 => Self::U16,
            ParseAsType::I16 => Self::I16,
            ParseAsType::U32 => Self::U32,
            ParseAsType::I32 => Self::I32,
            ParseAsType::U64 => Self::U64,
            ParseAsType::I64 => Self::I64,
            ParseAsType::F32 => Self::F32,
            ParseAsType::F64 => Self::F64,
            ParseAsType::Bool => Self::Bool,
            ParseAsType::String => Self::String,
            ParseAsType::Bytes => Self::Bytes,
            ParseAsType::Datetime => Self::Datetime,
            ParseAsType::Array { .. } => Self::FixedList,
            ParseAsType::Vec { .. } => Self::List,
        }
    }
}

/// What a field type names: a scalar, or one of the two kinds of list.
pub enum TypeKind {
    Scalar(ParseAsType),
    List,
    FixedList,
}

impl FieldType {
    /// Reads a type a host passed as an integer.
    pub fn from_host(field_type: i32) -> Result<Self, Failure> {
        Self::from_repr(field_type)
            .ok_or_else(|| Failure::invalid_argument("type", "is not an nx_type"))
    }

    /// The scalar this type names, or the kind of list it is.
    pub fn kind(self) -> TypeKind {
        match self {
            Self::U8 => TypeKind::Scalar(ParseAsType::U8),
            Self::I8 => TypeKind::Scalar(ParseAsType::I8),
            Self::U16 => TypeKind::Scalar(ParseAsType::U16),
            Self::I16 => TypeKind::Scalar(ParseAsType::I16),
            Self::U32 => TypeKind::Scalar(ParseAsType::U32),
            Self::I32 => TypeKind::Scalar(ParseAsType::I32),
            Self::U64 => TypeKind::Scalar(ParseAsType::U64),
            Self::I64 => TypeKind::Scalar(ParseAsType::I64),
            Self::F32 => TypeKind::Scalar(ParseAsType::F32),
            Self::F64 => TypeKind::Scalar(ParseAsType::F64),
            Self::Bool => TypeKind::Scalar(ParseAsType::Bool),
            Self::String => TypeKind::Scalar(ParseAsType::String),
            Self::Bytes => TypeKind::Scalar(ParseAsType::Bytes),
            Self::Datetime => TypeKind::Scalar(ParseAsType::Datetime),
            Self::List => TypeKind::List,
            Self::FixedList => TypeKind::FixedList,
        }
    }

    /// The bytes one value of a fixed-width type takes in a copied column.
    pub fn fixed_width(self) -> Option<usize> {
        match self {
            Self::U8 | Self::I8 | Self::Bool => Some(1),
            Self::U16 | Self::I16 => Some(2),
            Self::U32 | Self::I32 | Self::F32 => Some(4),
            Self::U64 | Self::I64 | Self::F64 | Self::Datetime => Some(8),
            Self::String | Self::Bytes | Self::FixedList | Self::List => None,
        }
    }
}

/// The levels of a field's type, from the field's own type down to its innermost element.
#[derive(Debug, Clone, Copy)]
pub struct TypeLevels<'a> {
    ty: &'a ParseAsType,
}

impl<'a> TypeLevels<'a> {
    pub fn of(ty: &'a ParseAsType) -> Self {
        Self { ty }
    }

    /// How many levels the type has: one, and one more for each list around its element.
    pub fn count(self) -> usize {
        let mut count = 1_usize;
        let mut ty = self.ty;
        while let ParseAsType::Array { element, .. } | ParseAsType::Vec { element } = ty {
            count = count
                .checked_add(1)
                .assured("a type nests fewer lists than a usize counts");
            ty = element;
        }
        count
    }

    /// The type at `level`, or `None` past the innermost element.
    pub fn get(self, level: usize) -> Option<&'a ParseAsType> {
        let mut ty = self.ty;
        for _ in 0..level {
            ty = match ty {
                ParseAsType::Array { element, .. } | ParseAsType::Vec { element } => element,
                _ => return None,
            };
        }
        Some(ty)
    }

    /// The type at `level`, refused as a host argument past the innermost element.
    pub fn level(self, level: usize) -> Result<&'a ParseAsType, Failure> {
        self.get(level).ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                format!(
                    "level {level} is past the {} levels of the field's type",
                    self.count()
                ),
            )
        })
    }
}

/// Which cells of a schema or a batch an accessor reads, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
#[repr(i32)]
pub enum Part {
    Rows = 1,
    BranchKey = 2,
}

impl Part {
    /// Reads a part a host passed as an integer.
    pub fn from_host(part: i32) -> Result<Self, Failure> {
        Self::from_repr(part).ok_or_else(|| Failure::invalid_argument("part", "is not an nx_part"))
    }
}

/// A subscription's row schema, shared with the events that follow it.
#[derive(Debug, Clone)]
pub struct Schema {
    schema: Arc<RowSchema>,
}

impl Schema {
    pub fn new(schema: Arc<RowSchema>) -> Self {
        Self { schema }
    }

    /// The schema of an endpoint's batches, whose rows are `fields` and which has no branch.
    pub fn of_fields(fields: Vec<SchemaField>) -> Self {
        Self::new(Arc::new(RowSchema {
            fields,
            branch: None,
        }))
    }

    /// The fields of one part. An unbranched relay's branch key has none.
    pub fn fields(&self, part: Part) -> &[SchemaField] {
        match part {
            Part::Rows => &self.schema.fields,
            Part::BranchKey => match &self.schema.branch {
                Some(branch) => branch.fields(),
                None => &[],
            },
        }
    }

    pub fn field(&self, part: Part, index: usize) -> Result<&SchemaField, Failure> {
        let fields = self.fields(part);
        fields.get(index).ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                format!(
                    "field {index} is past the {} fields of the part",
                    fields.len()
                ),
            )
        })
    }

    pub fn branch(&self) -> Option<&str> {
        match &self.schema.branch {
            Some(branch) => Some(branch.branch().as_str()),
            None => None,
        }
    }
}
