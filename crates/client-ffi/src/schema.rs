//! The schema a subscription's rows follow: `nx_schema`.
//!
//! - **Owns.** The field types and parts the header names, and reading a row schema's fields and
//!   branch.
//! - **Depends on.** The wire contract's row schema and the vocabulary's field types.
//! - **Must not know.** Where the schema came from.

use nervix_client_core::RowSchema;
use nervix_models::{ParseAsType, SchemaField};
use triomphe::Arc;

use crate::{
    abi,
    failure::{Failure, FailureKind},
};

/// The type of a schema field, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl FieldType {
    /// The bytes one value of a fixed-width type takes in a copied column.
    pub(crate) fn fixed_width(self) -> Option<usize> {
        match self {
            Self::U8 | Self::I8 | Self::Bool => Some(1),
            Self::U16 | Self::I16 => Some(2),
            Self::U32 | Self::I32 | Self::F32 => Some(4),
            Self::U64 | Self::I64 | Self::F64 | Self::Datetime => Some(8),
            Self::String | Self::Bytes | Self::FixedList | Self::List => None,
        }
    }
}

/// Which cells of a schema or a batch an accessor reads, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
#[repr(i32)]
pub enum Part {
    Rows = 1,
    BranchKey = 2,
}

impl Part {
    /// Reads a part a host passed as an integer.
    pub(crate) fn from_host(part: i32) -> Result<Self, Failure> {
        Self::from_repr(part).ok_or_else(|| Failure::invalid_argument("part", "is not an nx_part"))
    }
}

/// A subscription's row schema, shared with the events that follow it.
#[derive(Debug, Clone)]
pub struct Schema {
    schema: Arc<RowSchema>,
}

impl Schema {
    pub(crate) fn new(schema: Arc<RowSchema>) -> Self {
        Self { schema }
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

    pub(crate) fn field(&self, part: Part, index: usize) -> Result<&SchemaField, Failure> {
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

/// # Safety
///
/// `schema` is a live schema this library returned; a non-null `count` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_schema_field_count(
    schema: *const Schema,
    part: i32,
    count: *mut usize,
) -> *mut Failure {
    // SAFETY: the header requires a live schema and a writable `count`.
    abi::outcome(unsafe { write_field_count(schema, part, count) })
}

/// # Safety
///
/// As [`nx_schema_field_count`].
unsafe fn write_field_count(
    schema: *const Schema,
    part: i32,
    count: *mut usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live schema.
    let schema = unsafe { abi::handle(schema, "schema") }?;
    abi::require_out(count, "count")?;
    let fields = schema.fields(Part::from_host(part)?);
    // SAFETY: `count` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(count, fields.len()) };
    Ok(())
}

/// # Safety
///
/// `schema` is a live schema this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_schema_field(
    schema: *const Schema,
    part: i32,
    index: usize,
    name: *mut *const u8,
    name_len: *mut usize,
    field_type: *mut FieldType,
    nullable: *mut bool,
    sensitive: *mut bool,
) -> *mut Failure {
    // SAFETY: the header requires a live schema and writable out-parameters.
    abi::outcome(unsafe {
        write_field(
            schema, part, index, name, name_len, field_type, nullable, sensitive,
        )
    })
}

/// # Safety
///
/// As [`nx_schema_field`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI returns each property of a field through its own out-parameter"
)]
unsafe fn write_field(
    schema: *const Schema,
    part: i32,
    index: usize,
    name: *mut *const u8,
    name_len: *mut usize,
    field_type: *mut FieldType,
    nullable: *mut bool,
    sensitive: *mut bool,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live schema.
    let schema = unsafe { abi::handle(schema, "schema") }?;
    let field = schema.field(Part::from_host(part)?, index)?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe {
        abi::write_bytes(name, name_len, field.name.as_str().as_bytes());
        abi::write(field_type, FieldType::from(&field.ty));
        abi::write(nullable, field.optional);
        abi::write(sensitive, field.sensitive);
    }
    Ok(())
}

/// # Safety
///
/// `schema` is a live schema this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_schema_branch(
    schema: *const Schema,
    name: *mut *const u8,
    name_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live schema.
    let schema = unsafe { abi::accessor(schema) };
    let Some(branch) = schema.branch() else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe { abi::write_bytes(name, name_len, branch.as_bytes()) };
    true
}

/// # Safety
///
/// A non-null `schema` is a schema this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_schema_free(schema: *mut Schema) {
    // SAFETY: the header requires an unreleased schema or null.
    unsafe { abi::release(schema) };
}
