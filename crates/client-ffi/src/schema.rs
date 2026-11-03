//! `nx_schema`: the schema a subscription's rows, or an endpoint's batches, follow.
//!
//! - **Owns.** Reading a schema's fields, the levels of a field's type and its branch through the
//!   C ABI, and freeing it.
//! - **Depends on.** The shared schema and the C ABI boundary.
//! - **Must not know.** Where the schema came from.

use nervix_client_host::{Failure, FieldType, Schema, TypeLevels};
use nervix_models::ParseAsType;

use crate::abi;

/// The element count of a fixed-size list type, and zero for every other type.
fn fixed_length(ty: &ParseAsType) -> u32 {
    match ty {
        ParseAsType::Array { len, .. } => len.get(),
        _ => 0,
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
    let fields = schema.fields(abi::part(part)?);
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
    let field = schema.field(abi::part(part)?, index)?;
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
/// `schema` is a live schema this library returned; a non-null `levels` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_schema_field_levels(
    schema: *const Schema,
    part: i32,
    index: usize,
    levels: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_field_levels(schema, part, index, levels) })
}

/// # Safety
///
/// As [`nx_schema_field_levels`].
unsafe fn write_field_levels(
    schema: *const Schema,
    part: i32,
    index: usize,
    levels: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(levels, "levels")?;
    // SAFETY: the caller guarantees a live schema.
    let schema = unsafe { abi::handle(schema, "schema") }?;
    let field = schema.field(abi::part(part)?, index)?;
    // SAFETY: `levels` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(levels, TypeLevels::of(&field.ty).count()) };
    Ok(())
}

/// # Safety
///
/// `schema` is a live schema this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_schema_field_level(
    schema: *const Schema,
    part: i32,
    index: usize,
    level: usize,
    field_type: *mut FieldType,
    length: *mut u32,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_field_level(schema, part, index, level, field_type, length) })
}

/// # Safety
///
/// As [`nx_schema_field_level`].
unsafe fn write_field_level(
    schema: *const Schema,
    part: i32,
    index: usize,
    level: usize,
    field_type: *mut FieldType,
    length: *mut u32,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live schema.
    let schema = unsafe { abi::handle(schema, "schema") }?;
    let field = schema.field(abi::part(part)?, index)?;
    let ty = TypeLevels::of(&field.ty).level(level)?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe {
        abi::write(field_type, FieldType::from(ty));
        abi::write(length, fixed_length(ty));
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
