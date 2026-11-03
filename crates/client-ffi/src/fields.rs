//! `nx_fields`: the fields a host expects an endpoint to have.
//!
//! - **Owns.** The field list handle a host builds one type level at a time and frees.
//! - **Depends on.** The shared field descriptions and the C ABI boundary.
//! - **Must not know.** Which types a field may have; the shared field descriptions refuse what
//!   cannot be a Nervix type.

use nervix_client_host::{Failure, Fields};

use crate::abi;

#[unsafe(no_mangle)]
pub extern "C" fn nx_fields_new() -> *mut Fields {
    abi::into_handle(Fields::default())
}

/// # Safety
///
/// `fields` is a live field list no other thread uses, and a non-null `name` addresses
/// `name_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_fields_add(
    fields: *mut Fields,
    name: *const u8,
    name_len: usize,
    field_type: i32,
    length: u32,
    nullable: bool,
    sensitive: bool,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_added(
            fields, name, name_len, field_type, length, nullable, sensitive,
        )
    })
}

/// # Safety
///
/// As [`nx_fields_add`].
unsafe fn write_added(
    fields: *mut Fields,
    name: *const u8,
    name_len: usize,
    field_type: i32,
    length: u32,
    nullable: bool,
    sensitive: bool,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live field list no other thread uses and a readable name.
    let (fields, name) = unsafe {
        (
            abi::handle_mut(fields, "fields")?,
            abi::text(name, name_len, "name")?,
        )
    };
    fields.add(
        name,
        abi::field_type(field_type)?,
        length,
        nullable,
        sensitive,
    )
}

/// # Safety
///
/// `fields` is a live field list no other thread uses.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_fields_element(
    fields: *mut Fields,
    field_type: i32,
    length: u32,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_element(fields, field_type, length) })
}

/// # Safety
///
/// As [`nx_fields_element`].
unsafe fn write_element(fields: *mut Fields, field_type: i32, length: u32) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live field list no other thread uses.
    let fields = unsafe { abi::handle_mut(fields, "fields") }?;
    fields.element(abi::field_type(field_type)?, length)
}

/// # Safety
///
/// A non-null `fields` is a field list this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_fields_free(fields: *mut Fields) {
    // SAFETY: the header requires an unreleased field list or null.
    unsafe { abi::release(fields) };
}
