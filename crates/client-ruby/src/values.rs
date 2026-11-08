//! Typed values and borrowed bytes at the Ruby boundary.
//!
//! Layer: edges.
//! - **Owns.** Exact scalar/list projection, binary strings and immutable owner-retaining views.
//! - **Depends on.** Ruby's IO::Buffer, Arrow's typed arrays and verified client wire cells.
//! - **Must not know.** Sessions, schema admission, routing or recovery.

use arrow_array::{Array, cast::AsArray as _, types};
use magnus::{
    Error, IntoValue, RString, RTypedData, Ruby, Value,
    prelude::*,
    rb_sys::{FromRawValue, protect},
};
use meticulous::OptionExt as _;
use nervix_client_core::wire::CellView;
use nervix_models::ParseAsType;

// Ruby's flags are a C bitmask. rb-sys emits a Rust enum that cannot represent their union;
// the C shim combines them in C without ever constructing an invalid Rust discriminant.
unsafe extern "C" {
    fn nervix_ruby_binary_string(length: std::ffi::c_long, bytes: *mut *mut u8) -> rb_sys::VALUE;
    fn nervix_ruby_readonly_buffer(
        base: *mut std::ffi::c_void,
        size: rb_sys::size_t,
    ) -> rb_sys::VALUE;
}

/// Copy host-owned bytes while the GVL prevents Ruby from changing them.
pub(crate) fn bytes(value: RString) -> Vec<u8> {
    // SAFETY: The slice is copied immediately, with no Ruby call or allocation until the borrow
    // ends. The native call frame keeps its argument alive and this thread holds the GVL.
    unsafe { value.as_slice() }.to_vec()
}

/// Fill one Ruby-owned binary allocation directly, without an intermediate native byte vector.
///
/// # Safety
/// `write` must call only native host accessors and make no Ruby call while its slice exists.
/// The GVL must be held throughout the call.
pub(crate) unsafe fn binary(
    len: usize,
    write: impl FnOnce(&mut [u8]) -> Result<(), nervix_client_host::Failure>,
) -> Result<RString, Error> {
    let length = std::ffi::c_long::try_from(len)
        .map_err(|_| crate::errors::invalid("column", "length exceeds Ruby's address space"))?;
    let mut bytes = std::ptr::null_mut();
    let raw = protect(|| {
        // SAFETY: length is non-negative and fits Ruby's signed length. The shim returns one
        // fresh binary String, initializes every byte, and writes its valid storage pointer.
        unsafe { nervix_ruby_binary_string(length, &mut bytes) }
    })?;
    let result = {
        // SAFETY: The fresh String is not exposed to Ruby code. Its pointer is valid for len
        // initialized bytes, the GVL is held, and the caller forbids Ruby calls in write.
        let output = unsafe { std::slice::from_raw_parts_mut(bytes, len) };
        write(output)
    };
    result.map_err(crate::errors::raise)?;
    // SAFETY: The protected shim returned a fresh Ruby String.
    let value = unsafe { Value::from_raw(raw) };
    Ok(RString::from_value(value).assured("the shim returns a Ruby String"))
}

/// Export immutable bytes whose exact native owner is kept by `owner`.
///
/// # Safety
/// `owner` must keep this slice valid and unchanged for its entire Ruby lifetime. In particular,
/// its public API must not release the storage while a view retains the object.
pub(crate) unsafe fn buffer(owner: Value, bytes: &[u8]) -> Result<Value, Error> {
    let pointer = bytes.as_ptr().cast_mut().cast();
    let size = rb_sys::size_t::try_from(bytes.len())
        .map_err(|_| crate::errors::invalid("buffer", "length exceeds Ruby's address space"))?;
    let raw = protect(|| {
        // SAFETY: The caller supplies immutable owned storage. EXTERNAL prevents Ruby freeing
        // it, READONLY prevents writes, and LOCKED prevents a root transfer losing its owner.
        unsafe { nervix_ruby_readonly_buffer(pointer, size) }
    })?;
    // SAFETY: protect returned a valid Ruby object allocated by rb_io_buffer_new.
    let value = unsafe { Value::from_raw(raw) };
    RTypedData::from_value(value)
        .assured("rb_io_buffer_new returns Ruby typed data")
        .ivar_set("@nervix_owner", owner)?;
    // Ruby marks this reference, and slices mark their root buffer. Freezing the root forbids
    // replacing/removing its owner ivar; its permanent native lock forbids pointer transfer.
    value.freeze();
    Ok(value)
}

pub(crate) fn cell(ruby: &Ruby, value: CellView<'_>) -> Result<Value, Error> {
    let value = match value {
        CellView::Null => ruby.qnil().as_value(),
        CellView::Redacted => ruby.to_symbol("redacted").as_value(),
        CellView::U8(value) => value.into_value_with(ruby),
        CellView::I8(value) => value.into_value_with(ruby),
        CellView::U16(value) => value.into_value_with(ruby),
        CellView::I16(value) => value.into_value_with(ruby),
        CellView::U32(value) => value.into_value_with(ruby),
        CellView::I32(value) => value.into_value_with(ruby),
        CellView::U64(value) => value.into_value_with(ruby),
        CellView::I64(value) => value.into_value_with(ruby),
        CellView::F32(value) => f64::from(value).into_value_with(ruby),
        CellView::F64(value) => value.into_value_with(ruby),
        CellView::Bool(value) => value.into_value_with(ruby),
        CellView::String(value) => ruby.str_new(value).as_value(),
        CellView::Bytes(value) => ruby.str_from_slice(value).as_value(),
        CellView::Datetime(value) => value.unix_nanos().into_value_with(ruby),
        CellView::List(items) => {
            let array = ruby.ary_new_capa(items.len());
            for item in items.iter() {
                array.push(cell(ruby, item)?)?;
            }
            array.as_value()
        }
    };
    Ok(value)
}

/// A scalar is converted only at this language boundary. DATETIME remains signed nanoseconds;
/// float columns also expose their complete original bits through the bulk fixed-width API.
pub(crate) fn arrow(
    ruby: &Ruby,
    array: &dyn Array,
    row: usize,
    ty: &ParseAsType,
) -> Result<Value, Error> {
    if array.is_null(row) {
        return Ok(ruby.qnil().as_value());
    }
    const TYPED: &str = "the host batch validated each array against its exact schema type";
    let value = match ty {
        ParseAsType::U8 => array
            .as_primitive_opt::<types::UInt8Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::I8 => array
            .as_primitive_opt::<types::Int8Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::U16 => array
            .as_primitive_opt::<types::UInt16Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::I16 => array
            .as_primitive_opt::<types::Int16Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::U32 => array
            .as_primitive_opt::<types::UInt32Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::I32 => array
            .as_primitive_opt::<types::Int32Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::U64 => array
            .as_primitive_opt::<types::UInt64Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::I64 => array
            .as_primitive_opt::<types::Int64Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::F32 => f64::from(
            array
                .as_primitive_opt::<types::Float32Type>()
                .assured(TYPED)
                .value(row),
        )
        .into_value_with(ruby),
        ParseAsType::F64 => array
            .as_primitive_opt::<types::Float64Type>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::Bool => array
            .as_boolean_opt()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::Datetime => array
            .as_primitive_opt::<types::TimestampNanosecondType>()
            .assured(TYPED)
            .value(row)
            .into_value_with(ruby),
        ParseAsType::String => ruby
            .str_new(array.as_string_opt::<i32>().assured(TYPED).value(row))
            .as_value(),
        ParseAsType::Bytes => ruby
            .str_from_slice(array.as_binary_opt::<i32>().assured(TYPED).value(row))
            .as_value(),
        ParseAsType::Vec { element } => {
            let elements = array.as_list_opt::<i32>().assured(TYPED).value(row);
            list(ruby, elements.as_ref(), element)?
        }
        ParseAsType::Array { element, .. } => {
            let elements = array.as_fixed_size_list_opt().assured(TYPED).value(row);
            list(ruby, elements.as_ref(), element)?
        }
    };
    Ok(value)
}

fn list(ruby: &Ruby, array: &dyn Array, ty: &ParseAsType) -> Result<Value, Error> {
    let values = ruby.ary_new_capa(array.len());
    for row in 0..array.len() {
        values.push(arrow(ruby, array, row, ty)?)?;
    }
    Ok(values.as_value())
}
