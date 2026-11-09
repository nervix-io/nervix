//! JNI inspection sections and independently retained verified Row frame handles.
//!
//! Layer: edges.
//! - **Owns.** Checked section/frame access and Java schema/buffer ownership.
//! - **Depends on.** The existing inspection and verified-frame owners and JNI leases.
//! - **Must not know.** State storage, cursor encoding, paging policy or row encoding.

use jni::{
    Env,
    objects::{JClass, JLongArray, JString},
    sys::{jint, jlong},
};
use nervix_primitives::sync::Arc;

use crate::{error::Error, handles::Value, marshal};

macro_rules! section {
    ($handle:expr, $unit:expr, $section:expr) => {
        value!($handle).inspection()?.section(
            marshal::size($unit, "unit")?,
            marshal::size($section, "section")?,
        )?
    };
}

native!(inspection_section_name(handle: long, unit: int, section: int) -> JString);
fn inspection_section_name<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
    section: jint,
) -> Result<JString<'local>, Error> {
    Ok(env.new_string(&section!(handle, unit, section).name)?)
}

native!(inspection_section_omitted_rows(handle: long, unit: int, section: int) -> long);
fn inspection_section_omitted_rows<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
    section: jint,
) -> Result<jlong, Error> {
    Ok(section!(handle, unit, section).omitted_rows.cast_signed())
}

native!(inspection_section_watermarks(handle: long, unit: int, section: int) -> [long]);
fn inspection_section_watermarks<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
    section: jint,
) -> Result<JLongArray<'local>, Error> {
    match section!(handle, unit, section).watermarks {
        Some(columns) => marshal::longs(env, &[i64::from(columns.low), i64::from(columns.high)]),
        None => Ok(JLongArray::null()),
    }
}

native!(inspection_section_schema(handle: long, unit: int, section: int) -> long);
fn inspection_section_schema<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
    section: jint,
) -> Result<jlong, Error> {
    let schema = value!(handle).inspection()?.schema(
        marshal::size(unit, "unit")?,
        marshal::size(section, "section")?,
    )?;
    Ok(Value::Schema(schema).into_handle())
}

native!(inspection_frames_len(handle: long, unit: int, section: int) -> int);
fn inspection_frames_len<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
    section: jint,
) -> Result<jint, Error> {
    marshal::count(section!(handle, unit, section).frames.len())
}

native!(inspection_frame(handle: long, unit: int, section: int, frame: int) -> long);
fn inspection_frame<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
    section: jint,
    frame: jint,
) -> Result<jlong, Error> {
    let frame = value!(handle).inspection()?.row_frame(
        marshal::size(unit, "unit")?,
        marshal::size(section, "section")?,
        marshal::size(frame, "frame")?,
    )?;
    Ok(Value::InspectionFrame(Arc::new(frame)).into_handle())
}

native!(inspection_frame_rows(handle: long) -> long);
fn inspection_frame_rows<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<jlong, Error> {
    Ok(value!(handle).inspection_frame()?.row_count().cast_signed())
}

native!(inspection_frame_schema(handle: long) -> long);
fn inspection_frame_schema<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<jlong, Error> {
    Ok(Value::Schema(value!(handle).inspection_frame()?.schema()).into_handle())
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(super) const METHODS: &[jni::NativeMethod] = &[
    inspection_section_name::METHOD,
    inspection_section_omitted_rows::METHOD,
    inspection_section_watermarks::METHOD,
    inspection_section_schema::METHOD,
    inspection_frames_len::METHOD,
    inspection_frame::METHOD,
    inspection_frame_rows::METHOD,
    inspection_frame_schema::METHOD,
];
