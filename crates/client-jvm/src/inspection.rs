//! JNI projection of the shared inspection owner and its independently retained frames.
//!
//! Layer: edges.
//! - **Owns.** Typed Java inspection metadata, nullable values and native owner handles.
//! - **Depends on.** The shared host inspection, JNI and existing Java lease ownership.
//! - **Must not know.** Storage, cursor encoding, paging decisions or command routing.

mod sections;
mod units;

use jni::{
    Env, jni_sig, jni_str,
    objects::{JClass, JObject, JString, JValue},
    sys::{jint, jlong},
};
use nervix_client_core::wire::StateInspectionCounts;
use nervix_primitives::sync::Arc;

use crate::{error::Error, handles::Value, marshal};

native!(outcome_inspection(handle: long) -> long);
fn outcome_inspection<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<jlong, Error> {
    match value!(handle).outcome()?.state_inspection() {
        Some(inspection) => Ok(Value::Inspection(Arc::new(inspection)).into_handle()),
        None => Ok(0),
    }
}

macro_rules! inspection_string {
    ($name:ident, $accessor:ident) => {
        native!($name(handle: long) -> JString);
        fn $name<'local>(env: &mut Env<'local>, _class: JClass<'local>, handle: jlong) -> Result<JString<'local>, Error> {
            Ok(env.new_string(value!(handle).inspection()?.$accessor())?)
        }
    };
}

inspection_string!(inspection_domain, domain);
inspection_string!(inspection_entity_kind, entity_kind);
inspection_string!(inspection_entity_name, entity_name);
inspection_string!(inspection_json, json);

native!(inspection_rejection(handle: long) -> int);
fn inspection_rejection<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<jint, Error> {
    // Zero is the JNI encoding of a nullable enum; the public Java value is null or a variant.
    Ok(match value!(handle).inspection()?.rejection() {
        Some(reason) => marshal::inspection_rejection(reason),
        None => 0,
    })
}

pub(super) fn counts<'local>(
    env: &mut Env<'local>,
    value: Option<StateInspectionCounts>,
) -> Result<JObject<'local>, Error> {
    let Some(value) = value else {
        return Ok(JObject::null());
    };
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$InspectionCounts"),
        jni_sig!("(JJJ)V"),
        &[
            JValue::Long(value.branches.cast_signed()),
            JValue::Long(value.rows.cast_signed()),
            JValue::Long(value.bytes.cast_signed()),
        ],
    )?)
}

pub(super) fn optional_unsigned<'local>(
    env: &mut Env<'local>,
    value: Option<u64>,
) -> Result<JObject<'local>, Error> {
    match value {
        Some(value) => Ok(env.new_object(
            jni_str!("java/lang/Long"),
            jni_sig!("(J)V"),
            &[JValue::Long(value.cast_signed())],
        )?),
        None => Ok(JObject::null()),
    }
}

native!(inspection_counts(handle: long) -> JObject);
fn inspection_counts<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<JObject<'local>, Error> {
    counts(env, value!(handle).inspection()?.counts())
}

native!(inspection_unit_count(handle: long) -> JObject);
fn inspection_unit_count<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<JObject<'local>, Error> {
    optional_unsigned(env, value!(handle).inspection()?.unit_count())
}

native!(inspection_continuation(handle: long) -> JString);
fn inspection_continuation<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<JString<'local>, Error> {
    marshal::string(env, value!(handle).inspection()?.continuation())
}

native!(inspection_units_len(handle: long) -> int);
fn inspection_units_len<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<jint, Error> {
    marshal::count(value!(handle).inspection()?.units().len())
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] = &[
    outcome_inspection::METHOD,
    inspection_domain::METHOD,
    inspection_entity_kind::METHOD,
    inspection_entity_name::METHOD,
    inspection_json::METHOD,
    inspection_rejection::METHOD,
    inspection_counts::METHOD,
    inspection_unit_count::METHOD,
    inspection_continuation::METHOD,
    inspection_units_len::METHOD,
];
#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const UNIT_METHODS: &[jni::NativeMethod] = units::METHODS;
#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const SECTION_METHODS: &[jni::NativeMethod] = sections::METHODS;
