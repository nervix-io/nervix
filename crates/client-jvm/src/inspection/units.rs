//! JNI metadata views of an inspection's immutable execution units.
//!
//! Layer: edges.
//! - **Owns.** Checked unit indices, exact owners/origins and Java metadata projections.
//! - **Depends on.** The retained inspection, shared vocabulary and JNI value conversion.
//! - **Must not know.** Owner selection, storage, cursor encoding or paging policy.

use jni::{
    Env,
    objects::{JByteArray, JClass, JObject, JObjectArray, JString},
    refs::Auto,
    sys::{jint, jlong},
};

use crate::{error::Error, marshal};

macro_rules! unit {
    ($handle:expr, $index:expr) => {
        value!($handle)
            .inspection()?
            .unit(marshal::size($index, "unit")?)?
    };
}

macro_rules! unit_string {
    ($name:ident, $($accessor:tt)+) => {
        native!($name(handle: long, unit: int) -> JString);
        fn $name<'local>(env: &mut Env<'local>, _class: JClass<'local>, handle: jlong, unit: jint) -> Result<JString<'local>, Error> {
            let text = unit!(handle, unit).$($accessor)+;
            Ok(env.new_string(text)?)
        }
    };
}

unit_string!(inspection_unit_owner, owner.as_str());
unit_string!(inspection_unit_origin_node, origin.node.node_id().as_str());

native!(inspection_unit_kind(handle: long, unit: int) -> int);
fn inspection_unit_kind<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
) -> Result<jint, Error> {
    let kind = value!(handle)
        .inspection()?
        .unit_kind(marshal::size(unit, "unit")?)?;
    Ok(marshal::inspection_unit_kind(kind))
}

native!(inspection_unit_index(handle: long, unit: int) -> JObject);
fn inspection_unit_index<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
) -> Result<JObject<'local>, Error> {
    let index = value!(handle)
        .inspection()?
        .unit_index(marshal::size(unit, "unit")?)?;
    super::optional_unsigned(env, index)
}

native!(inspection_unit_replicas(handle: long, unit: int) -> [JObject]);
fn inspection_unit_replicas<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
) -> Result<JObjectArray<'local>, Error> {
    let replicas = &unit!(handle, unit).replicas;
    marshal::count(replicas.len())?;
    let values = JObjectArray::<JObject>::new(env, replicas.len(), JObject::null())?;
    for (index, node) in replicas.iter().enumerate() {
        let text = Auto::new(env.new_string(node.as_str())?);
        let object: &JObject<'_> = text.as_ref();
        values.set_element(env, index, object)?;
    }
    Ok(values)
}

macro_rules! unit_unsigned {
    ($name:ident, $($accessor:tt)+) => {
        native!($name(handle: long, unit: int) -> long);
        fn $name<'local>(_env: &mut Env<'local>, _class: JClass<'local>, handle: jlong, unit: jint) -> Result<jlong, Error> {
            Ok(unit!(handle, unit).$($accessor)+.cast_signed())
        }
    };
}
unit_unsigned!(
    inspection_unit_origin_incarnation,
    origin.node.incarnation().get()
);
unit_unsigned!(inspection_unit_origin_generation, origin.generation);
unit_unsigned!(inspection_unit_origin_revision, origin.revision);

native!(inspection_unit_origin_fingerprint(handle: long, unit: int) -> [byte]);
fn inspection_unit_origin_fingerprint<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
) -> Result<JByteArray<'local>, Error> {
    Ok(env.byte_array_from_slice(&unit!(handle, unit).origin.fingerprint)?)
}

native!(inspection_unit_origin_stage(handle: long, unit: int) -> int);
fn inspection_unit_origin_stage<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
) -> Result<jint, Error> {
    Ok(marshal::inspection_stage(unit!(handle, unit).origin.stage))
}

native!(inspection_unit_counts(handle: long, unit: int) -> JObject);
fn inspection_unit_counts<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
) -> Result<JObject<'local>, Error> {
    super::counts(env, Some(unit!(handle, unit).counts))
}

native!(inspection_sections_len(handle: long, unit: int) -> int);
fn inspection_sections_len<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
    unit: jint,
) -> Result<jint, Error> {
    marshal::count(unit!(handle, unit).sections.len())
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(super) const METHODS: &[jni::NativeMethod] = &[
    inspection_unit_kind::METHOD,
    inspection_unit_index::METHOD,
    inspection_unit_owner::METHOD,
    inspection_unit_replicas::METHOD,
    inspection_unit_origin_node::METHOD,
    inspection_unit_origin_incarnation::METHOD,
    inspection_unit_origin_fingerprint::METHOD,
    inspection_unit_origin_generation::METHOD,
    inspection_unit_origin_revision::METHOD,
    inspection_unit_origin_stage::METHOD,
    inspection_unit_counts::METHOD,
    inspection_sections_len::METHOD,
];
