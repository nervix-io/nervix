//! JNI access to a retained typed entity metadata read.
//!
//! Layer: edges.
//! - **Owns.** Exact Java metadata projections and independent native snapshot handles.
//! - **Depends on.** The shared host snapshot, existing handle leases and JNI.
//! - **Must not know.** Display parsing, registry storage or execution tasks.

#![cfg_attr(nervix_lint, nervix::layer(edge, reason = "owns JVM metadata views"))]

use jni::{
    Env,
    objects::{JByteArray, JClass, JObject, JObjectArray, JString},
    refs::Auto,
    sys::{jint, jlong},
};
use meticulous::OptionExt as _;
use nervix_client_host::{EntityLabelViewKind, Failure, FailureKind};
use nervix_primitives::sync::Arc;

use crate::{error::Error, handles::Value, marshal};

native!(outcome_entity_labels(handle: long) -> long);
fn outcome_entity_labels<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<jlong, Error> {
    Ok(match value!(handle).outcome()?.entity_labels() {
        Some(labels) => Value::EntityLabels(Arc::new(labels)).into_handle(),
        None => 0,
    })
}

macro_rules! label_string {
    ($name:ident, $getter:ident) => {
        native!($name(handle: long) -> JString);
        fn $name<'local>(
            env: &mut Env<'local>,
            _class: JClass<'local>,
            handle: jlong,
        ) -> Result<JString<'local>, Error> {
            Ok(env.new_string(value!(handle).entity_labels()?.$getter())?)
        }
    };
}
label_string!(entity_labels_domain, domain);
label_string!(entity_labels_entity_kind, entity_kind);
label_string!(entity_labels_entity_name, entity_name);

macro_rules! label_unsigned {
    ($name:ident, $getter:ident) => {
        native!($name(handle: long) -> long);
        fn $name<'local>(
            _env: &mut Env<'local>,
            _class: JClass<'local>,
            handle: jlong,
        ) -> Result<jlong, Error> {
            Ok(value!(handle).entity_labels()?.$getter().cast_signed())
        }
    };
}
label_unsigned!(entity_labels_policy_revision, policy_revision);
label_unsigned!(entity_labels_max_count, max_count);
label_unsigned!(entity_labels_max_key_bytes, max_key_bytes);
label_unsigned!(entity_labels_max_value_bytes, max_value_bytes);
label_unsigned!(entity_labels_max_bytes, max_bytes);

macro_rules! label_optional_unsigned {
    ($name:ident, $getter:ident) => {
        native!($name(handle: long) -> JObject);
        fn $name<'local>(
            env: &mut Env<'local>,
            _class: JClass<'local>,
            handle: jlong,
        ) -> Result<JObject<'local>, Error> {
            crate::inspection::optional_unsigned(env, value!(handle).entity_labels()?.$getter())
        }
    };
}
label_optional_unsigned!(entity_labels_revision, revision);
label_optional_unsigned!(entity_labels_position, position);

native!(entity_labels_view(handle: long) -> int);
fn entity_labels_view<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<jint, Error> {
    Ok(match value!(handle).entity_labels()?.view() {
        EntityLabelViewKind::Committed => 1,
        EntityLabelViewKind::Staged => 2,
    })
}

native!(entity_labels_transaction_id(handle: long) -> JString);
fn entity_labels_transaction_id<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<JString<'local>, Error> {
    marshal::string(env, value!(handle).entity_labels()?.transaction_id())
}

native!(entity_labels_planning_basis(handle: long) -> [jbyte]);
fn entity_labels_planning_basis<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<JByteArray<'local>, Error> {
    match value!(handle).entity_labels()?.planning_basis() {
        Some(bytes) => Ok(env.byte_array_from_slice(bytes)?),
        None => Ok(JByteArray::null()),
    }
}

native!(entity_labels_entries(handle: long) -> [JObject]);
fn entity_labels_entries<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<JObjectArray<'local>, Error> {
    let labels = value!(handle).entity_labels()?;
    let length = labels.count().checked_mul(2).ok_or_else(|| {
        Failure::new(
            FailureKind::InvalidArgument,
            "the label entry array exceeds this host's size",
        )
    })?;
    marshal::count(length)?;
    let entries = JObjectArray::<JObject>::new(env, length, JObject::null())?;
    for (index, (key, value)) in labels.labels().iter().enumerate() {
        let offset = index
            .checked_mul(2)
            .assured("the complete double-width array length fits this host");
        let key = Auto::new(env.new_string(key.as_str())?);
        let value = Auto::new(env.new_string(value)?);
        let key_object: &JObject<'_> = key.as_ref();
        let value_object: &JObject<'_> = value.as_ref();
        entries.set_element(env, offset, key_object)?;
        entries.set_element(env, offset + 1, value_object)?;
    }
    Ok(entries)
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] = &[
    outcome_entity_labels::METHOD,
    entity_labels_domain::METHOD,
    entity_labels_entity_kind::METHOD,
    entity_labels_entity_name::METHOD,
    entity_labels_policy_revision::METHOD,
    entity_labels_max_count::METHOD,
    entity_labels_max_key_bytes::METHOD,
    entity_labels_max_value_bytes::METHOD,
    entity_labels_max_bytes::METHOD,
    entity_labels_revision::METHOD,
    entity_labels_position::METHOD,
    entity_labels_view::METHOD,
    entity_labels_transaction_id::METHOD,
    entity_labels_planning_basis::METHOD,
    entity_labels_entries::METHOD,
];
