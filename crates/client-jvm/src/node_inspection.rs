//! JNI access to independently retained cluster node inspection replies.
//!
//! Layer: edges.
//! - **Owns.** Projecting a node inspection through Java's native handle leases.
//! - **Depends on.** The shared host inspection and JNI.
//! - **Must not know.** Command text or record storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "projects node inspections to the JVM")
)]

use jni::{
    Env,
    objects::{JClass, JObject, JObjectArray, JString},
    refs::Auto,
    sys::jlong,
};
use nervix_primitives::sync::Arc;

use crate::{error::Error, handles::Value, marshal};

native!(outcome_node_inspection(handle: long) -> long);
fn outcome_node_inspection<'local>(
    _env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<jlong, Error> {
    match value!(handle).outcome()?.node_inspection() {
        Some(inspection) => Ok(Value::NodeInspection(Arc::new(inspection)).into_handle()),
        None => Ok(0),
    }
}

native!(node_inspection_json(handle: long) -> JString);
fn node_inspection_json<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<JString<'local>, Error> {
    Ok(env.new_string(value!(handle).node_inspection()?.json())?)
}

native!(node_inspection_names(handle: long) -> [JObject]);
fn node_inspection_names<'local>(
    env: &mut Env<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> Result<JObjectArray<'local>, Error> {
    let value = value!(handle);
    let nodes = value.node_inspection()?.nodes();
    marshal::count(nodes.len())?;
    let names = JObjectArray::<JObject>::new(env, nodes.len(), JObject::null())?;
    for (index, node) in nodes.iter().enumerate() {
        let name = Auto::new(env.new_string(node.name.as_str())?);
        let name: &JObject<'_> = name.as_ref();
        names.set_element(env, index, name)?;
    }
    Ok(names)
}
