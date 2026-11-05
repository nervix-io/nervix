//! Field descriptions, typed batch construction and bulk column copies.
//!
//! Layer: edges.
//! - **Owns.** JNI field/batch handles and Java array copies at one column level at a time.
//! - **Depends on.** The shared host's validated fields, builders, batch readers and IPC.
//! - **Must not know.** How batches are submitted, acknowledged or encoded on the wire.

use jni::{
    Env, jni_sig, jni_str,
    objects::{JObject, JValue},
    sys::{jboolean, jint, jlong},
};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;

use crate::{error::Error, handles::Value, marshal};

native!(fields_new() -> long);

fn fields_new<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Fields(host::Fields::default()).into_handle())
}

native!(
    fields_add(
        handle: long,
        name: JString,
        kind: int,
        length: long,
        nullable: boolean,
        sensitive: boolean,
    ) -> ()
);

#[expect(
    clippy::too_many_arguments,
    reason = "JNI adds its environment and class to the foreign field declaration"
)]
fn fields_add<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    name: jni::objects::JString<'local>,
    kind: jint,
    length: jlong,
    nullable: jboolean,
    sensitive: jboolean,
) -> Result<(), crate::error::Error> {
    let name = name.try_to_string(env)?;
    let length = u32::try_from(length)
        .map_err(|_| Error::argument("length", "must fit an unsigned 32-bit element count"))?;
    mutable!(handle).fields_mut()?.add(
        &name,
        marshal::field_type(kind)?,
        length,
        nullable,
        sensitive,
    )?;
    Ok(())
}

native!(fields_element(handle: long, kind: int, length: long) -> ());

fn fields_element<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    kind: jint,
    length: jlong,
) -> Result<(), crate::error::Error> {
    let length = u32::try_from(length)
        .map_err(|_| Error::argument("length", "must fit an unsigned 32-bit element count"))?;
    mutable!(handle)
        .fields_mut()?
        .element(marshal::field_type(kind)?, length)?;
    Ok(())
}

native!(fields_schema(handle: long) -> long);

fn fields_schema<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    let fields = value!(handle).fields()?.schema_fields()?;
    Ok(Value::Schema(host::Schema::of_fields(fields)).into_handle())
}

native!(schema_fields(handle: long, part: int) -> [JObject]);

fn schema_fields<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    part: jint,
) -> Result<jni::objects::JObjectArray<'local>, crate::error::Error> {
    marshal::fields(env, value!(handle).schema()?, marshal::part(part)?)
}

native!(schema_branch(handle: long) -> JString);

fn schema_branch<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JString<'local>, crate::error::Error> {
    marshal::string(env, value!(handle).schema()?.branch())
}

native!(builder_new(schema: long, rows: int) -> long);

fn builder_new<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    schema: jlong,
    rows: jint,
) -> Result<jlong, crate::error::Error> {
    let schema = value!(schema).schema()?.clone();
    Ok(Value::Builder(host::BatchBuilder::new(
        schema,
        marshal::size(rows, "rows")?,
    ))
    .into_handle())
}

native!(builder_states(handle: long, column: int, states: [byte]) -> ());

fn builder_states<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
    states: jni::objects::JByteArray<'local>,
) -> Result<(), crate::error::Error> {
    let states = env.convert_byte_array(states)?;
    mutable!(handle)
        .builder_mut()?
        .states(marshal::size(column, "column")?, &states)?;
    Ok(())
}

native!(builder_offsets(handle: long, column: int, level: int, offsets: [long]) -> ());

fn builder_offsets<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
    level: jint,
    offsets: jni::objects::JLongArray<'local>,
) -> Result<(), crate::error::Error> {
    let offsets = marshal::offsets(env, &offsets)?;
    mutable!(handle).builder_mut()?.offsets(
        marshal::size(column, "column")?,
        marshal::size(level, "level")?,
        &offsets,
    )?;
    Ok(())
}

native!(builder_fixed(handle: long, column: int, level: int, data: [byte]) -> ());

fn builder_fixed<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
    level: jint,
    data: jni::objects::JByteArray<'local>,
) -> Result<(), crate::error::Error> {
    let data = env.convert_byte_array(data)?;
    mutable!(handle).builder_mut()?.fixed(
        marshal::size(column, "column")?,
        marshal::size(level, "level")?,
        &data,
    )?;
    Ok(())
}

native!(builder_varlen(handle: long, column: int, level: int, offsets: [long], data: [byte]) -> ());

fn builder_varlen<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
    level: jint,
    offsets: jni::objects::JLongArray<'local>,
    data: jni::objects::JByteArray<'local>,
) -> Result<(), crate::error::Error> {
    let offsets = marshal::offsets(env, &offsets)?;
    let data = env.convert_byte_array(data)?;
    mutable!(handle).builder_mut()?.varlen(
        marshal::size(column, "column")?,
        marshal::size(level, "level")?,
        &offsets,
        &data,
    )?;
    Ok(())
}

native!(builder_finish(handle: long) -> long);

fn builder_finish<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    let batch = mutable!(handle).builder_mut()?.finish()?;
    Ok(Value::Batch(Arc::new(batch)).into_handle())
}

native!(batch_from_ipc(schema: long, rows: int, ipc: [byte]) -> long);

fn batch_from_ipc<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    schema: jlong,
    rows: jint,
    ipc: jni::objects::JByteArray<'local>,
) -> Result<jlong, crate::error::Error> {
    let schema = value!(schema).schema()?.clone();
    let rows = u32::try_from(rows).map_err(|_| Error::argument("rows", "must not be negative"))?;
    let ipc = env.convert_byte_array(ipc)?;
    let batch = host::Batch::decode(ipc.into(), schema, rows)?;
    Ok(Value::Batch(Arc::new(batch)).into_handle())
}

native!(batch_rows(handle: long) -> int);

fn batch_rows<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    marshal::count(value!(handle).batch()?.row_count())
}

native!(batch_schema(handle: long) -> long);

fn batch_schema<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Schema(value!(handle).batch()?.schema().clone()).into_handle())
}

native!(batch_ipc(handle: long) -> [byte]);

fn batch_ipc<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    Ok(env.byte_array_from_slice(value!(handle).batch()?.ipc()?)?)
}

native!(batch_cells(handle: long, column: int, level: int) -> int);

fn batch_cells<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
    level: jint,
) -> Result<jint, crate::error::Error> {
    marshal::count(value!(handle).batch()?.cells(
        marshal::size(column, "column")?,
        marshal::size(level, "level")?,
    )?)
}

native!(batch_states(handle: long, column: int) -> [byte]);

fn batch_states<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    let batch = value!(handle).batch()?;
    let column = marshal::size(column, "column")?;
    batch.schema().field(host::Part::Rows, column)?;
    marshal::count(batch.row_count())?;
    let mut states = vec![0; batch.row_count()];
    batch.states(column, &mut states)?;
    Ok(env.byte_array_from_slice(&states)?)
}

native!(batch_offsets(handle: long, column: int, level: int) -> [long]);

fn batch_offsets<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
    level: jint,
) -> Result<jni::objects::JLongArray<'local>, crate::error::Error> {
    let batch = value!(handle).batch()?;
    let column = marshal::size(column, "column")?;
    let level = marshal::size(level, "level")?;
    let length = batch
        .cells(column, level)?
        .checked_add(1)
        .ok_or_else(|| Error::argument("length", "overflows"))?;
    marshal::count(length)?;
    let mut offsets = vec![0; length];
    batch.offsets(column, level, &mut offsets)?;
    marshal::unsigned_longs(env, &offsets)
}

native!(batch_fixed(handle: long, column: int, level: int) -> [byte]);

fn batch_fixed<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
    level: jint,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    let batch = value!(handle).batch()?;
    let column = marshal::size(column, "column")?;
    let level = marshal::size(level, "level")?;
    let length = batch.fixed_len(column, level)?;
    marshal::count(length)?;
    let mut data = vec![0; length];
    batch.fixed(column, level, &mut data)?;
    Ok(env.byte_array_from_slice(&data)?)
}

native!(batch_varlen(handle: long, column: int, level: int) -> JObject);

fn batch_varlen<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    column: jint,
    level: jint,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    let batch = value!(handle).batch()?;
    let column = marshal::size(column, "column")?;
    let level = marshal::size(level, "level")?;
    let length = batch
        .cells(column, level)?
        .checked_add(1)
        .ok_or_else(|| Error::argument("length", "overflows"))?;
    marshal::count(length)?;
    let bytes = batch.varlen_len(column, level)?;
    marshal::count(bytes)?;
    let mut offsets = vec![0; length];
    let mut data = vec![0; bytes];
    batch.varlen(column, level, &mut offsets, &mut data)?;
    varlen(env, &offsets, &data)
}

pub(crate) fn varlen<'local>(
    env: &mut Env<'local>,
    offsets: &[u64],
    data: &[u8],
) -> Result<JObject<'local>, Error> {
    let offsets = marshal::unsigned_longs(env, offsets)?;
    let data = env.byte_array_from_slice(data)?;
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$Varlen"),
        jni_sig!("([J[B)V"),
        &[
            JValue::Object(offsets.as_ref()),
            JValue::Object(data.as_ref()),
        ],
    )?)
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] = &[
    fields_new::METHOD,
    fields_add::METHOD,
    fields_element::METHOD,
    fields_schema::METHOD,
    schema_fields::METHOD,
    schema_branch::METHOD,
    builder_new::METHOD,
    builder_states::METHOD,
    builder_offsets::METHOD,
    builder_fixed::METHOD,
    builder_varlen::METHOD,
    builder_finish::METHOD,
    batch_from_ipc::METHOD,
    batch_rows::METHOD,
    batch_schema::METHOD,
    batch_ipc::METHOD,
    batch_cells::METHOD,
    batch_states::METHOD,
    batch_offsets::METHOD,
    batch_fixed::METHOD,
    batch_varlen::METHOD,
];
