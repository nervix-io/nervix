//! Subscription events, verified frame views and bulk row columns.
//!
//! Layer: edges.
//! - **Owns.** Java event accessors and the borrowed direct-buffer projection.
//! - **Depends on.** Host events, schemas and verified frame storage.
//! - **Must not know.** Subscription recovery, decoding or delivery policy.
//!
//! Native.buffer is called with an independent lease. Java registers that lease on the root
//! direct buffer before handing out a read-only view; duplicates and slices retain the root.
//! The root is never exposed writable, and its Cleaner ends the lease after its last view.

use jni::{
    Env, jni_sig, jni_str,
    objects::{JObject, JValue},
    sys::{jint, jlong},
};
use nervix_client_core::{SubscriptionEvent, SubscriptionHandle};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;

use crate::{error::Error, handles::Value, marshal};

pub(crate) fn subscription<'local>(
    env: &mut Env<'local>,
    handle: &SubscriptionHandle,
) -> Result<JObject<'local>, Error> {
    let name = env.new_string(handle.name.as_str())?;
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$Subscription"),
        jni_sig!("(Ljava/lang/String;J)V"),
        &[
            JValue::Object(name.as_ref()),
            JValue::Long(handle.generation.get().cast_signed()),
        ],
    )?)
}

pub(crate) fn restoration<'local>(
    env: &mut Env<'local>,
    cause: host::RestorationCause,
    message: &str,
    retry_after: std::time::Duration,
) -> Result<JObject<'local>, Error> {
    let message = env.new_string(message)?;
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$RestorationFailure"),
        jni_sig!("(ILjava/lang/String;J)V"),
        &[
            JValue::Int(marshal::restoration_cause(cause)),
            JValue::Object(message.as_ref()),
            JValue::Long(host::nanos(retry_after).cast_signed()),
        ],
    )?)
}

native!(event_kind(handle: long) -> int);

fn event_kind<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::event_kind(value!(handle).event()?.kind()))
}
native!(event_rows(handle: long) -> long);

fn event_rows<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(value!(handle).event()?.row_count().cast_signed())
}
native!(event_subscription(handle: long) -> JObject);

fn event_subscription<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    subscription(
        env,
        value!(handle).event()?.subscription_event().subscription(),
    )
}
native!(event_schema(handle: long) -> long);

fn event_schema<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Schema(value!(handle).event()?.schema()?).into_handle())
}
native!(event_batch(handle: long) -> long);

fn event_batch<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Batch(Arc::new(value!(handle).event()?.rows_batch()?)).into_handle())
}
native!(event_restoration(handle: long) -> JObject);

fn event_restoration<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    let event = value!(handle).event()?;
    match event.subscription_event() {
        SubscriptionEvent::RestorationFailed(failure) => restoration(
            env,
            event.restoration_cause()?,
            &failure.message,
            failure.retry_after,
        ),
        _ => Ok(JObject::null()),
    }
}
native!(event_end(handle: long) -> int);

fn event_end<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::end_reason(value!(handle).event()?.end_reason()?))
}

native!(buffer(handle: long) -> JObject);

fn buffer<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    let bytes = match value!(handle) {
        Value::Event(event) => event.frame()?,
        Value::Batch(batch) => batch.ipc()?,
        Value::Delivery(delivery) => delivery.ipc(),
        _ => {
            return Err(Error::argument(
                "buffer",
                "the handle carries no frame or IPC stream",
            ));
        }
    };
    marshal::count(bytes.len())?;
    // SAFETY: Java owns the independent lease passed here until the root buffer's Cleaner
    // runs. The root never escapes writable; every returned view is read-only and keeps it.
    let buffer = unsafe { env.new_direct_byte_buffer(bytes.as_ptr().cast_mut(), bytes.len()) }?;
    Ok(buffer.into())
}

fn cells(event: &host::Event, part: host::Part, column: usize) -> Result<usize, Error> {
    // Establish the host's row and field contract before a non-row count can size a Java array.
    let schema = event.schema()?;
    schema.field(part, column)?;
    let count = match part {
        host::Part::Rows => event.row_count(),
        host::Part::BranchKey => 1,
    };
    let count = usize::try_from(count)
        .map_err(|_| Error::argument("cells", "exceeds this host's address space"))?;
    marshal::count(count)?;
    Ok(count)
}

native!(event_states(handle: long, part: int, column: int) -> [byte]);

fn event_states<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    part: jint,
    column: jint,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    let event = value!(handle).event()?;
    let part = marshal::part(part)?;
    let column = marshal::size(column, "column")?;
    let mut states = vec![0; cells(event, part, column)?];
    event.column_states(part, column, &mut states)?;
    Ok(env.byte_array_from_slice(&states)?)
}

native!(event_fixed(handle: long, part: int, column: int) -> [byte]);

fn event_fixed<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    part: jint,
    column: jint,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    let event = value!(handle).event()?;
    let part = marshal::part(part)?;
    let column = marshal::size(column, "column")?;
    let length = event.fixed_len(part, column)?;
    marshal::count(length)?;
    let mut data = vec![0; length];
    event.column_fixed(part, column, &mut data)?;
    Ok(env.byte_array_from_slice(&data)?)
}

native!(event_varlen(handle: long, part: int, column: int) -> JObject);

fn event_varlen<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    part: jint,
    column: jint,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    let event = value!(handle).event()?;
    let part = marshal::part(part)?;
    let column = marshal::size(column, "column")?;
    let length = cells(event, part, column)?
        .checked_add(1)
        .ok_or_else(|| Error::argument("cells", "overflows"))?;
    marshal::count(length)?;
    let bytes = event.column_varlen_len(part, column)?;
    marshal::count(bytes)?;
    let mut offsets = vec![0; length];
    let mut data = vec![0; bytes];
    event.column_varlen(part, column, &mut offsets, &mut data)?;
    crate::batches::varlen(env, &offsets, &data)
}

native!(event_cell(handle: long, part: int, row: int, column: int) -> [byte]);

fn event_cell<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    part: jint,
    row: jint,
    column: jint,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    let bytes = value!(handle).event()?.cell_varlen(
        marshal::part(part)?,
        marshal::size(row, "row")?,
        marshal::size(column, "column")?,
    )?;
    Ok(env.byte_array_from_slice(bytes)?)
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] = &[
    event_kind::METHOD,
    event_rows::METHOD,
    event_subscription::METHOD,
    event_schema::METHOD,
    event_batch::METHOD,
    event_restoration::METHOD,
    event_end::METHOD,
    buffer::METHOD,
    event_states::METHOD,
    event_fixed::METHOD,
    event_varlen::METHOD,
    event_cell::METHOD,
];
