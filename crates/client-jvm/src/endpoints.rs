//! JNI operations on producers, consumers, deliveries and submission outcomes.
//!
//! Layer: edges.
//! - **Owns.** Endpoint descriptions and bulk payload copies at the Java boundary.
//! - **Depends on.** Shared host endpoint objects, cancellation and typed vocabulary.
//! - **Must not know.** Admission, retry, settlement or attachment recovery decisions.

use jni::{
    Env, jni_sig, jni_str,
    objects::{JByteArray, JIntArray, JObject, JObjectArray, JValue},
    refs::Auto,
    sys::{jint, jlong},
};
use nervix_client_core::{ClientConsumerLimits, ClientProducerLimits};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;

use crate::{error::Error, handles::Value, marshal};

fn credit(
    batches: jint,
    bytes: jlong,
) -> Result<(std::num::NonZeroU32, std::num::NonZeroU64), Error> {
    let batches =
        u32::try_from(batches).map_err(|_| Error::argument("batches", "must be positive"))?;
    Ok(host::credit(batches, bytes.cast_unsigned())?)
}
fn description<'local>(
    env: &mut Env<'local>,
    numbers: &[jlong],
    contract: &[u8],
    attachment: Option<&str>,
) -> Result<JObject<'local>, Error> {
    let numbers = marshal::longs(env, numbers)?;
    let contract = env.byte_array_from_slice(contract)?;
    let attachment = marshal::string(env, attachment)?;
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$EndpointDescription"),
        jni_sig!("([J[BLjava/lang/String;)V"),
        &[
            JValue::Object(numbers.as_ref()),
            JValue::Object(contract.as_ref()),
            JValue::Object(attachment.as_ref()),
        ],
    )?)
}
fn reopen<'local>(
    env: &mut Env<'local>,
    reason: Option<host::Reopen>,
) -> Result<JIntArray<'local>, Error> {
    match reason {
        None => Ok(JIntArray::null()),
        Some(reason) => {
            let refusal = match reason.refusal() {
                Some(refusal) => marshal::open_refusal(refusal),
                None => 0,
            };
            let values = [marshal::reopen_reason(reason.reason()), refusal];
            let array = env.new_int_array(values.len())?;
            array.set_region(env, 0, &values)?;
            Ok(array)
        }
    }
}

native!(
    producer_open(
        session: long,
        domain: JString,
        name: JString,
        fields: long,
        batches: int,
        bytes: long,
        token: long,
    ) -> long
);

#[expect(
    clippy::too_many_arguments,
    reason = "the foreign operation has explicit handles and credit or completion limits, plus \
              JNI environment and class"
)]
fn producer_open<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    session: jlong,
    domain: jni::objects::JString<'local>,
    name: jni::objects::JString<'local>,
    fields: jlong,
    batches: jint,
    bytes: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let (batches, bytes) = credit(batches, bytes)?;
    let domain = domain.try_to_string(env)?;
    let name = name.try_to_string(env)?;
    let producer = host::Producer::open(
        value!(session).session()?,
        &domain,
        &name,
        value!(fields).fields()?,
        ClientProducerLimits { batches, bytes },
        cancel!(token),
    )?;
    Ok(Value::Producer(Arc::new(producer)).into_handle())
}
native!(
    consumer_open(
        session: long,
        domain: JString,
        name: JString,
        fields: long,
        batches: int,
        bytes: long,
        token: long,
    ) -> long
);

#[expect(
    clippy::too_many_arguments,
    reason = "the foreign operation has explicit handles and credit or completion limits, plus \
              JNI environment and class"
)]
fn consumer_open<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    session: jlong,
    domain: jni::objects::JString<'local>,
    name: jni::objects::JString<'local>,
    fields: jlong,
    batches: jint,
    bytes: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let (batches, bytes) = credit(batches, bytes)?;
    let domain = domain.try_to_string(env)?;
    let name = name.try_to_string(env)?;
    let consumer = host::Consumer::open(
        value!(session).session()?,
        &domain,
        &name,
        value!(fields).fields()?,
        ClientConsumerLimits { batches, bytes },
        cancel!(token),
    )?;
    Ok(Value::Consumer(Arc::new(consumer)).into_handle())
}
native!(producer_schema(handle: long) -> long);

fn producer_schema<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Schema(value!(handle).producer()?.schema().clone()).into_handle())
}
native!(consumer_schema(handle: long) -> long);

fn consumer_schema<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Schema(value!(handle).consumer()?.schema().clone()).into_handle())
}
native!(producer_state(handle: long) -> int);

fn producer_state<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::endpoint_state(value!(handle).producer()?.state()))
}
native!(consumer_state(handle: long) -> int);

fn consumer_state<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::endpoint_state(value!(handle).consumer()?.state()))
}
native!(producer_admission(handle: long) -> int);

fn producer_admission<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::admission(value!(handle).producer()?.admission()))
}
native!(producer_reopen(handle: long) -> [int]);

fn producer_reopen<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JIntArray<'local>, crate::error::Error> {
    reopen(env, value!(handle).producer()?.reopen())
}
native!(consumer_reopen(handle: long) -> [int]);

fn consumer_reopen<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JIntArray<'local>, crate::error::Error> {
    reopen(env, value!(handle).consumer()?.reopen())
}
native!(producer_description(handle: long) -> JObject);

fn producer_description<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    let d = value!(handle).producer()?.description();
    let window = host::Window::from(d.policy.window);
    description(
        env,
        &[
            d.generation.cast_signed(),
            i64::from(d.grant.batches.get()),
            d.grant.bytes.get().cast_signed(),
            i64::from(d.grant.max_batch_rows.get()),
            d.grant.max_batch_bytes.get().cast_signed(),
            i64::from(marshal::window(window.kind)),
            window.outstanding.cast_signed(),
            host::nanos(d.policy.ack_timeout).cast_signed(),
            host::nanos(d.policy.retry_backoff).cast_signed(),
            host::nanos(d.policy.retry_max_backoff).cast_signed(),
        ],
        d.contract.as_digest(),
        Some(&d.attachment.to_string()),
    )
}
native!(consumer_description(handle: long) -> JObject);

fn consumer_description<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    let d = value!(handle).consumer()?.description();
    let window = host::Window::from(d.window);
    description(
        env,
        &[
            d.generation.cast_signed(),
            i64::from(d.granted.batches.get()),
            d.granted.bytes.get().cast_signed(),
            i64::from(d.max_batch_rows),
            d.max_batch_bytes.cast_signed(),
            i64::from(marshal::window(window.kind)),
            window.outstanding.cast_signed(),
            host::nanos(d.ack_timeout).cast_signed(),
            host::nanos(d.retry_backoff).cast_signed(),
            host::nanos(d.retry_max_backoff).cast_signed(),
        ],
        d.contract.as_digest(),
        None,
    )
}
native!(producer_submit(handle: long, batch: long, token: long) -> long);

fn producer_submit<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    batch: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(value!(handle)
        .producer()?
        .submit(value!(batch).batch()?, cancel!(token))?
        .cast_signed())
}
native!(producer_submit_ipc(handle: long, ipc: [byte], token: long) -> long);

fn producer_submit_ipc<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    ipc: jni::objects::JByteArray<'local>,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let ipc = env.convert_byte_array(ipc)?;
    Ok(value!(handle)
        .producer()?
        .submit_ipc(&ipc, cancel!(token))?
        .cast_signed())
}
native!(producer_rejoin(handle: long, id: long, token: long) -> long);

fn producer_rejoin<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    id: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let outcome = value!(handle)
        .producer()?
        .rejoin(id.cast_unsigned(), cancel!(token))?;
    Ok(Value::Submission(Arc::new(outcome)).into_handle())
}
native!(producer_release(handle: long, id: long) -> long);

fn producer_release<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    id: jlong,
) -> Result<jlong, crate::error::Error> {
    match value!(handle).producer()?.release(id.cast_unsigned())? {
        Some(outcome) => Ok(Value::Submission(Arc::new(outcome)).into_handle()),
        None => Ok(0),
    }
}
native!(producer_close(handle: long, token: long) -> ());

fn producer_close<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    token: jlong,
) -> Result<(), crate::error::Error> {
    Ok(value!(handle).producer()?.close(cancel!(token))?)
}
native!(consumer_close(handle: long, token: long) -> ());

fn consumer_close<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    token: jlong,
) -> Result<(), crate::error::Error> {
    Ok(value!(handle).consumer()?.close(cancel!(token))?)
}
native!(consumer_next(handle: long, token: long) -> long);

fn consumer_next<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Delivery(Arc::new(value!(handle).consumer()?.next(cancel!(token))?)).into_handle())
}
native!(delivery_identity(handle: long) -> [byte]);

fn delivery_identity<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    Ok(env.byte_array_from_slice(value!(handle).delivery()?.identity())?)
}
native!(delivery_reference(handle: long) -> [byte]);

fn delivery_reference<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    Ok(env.byte_array_from_slice(value!(handle).delivery()?.reference())?)
}
native!(delivery_source(handle: long) -> JString);

fn delivery_source<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JString<'local>, crate::error::Error> {
    Ok(env.new_string(value!(handle).delivery()?.source_relay())?)
}
native!(delivery_branch(handle: long) -> [byte]);

fn delivery_branch<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    match value!(handle).delivery()?.branch_fingerprint() {
        Some(bytes) => Ok(env.byte_array_from_slice(bytes)?),
        None => Ok(JByteArray::null()),
    }
}
native!(delivery_members(handle: long) -> long);

fn delivery_members<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(i64::from(value!(handle).delivery()?.members()))
}
native!(delivery_now(handle: long) -> long);

fn delivery_now<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(value!(handle).delivery()?.execution_now())
}
native!(delivery_ipc(handle: long) -> [byte]);

fn delivery_ipc<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JByteArray<'local>, crate::error::Error> {
    Ok(env.byte_array_from_slice(value!(handle).delivery()?.ipc())?)
}
native!(delivery_batch(handle: long) -> long);

fn delivery_batch<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Batch(value!(handle).delivery()?.batch()?.clone()).into_handle())
}
native!(delivery_ack(handle: long, token: long) -> int);

fn delivery_ack<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    token: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::settlement(
        value!(handle).delivery()?.ack(cancel!(token))?,
    ))
}
native!(delivery_retry(handle: long, token: long) -> int);

fn delivery_retry<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    token: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::settlement(
        value!(handle).delivery()?.retry(cancel!(token))?,
    ))
}
native!(delivery_reject(handle: long, reason: JString, token: long) -> int);

fn delivery_reject<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    reason: jni::objects::JString<'local>,
    token: jlong,
) -> Result<jint, crate::error::Error> {
    let reason = reason.try_to_string(env)?;
    Ok(marshal::settlement(
        value!(handle).delivery()?.reject(&reason, cancel!(token))?,
    ))
}

fn summary<'local>(
    env: &mut Env<'local>,
    outcome: &host::SubmissionOutcome,
) -> Result<JObject<'local>, Error> {
    // Zero is the private JNI constructor's encoding of an omitted cause. The Java object
    // immediately exposes each cause as its typed enum or null, never as this raw encoding.
    #[derive(Default)]
    struct CauseCodes {
        refusal: jint,
        defect: jint,
        failure: jint,
        uncertainty: jint,
    }
    let mut causes = CauseCodes::default();
    match outcome.result() {
        host::SubmissionResult::NotAdmitted => {
            let refusal = outcome.refusal()?;
            causes.refusal = marshal::submission_refusal(refusal);
            if refusal == host::SubmissionRefusal::InvalidBatch {
                causes.defect = marshal::batch_defect(outcome.defect()?);
            }
        }
        host::SubmissionResult::Completed => {}
        host::SubmissionResult::ProcessingFailed => {
            causes.failure = marshal::processing_failure(outcome.failure()?);
        }
        host::SubmissionResult::OutcomeUnknown => {
            causes.uncertainty = marshal::uncertainty(outcome.uncertainty()?);
        }
    }
    let message = env.new_string(outcome.message())?;
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$SubmissionInfo"),
        jni_sig!("(ILjava/lang/String;IIII)V"),
        &[
            JValue::Int(marshal::submission_result(outcome.result())),
            JValue::Object(message.as_ref()),
            JValue::Int(causes.refusal),
            JValue::Int(causes.defect),
            JValue::Int(causes.failure),
            JValue::Int(causes.uncertainty),
        ],
    )?)
}
native!(submission_info(handle: long) -> JObject);

fn submission_info<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    summary(env, value!(handle).submission()?)
}
native!(producer_pending(handle: long) -> [JObject]);

fn producer_pending<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObjectArray<'local>, crate::error::Error> {
    let pending = value!(handle).producer()?.pending_submissions();
    marshal::count(pending.len())?;
    let array = JObjectArray::<JObject>::new(env, pending.len(), JObject::null())?;
    for (index, pending) in pending.into_iter().enumerate() {
        let outcome = Auto::new(match pending.outcome {
            Some(outcome) => summary(env, &host::SubmissionOutcome::new(outcome))?,
            None => JObject::null(),
        });
        let object = Auto::new(env.new_object(
            jni_str!("io/nervix/client/Nervix$Pending"),
            jni_sig!("(JLio/nervix/client/Nervix$SubmissionInfo;)V"),
            &[
                JValue::Long(pending.id.get().get().cast_signed()),
                JValue::Object(outcome.as_ref()),
            ],
        )?);
        array.set_element(env, index, object.as_ref())?;
    }
    Ok(array)
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] = &[
    producer_open::METHOD,
    consumer_open::METHOD,
    producer_schema::METHOD,
    consumer_schema::METHOD,
    producer_state::METHOD,
    consumer_state::METHOD,
    producer_admission::METHOD,
    producer_reopen::METHOD,
    consumer_reopen::METHOD,
    producer_description::METHOD,
    consumer_description::METHOD,
    producer_submit::METHOD,
    producer_submit_ipc::METHOD,
    producer_rejoin::METHOD,
    producer_release::METHOD,
    producer_close::METHOD,
    consumer_close::METHOD,
    consumer_next::METHOD,
    delivery_identity::METHOD,
    delivery_reference::METHOD,
    delivery_source::METHOD,
    delivery_branch::METHOD,
    delivery_members::METHOD,
    delivery_now::METHOD,
    delivery_ipc::METHOD,
    delivery_batch::METHOD,
    delivery_ack::METHOD,
    delivery_retry::METHOD,
    delivery_reject::METHOD,
    submission_info::METHOD,
    producer_pending::METHOD,
];
