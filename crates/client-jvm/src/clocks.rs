//! Clock event snapshots and projections of an attached domain clock.
//!
//! Layer: edges.
//! - **Owns.** The Java projection of clock snapshots, ticks and their arithmetic.
//! - **Depends on.** The host's clock reads and projections and the clock vocabulary.
//! - **Must not know.** Clock authority, transport, restoration or scheduling.

use jni::{
    Env, jni_sig, jni_str,
    objects::{JLongArray, JObject, JValue},
    sys::{jboolean, jint, jlong},
};
use nervix_client_core::DomainClockEvent;
use nervix_models::{DomainClockTickObservation, PacedDomainClock, Timestamp};

use crate::{error::Error, marshal};

fn paced<'local>(
    env: &mut Env<'local>,
    generation: u64,
    clock: &PacedDomainClock,
) -> Result<JObject<'local>, Error> {
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$PacedClock"),
        jni_sig!("(JJJJJD)V"),
        &[
            JValue::Long(generation.cast_signed()),
            JValue::Long(clock.period.as_nanos().cast_signed()),
            JValue::Long(clock.skew.as_nanos().cast_signed()),
            JValue::Long(clock.mapping.logical_start().unix_nanos()),
            JValue::Long(clock.mapping.wall_started_at().unix_nanos()),
            JValue::Double(clock.mapping.time_rate().get()),
        ],
    )?)
}

fn tick<'local>(
    env: &mut Env<'local>,
    tick: &DomainClockTickObservation,
) -> Result<JObject<'local>, Error> {
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$Tick"),
        jni_sig!("(JJJJJ)V"),
        &[
            JValue::Long(tick.generation.cast_signed()),
            JValue::Long(tick.tick_id.cast_signed()),
            JValue::Long(tick.logical_boundary.unix_nanos()),
            JValue::Long(tick.authority_utc.unix_nanos()),
            JValue::Long(tick.serving_logical.unix_nanos()),
        ],
    )?)
}

native!(clock_event_kind(handle: long) -> int);

fn clock_event_kind<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::clock_kind(value!(handle).clock_event()?.kind()))
}
native!(clock_event_domain(handle: long) -> JString);

fn clock_event_domain<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JString<'local>, crate::error::Error> {
    Ok(env.new_string(value!(handle).clock_event()?.domain().as_str())?)
}
native!(clock_event_generation(handle: long) -> long);

fn clock_event_generation<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(value!(handle).clock_event()?.generation()?.cast_signed())
}
native!(clock_event_state(handle: long) -> int);

fn clock_event_state<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::clock_state(value!(handle).clock_event()?.state()?))
}
native!(clock_event_paced(handle: long) -> JObject);

fn clock_event_paced<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    let event = value!(handle).clock_event()?;
    paced(env, event.generation()?, event.paced()?)
}
native!(clock_event_tick(handle: long) -> JObject);

fn clock_event_tick<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    tick(env, value!(handle).clock_event()?.tick()?)
}
native!(clock_event_end(handle: long) -> int);

fn clock_event_end<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::clock_end(
        value!(handle).clock_event()?.end_reason()?,
    ))
}
native!(clock_event_restoration(handle: long) -> JObject);

fn clock_event_restoration<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    match value!(handle).clock_event()?.domain_clock_event() {
        DomainClockEvent::RestorationFailed(failure) => {
            crate::events::restoration(env, &failure.message, failure.retry_after)
        }
        _ => Ok(JObject::null()),
    }
}
native!(clock_domain(handle: long) -> JString);

fn clock_domain<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JString<'local>, crate::error::Error> {
    Ok(env.new_string(value!(handle).clock()?.domain().as_str())?)
}
native!(clock_generation(handle: long) -> long);

fn clock_generation<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(value!(handle).clock()?.generation().cast_signed())
}
native!(clock_state(handle: long) -> int);

fn clock_state<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::clock_state(value!(handle).clock()?.state()))
}
native!(clock_paced(handle: long) -> JObject);

fn clock_paced<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    let clock = value!(handle).clock()?;
    paced(env, clock.generation(), clock.paced()?)
}
native!(clock_tick(handle: long) -> JObject);

fn clock_tick<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    match value!(handle).clock()?.tick() {
        Some(value) => tick(env, value),
        None => Ok(JObject::null()),
    }
}
native!(clock_logical(handle: long, utc: long) -> long);

fn clock_logical<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    utc: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(value!(handle)
        .clock()?
        .logical_time_at(Timestamp::from_unix_nanos(utc))?
        .unix_nanos())
}
native!(clock_wait(handle: long, utc: long, target: long) -> long);

fn clock_wait<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    utc: jlong,
    target: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(value!(handle)
        .clock()?
        .wall_nanos_until(
            Timestamp::from_unix_nanos(utc),
            Timestamp::from_unix_nanos(target),
        )?
        .cast_signed())
}
native!(clock_window(handle: long, utc: long) -> [long]);

fn clock_window<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    utc: jlong,
) -> Result<jni::objects::JLongArray<'local>, crate::error::Error> {
    match value!(handle)
        .clock()?
        .admission_window(Timestamp::from_unix_nanos(utc))?
    {
        Some(window) => marshal::longs(
            env,
            &[
                window.earliest_center().unix_nanos(),
                window.latest_center().unix_nanos(),
            ],
        ),
        None => Ok(JLongArray::null()),
    }
}
native!(clock_admits(handle: long, utc: long, event: long) -> boolean);

fn clock_admits<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    utc: jlong,
    event: jlong,
) -> Result<jboolean, crate::error::Error> {
    Ok(value!(handle).clock()?.admits(
        Timestamp::from_unix_nanos(utc),
        Timestamp::from_unix_nanos(event),
    )?)
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] = &[
    clock_event_kind::METHOD,
    clock_event_domain::METHOD,
    clock_event_generation::METHOD,
    clock_event_state::METHOD,
    clock_event_paced::METHOD,
    clock_event_tick::METHOD,
    clock_event_end::METHOD,
    clock_event_restoration::METHOD,
    clock_domain::METHOD,
    clock_generation::METHOD,
    clock_state::METHOD,
    clock_paced::METHOD,
    clock_tick::METHOD,
    clock_logical::METHOD,
    clock_wait::METHOD,
    clock_window::METHOD,
    clock_admits::METHOD,
];
