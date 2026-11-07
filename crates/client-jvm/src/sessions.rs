//! Sessions, connection options, prepared executions and command outcomes.
//!
//! Layer: edges.
//! - **Owns.** The JNI projection of the host's options, commands, cancellation and completion.
//! - **Depends on.** The shared client host and JNI strings and arrays.
//! - **Must not know.** Routing, reconnect policy, execution recovery or the C ABI.

use std::time::Duration;

use jni::{
    jni_sig, jni_str,
    objects::{JLongArray, JObject, JObjectArray, JString, JValue},
    refs::Auto,
    sys::{jdouble, jint, jlong},
};
use nervix_client_core::{ConnectOptions, DomainName, TlsRequirement};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;
use url::Url;

use crate::{error::Error, handles::Value, marshal};

fn seconds(value: f64, name: &'static str) -> Result<Duration, Error> {
    Duration::try_from_secs_f64(value).map_err(|error| {
        Error::argument(
            name,
            &format!("must be a finite nonnegative number of seconds: {error}"),
        )
    })
}

native!(options_new() -> long);

fn options_new<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Options(ConnectOptions::default()).into_handle())
}

native!(options_auth(handle: long, username: JString, password: JString) -> ());

fn options_auth<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    username: jni::objects::JString<'local>,
    password: jni::objects::JString<'local>,
) -> Result<(), crate::error::Error> {
    let username = marshal::optional_string(env, &username)?;
    let password = marshal::optional_string(env, &password)?;
    if username.is_some() != password.is_some() {
        return Err(Error::argument(
            "username",
            "and password must both be present or both be absent",
        ));
    }
    let options = mutable!(handle).options_mut()?;
    options.username = username;
    options.password = password;
    Ok(())
}

native!(options_tls(handle: long, requirement: int) -> ());

fn options_tls<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    requirement: jint,
) -> Result<(), crate::error::Error> {
    let requirement = match requirement {
        1 => TlsRequirement::Preferred,
        2 => TlsRequirement::Required,
        _ => return Err(Error::argument("tls", "does not name a TLS requirement")),
    };
    mutable!(handle).options_mut()?.tls_requirement = Some(requirement);
    Ok(())
}

native!(options_ca(handle: long, pem: [byte]) -> ());

fn options_ca<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    pem: jni::objects::JByteArray<'local>,
) -> Result<(), crate::error::Error> {
    let pem = env.convert_byte_array(pem)?;
    mutable!(handle).options_mut()?.ca_certificate_pem = Some(pem);
    Ok(())
}

native!(options_seeds(handle: long, seeds: [JObject]) -> ());

fn options_seeds<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    seeds: jni::objects::JObjectArray<'local>,
) -> Result<(), crate::error::Error> {
    let count = seeds.len(env)?;
    let mut parsed = Vec::with_capacity(count);
    for index in 0..count {
        let value = seeds.get_element(env, index)?;
        let value = Auto::new(JString::cast_local(env, value)?);
        let value = value.try_to_string(env)?;
        let url =
            Url::parse(&value).map_err(|error| Error::argument("seeds", &error.to_string()))?;
        parsed.push(url);
    }
    mutable!(handle).options_mut()?.seed_servers = parsed;
    Ok(())
}

native!(options_timeout(handle: long, kind: int, value: double) -> ());

fn options_timeout<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
    kind: jint,
    value: jdouble,
) -> Result<(), crate::error::Error> {
    let duration = seconds(value, "timeout")?;
    let options = mutable!(handle).options_mut()?;
    match kind {
        1 => options.connect_timeout = duration,
        2 => options.request_timeout = duration,
        3 => options.retry_timeout = duration,
        4 => options.backup_wait_timeout = duration,
        _ => {
            return Err(Error::argument(
                "timeout",
                "does not name a connection timeout",
            ));
        }
    }
    Ok(())
}

native!(cancel_new() -> long);

fn cancel_new<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
) -> Result<jlong, crate::error::Error> {
    Ok(Value::Cancel(Arc::new(host::Cancel::default())).into_handle())
}
native!(cancel_deadline(value: double) -> long);

fn cancel_deadline<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    value: jdouble,
) -> Result<jlong, crate::error::Error> {
    let token = host::Cancel::with_deadline(seconds(value, "deadline")?)?;
    Ok(Value::Cancel(Arc::new(token)).into_handle())
}
native!(cancel_trigger(handle: long) -> ());

fn cancel_trigger<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<(), crate::error::Error> {
    value!(handle).cancel()?.trigger();
    Ok(())
}

native!(connect(server: JString, domain: JString, options: long, token: long) -> long);

fn connect<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    server: jni::objects::JString<'local>,
    domain: jni::objects::JString<'local>,
    options: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let server = server.try_to_string(env)?;
    let domain = marshal::optional_string(env, &domain)?;
    let options = value!(options).options()?.clone();
    let session = host::Session::connect(&server, domain.as_deref(), options, cancel!(token))?;
    Ok(Value::Session(session).into_handle())
}

native!(prepare(session: long, query: JString, token: long) -> long);

fn prepare<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    session: jlong,
    query: jni::objects::JString<'local>,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let query = query.try_to_string(env)?;
    let execution = value!(session).session()?.prepare(&query, cancel!(token))?;
    Ok(Value::Execution(Arc::new(execution)).into_handle())
}

native!(execute(session: long, execution: long, token: long) -> long);

fn execute<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    session: jlong,
    execution: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let outcome = value!(session)
        .session()?
        .execute(value!(execution).execution()?, cancel!(token))?;
    Ok(Value::Outcome(Arc::new(outcome)).into_handle())
}

native!(next_event(session: long, token: long) -> long);

fn next_event<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    session: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let event = value!(session).session()?.next_event(cancel!(token))?;
    Ok(Value::Event(Arc::new(event)).into_handle())
}

native!(next_clock_event(session: long, token: long) -> long);

fn next_clock_event<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    session: jlong,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let event = value!(session)
        .session()?
        .next_clock_event(cancel!(token))?;
    Ok(Value::ClockEvent(Arc::new(event)).into_handle())
}

native!(domain_clock(session: long, domain: JString) -> long);

fn domain_clock<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    session: jlong,
    domain: jni::objects::JString<'local>,
) -> Result<jlong, crate::error::Error> {
    let domain = domain.try_to_string(env)?;
    let domain = DomainName::try_from(domain.as_str())
        .map_err(|error| Error::argument("domain", &error.to_string()))?;
    match value!(session).session()?.domain_clock(&domain) {
        Some(clock) => Ok(Value::Clock(Arc::new(clock)).into_handle()),
        None => Ok(0),
    }
}

native!(outcome_disposition(handle: long) -> int);

fn outcome_disposition<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::disposition(
        value!(handle).outcome()?.disposition(),
    ))
}
native!(outcome_upload_failure(handle: long) -> int);

fn outcome_upload_failure<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    match value!(handle).outcome()?.upload_failure() {
        Some(failure) => Ok(marshal::upload_failure(failure)),
        None => Ok(0),
    }
}

native!(outcome_message(handle: long) -> JString);

fn outcome_message<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JString<'local>, crate::error::Error> {
    Ok(env.new_string(&value!(handle).outcome()?.command().message)?)
}
native!(outcome_reference(handle: long) -> JString);

fn outcome_reference<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JString<'local>, crate::error::Error> {
    let reference = value!(handle)
        .outcome()?
        .command()
        .execution_reference
        .as_ref()
        .map(|reference| reference.as_str());
    marshal::string(env, reference)
}
native!(outcome_schema(handle: long) -> long);

fn outcome_schema<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    match value!(handle).outcome()?.opened_schema() {
        Some(schema) => Ok(Value::Schema(schema.clone()).into_handle()),
        None => Ok(0),
    }
}
native!(outcome_subscription(handle: long) -> JObject);

fn outcome_subscription<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    match &value!(handle).outcome()?.command().subscription {
        Some(opened) => crate::events::subscription(env, &opened.subscription),
        None => Ok(JObject::null()),
    }
}

native!(outcome_diagnostics(handle: long) -> [JObject]);

fn outcome_diagnostics<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObjectArray<'local>, crate::error::Error> {
    let diagnostics = &value!(handle).outcome()?.command().diagnostics;
    marshal::count(diagnostics.len())?;
    let array = JObjectArray::<JObject>::new(env, diagnostics.len(), JObject::null())?;
    for (index, diagnostic) in diagnostics.iter().enumerate() {
        let message = Auto::new(env.new_string(&diagnostic.message)?);
        let span = match diagnostic.span {
            Some(span) => marshal::longs(env, &[i64::from(span.start()), i64::from(span.end())])?,
            None => JLongArray::null(),
        };
        let span = Auto::new(span);
        let object = Auto::new(env.new_object(
            jni_str!("io/nervix/client/Nervix$Diagnostic"),
            jni_sig!("(Ljava/lang/String;[J)V"),
            &[
                JValue::Object(message.as_ref()),
                JValue::Object(span.as_ref()),
            ],
        )?);
        array.set_element(env, index, object.as_ref())?;
    }
    Ok(array)
}

native!(outcome_backup(handle: long) -> JObject);

fn outcome_backup<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    match &value!(handle).outcome()?.command().backup {
        None => Ok(JObject::null()),
        Some(backup) => {
            let digest = env.byte_array_from_slice(backup.digest.as_bytes())?;
            Ok(env.new_object(
                jni_str!("io/nervix/client/Nervix$Backup"),
                jni_sig!("(J[B)V"),
                &[
                    JValue::Long(backup.total_bytes.get().cast_signed()),
                    JValue::Object(digest.as_ref()),
                ],
            )?)
        }
    }
}

native!(outcome_restore(handle: long) -> JObject);

fn outcome_restore<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObject<'local>, crate::error::Error> {
    match value!(handle).outcome()?.restore_summary() {
        None => Ok(JObject::null()),
        Some(summary) => Ok(env.new_object(
            jni_str!("io/nervix/client/Nervix$Restore"),
            jni_sig!("(ZJJJZ)V"),
            &[
                JValue::Bool(summary.dry_run),
                JValue::Long(summary.domains.cast_signed()),
                JValue::Long(summary.resource_versions.cast_signed()),
                JValue::Long(summary.models.cast_signed()),
                JValue::Bool(summary.failed),
            ],
        )?),
    }
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] = &[
    options_new::METHOD,
    options_auth::METHOD,
    options_tls::METHOD,
    options_ca::METHOD,
    options_seeds::METHOD,
    options_timeout::METHOD,
    cancel_new::METHOD,
    cancel_deadline::METHOD,
    cancel_trigger::METHOD,
    connect::METHOD,
    prepare::METHOD,
    execute::METHOD,
    next_event::METHOD,
    next_clock_event::METHOD,
    domain_clock::METHOD,
    outcome_disposition::METHOD,
    outcome_upload_failure::METHOD,
    outcome_message::METHOD,
    outcome_reference::METHOD,
    outcome_schema::METHOD,
    outcome_subscription::METHOD,
    outcome_diagnostics::METHOD,
    outcome_backup::METHOD,
    outcome_restore::METHOD,
];
