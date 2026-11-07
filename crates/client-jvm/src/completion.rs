//! Bounded completion pages projected into Java values.
//!
//! Layer: edges.
//! - **Owns.** Completion pages, source-byte edits and continuation access.
//! - **Depends on.** The shared host's completion API and vocabulary.
//! - **Must not know.** Grammar lookup or transaction context recovery.

use jni::{
    jni_sig, jni_str,
    objects::{JObject, JObjectArray, JValue},
    refs::Auto,
    sys::{jint, jlong},
};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;

use crate::{error::Error, handles::Value, marshal};

native!(
    suggest(
        session: long,
        input: JString,
        cursor: int,
        page_size: int,
        continuation: JString,
        token: long,
    ) -> long
);

#[expect(
    clippy::too_many_arguments,
    reason = "the foreign operation has explicit handles and credit or completion limits, plus \
              JNI environment and class"
)]
fn suggest<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    session: jlong,
    input: jni::objects::JString<'local>,
    cursor: jint,
    page_size: jint,
    continuation: jni::objects::JString<'local>,
    token: jlong,
) -> Result<jlong, crate::error::Error> {
    let input = input.try_to_string(env)?;
    let continuation = marshal::optional_string(env, &continuation)?;
    let cursor = marshal::size(cursor, "cursor")?;
    let page_size = u16::try_from(page_size)
        .map_err(|_| Error::argument("pageSize", "must fit an unsigned 16-bit count"))?;
    let outcome = value!(session).session()?.suggest_page(
        &input,
        cursor,
        page_size,
        continuation.as_deref(),
        cancel!(token),
    )?;
    Ok(Value::Suggestions(Arc::new(host::Suggestions::new(outcome))).into_handle())
}
native!(suggestions_status(handle: long) -> int);

fn suggestions_status<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jint, crate::error::Error> {
    Ok(marshal::completion_status(host::CompletionStatus::from(
        value!(handle).suggestions()?.outcome().status,
    )))
}
native!(suggestions_continuation(handle: long) -> JString);

fn suggestions_continuation<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JString<'local>, crate::error::Error> {
    marshal::string(
        env,
        value!(handle)
            .suggestions()?
            .outcome()
            .continuation
            .as_deref(),
    )
}
native!(suggestions_values(handle: long) -> [JObject]);

fn suggestions_values<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JObjectArray<'local>, crate::error::Error> {
    let values = &value!(handle).suggestions()?.outcome().suggestions;
    marshal::count(values.len())?;
    let array = JObjectArray::<JObject>::new(env, values.len(), JObject::null())?;
    for (index, suggestion) in values.iter().enumerate() {
        let value = Auto::new(env.new_string(&suggestion.value)?);
        let replacement = Auto::new(env.new_string(&suggestion.edit.replacement)?);
        let object = Auto::new(env.new_object(
            jni_str!("io/nervix/client/Nervix$Suggestion"),
            jni_sig!("(Ljava/lang/String;IJJLjava/lang/String;)V"),
            &[
                JValue::Object(value.as_ref()),
                JValue::Int(marshal::completion_kind(host::CompletionKind::from(
                    suggestion.kind,
                ))),
                JValue::Long(i64::from(suggestion.edit.start)),
                JValue::Long(i64::from(suggestion.edit.end)),
                JValue::Object(replacement.as_ref()),
            ],
        )?);
        array.set_element(env, index, object.as_ref())?;
    }
    Ok(array)
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] = &[
    suggest::METHOD,
    suggestions_status::METHOD,
    suggestions_continuation::METHOD,
    suggestions_values::METHOD,
];
