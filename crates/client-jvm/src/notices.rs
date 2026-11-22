//! Server notices and the class and producing node of the error a runtime notice reports.
//!
//! Layer: edges.
//! - **Owns.** The Java value of a host notice: its level and kind codes, its text, and the class,
//!   node kind and node name of the error it reports, each absent as a Java null.
//! - **Depends on.** The host's notices and the binding's Java encodings of its vocabulary.
//! - **Must not know.** How notices are queued, coalesced, routed or continued across sessions.
//!
//! A notice holds only scalars and text, so Java receives a plain value that owns nothing native
//! and needs no close.

use jni::{
    Env, jni_sig, jni_str,
    objects::{JObject, JValue},
};
use nervix_client_host as host;

use crate::{error::Error, marshal};

/// Projects `notice` into a Java `Notice`; its error fields are all present or all null.
pub(crate) fn notice<'local>(
    env: &mut Env<'local>,
    notice: &host::Notice,
) -> Result<JObject<'local>, Error> {
    let message = env.new_string(notice.message())?;
    let class = marshal::string(env, notice.error_class())?;
    let node_kind = marshal::string(env, notice.error_node_kind())?;
    let node = marshal::string(env, notice.error_node())?;
    Ok(env.new_object(
        jni_str!("io/nervix/client/Nervix$Notice"),
        jni_sig!("(IILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V"),
        &[
            JValue::Int(marshal::notice_level(notice.level())),
            JValue::Int(marshal::notice_kind(notice.kind())),
            JValue::Object(message.as_ref()),
            JValue::Object(class.as_ref()),
            JValue::Object(node_kind.as_ref()),
            JValue::Object(node.as_ref()),
        ],
    )?)
}
