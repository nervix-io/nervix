//! The Java 21 binding of the Nervix client.
//!
//! Layer: edges.
//! - **Owns.** JNI calls, Java-owned handles, typed exceptions and bulk Java array access.
//! - **Depends on.** JNI, the shared client host runtime and its client vocabulary.
//! - **Must not know.** The server, C ABI, or how sessions recover, route or decide outcomes.
//!
//! Java's Cleaner owns every handle. Short calls borrow it under its Java State monitor;
//! the State fences reentrant access and defers close until an admitted call returns.
//! blocking calls take an independent shared lease before releasing that monitor. No monitor
//! is held while a host call waits, and no library thread calls Java. Mutable field lists and
//! batch builders are accessed exclusively under their Java State monitor.

#![cfg_attr(nervix_lint, nervix::layer(edge, reason = "owns the JVM client edge"))]
#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "JNI calls run on the calling JVM thread")
)]

mod error;
mod handles;
mod marshal;

/// Declares a checked JNI export with panic containment and the binding's exception policy.
macro_rules! native {
    ($name:ident($($arg:ident: $jty:tt),* $(,)?) -> $jret:tt) => {
        mod $name {
            use crate::error::JavaPolicy;
            #[expect(deprecated, reason = "jni 0.22.4 emits AtomicBool::fetch_update, renamed by Rust 1.99; the checked JNI export owns that external expansion")]
            pub(crate) const METHOD: jni::NativeMethod = {
                #[cfg_attr(nervix_lint, expect(nervix::discarded_outcome, reason = "jni 0.22.4's ABI guard ignores fetch_update contention: Ok and Err both establish that the ABI check passed; an ABI failure panics before either result"))]
                let method = jni::native_method! {
                    java_type = "io.nervix.client.Native",
                    error_policy = JavaPolicy,
                    static extern fn super::$name($($arg: $jty),*) -> $jret,
                };
                method
            };
        }
        const _: jni::NativeMethod = $name::METHOD;
    };
}

/// Borrows the handle Java's State monitor or shared lease keeps alive for this native call.
macro_rules! value {
    ($handle:expr) => {{
        // SAFETY: Native is package-private. Its only callers hold the owning State monitor
        // for a short call or an independent lease for a blocking call. State defers
        // reentrant close until that call returns, so the allocation remains live.
        unsafe { crate::handles::Value::borrow($handle)? }
    }};
}

/// Borrows a builder exclusively under its owning Java State monitor.
macro_rules! mutable {
    ($handle:expr) => {{
        // SAFETY: Fields and BatchBuilder expose no shared leases. Every read, mutation and
        // cleanup of their allocation is serialized by the same Java State monitor;
        // State also refuses reentrant access while a mutable borrow is live.
        unsafe { crate::handles::Value::borrow_mut($handle)? }
    }};
}

/// JNI encodes an omitted cancellation token as a null handle only at this boundary.
macro_rules! cancel {
    ($handle:expr) => {
        if $handle == 0 {
            None
        } else {
            Some(value!($handle).cancel()?)
        }
    };
}

mod batches;
mod clocks;
mod completion;
mod endpoints;
mod entity_labels;
mod events;
mod inspection;
mod sessions;

use jni::sys::jlong;

native!(retain(handle: long) -> long);

fn retain<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jlong, crate::error::Error> {
    Ok(value!(handle).duplicate()?.into_handle())
}

native!(release(handle: long) -> ());

fn release<'local>(
    _env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<(), crate::error::Error> {
    // SAFETY: State removes its handle under its monitor after the last admitted call,
    // exactly once. A lease owns a separate allocation, so releasing either never races
    // its borrows, including a close reentered during JNI marshalling.
    unsafe { handles::Value::release(handle)? };
    Ok(())
}

native!(execution_reference(handle: long) -> JString);

fn execution_reference<'local>(
    env: &mut jni::Env<'local>,
    _class: jni::objects::JClass<'local>,
    handle: jlong,
) -> Result<jni::objects::JString<'local>, crate::error::Error> {
    Ok(env.new_string(value!(handle).execution()?.reference())?)
}

#[cfg(all(test, feature = "jvm-tests"))]
pub(crate) const METHODS: &[jni::NativeMethod] =
    &[retain::METHOD, release::METHOD, execution_reference::METHOD];

#[cfg(all(test, feature = "jvm-tests"))]
mod tests;
