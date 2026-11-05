//! The embedded-JVM boundary checks and current-value representation properties.
//!
//! Layer: test harness.
//! - **Owns.** One embedded JVM, JNI method registration and complete Java round-trip oracles.
//! - **Depends on.** The actual binding exports, Java 21 fixtures and Bolero.
//! - **Must not know.** Server execution or alternate representations of the client protocol.

use jni::{Env, InitArgsBuilder, JNIVersion, JavaVM, jni_sig, jni_str, objects::JValue};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_host::{Failure, FailureKind};
use nervix_primitives::sync::blocking::OnceLock;

static VM: OnceLock<JavaVM> = OnceLock::new();

unsafe extern "C" {
    fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
}

extern "C" fn stop_vm() {
    if let Some(vm) = VM.get() {
        // SAFETY: libtest has joined every test thread before process exit. All JNI scopes and
        // local references have ended; no Rust thread calls Java in the background. Registering
        // after JVM creation runs this callback before HotSpot's C++ static destructors. The VM
        // and its static method caches are never accessed again after this callback.
        if unsafe { vm.destroy() }.is_err() {
            std::process::abort();
        }
    }
}

fn vm() -> &'static JavaVM {
    VM.get_or_init(|| {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let artifacts = match std::env::var_os("NERVIX_CLIENT_JVM_TEST_ARTIFACTS") {
            Some(path) => root.join(path),
            None => root.join("target/client-conformance/java"),
        };
        let jar = artifacts.join("nervix-client.jar");
        assert!(
            jar.is_file(),
            "run just build-client-jvm-tests before embedding the JVM (missing {})",
            jar.display()
        );
        let classpath = format!(
            "-Djava.class.path={}:{}",
            jar.display(),
            artifacts.join("tests").display()
        );
        let args = InitArgsBuilder::new()
            .version(JNIVersion::V21)
            .option("-Xcheck:jni")
            .option("-Xms16m")
            .option("-Xmx64m")
            .option("-XX:+UseSerialGC")
            .option(classpath)
            .build()
            .assured("the test's JVM options are valid");
        let vm = JavaVM::new(args).assured("a Java 21 JVM is installed");
        vm.attach_current_thread(|env| -> jni::errors::Result<()> {
            let class = env.find_class(jni_str!("io/nervix/client/Native"))?;
            for methods in [
                crate::METHODS,
                crate::batches::METHODS,
                crate::clocks::METHODS,
                crate::completion::METHODS,
                crate::endpoints::METHODS,
                crate::events::METHODS,
                crate::sessions::METHODS,
            ] {
                // SAFETY: Every descriptor comes from native_method!'s checked static export,
                // with JClass as its second argument. Registering this harness's implementations
                // makes ordinary and sanitizer properties execute the same authored JNI code;
                // the jar still supplies the production Java classes and library loader.
                unsafe { env.register_native_methods(&class, methods)? };
            }
            Ok(())
        })
        .assured("the Java declarations match every checked JNI export");
        // SAFETY: stop_vm has the C callback ABI, captures no stack state and does not unwind.
        // This owns only the embedded test JVM; a Java application owns its normal VM lifecycle.
        assert_eq!(unsafe { atexit(stop_vm) }, 0);
        vm
    })
}

fn property(method: &jni::strings::JNIStr, input: &[u8]) {
    vm().attach_current_thread(|env| -> jni::errors::Result<()> {
        let input = env.byte_array_from_slice(input)?;
        let result = env.call_static_method(
            jni_str!("io/nervix/client/Properties"),
            method,
            jni_sig!("([B)V"),
            &[JValue::Object(input.as_ref())],
        );
        if result.is_err() {
            env.exception_describe();
            env.exception_clear();
        }
        result?;
        Ok(())
    })
    .assured("the complete Java representation round trip holds");
}

#[test]
fn bolero_columns_round_trip_through_jni() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|input: &[u8]| property(jni_str!("columnsRoundTrip"), input));
}

#[test]
fn bolero_field_descriptions_round_trip_through_jni() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|input: &[u8]| property(jni_str!("fieldsRoundTrip"), input));
}

fn check_failure(env: &mut Env<'_>, kind: FailureKind, index: usize) -> jni::errors::Result<()> {
    let message = "a typed cause: its complete diagnostic chain";
    let failure = Failure::new(kind, message);
    let raised = crate::error::Error::from(failure).raise(env);
    assert!(matches!(raised, jni::errors::Error::JavaException));
    let exception = env
        .exception_occurred()
        .assured("raising a host failure installs an exception");
    env.exception_clear();
    let value = env
        .call_method(
            &exception,
            jni_str!("kind"),
            jni_sig!("()Lio/nervix/client/Nervix$FailureKind;"),
            &[],
        )?
        .l()?;
    let code = env
        .call_method(&value, jni_str!("code"), jni_sig!("()I"), &[])?
        .i()?;
    assert_eq!(
        code,
        i32::try_from(index + 1).assured("thirteen failure kinds fit i32")
    );
    let text = exception.get_message(env)?;
    assert_eq!(text.to_string(), message);
    Ok(())
}

#[test]
fn every_failure_kind_keeps_its_classification_and_message() {
    let kinds = [
        FailureKind::InvalidArgument,
        FailureKind::Connect,
        FailureKind::Transport,
        FailureKind::Uncertain,
        FailureKind::Rejected,
        FailureKind::Deadline,
        FailureKind::Cancelled,
        FailureKind::Overflow,
        FailureKind::Protocol,
        FailureKind::Type,
        FailureKind::Closed,
        FailureKind::Interrupted,
        FailureKind::ReopenRequired,
    ];
    vm().attach_current_thread(|env| -> jni::errors::Result<()> {
        for (index, kind) in kinds.into_iter().enumerate() {
            check_failure(env, kind, index)?;
        }
        Ok(())
    })
    .assured("every host failure projects to its Java class");
}

/// A test-owned allocation is an independent native lease for the duration of its JNI call.
struct TestHandle(jni::sys::jlong);

impl Drop for TestHandle {
    fn drop(&mut self) {
        // SAFETY: The test uniquely owns this allocation, releases it once, and keeps it alive
        // until the synchronous JNI call and every borrowed read of the host value have returned.
        unsafe { crate::handles::Value::release(self.0) }
            .assured("the test owns a live allocation");
    }
}

fn restoration(
    env: &mut Env<'_>,
    value: crate::handles::Value,
    method: &jni::strings::JNIStr,
) -> jni::errors::Result<()> {
    let handle = TestHandle(value.into_handle());
    let restored = env
        .call_static_method(
            jni_str!("io/nervix/client/Native"),
            method,
            jni_sig!("(J)Ljava/lang/Object;"),
            &[JValue::Long(handle.0)],
        )?
        .l()?;
    let message = env
        .call_method(
            &restored,
            jni_str!("message"),
            jni_sig!("()Ljava/lang/String;"),
            &[],
        )?
        .l()?;
    let message = jni::objects::JString::cast_local(env, message)?;
    assert_eq!(
        message.try_to_string(env)?,
        "restore refused: precise cause"
    );
    let wait = env
        .call_method(&restored, jni_str!("waitNanos"), jni_sig!("()J"), &[])?
        .j()?;
    assert_eq!(wait, 7_000_000_009);
    Ok(())
}

#[test]
fn restoration_events_keep_the_complete_message_and_physical_wait() {
    use nervix_client_core::{
        DomainClockEvent, DomainClockRestorationFailure, SubscriptionEvent,
        SubscriptionRestorationFailure,
    };
    use nervix_primitives::sync::Arc;
    vm().attach_current_thread(|env| -> jni::errors::Result<()> {
        let clock = DomainClockEvent::RestorationFailed(DomainClockRestorationFailure {
            domain: "example".try_into().assured("the domain name is valid"),
            message: "restore refused: precise cause".into(),
            retry_after: std::time::Duration::from_nanos(7_000_000_009),
        });
        restoration(
            env,
            crate::handles::Value::ClockEvent(Arc::new(nervix_client_host::ClockEvent::new(clock))),
            jni_str!("clockEventRestoration"),
        )?;
        let event = SubscriptionEvent::RestorationFailed(SubscriptionRestorationFailure {
            subscription: nervix_client_core::wire::SubscriptionHandle {
                name: "events"
                    .try_into()
                    .assured("the subscription name is valid"),
                generation: std::num::NonZeroU64::new(u64::MAX)
                    .assured("the generation is nonzero"),
            },
            message: "restore refused: precise cause".into(),
            retry_after: std::time::Duration::from_nanos(7_000_000_009),
        });
        let event =
            nervix_client_host::Event::new(event).assured("the restoration event needs no rows");
        restoration(
            env,
            crate::handles::Value::Event(Arc::new(event)),
            jni_str!("eventRestoration"),
        )?;
        Ok(())
    })
    .assured("both restoration event kinds retain their message and wait through JNI");
}

#[test]
fn a_loss_event_refuses_column_reads_before_interpreting_its_loss_count() {
    use nervix_client_core::{
        SubscriptionEvent,
        wire::{SubscriptionDeliveryLost, SubscriptionHandle},
    };
    use nervix_primitives::sync::Arc;
    vm().attach_current_thread(|env| -> jni::errors::Result<()> {
        let lost = SubscriptionEvent::DeliveryLost(SubscriptionDeliveryLost {
            subscription: SubscriptionHandle {
                name: "events".try_into().assured("the name is valid"),
                generation: std::num::NonZeroU64::new(1).assured("the generation is nonzero"),
            },
            dropped_rows: std::num::NonZeroU64::new(u64::MAX).assured("the loss count is nonzero"),
        });
        let event = nervix_client_host::Event::new(lost).assured("a loss event needs no row frame");
        let handle = TestHandle(crate::handles::Value::Event(Arc::new(event)).into_handle());
        let read = env.call_static_method(
            jni_str!("io/nervix/client/Native"),
            jni_str!("eventStates"),
            jni_sig!("(JII)[B"),
            &[JValue::Long(handle.0), JValue::Int(1), JValue::Int(0)],
        );
        assert!(read.is_err());
        let exception = env
            .exception_occurred()
            .assured("the wrong event kind raises its typed exception");
        env.exception_clear();
        assert!(
            env.is_instance_of(
                exception,
                jni_str!("io/nervix/client/Nervix$TypeMismatchException")
            )?,
            "a loss count is no column size: reject its event kind before allocating or narrowing \
             it"
        );
        Ok(())
    })
    .assured("the JNI read returns the host's event-kind failure");
}
