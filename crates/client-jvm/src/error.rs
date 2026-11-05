//! Failures at the JNI boundary.
//!
//! Layer: edges.
//! - **Owns.** Projecting already classified host failures into typed Java exceptions.
//! - **Depends on.** The host failure kind and JNI's exception contract.
//! - **Must not know.** How a client failure is classified or recovered.

use jni::{
    Env, jni_sig, jni_str,
    objects::{JObject, JValue},
    strings::JNIStr,
};
use nervix_client_host::{Failure, FailureKind};

#[derive(Debug, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        library,
        reason = "JNI returns its library error or a Java exception projected from an already \
                  classified host outcome"
    )
)]
pub(crate) enum Error {
    #[error(transparent)]
    Jni(#[from] jni::errors::Error),
    #[error("{}", .0.message())]
    Host(Failure),
}

impl From<Failure> for Error {
    fn from(failure: Failure) -> Self {
        Self::Host(failure)
    }
}

impl Error {
    pub(crate) fn argument(name: &'static str, problem: &str) -> Self {
        Failure::invalid_argument(name, problem).into()
    }

    pub(crate) fn raise(self, env: &mut Env<'_>) -> jni::errors::Error {
        match self {
            Self::Jni(error) => error,
            Self::Host(failure) => match Self::throw(env, failure) {
                Ok(()) => jni::errors::Error::JavaException,
                Err(error) => error,
            },
        }
    }

    fn class(kind: FailureKind) -> &'static JNIStr {
        match kind {
            FailureKind::InvalidArgument => {
                jni_str!("io/nervix/client/Nervix$InvalidArgumentException")
            }
            FailureKind::Connect => jni_str!("io/nervix/client/Nervix$ConnectException"),
            FailureKind::Transport => jni_str!("io/nervix/client/Nervix$TransportException"),
            FailureKind::Uncertain => jni_str!("io/nervix/client/Nervix$UncertainOutcomeException"),
            FailureKind::Rejected => jni_str!("io/nervix/client/Nervix$RejectedException"),
            FailureKind::Deadline => jni_str!("io/nervix/client/Nervix$DeadlineException"),
            FailureKind::Cancelled => jni_str!("io/nervix/client/Nervix$CancelledException"),
            FailureKind::Overflow => jni_str!("io/nervix/client/Nervix$EventOverflowException"),
            FailureKind::Protocol => jni_str!("io/nervix/client/Nervix$ProtocolException"),
            FailureKind::Type => jni_str!("io/nervix/client/Nervix$TypeMismatchException"),
            FailureKind::Closed => jni_str!("io/nervix/client/Nervix$ClosedException"),
            FailureKind::Interrupted => {
                jni_str!("io/nervix/client/Nervix$ConsumerInterruptedException")
            }
            FailureKind::ReopenRequired => {
                jni_str!("io/nervix/client/Nervix$ReopenRequiredException")
            }
        }
    }

    fn throw(env: &mut Env<'_>, failure: Failure) -> jni::errors::Result<()> {
        let message = env.new_string(failure.message())?;
        let reference: JObject<'_> = match failure.execution_reference() {
            Some(reference) => env.new_string(reference.as_str())?.into(),
            None => JObject::null(),
        };
        let refusal = match failure.open_refusal() {
            Some(refusal) => crate::marshal::open_refusal(refusal),
            None => 0,
        };
        let exception = env.new_object(
            Self::class(failure.kind()),
            jni_sig!("(Ljava/lang/String;Ljava/lang/String;I)V"),
            &[
                JValue::Object(message.as_ref()),
                JValue::Object(&reference),
                JValue::Int(refusal),
            ],
        )?;
        let exception = jni::objects::JThrowable::cast_local(env, exception)?;
        env.throw(exception)
    }
}

/// Applies the typed host projection, delegating JNI and unwind handling to JNI's policy.
pub(crate) struct JavaPolicy;

impl<T: Default> jni::errors::ErrorPolicy<T, Error> for JavaPolicy {
    type Captures<'unowned_env_local: 'native_method, 'native_method> = ();

    fn on_error<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        captures: &mut (),
        error: Error,
    ) -> jni::errors::Result<T> {
        let error = error.raise(env);
        <jni::errors::ThrowRuntimeExAndDefault as jni::errors::ErrorPolicy<T, jni::errors::Error>>::on_error(env, captures, error)
    }

    fn on_panic<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        captures: &mut (),
        payload: Box<dyn std::any::Any + Send + 'static>,
    ) -> jni::errors::Result<T> {
        <jni::errors::ThrowRuntimeExAndDefault as jni::errors::ErrorPolicy<T, jni::errors::Error>>::on_panic(env, captures, payload)
    }
}
