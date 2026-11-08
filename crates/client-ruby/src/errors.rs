//! Ruby failure projection.
//!
//! Layer: edges.
//! - **Owns.** Typed exception classes and complete failure attributes at the Ruby boundary.
//! - **Depends on.** The host runtime's already-classified failures and magnus exceptions.
//! - **Must not know.** How a failure is classified or how a command is recovered.

use magnus::{Error, Exception, RModule, RObject, Ruby, module::Attr, prelude::*};
use meticulous::ResultExt as _;
use nervix_client_host::{Failure, FailureKind};

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let base = module.define_error("ClientError", ruby.exception_standard_error())?;
    for name in [
        "InvalidArgumentError",
        "ConnectError",
        "TransportError",
        "UncertainOutcomeError",
        "RejectedError",
        "DeadlineError",
        "CancelledError",
        "EventOverflowError",
        "ProtocolError",
        "TypeMismatchError",
        "ClosedError",
        "ConsumerInterruptedError",
        "ReopenRequiredError",
    ] {
        module.define_error(name, base)?;
    }
    base.define_attr("kind", Attr::Read)?;
    base.define_attr("execution_reference", Attr::Read)?;
    base.define_attr("open_refusal", Attr::Read)?;
    Ok(())
}

pub(crate) fn raise(failure: Failure) -> Error {
    let ruby = Ruby::get().assured("a native Ruby callback holds the GVL");
    let result = annotated(&ruby, &failure);
    match result {
        Ok(error) | Err(error) => error,
    }
}

fn annotated(ruby: &Ruby, failure: &Failure) -> Result<Error, Error> {
    let class_name = match failure.kind() {
        FailureKind::InvalidArgument => "InvalidArgumentError",
        FailureKind::Connect => "ConnectError",
        FailureKind::Transport => "TransportError",
        FailureKind::Uncertain => "UncertainOutcomeError",
        FailureKind::Rejected => "RejectedError",
        FailureKind::Deadline => "DeadlineError",
        FailureKind::Cancelled => "CancelledError",
        FailureKind::Overflow => "EventOverflowError",
        FailureKind::Protocol => "ProtocolError",
        FailureKind::Type => "TypeMismatchError",
        FailureKind::Closed => "ClosedError",
        FailureKind::Interrupted => "ConsumerInterruptedError",
        FailureKind::ReopenRequired => "ReopenRequiredError",
    };
    let module: RModule = ruby.class_object().const_get("Nervix")?;
    let module: RModule = module.const_get("Client")?;
    let class: magnus::ExceptionClass = module.const_get(class_name)?;
    let exception: Exception = class.new_instance((failure.message(),))?;
    let object = RObject::try_convert(exception.as_value())?;
    object.ivar_set("@kind", crate::enums::FailureKind::code(failure.kind()))?;
    let reference = failure
        .execution_reference()
        .map(|reference| reference.as_str());
    object.ivar_set("@execution_reference", reference)?;
    let refusal = failure.open_refusal().map(crate::enums::OpenRefusal::code);
    object.ivar_set("@open_refusal", refusal)?;
    Ok(Error::from(exception))
}

pub(crate) fn invalid(argument: &'static str, message: &str) -> Error {
    raise(Failure::invalid_argument(argument, message))
}

pub(crate) fn closed(object: &str) -> Error {
    raise(Failure::new(
        FailureKind::Closed,
        format!("the {object} was closed"),
    ))
}

pub(crate) fn borrowed(object: &str) -> Error {
    invalid("object", &format!("the {object} is already being changed"))
}
