//! The exceptions a call raises: one class for each kind of failure, under one base class.
//!
//! - **Owns.** The package's exception classes, and raising a failure of the host session runtime
//!   as the exception of its kind, with the report's whole chain as its message, the execution
//!   reference of the command it concerns and the refusal of an open.
//! - **Depends on.** The host session runtime's failure and its classification, and pyo3's
//!   exceptions.
//! - **Must not know.** How a failure is classified; the host session runtime decides that, by the
//!   report's context and never by its text.

use nervix_client_host::{Failure, FailureKind as HostKind};
use pyo3::{create_exception, exceptions::PyException, prelude::*};

use crate::enums::{FailureKind, OpenRefusal};

create_exception!(
    nervix_client,
    ClientError,
    PyException,
    "A call to the Nervix client failed. Every failure is an instance of one subclass, named \
     after its kind, and carries `kind`, `message`, `execution_reference` and `open_refusal`."
);
create_exception!(
    nervix_client,
    InvalidArgumentError,
    ClientError,
    "An argument the call was given cannot be used, and nothing was sent."
);
create_exception!(
    nervix_client,
    ConnectError,
    ClientError,
    "The session could not be opened, or reached no server."
);
create_exception!(
    nervix_client,
    TransportError,
    ClientError,
    "The session failed while the call used it."
);
create_exception!(
    nervix_client,
    UncertainOutcomeError,
    ClientError,
    "The outcome is not known. Running the same execution again recovers a command's outcome."
);
create_exception!(
    nervix_client,
    RejectedError,
    ClientError,
    "The server refused the call."
);
create_exception!(
    nervix_client,
    DeadlineError,
    ClientError,
    "The call's deadline passed. Admitted work is not rolled back."
);
create_exception!(
    nervix_client,
    CancelledError,
    ClientError,
    "The call's cancellation token was triggered. Admitted work is not rolled back."
);
create_exception!(
    nervix_client,
    EventOverflowError,
    ClientError,
    "The session's events overflowed what it buffers."
);
create_exception!(
    nervix_client,
    ProtocolError,
    ClientError,
    "The server answered outside the session protocol."
);
create_exception!(
    nervix_client,
    TypeMismatchError,
    ClientError,
    "A value was read as a type it does not have, or a field its object does not carry."
);
create_exception!(
    nervix_client,
    ClosedError,
    ClientError,
    "The session or the endpoint ended."
);
create_exception!(
    nervix_client,
    ConsumerInterruptedError,
    ClientError,
    "The consumer's session was lost: output may have been missed, and the consumer is restored."
);
create_exception!(
    nervix_client,
    ReopenRequiredError,
    ClientError,
    "The endpoint changed and has to be opened again."
);

/// Adds every exception class to the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("ClientError", py.get_type::<ClientError>())?;
    module.add(
        "InvalidArgumentError",
        py.get_type::<InvalidArgumentError>(),
    )?;
    module.add("ConnectError", py.get_type::<ConnectError>())?;
    module.add("TransportError", py.get_type::<TransportError>())?;
    module.add(
        "UncertainOutcomeError",
        py.get_type::<UncertainOutcomeError>(),
    )?;
    module.add("RejectedError", py.get_type::<RejectedError>())?;
    module.add("DeadlineError", py.get_type::<DeadlineError>())?;
    module.add("CancelledError", py.get_type::<CancelledError>())?;
    module.add("EventOverflowError", py.get_type::<EventOverflowError>())?;
    module.add("ProtocolError", py.get_type::<ProtocolError>())?;
    module.add("TypeMismatchError", py.get_type::<TypeMismatchError>())?;
    module.add("ClosedError", py.get_type::<ClosedError>())?;
    module.add(
        "ConsumerInterruptedError",
        py.get_type::<ConsumerInterruptedError>(),
    )?;
    module.add("ReopenRequiredError", py.get_type::<ReopenRequiredError>())?;
    Ok(())
}

/// The exception a failure raises: the class of its kind, with the report's whole chain as its
/// message and the failure's kind, execution reference and open refusal as its attributes.
pub(crate) fn raise(py: Python<'_>, failure: Failure) -> PyErr {
    let message = failure.message().to_owned();
    let error = match failure.kind() {
        HostKind::InvalidArgument => InvalidArgumentError::new_err(message),
        HostKind::Connect => ConnectError::new_err(message),
        HostKind::Transport => TransportError::new_err(message),
        HostKind::Uncertain => UncertainOutcomeError::new_err(message),
        HostKind::Rejected => RejectedError::new_err(message),
        HostKind::Deadline => DeadlineError::new_err(message),
        HostKind::Cancelled => CancelledError::new_err(message),
        HostKind::Overflow => EventOverflowError::new_err(message),
        HostKind::Protocol => ProtocolError::new_err(message),
        HostKind::Type => TypeMismatchError::new_err(message),
        HostKind::Closed => ClosedError::new_err(message),
        HostKind::Interrupted => ConsumerInterruptedError::new_err(message),
        HostKind::ReopenRequired => ReopenRequiredError::new_err(message),
    };
    match annotate(py, &error, &failure) {
        Ok(()) => error,
        Err(problem) => problem,
    }
}

/// Sets the attributes a host reads from a raised failure.
fn annotate(py: Python<'_>, error: &PyErr, failure: &Failure) -> PyResult<()> {
    let value = error.value(py);
    value.setattr("kind", FailureKind::from(failure.kind()))?;
    value.setattr("message", failure.message())?;
    let reference = failure
        .execution_reference()
        .map(|reference| reference.as_str());
    value.setattr("execution_reference", reference)?;
    let refusal = failure.open_refusal().map(OpenRefusal::from);
    value.setattr("open_refusal", refusal)?;
    Ok(())
}

/// Raises `argument` as invalid for `problem`, as the host session runtime refuses its own
/// arguments.
pub(crate) fn invalid_argument(py: Python<'_>, argument: &'static str, problem: &str) -> PyErr {
    raise(py, Failure::invalid_argument(argument, problem))
}
