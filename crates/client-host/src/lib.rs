//! The host session runtime every language binding of the Nervix client shares: the C ABI that C
//! and C++ load, and the bindings that Python, Ruby and the JVM load.
//!
//! Layer: edges.
//!
//! - **Owns.** The session a host holds and the runtime it runs on, a blocking call on a host's
//!   thread with cancellation and deadlines, how every client failure is classified into the
//!   kinds a host branches on, prepared commands and their outcomes, completion pages,
//!   subscription events with bulk column access to the rows of a verified frame, domain clock
//!   events and the clock of a followed domain with the arithmetic that projects it, producers and
//!   consumers of client endpoints with their typed outcomes and settlements, the field
//!   descriptions a host builds, building and reading their Arrow batches one column level at a
//!   time, and exchanging a batch with a host's Arrow library through the Arrow C data interface.
//! - **Depends on.** The Rust session client and its wire contract, the vocabulary it names,
//!   Arrow's arrays and C data interface, and Tokio to run the session on threads the library
//!   owns.
//! - **Must not know.** Any host language, how a host holds or releases what it is given, or how a
//!   binding hands it data; the server, the parser, or how an Arrow IPC stream is encoded or
//!   decoded, which the Rust client does. A rows event reads its verified frame, and builds an
//!   Arrow batch of its rows only when a host asks for one.
//!
//! Every binding drives the same [`Client`] state machine the Rust client uses, so correlation,
//! redirects, reconnection, execution identity, subscription restoration, domain clock
//! re-attachment, and the restoration, credit, outcomes and settlements of producers and
//! consumers are decided once, in the Rust client, and never reinterpreted by a binding or a
//! host. The enumerations a host reads keep one stable value per variant, which every binding
//! exposes unchanged.
//!
//! [`Client`]: nervix_client_core::Client

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "a host binding's session runtime and blocking calls belong to the client edge"
    )
)]

mod arrow;
mod batch;
mod batch_builder;
mod cancel;
mod clock_event;
mod consumer;
mod delivery;
mod domain_clock;
mod endpoint;
mod event;
mod failure;
mod fields;
mod outcome;
mod producer;
mod schema;
mod session;
mod submission;
mod suggestions;

pub use arrow::ExportedArray;
pub use batch::Batch;
pub use batch_builder::BatchBuilder;
pub use cancel::Cancel;
pub use clock_event::{ClockEndReason, ClockEvent, ClockEventKind, ClockState};
pub use consumer::Consumer;
pub use delivery::{Delivery, Settlement};
pub use domain_clock::DomainClock;
pub use endpoint::{
    EndpointState, OpenRefusal, Reopen, ReopenReason, Window, WindowKind, credit, nanos,
};
pub use event::{CellState, EndReason, Event, EventKind, RestorationCause};
pub use failure::{Failure, FailureKind};
pub use fields::Fields;
pub use outcome::{Disposition, Outcome, RestoreSummary, UploadFailure};
pub use producer::{Admission, Producer};
pub use schema::{FieldType, Part, Schema, TypeKind, TypeLevels};
pub use session::{Execution, Session};
pub use submission::{
    BatchDefect, ProcessingFailure, SubmissionOutcome, SubmissionRefusal, SubmissionResult,
    Uncertainty,
};
pub use suggestions::{CompletionKind, CompletionStatus, Suggestions};

#[cfg(test)]
mod tests;
